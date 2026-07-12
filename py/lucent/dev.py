"""Lucent dev supervisor: spawns the local cluster as real OS processes.

`lucent dev --shards N` starts embed, collector/gateway, coordinator, and N
shard processes, waits for readiness, and tails merged logs. The control
server (127.0.0.1:{supervisor_ctl}) exposes kill/restart/spawn — chaos "kill"
is a real signal to a real process (protocol.md §5), which is the point.
"""

from __future__ import annotations

import http.server
import json
import logging
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

import yaml

log = logging.getLogger("lucent.dev")

COLORS = ["\033[36m", "\033[33m", "\033[35m", "\033[32m", "\033[34m", "\033[31m"]
RESET = "\033[0m"


@dataclass
class ProcSpec:
    node_id: str
    role: str  # embed | gateway | coordinator | shard
    argv: list[str]
    port: int  # readiness probe target
    cwd: str | None = None


@dataclass
class Proc:
    spec: ProcSpec
    popen: subprocess.Popen | None = None
    started_at: float = 0.0
    color: str = ""
    restarts: int = field(default=0)

    @property
    def state(self) -> str:
        if self.popen is None:
            return "spawning"
        return "running" if self.popen.poll() is None else "exited"


def find_repo_root(config_path: pathlib.Path) -> pathlib.Path:
    return config_path.resolve().parent


def find_binary(repo_root: pathlib.Path, name: str, subdir: str) -> str:
    candidates = [
        repo_root / "build" / "dev" / "cpp" / subdir / name,
        repo_root / "build" / "release" / "cpp" / subdir / name,
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    on_path = shutil.which(name)
    if on_path:
        return on_path
    raise FileNotFoundError(
        f"{name} not found (looked in build/dev, build/release, PATH). "
        "Build first: cmake --preset dev && cmake --build --preset dev"
    )


def build_specs(
    repo_root: pathlib.Path, config_path: pathlib.Path, cfg, replicas: int,
    fake_embed: bool = False,
) -> list[ProcSpec]:
    """Process set for the cluster. Replica 'b' processes spawn only when
    replicas=2 (they idle EMPTY until replication lands at M3)."""
    lucent_bin = str(pathlib.Path(sys.executable).parent / "lucent")
    shard_bin = find_binary(repo_root, "lucent-shard", "shard")
    coord_bin = find_binary(repo_root, "lucent-coord", "coordinator")
    gateway_js = repo_root / "gateway" / "dist" / "index.js"
    if not gateway_js.exists():
        raise FileNotFoundError(f"{gateway_js} missing — run: cd gateway && npm run build")

    embed_argv = [lucent_bin, "embedsvc", "--config", str(config_path)]
    if fake_embed:
        embed_argv.append("--fake")
    specs = [
        ProcSpec("embed-0", "embed", embed_argv, cfg.ports.embed),
        ProcSpec("collector-0", "gateway",
                 ["node", str(gateway_js), "--config", str(config_path),
                  "--web-dist", str(repo_root / "web" / "dist")],
                 cfg.ports.gateway_http),
        ProcSpec("coord-0", "coordinator",
                 [coord_bin, "--node-id", "coord-0", "--config", str(config_path)],
                 cfg.ports.coordinator),
    ]
    for s in range(cfg.cluster.shards):
        for r in ("a", "b")[: replicas]:
            node = f"shard-{s}{r}"
            specs.append(ProcSpec(node, "shard",
                                  [shard_bin, "--node-id", node,
                                   "--config", str(config_path)],
                                  cfg.shard_port(s, r)))
    return specs


def port_open(port: int, timeout: float = 0.25) -> bool:
    with socket.socket() as sk:
        sk.settimeout(timeout)
        return sk.connect_ex(("127.0.0.1", port)) == 0


class Supervisor:
    def __init__(self, specs: list[ProcSpec]):
        self.procs: dict[str, Proc] = {
            s.node_id: Proc(s, color=COLORS[i % len(COLORS)])
            for i, s in enumerate(specs)
        }
        self._lock = threading.Lock()
        self._stopping = False

    # -- lifecycle ---------------------------------------------------------

    def start_all(self) -> None:
        for proc in self.procs.values():
            self._spawn(proc)

    def _spawn(self, proc: Proc) -> None:
        spec = proc.spec
        popen = subprocess.Popen(
            spec.argv, cwd=spec.cwd, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1,
        )
        proc.popen = popen
        proc.started_at = time.time()
        threading.Thread(
            target=self._pump_logs, args=(proc,), daemon=True
        ).start()
        log.info("%s started (pid %d)", spec.node_id, popen.pid)

    def _pump_logs(self, proc: Proc) -> None:
        assert proc.popen and proc.popen.stdout
        prefix = f"{proc.color}[{proc.spec.node_id:>12}]{RESET} "
        for line in proc.popen.stdout:
            sys.stdout.write(prefix + line)
        code = proc.popen.wait()
        if not self._stopping:
            sys.stdout.write(f"{prefix}exited with code {code}\n")

    def wait_ready(self, timeout_s: float = 120.0) -> bool:
        deadline = time.time() + timeout_s
        pending = dict(self.procs)
        while pending and time.time() < deadline:
            for node_id in list(pending):
                if port_open(pending[node_id].spec.port):
                    log.info("%s ready on :%d", node_id, pending[node_id].spec.port)
                    del pending[node_id]
            time.sleep(0.5)
        for node_id in pending:
            log.error("%s NOT ready (port %d)", node_id, pending[node_id].spec.port)
        return not pending

    def stop_all(self) -> None:
        self._stopping = True
        for proc in self.procs.values():
            if proc.popen and proc.popen.poll() is None:
                proc.popen.terminate()
        deadline = time.time() + 5
        for proc in self.procs.values():
            if proc.popen is None:
                continue
            remaining = max(0.1, deadline - time.time())
            try:
                proc.popen.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.popen.kill()

    # -- control API -------------------------------------------------------

    def snapshot(self) -> list[dict]:
        return [
            {"nodeId": p.spec.node_id, "role": p.spec.role,
             "pid": p.popen.pid if p.popen else None, "state": p.state,
             "port": p.spec.port,
             "uptimeS": round(time.time() - p.started_at, 1) if p.popen else 0}
            for p in self.procs.values()
        ]

    def kill(self, node_id: str, sig: str = "SIGKILL") -> dict:
        with self._lock:
            proc = self.procs.get(node_id)
            if proc is None or proc.popen is None:
                return {"error": f"unknown node {node_id}"}
            signo = signal.SIGKILL if sig == "SIGKILL" else signal.SIGTERM
            proc.popen.send_signal(signo)  # a REAL signal — not a simulation
            return {"nodeId": node_id, "signal": sig, "pid": proc.popen.pid}

    def restart(self, node_id: str) -> dict:
        with self._lock:
            proc = self.procs.get(node_id)
            if proc is None:
                return {"error": f"unknown node {node_id}"}
            if proc.popen and proc.popen.poll() is None:
                proc.popen.terminate()
                try:
                    proc.popen.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.popen.kill()
            proc.restarts += 1
            self._spawn(proc)
            return {"nodeId": node_id, "pid": proc.popen.pid,  # type: ignore[union-attr]
                    "restarts": proc.restarts}


def make_control_handler(sup: Supervisor):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args) -> None:  # quiet
            pass

        def _json(self, code: int, payload) -> None:
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/procs":
                self._json(200, sup.snapshot())
            elif self.path == "/health":
                self._json(200, {"ok": True})
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("content-length", 0))
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "bad json"})
                return
            if self.path == "/kill":
                self._json(200, sup.kill(body.get("nodeId", ""),
                                         body.get("signal", "SIGKILL")))
            elif self.path == "/restart":
                self._json(200, sup.restart(body.get("nodeId", "")))
            else:
                self._json(404, {"error": "not found"})

    return Handler


def run_dev(config_path: str, shards: int, replicas: int, partitioning: str,
            fake_embed: bool = False) -> int:
    """Blocking `lucent dev` entrypoint. Returns exit code."""
    from lucent import config as config_mod

    logging.basicConfig(level=logging.INFO, format="[lucent.dev] %(message)s")
    src = pathlib.Path(config_path).resolve()
    repo_root = find_repo_root(src)

    # Derive an effective config (CLI flags override cluster.yaml) and write
    # it where every process reads the same truth.
    raw = yaml.safe_load(src.read_text())
    raw["cluster"]["shards"] = shards
    raw["cluster"]["replicas"] = replicas
    raw["cluster"]["partitioning"] = partitioning
    derived_dir = repo_root / ".lucent"
    derived_dir.mkdir(exist_ok=True)
    derived = derived_dir / "cluster-dev.yaml"
    derived.write_text(yaml.safe_dump(raw, sort_keys=False))

    cfg = config_mod.load(derived)
    specs = build_specs(repo_root, derived, cfg, replicas, fake_embed=fake_embed)
    sup = Supervisor(specs)

    ctl = http.server.ThreadingHTTPServer(
        ("127.0.0.1", cfg.ports.supervisor_ctl), make_control_handler(sup))
    threading.Thread(target=ctl.serve_forever, daemon=True).start()
    log.info("control server on 127.0.0.1:%d", cfg.ports.supervisor_ctl)

    sup.start_all()
    ready = sup.wait_ready()
    if ready:
        log.info("cluster ready: %d procs — http://127.0.0.1:%d",
                 len(specs), cfg.ports.gateway_http)
    else:
        log.error("cluster NOT fully ready — check logs above")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    stop.wait()
    log.info("shutting down ...")
    ctl.shutdown()
    sup.stop_all()
    return 0 if ready else 1
