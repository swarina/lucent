// Right-click chaos menu + result toast (frontend.md §5.3). A context menu on a
// stage node offers the same six commands as the ops-panel chaos block, built
// from the shared CHAOS_PRESETS. Dismisses on outside-click or Escape.

import { useEffect } from "react";

import { useLucent } from "../state/store";
import { presetsFor, runChaos } from "./chaos";

export function ChaosMenu() {
  const menu = useLucent((s) => s.chaosMenu);
  const setMenu = useLucent((s) => s.setChaosMenu);
  const source = useLucent((s) => s.source);
  const toast = useLucent((s) => s.chaosToast);
  const setToast = useLucent((s) => s.setChaosToast);

  // Dismiss on Escape or any click outside the menu.
  useEffect(() => {
    if (!menu) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setMenu(null);
    const onClick = () => setMenu(null);
    window.addEventListener("keydown", onKey);
    // defer so the opening click doesn't immediately close it
    const id = setTimeout(() => window.addEventListener("click", onClick), 0);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("click", onClick);
      clearTimeout(id);
    };
  }, [menu, setMenu]);

  // Auto-clear the toast after a few seconds.
  useEffect(() => {
    if (!toast) return;
    const id = setTimeout(() => setToast(null), 3500);
    return () => clearTimeout(id);
  }, [toast, setToast]);

  const fire = async (label: string, cmd: ReturnType<typeof presetsFor>[number]["cmd"]) => {
    setMenu(null);
    if (!source || !menu) return;
    try {
      const text = await runChaos(source, cmd(menu.nodeId));
      setToast({ text, error: false });
    } catch (err) {
      setToast({ text: `${menu.nodeId}: ${label} failed — ${(err as Error).message}`, error: true });
    }
  };

  return (
    <>
      {menu && (
        <div
          className="chaos-menu"
          style={{ left: menu.x, top: menu.y }}
          onClick={(e) => e.stopPropagation()}
          role="menu"
        >
          <div className="chaos-menu-head mono">{menu.nodeId}</div>
          {presetsFor(menu.nodeId).map((p) => (
            <button
              key={p.label}
              className={`chaos-item ${p.danger ? "danger" : ""}`}
              onClick={() => void fire(p.label, p.cmd)}
            >
              {p.label}
            </button>
          ))}
        </div>
      )}
      {toast && (
        <div className={`chaos-toast mono ${toast.error ? "err" : ""}`}>{toast.text}</div>
      )}
    </>
  );
}
