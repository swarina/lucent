#!/usr/bin/env bash
# Generate C++, Python, and TypeScript bindings from proto/lucent/v1/*.proto.
#
# STUB (M0-T1): the proto files and real codegen land in M0-T2. This script is
# wired now so CI can assert "regenerate + git diff is clean" the moment protos
# exist. Until then it is a no-op that succeeds when there are no protos yet.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$ROOT/proto"

shopt -s nullglob
protos=("$PROTO_DIR"/lucent/v1/*.proto)
shopt -u nullglob

if [[ ${#protos[@]} -eq 0 ]]; then
  echo "gen-proto: no protos yet (M0-T2 pending) — nothing to generate."
  exit 0
fi

echo "gen-proto: found ${#protos[@]} proto file(s). Real codegen lands in M0-T2:"
printf '  %s\n' "${protos[@]}"
echo "  -> C++    : protoc + grpc_cpp_plugin   -> cpp/gen/"
echo "  -> Python : grpc_tools.protoc          -> py/lucent/gen/"
echo "  -> TS     : protoc + ts-proto          -> gateway/src/gen/ (+ web symlink)"
exit 0
