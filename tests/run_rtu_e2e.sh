#!/bin/sh
# Optional external peer, isolated from the library's build/runtime dependencies.
set -eu
cd "$(dirname "$0")/.."
if [ -n "${RTU_TEST_PYTHON:-}" ]; then
    peer_python=$RTU_TEST_PYTHON
elif [ -x .venv/bin/python ] && .venv/bin/python -c 'import c104' 2>/dev/null; then
    peer_python=.venv/bin/python
else
    peer_python=python3
fi
"$peer_python" -c 'import c104'
if [ -n "${RTU_TEST_SERVER:-}" ]; then
    server_binary=$RTU_TEST_SERVER
else
    cargo build --locked --example rtu_server
    server_binary=${CARGO_TARGET_DIR:-target}/debug/examples/rtu_server
fi
exec "$peer_python" tests/run_rtu_e2e.py "$server_binary"
