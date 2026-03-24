#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -d ".venv" ]]; then
  echo "Missing virtual environment at $SCRIPT_DIR/.venv" >&2
  echo "Create it first, then install requirements." >&2
  exit 1
fi

# Use the python in the venv explicitly
PYTHON="$SCRIPT_DIR/.venv/bin/python"
FLASK="$SCRIPT_DIR/.venv/bin/flask"

if [[ ! -f "$PYTHON" ]]; then
  echo "Python not found in .venv. Did you create it?" >&2
  exit 1
fi

if ! "$PYTHON" -c "import flask" &>/dev/null; then
  echo "Flask is not installed in the virtual environment." >&2
  echo "Run: source .venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

HOST="${HOST:-127.0.0.1}"
PORT_WAS_SET="${PORT+x}"
PORT="${PORT:-5000}"

find_free_port() {
  local host="$1"
  local start_port="$2"
  local port
  for ((port=start_port; port<start_port+20; port++)); do
    if ! "$PYTHON" - "$host" "$port" <<'PY'
import socket
import sys
try:
    host, port = sys.argv[1], int(sys.argv[2])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
PY
    then
      continue
    fi
    echo "$port"
    return 0
  done
  return 1
}

# Check if initial port is busy
if ! "$PYTHON" - "$HOST" "$PORT" <<'PY'
import socket
import sys
try:
    host, port = sys.argv[1], int(sys.argv[2])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
PY
then
  if [[ -n "$PORT_WAS_SET" ]]; then
    echo "Port $PORT is already in use on $HOST." >&2
    exit 1
  fi
  NEXT_PORT="$(find_free_port "$HOST" "$((PORT + 1))")" || {
    echo "Could not find a free port near $PORT." >&2
    exit 1
  }
  echo "Port $PORT is busy on $HOST. Starting on $NEXT_PORT instead."
  PORT="$NEXT_PORT"
fi

echo "Starting IGReelScraper at http://$HOST:$PORT"

export FLASK_APP=app.py
export FLASK_DEBUG=1

exec "$FLASK" run --host="$HOST" --port="$PORT"
