#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -d ".venv" ]]; then
  echo "Missing virtual environment at $SCRIPT_DIR/.venv" >&2
  echo "Create it first, then install requirements." >&2
  exit 1
fi

source ".venv/bin/activate"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5000}"

python - <<PY
from app import create_app

app = create_app()
app.run(host="${HOST}", port=${PORT}, debug=False)
PY
