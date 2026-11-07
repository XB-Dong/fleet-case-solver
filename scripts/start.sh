#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
[ -d .venv ] || python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
[ -d node_modules ] || npm install
node node_modules/typescript/bin/tsc --noEmit
node node_modules/vite/bin/vite.js build
exec .venv/bin/python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765
