#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f ".venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

: "${UPDATE_CHECK_ENABLED:=true}"
: "${UPDATE_CHECK_FINAL_ONLY:=true}"
: "${UPDATE_CHECK_TIMEOUT_MS:=2500}"
: "${UPDATE_CHECK_ALLOW_SLOW:=false}"
: "${MAX_ROWS:=0}"

export UPDATE_CHECK_ENABLED UPDATE_CHECK_FINAL_ONLY UPDATE_CHECK_TIMEOUT_MS UPDATE_CHECK_ALLOW_SLOW MAX_ROWS

python main.py
