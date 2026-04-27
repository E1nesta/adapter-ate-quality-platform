#!/usr/bin/env bash
set -euo pipefail

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

if ! command -v g++ >/dev/null 2>&1; then
  echo "g++ with C++17 support is required" >&2
  exit 1
fi

create_venv() {
  if python3 -m venv .venv 2>/dev/null; then
    return
  fi

  if python3 -m virtualenv .venv >/dev/null 2>&1; then
    return
  fi

  echo "Failed to create .venv." >&2
  echo "Install python3-venv or virtualenv, then run this script again." >&2
  exit 1
}

if [[ ! -d .venv ]]; then
  create_venv
fi

.venv/bin/python -m pip install -r requirements.txt

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

bash scripts/run_extension_demo.sh
