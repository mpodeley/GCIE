#!/usr/bin/env bash
set -euo pipefail

WITH_NETWORK=0
if [[ "${1:-}" == "--with-network" ]]; then
  WITH_NETWORK=1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${REPO_ROOT}/.venv"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found" >&2
  exit 1
fi

echo "Repo root: ${REPO_ROOT}"
echo "Virtualenv: ${VENV_PATH}"

if [[ ! -d "${VENV_PATH}" ]]; then
  echo "Creating virtualenv..."
  python3 -m venv "${VENV_PATH}"
fi

VENV_PYTHON="${VENV_PATH}/bin/python"

echo "Upgrading pip..."
"${VENV_PYTHON}" -m pip install --upgrade pip

echo "Installing base requirements..."
"${VENV_PYTHON}" -m pip install -r "${REPO_ROOT}/requirements.txt"

if [[ "${WITH_NETWORK}" -eq 1 ]]; then
  echo "Installing optional network requirements..."
  "${VENV_PYTHON}" -m pip install -r "${REPO_ROOT}/requirements-network.txt"
fi

echo
echo "Bootstrap complete."
echo "Activate with:"
echo "  source ./.venv/bin/activate"
