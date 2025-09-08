#!/usr/bin/env bash
set -euo pipefail

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]

# Enable argcomplete for the current shell
eval "$(register-python-argcomplete barrow)"

echo "Virtual environment created and completion enabled."
