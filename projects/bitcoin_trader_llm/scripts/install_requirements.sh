#!/usr/bin/env bash
set -euo pipefail

echo "Installing python requirements into current environment (pip)."
# Edit this to install into a venv if desired.
python3 -m pip install --upgrade pip
python3 -m pip install pyupbit pandas ta sqlalchemy pymysql python-dotenv openai requests schedule

echo "Done."
