#!/usr/bin/env bash
# start_paper.sh - start 2-week live paper trading for Top2 candidates
WORKDIR="/root/.openclaw/workspace/projects/bitcoin_trader_llm"
cd "$WORKDIR"
PY="/root/.openclaw/workspace/projects/bitcoin_trader_llm/venv/bin/python"
CONFIG="/root/.openclaw/workspace/projects/bitcoin_trader_llm/logs/paper_config.json"
# This script should launch the paper runner with the given config.
# Placeholder: user should implement scripts/paper_runner.py to perform real-time paper trading.
# Example: $PY scripts/paper_runner.py --config "$CONFIG" --daemonize

if [ -x "$PY" ]; then
  echo "Paper start placeholder: $PY scripts/paper_runner.py --config $CONFIG"
  # touch a file to indicate 'would-start'
  touch /root/.openclaw/workspace/projects/bitcoin_trader_llm/logs/paper_start_ready.flag
else
  echo "Python venv not found: $PY"
fi
