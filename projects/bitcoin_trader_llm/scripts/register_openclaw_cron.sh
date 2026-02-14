#!/usr/bin/env bash
# Sample script to register a system cron and show an example openclaw cron entry.
# The script does NOT automatically modify system cron; run it manually if you want to register the cron.

PROJECT=/root/.openclaw/workspace/projects/bitcoin_trader_llm
CRON_EXPRESSION="5,35 * * * *"
CMD="/usr/bin/python3 $PROJECT/run_cron.py >> $PROJECT/logs/run_cron.log 2>&1"

echo "To register system cron, run the following command as the user that should execute the cron (example):"
echo "(crontab -l 2>/dev/null; echo \"$CRON_EXPRESSION $CMD\") | crontab -"

echo

echo "OpenClaw cron (example) - to schedule an agentTurn that executes pending signals in OpenClaw's scheduler, use openclaw cron add with payload.kind=agentTurn. Sample (edit before running):"
echo "openclaw cron add --name 'trading:execute-signals' --schedule '{\"kind\":\"every\",\"everyMs\":1800000}' --payload '{\"kind\":\"agentTurn\",\"message\":\"Execute pending trading signals now\"}' --sessionTarget isolated"

echo

echo "Note: Do NOT run commands automatically unless you are ready. This script only prints recommended commands."
