#!/usr/bin/env bash
set -e
WORKDIR="/root/.openclaw/workspace/projects/bitcoin_trader_llm"
PY="$WORKDIR/venv/bin/python"
LOG="$WORKDIR/logs/run_candidate_sweep_wick.log"
cd "$WORKDIR"

# wait until the sweep process is gone
while pgrep -f "run_candidate_sweep_wick.sh" >/dev/null; do
  sleep 10
done

# small buffer to ensure log files flushed
sleep 3

# run analyze_candidates_no_retest.py (console summary)
$PY scripts/analyze_candidates_no_retest.py > logs/analyze_candidates_summary.txt 2>&1 || true

# run final analyzer to produce CSV/JSON/TOP5/TOP2/conclusion
$PY scripts/analyze_final_no_retest.py > logs/analyze_final_exec.log 2>&1 || true

# If TOP2 produced, kick off the post-Top2 pipeline in background (break-even, retest, paper-plan)
# The pipeline will run in background and send a single Discord notification when finished (if DISCORD_WEBHOOK_URL is set).
if [ -f "$WORKDIR/logs/top2_candidates.json" ]; then
  echo "Starting post-Top2 pipeline in background..." >> "$LOG"
  nohup $PY scripts/post_top2_pipeline.py > logs/post_top2_pipeline.log 2>&1 &
else
  echo "top2_candidates.json not found; skipping post-Top2 pipeline." >> "$LOG"
fi

# mark analysis done
echo "ANALYSIS_DONE" >> "$LOG"
