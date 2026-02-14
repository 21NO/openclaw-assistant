#!/usr/bin/env bash
set -e
WORKDIR="/root/.openclaw/workspace/projects/bitcoin_trader_llm"
PY="$WORKDIR/venv/bin/python"
SCRIPT="$WORKDIR/scripts/backtest_dce.py"
LOGDIR="$WORKDIR/logs"
mkdir -p "$LOGDIR"
CANDLE_INTERVAL=minutes30
# ensure environment variable used by config
export CANDLE_INTERVAL

VOL_VALUES=(2.0 2.5)
WICK_VALUES=("" 0.4 0.5)
SLIPPAGE_VALUES=(0.001 0.0005)

for vol in "${VOL_VALUES[@]}"; do
  for wick in "${WICK_VALUES[@]}"; do
    for slip in "${SLIPPAGE_VALUES[@]}"; do
      echo "Running vol=${vol} wick=${wick:-OFF} slippage=${slip}"
      ARGS=(--months 6 --mode regime --tp-pct 0.015 --vol-entry-mult "$vol" --vol-mult 2.0 --stop-rel 2.0 --risk-pct 1.0 --sl-pct 0.008 --disable-time-exit --gatekeeper-only --adx-threshold 30 --htf-require both)
      # slippage arg name is --slippage
      ARGS+=(--slippage "$slip")
      if [ -n "$wick" ]; then
        ARGS+=(--upper-wick-pct "$wick")
      fi
      # run
      echo "CMD: CANDLE_INTERVAL=minutes30 $PY $SCRIPT ${ARGS[*]}"
      CANDLE_INTERVAL=minutes30 "$PY" "$SCRIPT" "${ARGS[@]}"
      sleep 1
    done
  done
done

# collect results
echo "Collecting results into logs/aggregate_backtests_summary.csv"
"$PY" "$WORKDIR/scripts/collect_backtest_results.py"

echo "Done."
