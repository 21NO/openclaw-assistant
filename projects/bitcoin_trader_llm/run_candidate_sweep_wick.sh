#!/usr/bin/env bash
set -e
WORKDIR="/root/.openclaw/workspace/projects/bitcoin_trader_llm"
PY="$WORKDIR/venv/bin/python"
SCRIPT="$WORKDIR/scripts/backtest_dce.py"
LOGDIR="$WORKDIR/logs"
export CANDLE_INTERVAL=minutes30

VOL_VALUES=(1.2 1.5)
ADX_VALUES=(20 25 30)
HTF_VALUES=(any both)
UPPER_WICK_VALUES=(OFF 0.4 0.5)
SLIP=0.001

for vol in "${VOL_VALUES[@]}"; do
  for adx in "${ADX_VALUES[@]}"; do
    for htf in "${HTF_VALUES[@]}"; do
      for uw in "${UPPER_WICK_VALUES[@]}"; do
        echo "Running vol=${vol} adx=${adx} htf=${htf} upper_wick=${uw} slippage=${SLIP}"
        ARGS=(--months 6 --mode regime --tp-pct 0.015 --vol-entry-mult "$vol" --vol-mult 2.0 --stop-rel 2.0 --risk-pct 1.0 --sl-pct 0.008 --disable-time-exit --gatekeeper-only --adx-threshold "$adx" --htf-require "$htf" --slippage "$SLIP")
        if [ "$uw" != "OFF" ]; then
          ARGS+=(--upper-wick-pct "$uw")
        fi
        "$PY" "$SCRIPT" "${ARGS[@]}"
        sleep 1
      done
    done
  done
done

echo "Candidate sweep (with upper_wick grid) done."