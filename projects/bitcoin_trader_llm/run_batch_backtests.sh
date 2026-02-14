#!/usr/bin/env bash
set -euo pipefail

WORKDIR=/root/.openclaw/workspace/projects/bitcoin_trader_llm
VENV_PY=$WORKDIR/venv/bin/python
LOGDIR=$WORKDIR/logs
INTERVALS=("minutes15" "minutes30" "minutes60")
SCENARIOS=("gatekeeper" "gatekeeper_earlyabort" "gatekeeper_varsl" "gatekeeper_atrtrail")

for inv in "${INTERVALS[@]}"; do
  for sc in "${SCENARIOS[@]}"; do
    echo "Running $sc on $inv"
    FLAGS="--months 3 --mode regime --tp-pct 0.015 --vol-entry-mult 1.5 --vol-mult 2.0 --stop-rel 2.0 --risk-pct 1.0 --sl-pct 0.008 --disable-time-exit"
    if [ "$sc" == "gatekeeper" ]; then
       FLAGS="$FLAGS --gatekeeper-only"
    elif [ "$sc" == "gatekeeper_earlyabort" ]; then
       FLAGS="$FLAGS --gatekeeper-only --early-abort --early-abort-pct 0.005 --early-abort-bars 2"
    elif [ "$sc" == "gatekeeper_varsl" ]; then
       # simulate VarSL by using wider fixed SL for comparison
       FLAGS="$FLAGS --gatekeeper-only --sl-pct 0.012"
    elif [ "$sc" == "gatekeeper_atrtrail" ]; then
       FLAGS="$FLAGS --gatekeeper-only --atr-trail"
    fi
    echo "CMD: CANDLE_INTERVAL=$inv $VENV_PY scripts/backtest_dce.py $FLAGS"
    CANDLE_INTERVAL=$inv $VENV_PY scripts/backtest_dce.py $FLAGS
    # locate newest files and rename with scenario tag
    newest=$(ls -1t $LOGDIR/backtest_*.json | head -n1)
    newest_csv=$(ls -1t $LOGDIR/backtest_trades_*.csv | head -n1)
    ts=$(basename "$newest" .json)
    mv "$newest" "$LOGDIR/${ts}_${inv}_${sc}.json"
    mv "$newest_csv" "$LOGDIR/${ts}_${inv}_${sc}.csv"
    echo "Saved: ${ts}_${inv}_${sc}.json"
  done
done

echo "Batch backtests complete."