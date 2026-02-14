#!/usr/bin/env bash
set -euo pipefail

WORKDIR=/root/.openclaw/workspace/projects/bitcoin_trader_llm
VENV_PY=$WORKDIR/venv/bin/python
LOGDIR=$WORKDIR/logs
INTERVAL=minutes30
ADX=(30 35 40)
VOL_ENTRY=(2.0 2.5)

for a in "${ADX[@]}"; do
  for v in "${VOL_ENTRY[@]}"; do
    echo "Running ADX=$a VOL_ENTRY=$v"
    FLAGS="--months 3 --mode regime --tp-pct 0.015 --vol-entry-mult ${v} --vol-mult 2.0 --stop-rel 2.0 --risk-pct 1.0 --sl-pct 0.008 --disable-time-exit --gatekeeper-only --adx-threshold ${a} --htf-require both"
    echo "CMD: CANDLE_INTERVAL=${INTERVAL} ${VENV_PY} scripts/backtest_dce.py ${FLAGS}"
    CANDLE_INTERVAL=${INTERVAL} ${VENV_PY} scripts/backtest_dce.py ${FLAGS}
    # rename newest files to include grid tags
    newest_json=$(ls -1t ${LOGDIR}/backtest_*.json | head -n1)
    newest_csv=$(ls -1t ${LOGDIR}/backtest_trades_*.csv | head -n1)
    ts=$(basename "${newest_json}" .json)
    mv "${newest_json}" "${LOGDIR}/${ts}_${INTERVAL}_adx${a}_vol${v}_htfboth.json"
    mv "${newest_csv}" "${LOGDIR}/${ts}_${INTERVAL}_adx${a}_vol${v}_htfboth.csv"
    echo "Saved: ${ts}_${INTERVAL}_adx${a}_vol${v}_htfboth.json"
  done
done

echo "30m gatekeeper grid complete."
