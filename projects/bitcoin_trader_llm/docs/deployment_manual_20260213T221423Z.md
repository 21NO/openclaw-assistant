# Deployment & Runbook (auto-generated)
Generated: 2026-02-13T22:14:23.505651

Top1 candidate:

```json
{
  "file": "backtest_20260213_214746.json",
  "json_path": "/root/.openclaw/workspace/projects/bitcoin_trader_llm/logs/backtest_20260213_214746.json",
  "vol": 1.5,
  "adx": 30.0,
  "htf": "both",
  "upper_wick": "0.5",
  "overall_pf": 2.9023029148618846,
  "total_pnl": 1309362.0911936164,
  "max_drawdown": 0.003943658589070816,
  "num_trades": 14,
  "block_count_wick": 32,
  "folds": [
    {
      "fold": 0,
      "test_trades": 1,
      "pf": null,
      "total_pnl": 270516.65552483697,
      "maxdd": 0.0
    },
    {
      "fold": 1,
      "test_trades": 2,
      "pf": 1.4169284705183252,
      "total_pnl": 79805.61160919786,
      "maxdd": 191413.19735249452
    },
    {
      "fold": 2,
      "test_trades": 3,
      "pf": null,
      "total_pnl": 664635.8773517348,
      "maxdd": 0.0
    },
    {
      "fold": 3,
      "test_trades": 0,
      "pf": null,
      "total_pnl": 0.0,
      "maxdd": 0.0
    }
  ],
  "ok_folds": false
}
```

Selection reason: break_even

Steps to go-live (MANUAL approval required):
1) Review candidate and backtest/retest/breakeven artifacts in logs/ directory
2) Ensure DRY_RUN=true in .env for paper/monitoring phase
3) Enable and start systemd unit start_paper.service/timer to begin 2-week paper run OR run scripts/start_paper.sh
4) Monitor logs/paper_runner.log and llm_reflections for performance
5) After paper run, obtain metrics and decide to go-live