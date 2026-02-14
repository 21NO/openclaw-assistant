#!/usr/bin/env python3
"""
Run rolling-fold 9-month backtests using PortfolioManager (Agent+Rule composition) + RiskEngine
Saves per-fold result JSONs and a summary file logs/rolling_folds_summary.json

Usage: python3 scripts/run_rolling_folds_pm.py
"""
from __future__ import annotations
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

# ensure project root on path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backtest_dce import run_backtest
from app import config

LOG_DIR = PROJECT_ROOT / 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# User-specified experiment parameters (from conversation)
FOLDS = 8
WINDOW_MONTHS = 9
STRIDE_MONTHS = 1
DRY_RUN = True

# PortfolioManager weights
PM_AGENT_WEIGHT = 0.6
PM_RULE_WEIGHT = 0.2
AGENT_PROPOSED_RISK_MULT = 2.0

# RiskEngine parameters (user requested)
RISK_INITIAL_PCT = 1.0
RISK_MIN_PCT = 0.2
RISK_MAX_REDUCTION_STEPS = 3
RISK_MULTIPLIER = 0.5
RISK_RECOVERY_CONSEC_WINS = 2
RISK_RECOVERY_STEP_PCT_OF_INITIAL = 20.0  # percent -> will convert to fraction
RISK_COOLDOWN_DAYS = 7
RISK_DAILY_LOSS_PCT = 1.0
RISK_MAX_DD_PCT = 10.0

# Other backtest params
MODE = 'regime'
VOL_ENTRY_MULT = 1.5
ADX_THRESHOLD = 30.0
HTF_REQUIRE = 'both'
UPPER_WICK_PCT = 0.5
SLIPPAGE = 0.001
FEE = 0.0005

# fold end dates: most recent end = now; earlier folds shift back by STRIDE_MONTHS*30 days
base_end = datetime.utcnow()

results = []
summary_rows = []

for i in range(FOLDS):
    fold_end = base_end - timedelta(days=30 * STRIDE_MONTHS * i)
    print(f"Running fold {i+1}/{FOLDS}: end={fold_end.isoformat()} window={WINDOW_MONTHS} months")

    # convert recovery pct from percent to fraction expected by run_backtest
    recovery_frac = float(RISK_RECOVERY_STEP_PCT_OF_INITIAL)
    if recovery_frac > 1.0:
        recovery_frac = recovery_frac / 100.0

    try:
        res = run_backtest(
            months=WINDOW_MONTHS,
            slippage_pct=SLIPPAGE,
            fee_pct=FEE,
            time_based_exit_min=60,
            vol_mult=2.0,
            stop_rel_default=1.5,
            risk_pct=None,
            mode=MODE,
            vol_entry_mult=VOL_ENTRY_MULT,
            tp_pct=None,
            sl_pct=None,
            disable_rsi_veto=False,
            atr_trail=False,
            gatekeeper_only=False,
            early_abort=False,
            early_abort_pct=0.005,
            early_abort_bars=2,
            adx_threshold=ADX_THRESHOLD,
            htf_require=HTF_REQUIRE,
            upper_wick_pct=UPPER_WICK_PCT,
            enable_risk_engine=True,
            risk_daily_loss_pct=RISK_DAILY_LOSS_PCT,
            risk_max_dd_pct=RISK_MAX_DD_PCT,
            risk_consec_losses=3,
            risk_consec_mult=RISK_MULTIPLIER,
            risk_initial_pct=RISK_INITIAL_PCT,
            risk_min_pct=RISK_MIN_PCT,
            risk_max_reduction_steps=RISK_MAX_REDUCTION_STEPS,
            risk_recovery_step_pct=recovery_frac,
            risk_recovery_consec_wins=RISK_RECOVERY_CONSEC_WINS,
            use_portfolio_manager=True,
            pm_agent_weight=PM_AGENT_WEIGHT,
            pm_rule_weight=PM_RULE_WEIGHT,
            agent_proposed_risk_mult=AGENT_PROPOSED_RISK_MULT,
            risk_cooldown_days=RISK_COOLDOWN_DAYS,
            end_dt=fold_end
        )
    except Exception as e:
        print(f"Fold {i+1} failed: {e}")
        res = None

    # Save fold result
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    fold_name = f'rolling_fold_{i+1}_{ts}.json'
    fold_path = LOG_DIR / fold_name
    try:
        with open(fold_path, 'w', encoding='utf-8') as f:
            json.dump({'fold': i+1, 'end': fold_end.isoformat(), 'result': res}, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print('Failed to write fold result:', e)

    if res:
        summary_rows.append({
            'fold': i+1,
            'end': fold_end.isoformat(),
            'profit_factor': res.get('profit_factor'),
            'total_pnl': res.get('total_pnl'),
            'num_trades': res.get('num_trades'),
            'win_rate': res.get('win_rate'),
            'max_dd': res.get('max_drawdown')
        })
    else:
        summary_rows.append({'fold': i+1, 'end': fold_end.isoformat(), 'profit_factor': None, 'total_pnl': None, 'num_trades': None, 'win_rate': None, 'max_dd': None})

# write summary
summary_path = LOG_DIR / 'rolling_folds_summary.json'
with open(summary_path, 'w', encoding='utf-8') as f:
    json.dump({'params': {
        'folds': FOLDS,
        'window_months': WINDOW_MONTHS,
        'stride_months': STRIDE_MONTHS,
        'pm_agent_weight': PM_AGENT_WEIGHT,
        'pm_rule_weight': PM_RULE_WEIGHT
    }, 'rows': summary_rows}, f, indent=2, ensure_ascii=False)

print('Rolling folds complete. Summary written to', summary_path)
