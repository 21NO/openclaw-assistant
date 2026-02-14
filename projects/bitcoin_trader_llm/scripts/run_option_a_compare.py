#!/usr/bin/env python3
"""
Compare baseline vs Option A (aggressive sizing) across rolling folds (months=9, folds=6)
Saves per-run JSONs into logs/ and writes a summary CSV/JSON.
"""
from __future__ import annotations
import os
import sys
import json
import csv
from datetime import datetime, timedelta
from pathlib import Path

# add project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backtest_dce import run_backtest
from app import config

LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

FOLDS = 6
WINDOW_MONTHS = 9
STRIDE_MONTHS = 1

PM_AGENT_WEIGHT = 0.6
PM_RULE_WEIGHT = 0.2
AGENT_PROPOSED_RISK_MULT = 2.0

# Baseline (current system/default before Option A)
BASELINE = {
    'name': 'baseline',
    'initial_risk_pct': 1.0,
    'min_risk_pct': 0.2,
    'consec_mult': 0.5,
    'max_reduction_steps': 3,
    'recovery_step_frac': 0.2,
    'recovery_consec_wins': 2,
    'cooldown_days': 7,
    'risk_daily_loss_pct': 1.0,
    'risk_max_dd_pct': 10.0
}

# Option A (user-specified aggressive set)
OPTION_A = {
    'name': 'option_a',
    'initial_risk_pct': 2.0,
    'min_risk_pct': 0.8,
    'consec_mult': 0.8,
    'max_reduction_steps': 3,
    'recovery_step_frac': 0.5,
    'recovery_consec_wins': 1,
    'cooldown_days': 7,
    'risk_daily_loss_pct': 1.0,
    'risk_max_dd_pct': 15.0
}

# TP settings: user requested TP=2% as baseline for Option A. We'll use tp_pct=0.02 for Option A.
TP_PCT = 0.02

# run configs: for both baseline and option_a, test atr_trail False and True (ATR trailing test)
RUN_VARIANTS = [
    {'label': 'no_atr', 'atr_trail': False},
    {'label': 'atr_trail', 'atr_trail': True}
]

# compute fold end dates (most recent = now)
base_end = datetime.utcnow()

summary_rows = []

configs = [BASELINE, OPTION_A]

for cfg in configs:
    for var in RUN_VARIANTS:
        variant_name = f"{cfg['name']}_{var['label']}"
        print('Running variant:', variant_name)
        for i in range(FOLDS):
            fold_end = base_end - timedelta(days=30 * STRIDE_MONTHS * i)
            print(f"  Fold {i+1}/{FOLDS}: end={fold_end.date()} | atr_trail={var['atr_trail']}")
            try:
                res = run_backtest(
                    months=WINDOW_MONTHS,
                    slippage_pct=0.001,
                    fee_pct=0.0005,
                    time_based_exit_min=60,
                    vol_mult=2.0,
                    stop_rel_default=1.5,
                    risk_pct=None,
                    mode='regime',
                    vol_entry_mult=1.5,
                    tp_pct=TP_PCT if cfg['name']=='option_a' else None,
                    sl_pct=None,
                    disable_rsi_veto=False,
                    atr_trail=var['atr_trail'],
                    gatekeeper_only=False,
                    early_abort=False,
                    early_abort_pct=0.005,
                    early_abort_bars=2,
                    adx_threshold=30.0,
                    htf_require='both',
                    upper_wick_pct=0.5,
                    enable_risk_engine=True,
                    risk_daily_loss_pct=cfg['risk_daily_loss_pct'],
                    risk_max_dd_pct=cfg['risk_max_dd_pct'],
                    risk_consec_losses=3,
                    risk_consec_mult=cfg['consec_mult'],
                    risk_initial_pct=cfg['initial_risk_pct'],
                    risk_min_pct=cfg['min_risk_pct'],
                    risk_max_reduction_steps=cfg['max_reduction_steps'],
                    risk_recovery_step_pct=cfg['recovery_step_frac'],
                    risk_recovery_consec_wins=cfg['recovery_consec_wins'],
                    use_portfolio_manager=True,
                    pm_agent_weight=PM_AGENT_WEIGHT,
                    pm_rule_weight=PM_RULE_WEIGHT,
                    agent_proposed_risk_mult=AGENT_PROPOSED_RISK_MULT,
                    risk_cooldown_days=cfg['cooldown_days'],
                    end_dt=fold_end
                )
            except Exception as e:
                print('    Fold failed:', e)
                res = None

            ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            outname = f'compare_{variant_name}_fold{i+1}_{ts}.json'
            outpath = LOG_DIR / outname
            try:
                with open(outpath, 'w', encoding='utf-8') as f:
                    json.dump({'variant': variant_name, 'fold': i+1, 'end': fold_end.isoformat(), 'result': res}, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print('    Failed to write fold file:', e)

            # collect summary row
            if res:
                row = {
                    'variant': variant_name,
                    'fold': i+1,
                    'end': fold_end.isoformat(),
                    'profit_factor': res.get('profit_factor'),
                    'total_pnl': res.get('total_pnl'),
                    'num_trades': res.get('num_trades'),
                    'win_rate': res.get('win_rate'),
                    'max_dd': res.get('max_drawdown'),
                    'risk_events': res.get('risk_events') if res.get('risk_events') is not None else {}
                }
            else:
                row = {'variant': variant_name, 'fold': i+1, 'end': fold_end.isoformat(), 'profit_factor': None, 'total_pnl': None, 'num_trades': None, 'win_rate': None, 'max_dd': None, 'risk_events': {}}
            summary_rows.append(row)

# write summary JSON and CSV
summary_path = LOG_DIR / 'compare_option_a_summary.json'
with open(summary_path, 'w', encoding='utf-8') as f:
    json.dump({'params': {'folds': FOLDS, 'window_months': WINDOW_MONTHS, 'pm_agent_weight': PM_AGENT_WEIGHT, 'pm_rule_weight': PM_RULE_WEIGHT, 'tp_pct_option_a': TP_PCT}, 'rows': summary_rows}, f, indent=2, ensure_ascii=False)

csv_path = LOG_DIR / 'compare_option_a_summary.csv'
with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['variant', 'fold', 'end', 'profit_factor', 'total_pnl', 'num_trades', 'win_rate', 'max_dd']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for r in summary_rows:
        writer.writerow({k: r.get(k) for k in fieldnames})

print('All runs complete. Summary written to', summary_path)
