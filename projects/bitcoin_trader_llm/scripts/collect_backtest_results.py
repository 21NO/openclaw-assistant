#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime, timedelta
import statistics

LOG_DIR = Path(__file__).resolve().parents[1] / 'logs'
OUT_CSV = LOG_DIR / 'aggregate_backtests_summary.csv'
OUT_JSON = LOG_DIR / 'aggregate_backtests_summary.json'

files = sorted(LOG_DIR.glob('backtest_*.json'))
rows = []
selected = []
for f in files:
    try:
        data = json.loads(f.read_text(encoding='utf-8'))
    except Exception:
        continue
    result = data.get('result') or {}
    params = result.get('params') or {}
    # filter: months=6 and interval minutes30
    if result.get('months') != 6:
        continue
    if result.get('interval') != 'minutes30':
        continue
    # only consider vol_entry_mult in 2.0,2.5 and upper_wick_pct in (None,0.4,0.5) and slippage 0.001 or 0.0005
    vol = float(params.get('vol_entry_mult') or 0.0)
    wick = params.get('upper_wick_pct')
    slip = float(result.get('slippage_pct') or 0.0)
    if vol not in (2.0, 2.5):
        continue
    if wick not in (None, 0.4, 0.5):
        # allow if 0.4 stored as 0.4
        try:
            if float(wick) not in (0.4, 0.5):
                continue
        except Exception:
            continue
    if slip not in (0.001, 0.0005):
        continue
    # compute metrics from trades
    trades = data.get('trades') or []
    trades_with_pnl = [t for t in trades if t.get('pnl') is not None]
    num_trades = len(trades_with_pnl)
    wins = [t for t in trades_with_pnl if t.get('pnl') > 0]
    losses = [t for t in trades_with_pnl if t.get('pnl') <= 0]
    win_pct = (len(wins) / num_trades * 100.0) if num_trades>0 else 0.0
    gross_profit = sum([t['pnl'] for t in wins]) if wins else 0.0
    gross_loss = -sum([t['pnl'] for t in losses]) if losses else 0.0
    pf = None
    if gross_loss > 0:
        pf = gross_profit / gross_loss
    total_pnl = float(result.get('total_pnl') or 0.0)
    final_nav = float(result.get('final_nav') or 0.0)
    maxdd = float(result.get('max_drawdown') or 0.0)
    avg_win = float(statistics.mean([t['pnl'] for t in wins])) if wins else None
    avg_loss = float(statistics.mean([t['pnl'] for t in losses])) if losses else None
    stop_count = len([t for t in trades_with_pnl if t.get('reason') == 'stop'])
    stop_pct = (stop_count / num_trades * 100.0) if num_trades>0 else 0.0
    # avg hold hours
    hold_hours = []
    for t in trades_with_pnl:
        et = t.get('entry_time')
        xt = t.get('exit_time')
        if et and xt:
            try:
                et_dt = datetime.fromisoformat(et)
                xt_dt = datetime.fromisoformat(xt)
                hold_hours.append((xt_dt - et_dt).total_seconds() / 3600.0)
            except Exception:
                pass
    avg_hold_hrs = statistics.mean(hold_hours) if hold_hours else None

    # train/test split: start = result['start'], split at start + 120 days
    try:
        start_dt = datetime.fromisoformat(result.get('start'))
    except Exception:
        start_dt = None
    train_end = (start_dt + timedelta(days=120)) if start_dt else None
    train_trades = []
    test_trades = []
    if train_end:
        for t in trades_with_pnl:
            et = t.get('entry_time')
            if not et:
                continue
            try:
                et_dt = datetime.fromisoformat(et)
            except Exception:
                continue
            if et_dt <= train_end:
                train_trades.append(t)
            else:
                test_trades.append(t)
    # compute train/test metrics
    def metrics_from(trs):
        if not trs:
            return {'num_trades':0,'pf':None,'total_pnl':0.0,'maxdd':None}
        w = [t for t in trs if t.get('pnl')>0]
        l = [t for t in trs if t.get('pnl')<=0]
        gp = sum([t['pnl'] for t in w]) if w else 0.0
        gl = -sum([t['pnl'] for t in l]) if l else 0.0
        pfv = (gp/gl) if gl>0 else None
        totalp = sum([t['pnl'] for t in trs])
        # maxdd not trivial per subset; leave None
        return {'num_trades':len(trs),'pf':pfv,'total_pnl':totalp,'maxdd':None}

    train_metrics = metrics_from(train_trades)
    test_metrics = metrics_from(test_trades)

    row = {
        'file': str(f.name),
        'vol_entry_mult': vol,
        'upper_wick_pct': wick,
        'slippage': slip,
        '#trades': num_trades,
        'win%': round(win_pct,2),
        'PF': round(pf,3) if pf is not None else None,
        'totalPnL': round(total_pnl,2),
        'finalNAV': round(final_nav,2),
        'MaxDD': round(maxdd,4),
        'avg_win': round(avg_win,2) if avg_win is not None else None,
        'avg_loss': round(avg_loss,2) if avg_loss is not None else None,
        'stop%': round(stop_pct,2),
        'avg_hold_hours': round(avg_hold_hrs,3) if avg_hold_hrs is not None else None,
        'train_num_trades': train_metrics['num_trades'],
        'train_pf': round(train_metrics['pf'],3) if train_metrics['pf'] is not None else None,
        'test_num_trades': test_metrics['num_trades'],
        'test_pf': round(test_metrics['pf'],3) if test_metrics['pf'] is not None else None
    }
    rows.append(row)
    selected.append({'file': str(f), 'data': data})

# write CSV
if rows:
    header = list(rows[0].keys())
    with open(OUT_CSV, 'w', encoding='utf-8') as f:
        f.write(','.join(header) + '\n')
        for r in rows:
            line = []
            for h in header:
                v = r.get(h)
                if v is None:
                    line.append('')
                else:
                    line.append(str(v))
            f.write(','.join(line) + '\n')

# write JSON summary
with open(OUT_JSON, 'w', encoding='utf-8') as f:
    json.dump({'summary': rows, 'files': [s['file'] for s in selected]}, f, indent=2, ensure_ascii=False)

print('Collected', len(rows), 'matching backtest.json files. Summary written to', OUT_CSV)
