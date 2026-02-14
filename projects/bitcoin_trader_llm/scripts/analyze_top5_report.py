#!/usr/bin/env python3
"""
Analyze backtest JSON logs and produce Top-5 candidate summary (break-even left as TBD).
Outputs:
 - logs/top5_candidates.csv
 - logs/top5_candidates.json
 - logs/top5_report.txt (human-readable)
"""
from pathlib import Path
import json
from datetime import datetime, timedelta
import csv

WORKDIR = Path(__file__).resolve().parents[1]
LOG_DIR = WORKDIR / 'logs'
OUT_CSV = LOG_DIR / 'top5_candidates.csv'
OUT_JSON = LOG_DIR / 'top5_candidates.json'
OUT_TXT = LOG_DIR / 'top5_report.txt'

FILES = sorted(LOG_DIR.glob('backtest_*.json'))

# selection filters (matches sweep grid)
VOL_SET = {1.2, 1.5}
ADX_SET = {20, 25, 30}
HTF_SET = {'any', 'both'}
SLIP_VAL = 0.001


def load_run(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def pf_from_trades(trs):
    wins = [t for t in trs if t.get('pnl') is not None and float(t.get('pnl')) > 0]
    losses = [t for t in trs if t.get('pnl') is not None and float(t.get('pnl')) <= 0]
    gp = sum([float(t['pnl']) for t in wins]) if wins else 0.0
    gl = -sum([float(t['pnl']) for t in losses]) if losses else 0.0
    pf = (gp / gl) if gl > 0 else None
    return pf, gp, gl


report = []
for f in FILES:
    data = load_run(f)
    if not data:
        continue
    result = data.get('result') or {}
    params = result.get('params') or {}
    months = int(result.get('months') or 0)
    if months != 6:
        continue
    try:
        vol = float(params.get('vol_entry_mult') or 0.0)
        adx = float(params.get('adx_threshold') or 0.0)
        htf = params.get('htf_require')
        slip = float(result.get('slippage_pct') or 0.0)
    except Exception:
        continue
    if vol not in VOL_SET or adx not in ADX_SET or htf not in HTF_SET or abs(slip - SLIP_VAL) > 1e-9:
        continue

    trades = data.get('trades') or []
    trades_with_pnl = [t for t in trades if t.get('pnl') is not None]

    overall_pf = result.get('profit_factor')
    if overall_pf is None:
        pf_calc, gp, gl = pf_from_trades(trades_with_pnl)
        overall_pf = pf_calc

    total_pnl = result.get('total_pnl')
    max_dd = result.get('max_drawdown')

    # block_count_wick
    gate_logs = data.get('gate_logs') or []
    block_count_wick = 0
    for g in gate_logs:
        uw = g.get('upper_wick_ok')
        passed = g.get('pass')
        if uw is False and not passed:
            block_count_wick += 1

    # folds
    folds = []
    start = result.get('start')
    try:
        start_dt = datetime.fromisoformat(start)
    except Exception:
        start_dt = None
    for i in range(4):
        if start_dt is None:
            folds.append({'fold': i, 'test_trades': 0, 'pf': None, 'total_pnl': 0.0, 'maxdd': 0.0})
            continue
        train_s = start_dt + timedelta(days=30 * i)
        train_e = train_s + timedelta(days=60)
        test_s = train_e
        test_e = test_s + timedelta(days=30)
        test_trs = []
        for t in trades_with_pnl:
            et = t.get('entry_time')
            if not et:
                continue
            try:
                et_dt = datetime.fromisoformat(et)
            except Exception:
                continue
            if et_dt >= test_s and et_dt < test_e:
                test_trs.append(t)
        num_test = len(test_trs)
        pf_test, gp_test, gl_test = pf_from_trades(test_trs)
        # compute maxdd on cumulative pnl
        cum = []
        s = 0.0
        for t in sorted(test_trs, key=lambda x: x.get('entry_time') or ''):
            s += float(t.get('pnl') or 0.0)
            cum.append(s)
        maxdd = 0.0
        if cum:
            peak = cum[0]
            for v in cum:
                if v > peak:
                    peak = v
                dd = peak - v
                if dd > maxdd:
                    maxdd = dd
        folds.append({'fold': i, 'test_trades': num_test, 'pf': pf_test, 'total_pnl': (gp_test - gl_test), 'maxdd': maxdd})

    upper = params.get('upper_wick_pct')
    upper_label = 'OFF' if upper is None else str(upper)

    report.append({
        'file': str(f.name),
        'vol': vol,
        'adx': adx,
        'htf': htf,
        'upper_wick': upper_label,
        'overall_pf': overall_pf,
        'total_pnl': total_pnl,
        'max_drawdown': max_dd,
        'block_count_wick': block_count_wick,
        'folds': folds,
        'num_trades': len(trades_with_pnl),
        'slip': slip,
        'json_path': str(f)
    })

# sort by overall_pf desc (None -> treat as -1)
report_sorted = sorted(report, key=lambda x: (x['overall_pf'] if x['overall_pf'] is not None else -1.0), reverse=True)
TOP5 = report_sorted[:5]

# Write CSV
with open(OUT_CSV, 'w', newline='', encoding='utf-8') as cf:
    writer = csv.writer(cf)
    header = ['file', 'vol', 'adx', 'htf', 'upper_wick', 'overall_pf', 'total_pnl', 'max_drawdown', 'block_count_wick',
              'fold0_pf', 'fold0_trades', 'fold1_pf', 'fold1_trades', 'fold2_pf', 'fold2_trades', 'fold3_pf', 'fold3_trades', 'break_even_slippage']
    writer.writerow(header)
    for r in TOP5:
        row = [r['file'], r['vol'], r['adx'], r['htf'], r['upper_wick'], r['overall_pf'], r['total_pnl'], r['max_drawdown'], r['block_count_wick']]
        for f in r['folds']:
            row.append(f['pf'])
            row.append(f['test_trades'])
        row.append('TBD')
        writer.writerow(row)

# Write JSON
with open(OUT_JSON, 'w', encoding='utf-8') as jf:
    json.dump({'top5': TOP5}, jf, indent=2, ensure_ascii=False)

# Write human-readable text
with open(OUT_TXT, 'w', encoding='utf-8') as tf:
    tf.write('Top-5 candidate summary (break-even slippage = TBD)\n')
    tf.write('Generated: {}\n\n'.format(datetime.utcnow().isoformat()))
    for i, r in enumerate(TOP5, start=1):
        tf.write(f"#{i} File: {r['file']}\n")
        tf.write(f"Params: vol={r['vol']} adx={r['adx']} htf={r['htf']} upper_wick={r['upper_wick']} slip={r['slip']}\n")
        tf.write(f"Months=6 overall_PF={r['overall_pf']} total_pnl={r['total_pnl']} max_drawdown={r['max_drawdown']} num_trades={r['num_trades']} block_count_wick={r['block_count_wick']}\n")
        tf.write('Folds:\n')
        for f in r['folds']:
            tf.write(f"  fold{f['fold']}: test_trades={f['test_trades']} pf={f['pf']} total_pnl={f['total_pnl']} maxdd={f['maxdd']}\n")
        tf.write('\n')

print('ANALYSIS_DONE: top5 written to', OUT_CSV, OUT_JSON, OUT_TXT)
