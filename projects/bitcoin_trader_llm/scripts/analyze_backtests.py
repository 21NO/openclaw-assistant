#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime, timedelta
import statistics

LOG_DIR = Path(__file__).resolve().parents[1] / 'logs'
files = sorted(LOG_DIR.glob('backtest_*.json'))

runs = {}

for f in files:
    try:
        data = json.loads(f.read_text(encoding='utf-8'))
    except Exception:
        continue
    result = data.get('result') or {}
    trades = data.get('trades') or []
    params = result.get('params') or {}
    months = int(result.get('months') or params.get('months') or 0)
    vol = float(params.get('vol_entry_mult') or 0.0)
    wick = params.get('upper_wick_pct')
    if wick is not None:
        try:
            wick = float(wick)
        except Exception:
            wick = None
    slip = float(result.get('slippage_pct') or 0.0)
    key = (months, vol, wick, slip)

    # process trades
    trades_with_pnl = [t for t in trades if t.get('pnl') is not None]
    # sort by exit_time if available else entry_time
    def tr_time(t):
        tstr = t.get('exit_time') or t.get('entry_time')
        try:
            return datetime.fromisoformat(tstr)
        except Exception:
            return datetime.min
    trades_sorted = sorted(trades_with_pnl, key=tr_time)
    num_trades = len(trades_sorted)
    wins = [t for t in trades_sorted if t.get('pnl') > 0]
    losses = [t for t in trades_sorted if t.get('pnl') <= 0]
    win_pct = (len(wins)/num_trades*100.0) if num_trades>0 else 0.0
    gross_profit = sum([t['pnl'] for t in wins]) if wins else 0.0
    gross_loss = -sum([t['pnl'] for t in losses]) if losses else 0.0
    pf = (gross_profit/gross_loss) if gross_loss>0 else None
    total_pnl = float(result.get('total_pnl') or sum([t['pnl'] for t in trades_sorted]) or 0.0)
    final_nav = float(result.get('final_nav') or 0.0)
    max_dd = float(result.get('max_drawdown') or 0.0)
    avg_win = statistics.mean([t['pnl'] for t in wins]) if wins else None
    avg_loss = statistics.mean([t['pnl'] for t in losses]) if losses else None
    stop_count = len([t for t in trades_sorted if t.get('reason')=='stop'])
    stop_pct = (stop_count/num_trades*100.0) if num_trades>0 else 0.0
    hold_hours = []
    for t in trades_sorted:
        et = t.get('entry_time'); xt = t.get('exit_time')
        if et and xt:
            try:
                et_dt = datetime.fromisoformat(et)
                xt_dt = datetime.fromisoformat(xt)
                hold_hours.append((xt_dt - et_dt).total_seconds()/3600.0)
            except Exception:
                pass
    avg_hold_hrs = statistics.mean(hold_hours) if hold_hours else None

    # max consecutive losses
    max_consec_losses = 0
    cur = 0
    for t in trades_sorted:
        if t.get('pnl') is not None and t.get('pnl') <= 0:
            cur += 1
            if cur > max_consec_losses:
                max_consec_losses = cur
        else:
            cur = 0

    # train/test split
    start_dt = None
    try:
        start_dt = datetime.fromisoformat(result.get('start'))
    except Exception:
        start_dt = None
    train_end = (start_dt + timedelta(days=120)) if start_dt else None
    train_trades = []
    test_trades = []
    if train_end:
        for t in trades_sorted:
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
    def metrics_subset(trs):
        if not trs:
            return {'num_trades':0,'pf':None,'total_pnl':0.0,'maxdd':None}
        w = [t for t in trs if t.get('pnl')>0]
        l = [t for t in trs if t.get('pnl')<=0]
        gp = sum([t['pnl'] for t in w]) if w else 0.0
        gl = -sum([t['pnl'] for t in l]) if l else 0.0
        pfv = (gp/gl) if gl>0 else None
        totalp = sum([t['pnl'] for t in trs])
        # approximate maxdd from cumulative p&l
        cum = []
        s = 0.0
        for t in trs:
            s += float(t.get('pnl') or 0.0)
            cum.append(s)
        maxdd = 0.0
        if cum:
            peak = cum[0]
            for v in cum:
                if v > peak:
                    peak = v
                dd = (peak - v)
                if dd > maxdd:
                    maxdd = dd
        return {'num_trades': len(trs), 'pf': pfv, 'total_pnl': totalp, 'maxdd': maxdd}

    train_metrics = metrics_subset(train_trades)
    test_metrics = metrics_subset(test_trades)

    runs[key] = {
        'file': str(f.name),
        'months': months,
        'vol': vol,
        'wick': wick,
        'slip': slip,
        '#trades': num_trades,
        'win%': round(win_pct,2),
        'PF': round(pf,3) if pf is not None else None,
        'totalPnL': round(total_pnl,2),
        'finalNAV': round(final_nav,2),
        'MaxDD': round(max_dd,6),
        'avg_win': round(avg_win,2) if avg_win is not None else None,
        'avg_loss': round(avg_loss,2) if avg_loss is not None else None,
        'stop%': round(stop_pct,2),
        'avg_hold_hours': round(avg_hold_hrs,3) if avg_hold_hrs is not None else None,
        'max_consec_losses': max_consec_losses,
        'train': train_metrics,
        'test': test_metrics
    }

# Now produce requested reports focusing on vol in {2.0,2.5}, wick in {None,0.4,0.5}, slip in {0.001,0.0005}
combos = []
for months in (6,):
    for vol in (2.0, 2.5):
        for wick in (None, 0.4, 0.5):
            for slip in (0.001, 0.0005):
                key = (months, vol, wick, slip)
                if key in runs:
                    combos.append(runs[key])

# 1) table for the 12 scenarios
print('TABLE_12_RUNS')
headers = ['vol_entry_mult','upper_wick_pct','slippage','#trades','win%','PF','totalPnL','finalNAV','MaxDD','avg_win','avg_loss','stop%','avg_hold_hours','max_consec_losses','notes']
print(','.join(headers))
for r in combos:
    notes = ''
    if r['#trades'] < 20:
        notes = '표본 신뢰 낮음'
        if r['PF'] is not None and r['PF']>1:
            notes += ('; 우연 가능성 경고')
    row = [str(r['vol']), str(r['wick']) if r['wick'] is not None else '', f"{r['slip']:.4f}", str(r['#trades']), str(r['win%']), str(r['PF']), str(r['totalPnL']), str(r['finalNAV']), str(r['MaxDD']), str(r['avg_win']), str(r['avg_loss']), str(r['stop%']), str(r['avg_hold_hours']), str(r['max_consec_losses']), notes]
    print(','.join(row))

# 2) months=3 vs months=6 comparison: find months=3 runs for same vol/wick/slip
print('\nCOMPARE_3_vs_6')
print('vol,wick,slip,PF_3,PF_6,delta_PF,totalPnL_3,totalPnL_6,delta_totalPnL,MaxDD_3,MaxDD_6,delta_MaxDD,#trades_3,#trades_6,trade_delta,PF_change_flag')
for vol in (2.0,2.5):
    for wick in (None,0.4,0.5):
        for slip in (0.001,0.0005):
            key6 = (6,vol,wick,slip)
            key3 = (3,vol,wick,slip)
            if key6 in runs:
                r6 = runs[key6]
                r3 = runs.get(key3)
                pf3 = r3['PF'] if r3 else None
                pf6 = r6['PF']
                delta_pf = (pf6 - pf3) if (pf3 is not None and pf6 is not None) else None
                tp3 = r3['totalPnL'] if r3 else None
                tp6 = r6['totalPnL']
                delta_tp = (tp6 - tp3) if (tp3 is not None) else None
                md3 = r3['MaxDD'] if r3 else None
                md6 = r6['MaxDD']
                delta_md = (md6 - md3) if (md3 is not None) else None
                n3 = r3['#trades'] if r3 else None
                n6 = r6['#trades']
                pf_flag = '유지' if (pf3 is not None and pf6 is not None and pf6>=pf3) else ('붕괴' if (pf3 is not None and pf6 is not None and pf6<pf3) else 'NA')
                out = [str(vol), str(wick) if wick is not None else '', f"{slip:.4f}", str(pf3) if pf3 is not None else '', str(pf6) if pf6 is not None else '', str(delta_pf) if delta_pf is not None else '', str(tp3) if tp3 is not None else '', str(tp6), str(delta_tp) if delta_tp is not None else '', str(md3) if md3 is not None else '', str(md6), str(delta_md) if delta_md is not None else '', str(n3) if n3 is not None else '', str(n6), str(n6 - (n3 or 0)), pf_flag]
                print(','.join(out))

# 3) Workforward train/test for months=6: print PF,totalPnL,MaxDD,#trades for train and test
print('\nWORKFORWARD_MONTHS6')
print('vol,wick,slip,train_num,train_PF,train_totalPnL,train_MaxDD,test_num,test_PF,test_totalPnL,test_MaxDD,overfit_flag')
for r in combos:
    tr = r['train']; te = r['test']
    overfit = ''
    if tr['pf'] is not None and te['pf'] is not None:
        if tr['pf'] > 1 and (te['pf'] is None or te['pf'] <= 1):
            overfit = '과최적화_의심'
        elif tr['pf'] > 1 and te['pf'] > 1:
            overfit = '유지'
        else:
            overfit = 'NA'
    out = [str(r['vol']), str(r['wick']) if r['wick'] is not None else '', f"{r['slip']:.4f}", str(tr['num_trades']), str(round(tr['pf'],3)) if tr['pf'] is not None else '', str(round(tr['total_pnl'],2)), str(round(tr['maxdd'],6)) if tr['maxdd'] is not None else '', str(te['num_trades']), str(round(te['pf'],3)) if te['pf'] is not None else '', str(round(te['total_pnl'],2)), str(round(te['maxdd'],6)) if te['maxdd'] is not None else '', overfit]
    print(','.join(out))

# 4) Slippage impact (0.001 vs 0.0005) for months=6
print('\nSLIPPAGE_IMPACT')
print('vol,wick,PF_slip0.001,PF_slip0.0005,delta_PF,totalPnL_0.001,totalPnL_0.0005,delta_totalPnL,sensitive_flag')
for vol in (2.0,2.5):
    for wick in (None,0.4,0.5):
        key1 = (6,vol,wick,0.001)
        key05 = (6,vol,wick,0.0005)
        if key1 in runs and key05 in runs:
            r1 = runs[key1]; r05 = runs[key05]
            pf1 = r1['PF']; pf05 = r05['PF']
            delta_pf = (pf05 - pf1) if (pf1 is not None and pf05 is not None) else None
            tp1 = r1['totalPnL']; tp05 = r05['totalPnL']
            delta_tp = (tp05 - tp1)
            # define sensitivity: delta_pf absolute >= 0.1 OR abs(delta_tp) >= 100000 KRW
            sensitive = (abs(delta_pf) >= 0.1) if delta_pf is not None else False
            sensitive = sensitive or (abs(delta_tp) >= 100000)
            out = [str(vol), str(wick) if wick is not None else '', str(pf1), str(pf05), str(delta_pf), str(tp1), str(tp05), str(delta_tp), ('민감' if sensitive else '비민감')]
            print(','.join(out))

# 5) Final classification rule and numbers
print('\nCLASSIFICATION')
# compute counts
months6_keys = [k for k in runs.keys() if k[0]==6]
pf_gt1_count = sum(1 for k in months6_keys if runs[k]['PF'] is not None and runs[k]['PF']>1)
pf_gt1_with_test_gt1 = sum(1 for k in months6_keys if runs[k]['PF'] is not None and runs[k]['PF']>1 and runs[k]['train']['pf'] is not None and runs[k]['test']['pf'] is not None and runs[k]['train']['pf']>1 and runs[k]['test']['pf']>1)
runs_with_trades_ge20 = sum(1 for k in months6_keys if runs[k]['#trades']>=20)
print(f"total_runs_months6={len(months6_keys)}, pf_gt1_count={pf_gt1_count}, pf_gt1_with_train_test_gt1={pf_gt1_with_test_gt1}, runs_trades_ge20={runs_with_trades_ge20}")
# apply rule:
# A if >=50% runs PF>1 and at least half of those have train&test PF>1 and majority have >=20 trades
# B if >=25% runs PF>1 but stability low or trade counts small
# C otherwise
n = len(months6_keys)
cls = 'C'
if pf_gt1_count >= (0.5 * n) and pf_gt1_with_test_gt1 >= (0.5 * pf_gt1_count) and runs_with_trades_ge20 >= (0.5 * n):
    cls = 'A'
elif pf_gt1_count >= (0.25 * n):
    cls = 'B'
else:
    cls = 'C'
print(f"CLASS={cls} (rule: A if >=50% PF>1 & >=50% of those stable(train/test)>1 & >=50% runs have >=20 trades; else B if >=25% PF>1; else C)")

# done
print('\nDONE')
