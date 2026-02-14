#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime, timedelta
import statistics
import math

LOG_DIR = Path(__file__).resolve().parents[1] / 'logs'
files = sorted(LOG_DIR.glob('backtest_*.json'))

# candidate parameter sets we ran
VOL = (1.2, 1.5)
ADX = (20,25,30)
HTF = ('any','both')
SLIP = 0.001

def load_run(fpath):
    with open(fpath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

runs = []
for f in files:
    data = load_run(f)
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
    if vol in VOL and adx in ADX and htf in HTF and abs(slip - SLIP) < 1e-9:
        runs.append((f, data))

# helper to compute PF
def pf_from_trades(trs):
    wins = [t for t in trs if t.get('pnl') is not None and t.get('pnl')>0]
    losses = [t for t in trs if t.get('pnl') is not None and t.get('pnl')<=0]
    gp = sum([t['pnl'] for t in wins]) if wins else 0.0
    gl = -sum([t['pnl'] for t in losses]) if losses else 0.0
    pf = (gp/gl) if gl>0 else None
    return pf, gp, gl

# compute rolling folds metrics for each run and block_count_wick
report = []
for f, data in runs:
    result = data.get('result')
    trades = data.get('trades') or []
    gate_logs = data.get('gate_logs') or []
    # block_count_wick: gate_logs where upper_wick_ok == False and pass == False
    block_count_wick = 0
    for g in gate_logs:
        uw = g.get('upper_wick_ok')
        passed = g.get('pass')
        if uw is False and not passed:
            block_count_wick += 1
    # overall PF
    trades_with_pnl = [t for t in trades if t.get('pnl') is not None]
    pf_overall, gp, gl = pf_from_trades(trades_with_pnl)
    # derive start_dt
    start = result.get('start')
    try:
        start_dt = datetime.fromisoformat(start)
    except Exception:
        start_dt = None
    # create 4 folds: i=0..3: train=[start+i*30, start+i*30+60), test=[start+i*30+60, start+i*30+90)
    folds = []
    ok_folds = True
    for i in range(4):
        train_s = start_dt + timedelta(days=30*i)
        train_e = train_s + timedelta(days=60)
        test_s = train_e
        test_e = test_s + timedelta(days=30)
        # select trades by entry_time
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
        # compute maxdd by cumulative pnl
        cum = []
        s = 0.0
        for t in sorted(test_trs, key=lambda x: x.get('entry_time') or ''):
            s += float(t.get('pnl') or 0.0)
            cum.append(s)
        maxdd = 0.0
        if cum:
            peak = cum[0]
            for v in cum:
                if v>peak:
                    peak=v
                dd = peak - v
                if dd>maxdd:
                    maxdd = dd
        folds.append({'fold':i,'test_trades':num_test,'pf':pf_test,'total_pnl':(gp_test - gl_test),'maxdd':maxdd})
        if num_test < 10:
            ok_folds = False
    slip_val = float(result.get('slippage_pct') or 0.0)
    upper_wick = result.get('params',{}).get('upper_wick_pct')
    report.append({'file': str(f.name), 'params': result.get('params'), 'overall_pf': pf_overall, 'num_trades': len(trades_with_pnl), 'block_count_wick': block_count_wick, 'folds': folds, 'ok_folds': ok_folds, 'slip': slip_val, 'upper_wick': upper_wick})

# print report summary
print('RUN_SUMMARY')
print('file,vol,adx,htf,upper_wick,slip,#trades,overall_PF,block_count_wick,ok_folds')
for r in report:
    p = r['params'] or {}
    uw = r.get('upper_wick')
    uw_str = '' if uw is None else str(uw)
    print(','.join([r['file'], str(p.get('vol_entry_mult')), str(p.get('adx_threshold')), str(p.get('htf_require')), uw_str, f"{r.get('slip'):.4f}", str(r['num_trades']), str(r['overall_pf']), str(r['block_count_wick']), str(r['ok_folds'])]))

# For runs with block_count_wick==0 and no upper_wick configured, re-run with upper_wick thresholds 0.3 and 0.2 to test effect
import subprocess
print('\nRETEST_UPPER_WICK')
retests = []
for r in report:
    if (r.get('block_count_wick',0) == 0) and (r.get('upper_wick') in (None, '')):
        params = r['params'] or {}
        vol = params.get('vol_entry_mult')
        adx = params.get('adx_threshold')
        htf = params.get('htf_require')
        slip = r.get('slip')
        for uw in (0.3, 0.2):
            cmd = ["/root/.openclaw/workspace/projects/bitcoin_trader_llm/venv/bin/python", "/root/.openclaw/workspace/projects/bitcoin_trader_llm/scripts/backtest_dce.py", "--months", "6", "--mode", "regime", "--tp-pct", "0.015", "--vol-entry-mult", str(vol), "--vol-mult", "2.0", "--stop-rel", "2.0", "--risk-pct", "1.0", "--sl-pct", "0.008", "--disable-time-exit", "--gatekeeper-only", "--adx-threshold", str(adx), "--htf-require", str(htf), "--slippage", str(slip), "--upper-wick-pct", str(uw)]
            print('Re-test:', 'vol', vol, 'adx', adx, 'htf', htf, 'upper_wick', uw)
            proc = subprocess.run(cmd, capture_output=True, text=True)
            out = proc.stdout + proc.stderr
            import re
            m = re.search(r'Details written to (.*backtest_\d+_\d+\.json)', out)
            if m:
                jpath = m.group(1).strip()
            else:
                jfiles = sorted(LOG_DIR.glob('backtest_*.json'))
                jpath = str(jfiles[-1])
            with open(jpath, 'r', encoding='utf-8') as jf:
                jd = json.load(jf)
            gate_logs = jd.get('gate_logs') or []
            block_w = 0
            for g in gate_logs:
                uw_ok = g.get('upper_wick_ok')
                passed = g.get('pass')
                if uw_ok is False and not passed:
                    block_w += 1
            pf_val = jd.get('result',{}).get('profit_factor')
            num_trades = len([t for t in jd.get('trades') or [] if t.get('pnl') is not None])
            retests.append({'file': r['file'], 'vol': vol, 'adx': adx, 'htf': htf, 'upper_wick_tested': uw, 'block_count_wick': block_w, 'pf': pf_val, 'num_trades': num_trades, 'json': jpath})

# print retest summary
print('\nRETEST_SUMMARY')
print('file,vol,adx,htf,upper_wick_tested,block_count_wick,pf,num_trades,json')
for rt in retests:
    print(','.join([rt['file'], str(rt['vol']), str(rt['adx']), str(rt['htf']), str(rt['upper_wick_tested']), str(rt['block_count_wick']), str(rt['pf']), str(rt['num_trades']), rt['json']]))

# select candidates: ok_folds==True and overall_pf>=1
candidates = [r for r in report if r['ok_folds'] and (r['overall_pf'] is not None and r['overall_pf']>=1.0)]
print('\nCANDIDATES')
for c in candidates:
    print(c['file'], c['params'], 'trades', c['num_trades'], 'PF', c['overall_pf'])

# If candidates found, compute break-even slippage by binary search for each candidate
# For each candidate, binary search slippage in [0.0, 0.02] for PF>=1
break_even = []
for c in candidates:
    params = c['params']
    vol = params.get('vol_entry_mult')
    adx = params.get('adx_threshold')
    htf = params.get('htf_require')
    upper = params.get('upper_wick_pct')
    low = 0.0
    high = 0.02
    be = None
    for it in range(10):
        mid = (low + high) / 2.0
        # run backtest with same params and slippage=mid
        cmd = ["/root/.openclaw/workspace/projects/bitcoin_trader_llm/venv/bin/python", "/root/.openclaw/workspace/projects/bitcoin_trader_llm/scripts/backtest_dce.py", "--months", "6", "--mode", "regime", "--tp-pct", "0.015", "--vol-entry-mult", str(vol), "--vol-mult", "2.0", "--stop-rel", "2.0", "--risk-pct", "1.0", "--sl-pct", "0.008", "--disable-time-exit", "--gatekeeper-only", "--adx-threshold", str(adx), "--htf-require", str(htf), "--slippage", str(mid)]
        if upper is not None:
            cmd += ["--upper-wick-pct", str(upper)]
        # execute
        print('Running break-even test', 'slip=', mid)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        # locate latest json from logs
        out = proc.stdout + proc.stderr
        # find backtest_YYYY.. in stdout prints
        import re
        m = re.search(r'Details written to (.*backtest_\d+_\d+\.json)', out)
        if m:
            jpath = m.group(1).strip()
        else:
            # fallback: find latest matching file in logs
            jfiles = sorted(LOG_DIR.glob('backtest_*.json'))
            jpath = str(jfiles[-1])
        # load and compute pf
        with open(jpath, 'r', encoding='utf-8') as jf:
            jd = json.load(jf)
        pf_val = jd.get('result',{}).get('profit_factor')
        if pf_val is None:
            # treat as fail
            pf_val = 0.0
        if pf_val >= 1.0:
            be = mid
            low = mid
        else:
            high = mid
    break_even.append({'file': c['file'], 'params': c['params'], 'break_even_slippage': be})

print('\nBREAK_EVEN')
for b in break_even:
    print(b['file'], b['params'], 'break_even_slippage=', b['break_even_slippage'])

# print finishing note
print('\nANALYSIS_DONE')
