#!/usr/bin/env python3
import subprocess, json, time, os
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / 'logs'
LOGS.mkdir(exist_ok=True)
venv_py = str(ROOT / 'venv' / 'bin' / 'python')
experiments = [
    {'label':'top1_baseline','mode':'dce','upper_wick':'0.5','risk':False},
    {'label':'top1_risk_only','mode':'dce','upper_wick':'0.5','risk':True},
    {'label':'top1_regime_only','mode':'regime','upper_wick':'0.5','risk':False},
    {'label':'top1_regime_risk','mode':'regime','upper_wick':'0.5','risk':True},
    {'label':'top2_baseline','mode':'dce','upper_wick':'0.4','risk':False},
    {'label':'top2_risk_only','mode':'dce','upper_wick':'0.4','risk':True},
    {'label':'top2_regime_only','mode':'regime','upper_wick':'0.4','risk':False},
    {'label':'top2_regime_risk','mode':'regime','upper_wick':'0.4','risk':True},
]
results = {}
for ex in experiments:
    label = ex['label']
    mode = ex['mode']
    upper = ex['upper_wick']
    risk = ex['risk']
    logfile = LOGS / f'ab_{label}.log'
    cmd = [venv_py, 'scripts/backtest_dce.py', '--months','9','--mode',mode,'--vol-entry-mult','1.5','--vol-mult','2.0','--adx-threshold','30','--htf-require','both','--upper-wick-pct',upper]
    if risk:
        cmd += ['--enable-risk-engine','--risk-daily-loss-pct','1.0','--risk-max-dd-pct','10.0','--risk-consec-losses','3','--risk-consec-mult','0.5','--risk-pct','1.0']
    print('Running', label, 'cmd=', ' '.join(cmd))
    with open(logfile, 'wb') as f:
        proc = subprocess.Popen(cmd, cwd=str(ROOT), stdout=f, stderr=subprocess.STDOUT)
        try:
            ret = proc.wait(timeout=900)
        except subprocess.TimeoutExpired:
            proc.kill()
            f.write(b"\nPROCESS TIMEOUT\n")
            results[label] = {'error':'timeout','log':str(logfile)}
            continue
    # find newest backtest JSON
    time.sleep(0.5)
    candidates = sorted(LOGS.glob('backtest_*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        results[label] = {'error':'no_json','log':str(logfile)}
        continue
    latest = candidates[0]
    # verify params
    try:
        content = json.loads(latest.read_text(encoding='utf-8'))
        params = content.get('result',{}).get('params',{})
        uw = params.get('upper_wick_pct')
        # convert types
        uw_str = (str(uw) if uw is not None else 'None')
        results[label] = {'json':str(latest),'params':params,'log':str(logfile)}
    except Exception as e:
        results[label] = {'error':'json_load_failed','exc':str(e),'file':str(latest),'log':str(logfile)}
    # small pause
    time.sleep(1)
# write manifest
MAN = LOGS / 'ab_experiments_manifest.json'
MAN.write_text(json.dumps(results, indent=2, ensure_ascii=False))
print('Finished experiments. Manifest written to', MAN)
