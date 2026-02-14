#!/usr/bin/env python3
import json
from pathlib import Path
import sys

LOG_DIR = Path(__file__).resolve().parents[1] / 'logs'
files = sorted(LOG_DIR.glob('backtest_*.json'))
summary = []
for f in files:
    try:
        j = json.load(open(f, 'r', encoding='utf-8'))
        r = j.get('result', {})
        params = r.get('params', {})
        # some files may include vol_mult etc in params
        vol = params.get('vol_mult') or r.get('params', {}).get('vol_mult') or (j.get('result',{}).get('params') or {}).get('vol_mult')
        stop = params.get('stop_rel') or params.get('stop_rel_default') or (j.get('result',{}).get('params') or {}).get('stop_rel')
        risk = params.get('risk_pct') or (j.get('result',{}).get('params') or {}).get('risk_pct')
        summary.append({
            'file': str(f.name),
            'start': r.get('start'),
            'end': r.get('end'),
            'vol_mult': vol,
            'stop_rel': stop,
            'risk_pct': risk,
            'final_nav': r.get('final_nav'),
            'total_pnl': r.get('total_pnl'),
            'num_trades': r.get('num_trades'),
            'win_rate': r.get('win_rate'),
            'profit_factor': r.get('profit_factor'),
            'max_drawdown': r.get('max_drawdown')
        })
    except Exception as e:
        print('err reading', f, e, file=sys.stderr)

# sort by profit_factor desc, then total_pnl desc
summary_sorted = sorted(summary, key=lambda x: ((x['profit_factor'] if x['profit_factor'] is not None else -9999), x['total_pnl'] if x['total_pnl'] is not None else -999999999), reverse=True)
print('file,vol_mult,stop_rel,risk_pct,final_nav,total_pnl,num_trades,win_rate,profit_factor,max_drawdown')
for s in summary_sorted:
    print(f"{s['file']},{s['vol_mult']},{s['stop_rel']},{s['risk_pct']},{s['final_nav']},{s['total_pnl']},{s['num_trades']},{s['win_rate']},{s['profit_factor']},{s['max_drawdown']}")

# also print top 3 in readable JSON
print('\nTOP3:')
import pprint
pprint.pprint(summary_sorted[:3])
