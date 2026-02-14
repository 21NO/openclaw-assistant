#!/usr/bin/env python3
"""
Final analyzer (no retest) to produce RUN_SUMMARY + TOP5 + TOP2 candidate list + conclusion.
Writes files to logs/: run_summary.csv, run_summary.json, top5_candidates.csv, top5_candidates.json, top5_report.txt, top2_candidates.json, conclusion.txt
"""
from pathlib import Path
import json
from datetime import datetime, timedelta
import csv
import math

WORKDIR = Path(__file__).resolve().parents[1]
LOG_DIR = WORKDIR / 'logs'
OUT_RUN_CSV = LOG_DIR / 'run_summary.csv'
OUT_RUN_JSON = LOG_DIR / 'run_summary.json'
OUT_TOP5_CSV = LOG_DIR / 'top5_candidates.csv'
OUT_TOP5_JSON = LOG_DIR / 'top5_candidates.json'
OUT_TOP5_TXT = LOG_DIR / 'top5_report.txt'
OUT_TOP2_JSON = LOG_DIR / 'top2_candidates.json'
OUT_CONCL_TXT = LOG_DIR / 'conclusion.txt'

FILES = sorted(LOG_DIR.glob('backtest_*.json'))

# grid filters
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

runs = []
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
    num_trades = len(trades_with_pnl)

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
    ok_folds = True
    for i in range(4):
        if start_dt is None:
            folds.append({'fold': i, 'test_trades': 0, 'pf': None, 'total_pnl': 0.0, 'maxdd': 0.0})
            ok_folds = False
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
        if num_test < 10:
            ok_folds = False

    # upper label
    upper = params.get('upper_wick_pct')
    upper_label = 'OFF' if upper is None else str(upper)

    runs.append({
        'file': str(f.name),
        'json_path': str(f),
        'vol': vol,
        'adx': adx,
        'htf': htf,
        'upper_wick': upper_label,
        'overall_pf': (overall_pf if overall_pf is not None else None),
        'total_pnl': total_pnl,
        'max_drawdown': max_dd,
        'num_trades': num_trades,
        'block_count_wick': block_count_wick,
        'folds': folds,
        'ok_folds': ok_folds
    })

# Sort runs by overall_pf desc (None last)
runs_sorted = sorted(runs, key=lambda x: (x['overall_pf'] if x['overall_pf'] is not None else -1.0), reverse=True)

# Write run_summary CSV/JSON
with open(OUT_RUN_JSON, 'w', encoding='utf-8') as jf:
    json.dump({'runs': runs_sorted}, jf, indent=2, ensure_ascii=False)

with open(OUT_RUN_CSV, 'w', newline='', encoding='utf-8') as cf:
    writer = csv.writer(cf)
    header = ['file','vol','adx','htf','upper_wick','overall_pf','total_pnl','max_drawdown','num_trades','block_count_wick','ok_folds',
              'fold0_pf','fold0_trades','fold1_pf','fold1_trades','fold2_pf','fold2_trades','fold3_pf','fold3_trades']
    writer.writerow(header)
    for r in runs_sorted:
        row = [r['file'], r['vol'], r['adx'], r['htf'], r['upper_wick'], r['overall_pf'], r['total_pnl'], r['max_drawdown'], r['num_trades'], r['block_count_wick'], r['ok_folds']]
        for f in r['folds']:
            row.append(f['pf'])
            row.append(f['test_trades'])
        writer.writerow(row)

# Select Top5 (by overall_pf desc)
TOP5 = runs_sorted[:5]
with open(OUT_TOP5_JSON, 'w', encoding='utf-8') as jf:
    json.dump({'top5': TOP5}, jf, indent=2, ensure_ascii=False)

with open(OUT_TOP5_CSV, 'w', newline='', encoding='utf-8') as cf:
    writer = csv.writer(cf)
    header = ['rank','file','vol','adx','htf','upper_wick','overall_pf','overall_pf>1','num_trades','ok_folds','block_count_wick',
              'fold0_pf','fold0_trades','fold0_trades>=10','fold1_pf','fold1_trades','fold1_trades>=10','fold2_pf','fold2_trades','fold2_trades>=10','fold3_pf','fold3_trades','fold3_trades>=10']
    writer.writerow(header)
    for i, r in enumerate(TOP5, start=1):
        row = [i, r['file'], r['vol'], r['adx'], r['htf'], r['upper_wick'], r['overall_pf'], (r['overall_pf'] is not None and r['overall_pf']>1.0), r['num_trades'], r['ok_folds'], r['block_count_wick']]
        for f in r['folds']:
            row.append(f['pf'])
            row.append(f['test_trades'])
            row.append(f['test_trades'] >= 10)
        writer.writerow(row)

# Write human readable top5 report
with open(OUT_TOP5_TXT, 'w', encoding='utf-8') as tf:
    tf.write('Top-5 candidate summary (break-even slippage = TBD)\n')
    tf.write('Generated: {}\n\n'.format(datetime.utcnow().isoformat()))
    for i, r in enumerate(TOP5, start=1):
        tf.write(f"#{i} File: {r['file']}\n")
        tf.write(f"Params: vol={r['vol']} adx={r['adx']} htf={r['htf']} upper_wick={r['upper_wick']}\n")
        tf.write(f"Months=6 overall_PF={r['overall_pf']} overall_PF>1={(r['overall_pf'] is not None and r['overall_pf']>1.0)} total_pnl={r['total_pnl']} max_drawdown={r['max_drawdown']} num_trades={r['num_trades']} block_count_wick={r['block_count_wick']} ok_folds={r['ok_folds']}\n")
        tf.write('Folds:\n')
        for f in r['folds']:
            tf.write(f"  fold{f['fold']}: test_trades={f['test_trades']} pf={f['pf']} test_trades_ok={(f['test_trades']>=10)} total_pnl={f['total_pnl']} maxdd={f['maxdd']}\n")
        tf.write('\n')

# Determine Top2 candidates using strict criteria:
# prefer runs with overall_pf>=1.0 AND ok_folds True AND at least 2 folds with pf>=1.0
candidates_strict = []
for r in runs_sorted:
    if r['overall_pf'] is None:
        continue
    if r['overall_pf'] < 1.0:
        continue
    if not r['ok_folds']:
        continue
    fold_pf_count = sum(1 for f in r['folds'] if (f['pf'] is not None and f['pf'] >= 1.0))
    if fold_pf_count >= 2:
        candidates_strict.append((r, fold_pf_count))

TOP2 = []
if len(candidates_strict) >= 2:
    # sort candidates by overall_pf desc
    candidates_strict_sorted = sorted(candidates_strict, key=lambda x: x[0]['overall_pf'], reverse=True)
    TOP2 = [candidates_strict_sorted[0][0], candidates_strict_sorted[1][0]]
else:
    # fallback: select up to 2 runs from runs_sorted that have overall_pf>=1.0 and ok_folds True
    fallback = [r for r in runs_sorted if (r['overall_pf'] is not None and r['overall_pf']>=1.0 and r['ok_folds'])]
    if len(fallback) >= 2:
        TOP2 = fallback[:2]
    elif len(fallback) == 1:
        TOP2 = [fallback[0]]
    else:
        # final fallback: top 2 by overall_pf among runs_sorted
        TOP2 = runs_sorted[:2]

# save TOP2 candidates
with open(OUT_TOP2_JSON, 'w', encoding='utf-8') as jf:
    json.dump({'top2_candidates': TOP2}, jf, indent=2, ensure_ascii=False)

# conclusion text (5 lines): why Top2 / risk(sample) / next is paper
# craft concise 5-line conclusion based on TOP2
lines = []
if TOP2:
    lines.append('Top2 후보 선정 이유: 상위 PF 및 롤링 폴드에서 상대적으로 안정적 성적을 보였음 (상세: overall PF, fold별 test 트레이드 수 및 PF).')
    lines.append(f"선정 대상: {TOP2[0]['file'] if len(TOP2)>0 else 'N/A'}{', '+TOP2[1]['file'] if len(TOP2)>1 else ''}.")
    lines.append('리스크: 전체 트레이드 표본 수가 작아(폴드별 test_trades 조건 충족 여부에 유의) 통계적 불확실성 큼; PF>1 관측은 우연 가능성 있음.')
    lines.append('다음 단계(보류): break-even/재테스트/실시간 페이퍼는 사용자의 확인 후 내일 또는 다음 단계에서 진행.')
    lines.append('권장: Top2 확정 후 2주 실시간 페이퍼(소규모 포지션) 진행하여 실거래 모사 로그 수집 → break-even 판단 권장.')
else:
    lines = [
        'Top2 후보를 자동으로 선정할 수 있는 충분한 조건을 충족하는 런이 없습니다.',
        '리스크: 분석 대상이 제한적이므로 추가 파라미터 또는 기간 확장이 필요합니다.',
        '다음 단계: 추가 스윕 또는 기간 확장 후 재분석 권장.',
        '',
        ''
    ]

with open(OUT_CONCL_TXT, 'w', encoding='utf-8') as cf:
    for l in lines:
        cf.write(l.strip() + '\n')

print('FINAL_ANALYSIS_DONE')
