#!/usr/bin/env python3
"""
Auto-apply Top1 candidate when post-Top2 pipeline finishes.
- Waits for breakeven_top2.json and retest_top2_months9_summary.json (or for post pipeline to finish)
- Decides Top1 using heuristics (prefer highest break-even slippage, then retest overall_pf, then months=6 overall_pf)
- Persists Top1 as active strategy via DBLogger.record_strategy_version(...)
- Writes deploy artifacts (config/top1_candidate.json, docs, start script placeholder) and commits them to a local git branch deploy/top1-<ts>
- Does NOT enable or start any systemd/service or perform real-money trading.
- Attempts to send Discord notification if DISCORD_WEBHOOK_URL is set in the environment.
"""
from __future__ import annotations
import time
import json
import os
import subprocess
from pathlib import Path
from datetime import datetime
from pprint import pformat

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / 'logs'
SCRIPTS = ROOT / 'scripts'
CONFIG_DIR = ROOT / 'config'
DOCS_DIR = ROOT / 'docs'
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

BREAKEVEN_JSON = LOGS / 'breakeven_top2.json'
RETEST_JSON = LOGS / 'retest_top2_months9_summary.json'
TOP2_JSON = LOGS / 'top2_candidates.json'
PAPER_PLAN = LOGS / 'paper_plan.md'
FINAL_REPORT = LOGS / 'pipeline_final_report.md'

WAIT_TIMEOUT = int(os.environ.get('AUTO_APPLY_TIMEOUT_SECS', str(60 * 60 * 2)))  # 2h default


def wait_for_pipeline_completion(timeout=WAIT_TIMEOUT):
    start = time.time()
    print(f"[auto_apply] waiting for breakeven and retest outputs (timeout={timeout}s)")
    while True:
        # prefer to wait for breakeven JSON; if both JSONs present, proceed
        if BREAKEVEN_JSON.exists() and RETEST_JSON.exists():
            print('[auto_apply] found breakeven + retest JSON')
            return True
        # fallback: if post_top2_pipeline is no longer running and BREAKEVEN_JSON exists, proceed
        # check for post_top2_pipeline process
        try:
            procs = subprocess.check_output(['pgrep', '-f', 'post_top2_pipeline.py']).decode().strip().split() if subprocess.call(['pgrep', '-f', 'post_top2_pipeline.py']) == 0 else []
        except Exception:
            procs = []
        if not procs and BREAKEVEN_JSON.exists():
            print('[auto_apply] post_top2_pipeline not running but breakeven found; proceeding')
            return True
        if not procs and (BREAKEVEN_JSON.exists() or RETEST_JSON.exists() or PAPER_PLAN.exists()):
            print('[auto_apply] pipeline not running and at least one artifact exists; proceeding')
            return True
        if time.time() - start > timeout:
            print('[auto_apply] timeout waiting for pipeline outputs')
            return False
        time.sleep(5)


def load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception as e:
        print(f"[auto_apply] failed to read JSON {path}: {e}")
        return None


def find_matching_breakeven(candidate, be_list):
    # match by vol/adx/htf/upper_wick
    for b in be_list:
        try:
            # normalize types
            if float(b.get('vol') or -999) == float(candidate.get('vol') or -999) and float(b.get('adx') or -999) == float(candidate.get('adx') or -999) and str(b.get('htf') or '').lower() == str(candidate.get('htf') or '').lower() and (str(b.get('upper_wick') or 'OFF') == str(candidate.get('upper_wick') or 'OFF')):
                return b
        except Exception:
            continue
    return None


def find_matching_retest(candidate, retest_list):
    for r in retest_list:
        try:
            if float(r.get('vol') or -999) == float(candidate.get('vol') or -999) and float(r.get('adx') or -999) == float(candidate.get('adx') or -999) and str(r.get('htf') or '').lower() == str(candidate.get('htf') or '').lower() and (str(r.get('upper_wick') or 'OFF') == str(candidate.get('upper_wick') or 'OFF')):
                return r
        except Exception:
            continue
    return None


def choose_top1(top2_candidates, be_list, retest_list):
    # 1) prefer highest break_even_slippage (non-null)
    best = None
    best_be = -1.0
    for c in top2_candidates:
        b = find_matching_breakeven(c, be_list) if be_list else None
        if b:
            be = b.get('break_even_slippage')
            try:
                if be is not None:
                    bef = float(be)
                    if bef > best_be:
                        best_be = bef
                        best = c
            except Exception:
                continue
    if best:
        return best, 'break_even'

    # 2) else prefer highest retest overall_pf
    best = None
    best_pf = -1.0
    for c in top2_candidates:
        r = find_matching_retest(c, retest_list) if retest_list else None
        if r:
            pf = r.get('overall_pf')
            try:
                if pf is not None:
                    pff = float(pf)
                    if pff > best_pf:
                        best_pf = pff
                        best = c
            except Exception:
                continue
    if best:
        return best, 'retest_pf'

    # 3) fallback to months=6 overall_pf in top2_candidates order
    sorted_by_pf = sorted(top2_candidates, key=lambda x: (x.get('overall_pf') or 0.0), reverse=True)
    if sorted_by_pf:
        return sorted_by_pf[0], 'months6_pf'

    return None, 'none'


def persist_top1_to_db(candidate):
    # record strategy version via DBLogger
    try:
        # ensure project root on sys.path for imports
        import sys
        # ROOT is defined at module level
        try:
            if str(ROOT) not in sys.path:
                sys.path.insert(0, str(ROOT))
        except Exception:
            pass
        from app.db_logger import DBLogger
        db = DBLogger()
        # build params mapping
        params = {
            'vol_mult': float(candidate.get('vol')) if candidate.get('vol') is not None else None,
            'vol_entry_mult': float(candidate.get('vol')) if candidate.get('vol') is not None else None,
            'adx_threshold': float(candidate.get('adx')) if candidate.get('adx') is not None else None,
            'htf_require': candidate.get('htf'),
            'upper_wick_pct': (None if str(candidate.get('upper_wick')).upper() == 'OFF' else float(candidate.get('upper_wick')) if candidate.get('upper_wick') is not None else None),
            'mode': 'regime',
            'tp_pct': 0.015,
            'sl_pct': 0.008,
            'stop_rel': 2.0,
            'risk_pct': 1.0
        }
        # remove None keys
        params = {k: v for k, v in params.items() if v is not None}
        name = 'momentum_breakout_30m'
        rec = db.record_strategy_version(name, params, reason='selected_top1_by_assistant')
        print(f"[auto_apply] recorded strategy version id={rec} params={params}")
        return True
    except Exception as e:
        print(f"[auto_apply] failed to persist to DB: {e}")
        return False


def write_deploy_artifacts(candidate, selection_reason):
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    branch = f"deploy/top1-{ts}"
    cfg_file = CONFIG_DIR / f'top1_candidate_{ts}.json'
    doc_file = DOCS_DIR / f'deployment_manual_{ts}.md'
    diagram_file = DOCS_DIR / f'deployment_diagram_{ts}.mmd'

    # write candidate config
    try:
        cfg_file.write_text(json.dumps(candidate, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f"[auto_apply] wrote candidate config: {cfg_file}")
    except Exception as e:
        print(f"[auto_apply] failed to write config: {e}")

    # write simple manual template
    manual_lines = []
    manual_lines.append('# Deployment & Runbook (auto-generated)')
    manual_lines.append(f'Generated: {datetime.utcnow().isoformat()}')
    manual_lines.append('')
    manual_lines.append('Top1 candidate:')
    manual_lines.append('')
    manual_lines.append('```json')
    manual_lines.append(json.dumps(candidate, indent=2, ensure_ascii=False))
    manual_lines.append('```')
    manual_lines.append('')
    manual_lines.append('Selection reason: ' + selection_reason)
    manual_lines.append('')
    manual_lines.append('Steps to go-live (MANUAL approval required):')
    manual_lines.append('1) Review candidate and backtest/retest/breakeven artifacts in logs/ directory')
    manual_lines.append('2) Ensure DRY_RUN=true in .env for paper/monitoring phase')
    manual_lines.append('3) Enable and start systemd unit start_paper.service/timer to begin 2-week paper run OR run scripts/start_paper.sh')
    manual_lines.append('4) Monitor logs/paper_runner.log and llm_reflections for performance')
    manual_lines.append('5) After paper run, obtain metrics and decide to go-live')
    manual_text = '\n'.join(manual_lines)
    try:
        doc_file.write_text(manual_text, encoding='utf-8')
        print(f"[auto_apply] wrote manual: {doc_file}")
    except Exception as e:
        print(f"[auto_apply] failed to write manual: {e}")

    # mermaid diagram placeholder
    mermaid = []
    mermaid.append('flowchart LR')
    mermaid.append('  MarketData["Market Data\n(ohlcv/orderbook)"] --> Compute["Feature Compute\n(indicators)"]')
    mermaid.append('  Compute --> Decider["Agent Decider\n(D+C+E)"]')
    mermaid.append('  Decider --> Signals["Signals (DB)"]')
    mermaid.append('  Signals --> Executor["Executor (paper/live)\n(DRY_RUN gate)"]')
    mermaid.append('  Executor --> Reflections["Reflections & Metrics\n(DB)"]')
    mermaid.append('  Reflections --> Analysis["Backtest/Retest/Break-even Analysis"]')
    try:
        diagram_file.write_text('\n'.join(mermaid), encoding='utf-8')
        print(f"[auto_apply] wrote mermaid diagram: {diagram_file}")
    except Exception as e:
        print(f"[auto_apply] failed to write diagram: {e}")

    # create a small git branch and commit
    try:
        subprocess.run(['git', 'checkout', '-b', branch], cwd=str(ROOT), check=True)
        subprocess.run(['git', 'add', str(cfg_file), str(doc_file), str(diagram_file)], cwd=str(ROOT), check=True)
        subprocess.run(['git', 'commit', '-m', f"Deploy: top1 candidate auto-apply ({ts})"], cwd=str(ROOT), check=True)
        print(f"[auto_apply] created git branch and committed artifacts: {branch}")
    except Exception as e:
        print(f"[auto_apply] git operations failed (continuing): {e}")

    return cfg_file, doc_file, diagram_file, branch


def send_discord_notification(summary_text):
    webhook = os.environ.get('DISCORD_WEBHOOK_URL')
    if not webhook:
        print('[auto_apply] DISCORD_WEBHOOK_URL not set; skipping Discord notification')
        # write pending message for operator
        pending = LOGS / 'discord_pending_message.txt'
        pending.write_text(summary_text, encoding='utf-8')
        return False
    try:
        import requests
        payload = {'content': summary_text}
        r = requests.post(webhook, json=payload, timeout=10)
        if r.status_code in (200, 204):
            print('[auto_apply] Discord notification sent')
            return True
        else:
            print(f"[auto_apply] Discord webhook returned {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"[auto_apply] failed to send Discord notification: {e}")
        return False


def main():
    ok = wait_for_pipeline_completion()
    if not ok:
        print('[auto_apply] pipeline artifacts not available; exiting')
        return

    top2 = load_json(TOP2_JSON) or {}
    top2_list = top2.get('top2_candidates') if isinstance(top2, dict) else None
    if not top2_list:
        print('[auto_apply] top2_candidates.json missing or empty; exiting')
        return

    be_list = load_json(BREAKEVEN_JSON) or []
    retest_list = load_json(RETEST_JSON) or []

    candidate, reason = choose_top1(top2_list, be_list, retest_list)
    if not candidate:
        print('[auto_apply] could not determine top1 candidate; exiting')
        return

    print(f"[auto_apply] selected top1 candidate by {reason}: {candidate}")

    # persist to DB
    persisted = persist_top1_to_db(candidate)

    # write artifacts and commit
    cfg, doc, diagram, branch = write_deploy_artifacts(candidate, reason)

    # final summary
    summary_lines = []
    summary_lines.append('Top1 candidate auto-apply finished')
    summary_lines.append(f'Selected by: {reason}')
    summary_lines.append('Candidate:')
    summary_lines.append(pformat(candidate))
    summary_lines.append('Persisted to DB: ' + str(persisted))
    summary_lines.append('Artifacts:')
    summary_lines.append(str(cfg))
    summary_lines.append(str(doc))
    summary_lines.append(str(diagram))
    summary_lines.append('Git branch: ' + branch)
    summary_text = '\n'.join(summary_lines)

    # save final report
    try:
        FINAL_REPORT.write_text(summary_text, encoding='utf-8')
    except Exception:
        pass

    # send discord
    send_discord_notification(summary_text)


if __name__ == '__main__':
    main()
