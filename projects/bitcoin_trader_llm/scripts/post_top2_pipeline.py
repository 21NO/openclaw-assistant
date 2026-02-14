#!/usr/bin/env python3
"""
Post-Top2 pipeline orchestrator.
Runs after analyze_final_no_retest generates top2_candidates.json.
Steps (for Top2 only):
  A) break-even slippage binary search -> logs/breakeven_top2.csv + .json
  B) retest Top2 with months=9 -> logs/retest_top2_months9_summary.csv + .json
  C) generate paper plan and start_paper.sh + systemd unit templates -> logs/paper_plan.md + logs/paper_config.json

Sends a single Discord notification (if DISCORD_WEBHOOK_URL env var present) summarizing created files.

Notes:
- Uses only upper_wick values present in Top2 (OFF/0.4/0.5). NEVER tries 0.3/0.2.
- Runs in foreground; caller should launch it in background if desired (nohup &).
"""
from __future__ import annotations
import time
import os
import json
import csv
import subprocess
import re
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / 'logs'
SCRIPTS = ROOT / 'scripts'
VENV_PY = ROOT / 'venv' / 'bin' / 'python'
BACKTEST_SCRIPT = SCRIPTS / 'backtest_dce.py'
TOP2_PATH = LOGS / 'top2_candidates.json'
BREAKEVEN_CSV = LOGS / 'breakeven_top2.csv'
BREAKEVEN_JSON = LOGS / 'breakeven_top2.json'
RETEST_CSV = LOGS / 'retest_top2_months9_summary.csv'
RETEST_JSON = LOGS / 'retest_top2_months9_summary.json'
PAPER_PLAN_MD = LOGS / 'paper_plan.md'
PAPER_CONFIG = LOGS / 'paper_config.json'
START_PAPER_SH = SCRIPTS / 'start_paper.sh'
SYSTEMD_DIR = ROOT / 'systemd'
POST_LOG = LOGS / 'post_top2_pipeline.log'

# Parameters
BREAKEVEN_ITERS = 10
BREAKEVEN_HIGH = 0.02
BREAKEVEN_LOW = 0.0
RETEST_MONTHS = int(os.environ.get('RETEST_MONTHS', '9'))  # default 9 months

# Utility functions

def log(msg: str):
    ts = datetime.utcnow().isoformat()
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(POST_LOG, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def wait_for_top2(timeout: int = 0):
    """Wait until TOP2_PATH exists and contains at least 1 candidate. If timeout==0 wait indefinitely."""
    log(f"Waiting for top2 file: {TOP2_PATH}")
    start = time.time()
    while True:
        if TOP2_PATH.exists():
            try:
                j = json.loads(TOP2_PATH.read_text(encoding='utf-8'))
                if isinstance(j, dict) and j.get('top2_candidates'):
                    if len(j.get('top2_candidates')) >= 1:
                        log(f"Found top2 file with {len(j.get('top2_candidates'))} candidates")
                        return j.get('top2_candidates')
            except Exception as e:
                log(f"Failed to parse top2 json: {e}")
        if timeout > 0 and (time.time() - start) > timeout:
            raise TimeoutError('Waiting for top2 timed out')
        time.sleep(5)


def run_backtest_and_get_json(params: dict, months: int, slippage: float, upper_wick_arg):
    """Run backtest_dce.py with given params and return path to result JSON (string)."""
    cmd = [str(VENV_PY), str(BACKTEST_SCRIPT),
           '--months', str(months), '--mode', 'regime', '--tp-pct', '0.015',
           '--vol-entry-mult', str(params['vol']), '--vol-mult', '2.0', '--stop-rel', '2.0',
           '--risk-pct', '1.0', '--sl-pct', '0.008', '--disable-time-exit', '--gatekeeper-only',
           '--adx-threshold', str(params['adx']), '--htf-require', str(params['htf']), '--slippage', str(slippage)]
    if upper_wick_arg is not None:
        cmd += ['--upper-wick-pct', str(upper_wick_arg)]
    log('Running backtest: ' + ' '.join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out = (proc.stdout or '') + '\n' + (proc.stderr or '')
    # try to find 'Details written to <path>' in stdout
    m = re.search(r'Details written to (.*backtest_\d+_\d+\.json)', out)
    if m:
        jpath = m.group(1).strip()
        jpath = jpath if jpath.startswith('/') else str(LOGS / jpath)
        if Path(jpath).exists():
            return jpath
    # fallback: find latest backtest_*.json in logs and return the newest
    candidates = sorted(LOGS.glob('backtest_*.json'), key=lambda p: p.stat().st_mtime)
    if candidates:
        return str(candidates[-1])
    # nothing
    raise RuntimeError('Backtest did not produce an output JSON')


def compute_breakeven_for_candidate(candidate: dict) -> dict:
    """Binary search break-even slippage for one candidate. Returns dict with result."""
    params = {
        'vol': candidate.get('vol'),
        'adx': candidate.get('adx'),
        'htf': candidate.get('htf')
    }
    upper = candidate.get('upper_wick')
    # normalize upper: 'OFF' -> None, otherwise numeric
    if isinstance(upper, str) and upper.upper() == 'OFF':
        upper_arg = None
    else:
        try:
            upper_arg = float(upper)
        except Exception:
            upper_arg = None

    low = BREAKEVEN_LOW
    high = BREAKEVEN_HIGH
    be = None
    last_pf = None

    for i in range(BREAKEVEN_ITERS):
        mid = (low + high) / 2.0
        try:
            jpath = run_backtest_and_get_json(params, months=6, slippage=mid, upper_wick_arg=upper_arg)
            # load
            j = json.loads(Path(jpath).read_text(encoding='utf-8'))
            pf = j.get('result', {}).get('profit_factor')
            pf = float(pf) if pf is not None else 0.0
            last_pf = pf
            log(f"BREAKEVEN: candidate vol={params['vol']} adx={params['adx']} htf={params['htf']} upper={upper_arg} mid={mid} pf={pf}")
            if pf >= 1.0:
                be = mid
                low = mid
            else:
                high = mid
        except Exception as e:
            log(f"BREAKEVEN run failed: {e}")
            # on failure, treat as non-success -> move high
            high = mid
        time.sleep(0.5)  # small gap to avoid tight loop

    return {
        'vol': params['vol'], 'adx': params['adx'], 'htf': params['htf'], 'upper_wick': (None if upper_arg is None else upper_arg),
        'break_even_slippage': be, 'last_pf': last_pf
    }


def save_csv_and_json(records: list, csv_path: Path, json_path: Path, csv_fields: list):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as jf:
        json.dump(records, jf, indent=2, ensure_ascii=False)
    with open(csv_path, 'w', newline='', encoding='utf-8') as cf:
        writer = csv.DictWriter(cf, fieldnames=csv_fields)
        writer.writeheader()
        for r in records:
            # ensure all fields exist
            row = {k: r.get(k) for k in csv_fields}
            writer.writerow(row)


def retest_candidate_months(candidate: dict, months: int = RETEST_MONTHS) -> dict:
    params = {
        'vol': candidate.get('vol'),
        'adx': candidate.get('adx'),
        'htf': candidate.get('htf')
    }
    upper = candidate.get('upper_wick')
    if isinstance(upper, str) and upper.upper() == 'OFF':
        upper_arg = None
    else:
        try:
            upper_arg = float(upper)
        except Exception:
            upper_arg = None
    # run backtest once with months value
    try:
        jpath = run_backtest_and_get_json(params, months=months, slippage=0.001, upper_wick_arg=upper_arg)
        j = json.loads(Path(jpath).read_text(encoding='utf-8'))
        res = j.get('result') or {}
        trades = j.get('trades') or []
        trades_with_pnl = [t for t in trades if t.get('pnl') is not None]
        # compute block_count_wick
        gate_logs = j.get('gate_logs') or []
        block_count_wick = 0
        for g in gate_logs:
            uw = g.get('upper_wick_ok')
            passed = g.get('pass')
            if uw is False and not passed:
                block_count_wick += 1
        return {
            'vol': params['vol'], 'adx': params['adx'], 'htf': params['htf'], 'upper_wick': (None if upper_arg is None else upper_arg),
            'months': months,
            'overall_pf': res.get('profit_factor'),
            'num_trades': len(trades_with_pnl),
            'total_pnl': res.get('total_pnl'),
            'max_drawdown': res.get('max_drawdown'),
            'block_count_wick': block_count_wick,
            'json': jpath
        }
    except Exception as e:
        log(f"RETEST failed for {params} months={months}: {e}")
        return {
            'vol': params['vol'], 'adx': params['adx'], 'htf': params['htf'], 'upper_wick': (None if upper_arg is None else upper_arg),
            'months': months, 'error': str(e)
        }


def generate_paper_plan(top2_list: list):
    # write paper_config.json and paper_plan.md and start_paper.sh and systemd unit templates
    plan = {
        'top2': top2_list,
        'duration_days': 14,
        'mode': 'real-time-paper',
        'start_script': str(START_PAPER_SH)
    }
    with open(PAPER_CONFIG, 'w', encoding='utf-8') as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)

    md_lines = []
    md_lines.append('# Paper Plan')
    md_lines.append(f'Generated: {datetime.utcnow().isoformat()}')
    md_lines.append('')
    md_lines.append('Top2 candidates to run as 2-week real-time paper:')
    for i, c in enumerate(top2_list, start=1):
        md_lines.append(f"\n## Candidate #{i}")
        md_lines.append(json.dumps(c, ensure_ascii=False, indent=2))
    md_lines.append('\nStart script: ' + str(START_PAPER_SH))
    md_lines.append('\nSystemd unit template available in systemd/ directory. Enable with:')
    md_lines.append('\n  systemctl --user enable --now start_paper.timer')

    LOGS.mkdir(parents=True, exist_ok=True)
    with open(PAPER_PLAN_MD, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md_lines))

    # create start_paper.sh (placeholder)
    start_sh = f"""#!/usr/bin/env bash
# start_paper.sh - start 2-week live paper trading for Top2 candidates
WORKDIR=\"{ROOT}\"
cd "$WORKDIR"
PY="{VENV_PY}"
CONFIG=\"{PAPER_CONFIG}\"
# This script should launch the paper runner with the given config.
# Placeholder: user should implement scripts/paper_runner.py to perform real-time paper trading.
# Example: $PY scripts/paper_runner.py --config "$CONFIG" --daemonize

if [ -x "$PY" ]; then
  echo "Paper start placeholder: $PY scripts/paper_runner.py --config $CONFIG"
  # touch a file to indicate 'would-start'
  touch {LOGS}/paper_start_ready.flag
else
  echo "Python venv not found: $PY"
fi
"""
    START_PAPER_SH.write_text(start_sh, encoding='utf-8')
    START_PAPER_SH.chmod(0o755)

    # create systemd unit and timer templates
    SYSTEMD_DIR.mkdir(parents=True, exist_ok=True)
    service = f"""[Unit]\nDescription=Start 2-week paper trading (placeholder)\nAfter=network.target\n\n[Service]\nType=oneshot\nExecStart={START_PAPER_SH}\nWorkingDirectory={ROOT}\nUser=$USER\n"""
    timer = f"""[Unit]\nDescription=Timer to start 2-week paper trading\n\n[Timer]\nOnCalendar=*-*-* *:00:00\n# OnCalendar can be customized by the operator. This is a placeholder.\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n"""
    (SYSTEMD_DIR / 'start_paper.service').write_text(service, encoding='utf-8')
    (SYSTEMD_DIR / 'start_paper.timer').write_text(timer, encoding='utf-8')


def send_discord_notification(message: str):
    webhook = os.environ.get('DISCORD_WEBHOOK_URL')
    if not webhook:
        log('DISCORD_WEBHOOK_URL not set; skipping Discord notification')
        return False
    try:
        import requests
        payload = {'content': message}
        r = requests.post(webhook, json=payload, timeout=10)
        if r.status_code in (200,204):
            log('Discord notification sent')
            return True
        else:
            log(f'Discord webhook returned status {r.status_code}: {r.text}')
            return False
    except Exception as e:
        log(f'Failed to send Discord notification: {e}')
        return False


def main():
    try:
        top2 = wait_for_top2(timeout=0)
    except Exception as e:
        log(f'Error waiting for top2: {e}')
        return

    # normalize top2 entries into simpler dicts
    candidates = []
    for c in top2[:2]:
        # c may be full run dict; look for params
        params = c.get('params') if isinstance(c, dict) else None
        if params:
            vol = params.get('vol_entry_mult')
            adx = params.get('adx_threshold')
            htf = params.get('htf_require')
            upper = params.get('upper_wick_pct') if params.get('upper_wick_pct') is not None else c.get('upper_wick') if c.get('upper_wick') is not None else None
            # normalize upper to OFF if None
            upper_label = 'OFF' if upper is None else upper
            candidates.append({'vol': vol, 'adx': adx, 'htf': htf, 'upper_wick': upper_label})
        else:
            # fallback: try to read keys directly
            vol = c.get('vol')
            adx = c.get('adx')
            htf = c.get('htf')
            upper = c.get('upper_wick')
            upper_label = 'OFF' if upper is None else upper
            candidates.append({'vol': vol, 'adx': adx, 'htf': htf, 'upper_wick': upper_label})

    # A) Break-even for top2
    be_results = []
    for cand in candidates:
        log(f"Starting break-even for candidate: {cand}")
        r = compute_breakeven_for_candidate(cand)
        be_results.append(r)
        # small gap between candidates
        time.sleep(1)

    # save breakeven results
    be_csv_fields = ['vol', 'adx', 'htf', 'upper_wick', 'break_even_slippage', 'last_pf']
    save_csv_and_json(be_results, BREAKEVEN_CSV, BREAKEVEN_JSON, be_csv_fields)
    log(f"Break-even results written: {BREAKEVEN_CSV}, {BREAKEVEN_JSON}")

    # B) Retest top2 months=RETEST_MONTHS
    retest_results = []
    for cand in candidates:
        log(f"Starting retest for candidate (months={RETEST_MONTHS}): {cand}")
        rr = retest_candidate_months(cand, months=RETEST_MONTHS)
        retest_results.append(rr)
        time.sleep(1)

    # save retest results
    retest_fields = ['vol', 'adx', 'htf', 'upper_wick', 'months', 'overall_pf', 'num_trades', 'total_pnl', 'max_drawdown', 'block_count_wick', 'json', 'error']
    save_csv_and_json(retest_results, RETEST_CSV, RETEST_JSON, retest_fields)
    log(f"Retest results written: {RETEST_CSV}, {RETEST_JSON}")

    # C) Generate paper plan and start script
    generate_paper_plan(candidates)
    log(f"Paper plan and config written: {PAPER_PLAN_MD}, {PAPER_CONFIG}; start script: {START_PAPER_SH}; systemd templates in {SYSTEMD_DIR}")

    # Send one Discord message summarizing artifacts
    paths = [str(BREAKEVEN_CSV), str(BREAKEVEN_JSON), str(RETEST_CSV), str(RETEST_JSON), str(PAPER_PLAN_MD), str(PAPER_CONFIG), str(START_PAPER_SH)]
    msg = 'Post-Top2 pipeline completed. Artifacts:\n' + '\n'.join(paths)
    send_discord_notification(msg)


if __name__ == '__main__':
    main()
