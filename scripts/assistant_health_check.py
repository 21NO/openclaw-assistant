#!/usr/bin/env python3
"""
assistant_health_check.py

Improved human-friendly output version.

Lightweight health-check script to determine why the assistant/service may be
unresponsive: distinguishes gateway/process load vs token-budget exhaustion vs
DB/queue backlog.

This script produces a compact, colored (if TTY) human summary and a JSON
result. It is non-destructive.

Usage examples:
  ./scripts/assistant_health_check.py --env-file /root/.openclaw/workspace/projects/bitcoin_trader_llm/.env --max-daily-tokens 100000

"""

import os
import sys
import argparse
import subprocess
import socket
import tempfile
import stat
import shlex
import json
import time
import datetime
import shutil
import re
import textwrap

# defaults
DEFAULT_ENV_FILES = [
    '/root/.openclaw/workspace/projects/bitcoin_trader_llm/.env',
    '/root/.openclaw/workspace/.env',
]
GATEWAY_PORT = 18789
EMBED_PORT = 9000
LANCEDB_PORT = 8001
MYSQL_PORT = 3306


def load_env_file(path):
    env = {}
    if not os.path.exists(path):
        return env
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip()
            # remove surrounding quotes
            if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
                v = v[1:-1]
            env[k] = v
    return env


def run_cmd(cmd, timeout=10, capture_output=True):
    try:
        if isinstance(cmd, str):
            args = shlex.split(cmd)
        else:
            args = cmd
        p = subprocess.run(args, stdout=subprocess.PIPE if capture_output else None,
                           stderr=subprocess.PIPE if capture_output else None,
                           timeout=timeout, text=True)
        return p.returncode, p.stdout if capture_output else None, p.stderr if capture_output else None
    except subprocess.TimeoutExpired:
        return 124, '', 'timeout'
    except FileNotFoundError as e:
        return 127, '', str(e)


def tcp_connect(host, port, timeout=2.0):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def systemctl_show(service):
    rc, out, err = run_cmd(['systemctl', '--user', 'show', service, '-p', 'ActiveState', '-p', 'SubState', '-p', 'MainPID', '-p', 'Environment'], timeout=5)
    if rc != 0:
        return None, out, err
    out = out.strip()
    # parse key=value lines
    data = {}
    for part in out.splitlines():
        if '=' in part:
            k, v = part.split('=', 1)
            data[k] = v
    return data, out, None


def parse_envstr(envstr):
    """Parse systemctl Environment string into dict. Handles quoted values."""
    if not envstr:
        return {}
    patt = re.findall(r"([A-Za-z_][A-Za-z0-9_]*)=(\"(?:[^\"\\]|\\.)*\"|'(?:[^\\'\\]|\\.)*'|[^ ]+)", envstr)
    d = {}
    for k, v in patt:
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]
        d[k] = v
    return d


def ps_cpu_mem(pid):
    try:
        pid = int(pid)
    except Exception:
        return None
    rc, out, err = run_cmd(['ps', '-p', str(pid), '-o', '%cpu,%mem,cmd', '--no-headers'], timeout=3)
    if rc != 0 or not out:
        return None
    out = out.strip()
    # split into cpu, mem, command
    m = re.match(r"\s*([0-9.]+)\s+([0-9.]+)\s+(.*)", out)
    if not m:
        return None
    return {'cpu_pct': float(m.group(1)), 'mem_pct': float(m.group(2)), 'cmd': m.group(3)}


def has_mysql_client():
    return shutil.which('mysql') is not None or shutil.which('mariadb') is not None


def mysql_exec_query(dbcfg, sql, db=None, timeout=10):
    """Execute SQL via mysql client using a temporary defaults file. Returns (rc, stdout, stderr)."""
    exe = shutil.which('mysql') or shutil.which('mariadb')
    if not exe:
        return 127, '', 'mysql client not found'
    # create temp defaults file
    with tempfile.NamedTemporaryFile('w', delete=False) as f:
        path = f.name
        f.write('[client]\n')
        if dbcfg.get('user'):
            f.write(f"user={dbcfg.get('user')}\n")
        if dbcfg.get('password'):
            f.write(f"password={dbcfg.get('password')}\n")
        if dbcfg.get('host'):
            f.write(f"host={dbcfg.get('host')}\n")
        if dbcfg.get('port'):
            f.write(f"port={dbcfg.get('port')}\n")
    os.chmod(path, 0o600)
    cmd = [exe, f"--defaults-extra-file={path}", '-N', '-B']
    if db:
        cmd += ['-D', db]
    cmd += ['-e', sql]
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        out = p.stdout or ''
        err = p.stderr or ''
        return p.returncode, out, err
    except subprocess.TimeoutExpired:
        return 124, '', 'timeout'
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


def query_token_table_summary(dbcfg):
    info = {'available': False}
    if not has_mysql_client():
        info['error'] = 'mysql client not available'
        return info
    # check table existence
    db = dbcfg.get('db') or dbcfg.get('database')
    if not db:
        info['error'] = 'no database provided'
        return info
    sql_exists = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='llm_token_usage';"
    rc, out, err = mysql_exec_query(dbcfg, sql_exists, db=db)
    if rc != 0:
        info['error'] = f'mysql error: {err.strip()}'
        return info
    try:
        exists = int(out.strip().splitlines()[0] or '0')
    except Exception:
        exists = 0
    info['available'] = bool(exists)
    if not exists:
        return info
    # tokens today
    sql_sum = "SELECT COALESCE(SUM(tokens),0) FROM llm_token_usage WHERE usage_date = CURDATE();"
    rc, out, err = mysql_exec_query(dbcfg, sql_sum, db=db)
    if rc == 0:
        try:
            tokens_today = int(out.strip().splitlines()[0] or '0')
        except Exception:
            tokens_today = None
    else:
        tokens_today = None
    info['tokens_today'] = tokens_today
    # last entries
    sql_last = "SELECT usage_date,component,tokens,note,created_at FROM llm_token_usage ORDER BY created_at DESC LIMIT 10;"
    rc, out, err = mysql_exec_query(dbcfg, sql_last, db=db)
    if rc == 0 and out:
        rows = []
        for line in out.strip().splitlines():
            parts = line.split('\t')
            rows.append(parts)
        info['recent'] = rows
    else:
        info['recent'] = []
    return info


def query_queue_counts(dbcfg):
    info = {'ok': False}
    if not has_mysql_client():
        info['error'] = 'mysql client not available'
        return info
    db = dbcfg.get('db') or dbcfg.get('database')
    if not db:
        info['error'] = 'no database provided'
        return info
    # awaiting decision requests
    sql1 = "SELECT COUNT(*) FROM llm_decision_requests WHERE status='awaiting';"
    rc, out, err = mysql_exec_query(dbcfg, sql1, db=db)
    if rc == 0:
        try:
            info['decision_requests_awaiting'] = int(out.strip().splitlines()[0] or '0')
        except Exception:
            info['decision_requests_awaiting'] = None
    else:
        info['decision_requests_awaiting'] = None
    # pending signals
    sql2 = "SELECT COUNT(*) FROM llm_signals WHERE status='pending';"
    rc, out, err = mysql_exec_query(dbcfg, sql2, db=db)
    if rc == 0:
        try:
            info['signals_pending'] = int(out.strip().splitlines()[0] or '0')
        except Exception:
            info['signals_pending'] = None
    else:
        info['signals_pending'] = None
    info['ok'] = True
    return info


def scan_logs_for_errors(log_dir, patterns=None):
    patterns = patterns or ['ExpiredAccessKey', 'error', 'exception']
    findings = []
    if not os.path.isdir(log_dir):
        return findings
    # search recent logs (last 10 files by mtime)
    try:
        files = sorted([os.path.join(log_dir, f) for f in os.listdir(log_dir)], key=lambda p: os.path.getmtime(p), reverse=True)[:10]
    except Exception:
        return findings
    for f in files:
        try:
            with open(f, 'r', encoding='utf-8', errors='ignore') as fh:
                tail = fh.read()[-20000:]
        except Exception:
            continue
        for pat in patterns:
            if pat in tail:
                # include a short snippet around first match
                idx = tail.find(pat)
                start = max(0, idx - 200)
                snippet = tail[start: start + 800]
                findings.append({'file': f, 'pattern': pat, 'snippet': snippet})
    return findings


def get_meminfo_kb():
    info = {}
    try:
        with open('/proc/meminfo', 'r') as fh:
            for line in fh:
                k, v = line.split(':', 1)
                v = v.strip().split()[0]
                info[k.strip()] = int(v)
    except Exception:
        return None
    # return relevant metrics in KB
    return {
        'MemTotal_kb': info.get('MemTotal'),
        'MemAvailable_kb': info.get('MemAvailable'),
        'SwapTotal_kb': info.get('SwapTotal'),
        'SwapFree_kb': info.get('SwapFree'),
    }


# printing helpers
CSI = '\x1b['
ENDC = CSI + '0m'
COLORS = {
    'green': CSI + '32m',
    'yellow': CSI + '33m',
    'red': CSI + '31m',
    'cyan': CSI + '36m',
    'bold': CSI + '1m',
}


def colorize(s, color, use_color=True):
    if not use_color:
        return s
    return COLORS.get(color, '') + s + ENDC


def print_section(title, use_color=True):
    print(colorize(f"== {title} ==", 'cyan', use_color))


def print_two_cols(k, v, width=40, use_color=True):
    left = f"{k}:".ljust(width)
    print(f"{colorize(left, 'bold', use_color)} {v}")


def human_summary(result, use_color=True):
    now = result.get('timestamp')
    print_section(f"Assistant health check — {now}", use_color)
    # Host / Gateway
    gw = result.get('gateway', {})
    active = gw.get('active')
    gw_line = 'running' if active else 'NOT RUNNING'
    gw_color = 'green' if active else 'red'
    print_two_cols('Gateway', colorize(gw_line, gw_color, use_color), use_color=use_color)
    if gw.get('mainpid'):
        print_two_cols('Gateway PID', gw.get('mainpid'), use_color=use_color)
    if gw.get('substate'):
        print_two_cols('Gateway substate', gw.get('substate'), use_color=use_color)

    # Ports
    print_section('Ports', use_color)
    ports = result.get('ports', {})
    for name, ok in ports.items():
        label = 'OK' if ok else 'NO'
        color = 'green' if ok else 'red'
        print(f"  {name.ljust(20)} {colorize(label, color, use_color)}")

    # Process / Load
    print_section('Process / Load', use_color)
    p = result.get('process', {}).get('openclaw_gateway')
    if p:
        print_two_cols('Gateway CPU%', f"{p.get('cpu_pct')}%", use_color=use_color)
        print_two_cols('Gateway MEM%', f"{p.get('mem_pct')}%", use_color=use_color)
        print_two_cols('Gateway CMD', p.get('cmd'), use_color=use_color)
    la = result.get('process', {}).get('loadavg')
    if la:
        print_two_cols('Load avg (1/5/15)', ', '.join([str(x) for x in la]), use_color=use_color)

    mem = result.get('system', {}).get('meminfo_kb')
    if mem:
        mt = mem.get('MemTotal_kb')
        ma = mem.get('MemAvailable_kb')
        st = mem.get('SwapTotal_kb')
        sf = mem.get('SwapFree_kb')
        if mt:
            used = mt - (ma or 0)
            used_pct = (used / mt) * 100
            print_two_cols('Memory used', f"{used//1024}MiB / {mt//1024}MiB ({used_pct:.0f}%)", use_color=use_color)
        if st is not None:
            swap_used = (st - (sf or 0))
            swap_pct = (swap_used / st * 100) if st else 0
            print_two_cols('Swap used', f"{swap_used//1024}MiB / {st//1024}MiB ({swap_pct:.0f}%)", use_color=use_color)

    # DB / Tokens
    print_section('Database / Tokens', use_color)
    t = result.get('db', {}).get('token_table', {})
    if 'error' in t:
        print_two_cols('Token table', f"error: {t.get('error')}", use_color=use_color)
    else:
        print_two_cols('Token table exists', str(t.get('available')) , use_color=use_color)
        print_two_cols('Tokens today', str(t.get('tokens_today')), use_color=use_color)
    tok_status = result.get('tokens', {}).get('status')
    if tok_status:
        col = 'red' if tok_status == 'EXCEEDED' else ('yellow' if tok_status == 'HIGH' else 'green')
        print_two_cols('Token status', colorize(tok_status, col, use_color), use_color=use_color)

    # Queues
    print_section('Queues', use_color)
    q = result.get('queues', {})
    if q.get('ok'):
        print_two_cols('DecisionRequests awaiting', str(q.get('decision_requests_awaiting')), use_color=use_color)
        print_two_cols('Signals pending', str(q.get('signals_pending')), use_color=use_color)
    else:
        print_two_cols('Queue check', f"error: {q.get('error')}", use_color=use_color)

    # Credentials
    print_section('Credentials & Flags', use_color)
    cred = result.get('credentials', {})
    print_two_cols('Upbit keys present', str(cred.get('upbit_keys_present')), use_color=use_color)
    print_two_cols('DRY_RUN', str(cred.get('drY_run')), use_color=use_color)

    # Logs
    print_section('Recent log findings', use_color)
    findings = result.get('logs', {}).get('recent_findings', [])
    if not findings:
        print('  None')
    else:
        for f in findings[:5]:
            print(f"  {os.path.basename(f.get('file'))}  pattern={f.get('pattern')}")
            snip = f.get('snippet') or ''
            for line in snip.splitlines()[:5]:
                print('    ' + line[:200])

    # Final summary / recommendations
    print_section('Summary / Recommendations', use_color)
    print('  ' + result.get('summary', 'No summary'))
    # quick recommendations
    recs = []
    if tok_status == 'EXCEEDED':
        recs.append('Token budget exceeded → rotate to agent-mode or replenish budget')
    elif tok_status == 'HIGH':
        recs.append('Token usage high → monitor and consider lowering model context or limits')
    if not result.get('ports', {}).get('gateway_18789'):
        recs.append('Gateway port not reachable → check gateway process or firewall')
    # port 2070 external bind warning
    if result.get('ports', {}).get('node_2070') is True:
        recs.append('Port 2070 is externally bound; verify if intentional')
    if recs:
        for r in recs:
            print('  - ' + r)
    print('\n')


def main():
    parser = argparse.ArgumentParser(description='Assistant health check (gateway/process vs token-budget vs queue)')
    parser.add_argument('--env-file', '-e', help='path to .env (used to load DB and keys)', default=None)
    parser.add_argument('--max-daily-tokens', '-m', type=int, help='token budget for today (optional)', default=None)
    parser.add_argument('--db-host', help='DB host override', default=None)
    parser.add_argument('--db-port', help='DB port override', default=None)
    parser.add_argument('--db-user', help='DB user override', default=None)
    parser.add_argument('--db-password', help='DB password override', default=None)
    parser.add_argument('--db-name', help='DB name override', default=None)
    parser.add_argument('--log-dir', help='project logs dir to scan', default='/root/.openclaw/workspace/projects/bitcoin_trader_llm/logs')
    parser.add_argument('--save-json', help='path to save JSON result', default=None)
    parser.add_argument('--no-json', action='store_true', help='do not print JSON blob')
    parser.add_argument('--test-upbit', action='store_true', help='(optional, dangerous) attempt to call Upbit get_balances to verify keys (only when pyupbit installed & keys present)')
    args = parser.parse_args()

    env = {}
    # load env file: priority: arg > default project files
    env_file = args.env_file
    if not env_file:
        for p in DEFAULT_ENV_FILES:
            if os.path.exists(p):
                env_file = p
                break
    if env_file and os.path.exists(env_file):
        env = load_env_file(env_file)
    # override with CLI DB args if provided
    dbcfg = {
        'host': args.db_host or env.get('DB_HOST') or '127.0.0.1',
        'port': args.db_port or env.get('DB_PORT') or str(MYSQL_PORT),
        'user': args.db_user or env.get('DB_USER') or None,
        'password': args.db_password or env.get('DB_PASSWORD') or None,
        'db': args.db_name or env.get('DB_NAME') or None,
    }

    result = {
        'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
        'gateway': {},
        'ports': {},
        'process': {},
        'db': {},
        'tokens': {},
        'queues': {},
        'logs': {},
        'credentials': {},
        'system': {},
        'summary': None,
    }

    # 1) systemctl show
    svc, raw, err = systemctl_show('openclaw-gateway.service')
    if svc is None:
        result['gateway']['error'] = f'systemctl show failed: {err or "unknown"}'
    else:
        result['gateway']['active'] = svc.get('ActiveState') == 'active'
        result['gateway']['substate'] = svc.get('SubState')
        result['gateway']['mainpid'] = svc.get('MainPID')
        # parse ENV string into dict
        envstr = svc.get('Environment', '')
        envpairs = parse_envstr(envstr)
        result['gateway']['env'] = {k: ('<redacted>' if 'KEY' in k or 'PASSWORD' in k or 'TOKEN' in k else v) for k, v in envpairs.items()}

    # 2) TCP ports
    result['ports']['gateway_18789'] = tcp_connect('127.0.0.1', GATEWAY_PORT, timeout=1)
    result['ports']['embed_9000'] = tcp_connect('127.0.0.1', EMBED_PORT, timeout=1)
    result['ports']['lancedb_8001'] = tcp_connect('127.0.0.1', LANCEDB_PORT, timeout=1)
    result['ports']['mysql_3306'] = tcp_connect(dbcfg.get('host') or '127.0.0.1', int(dbcfg.get('port') or MYSQL_PORT), timeout=1)

    # best-effort: detect if a node process binds 2070 (historical)
    try:
        # check ss for :2070 listener
        rc, out, err = run_cmd(['ss', '-ltnp'], timeout=2)
        if rc == 0 and out and ':2070' in out:
            result['ports']['node_2070'] = True
        else:
            result['ports']['node_2070'] = False
    except Exception:
        result['ports']['node_2070'] = False

    # 3) process stats for gateway
    mainpid = None
    try:
        mainpid = int(svc.get('MainPID') or 0)
    except Exception:
        mainpid = None
    if mainpid:
        pstat = ps_cpu_mem(mainpid)
        result['process']['openclaw_gateway'] = pstat
    else:
        result['process']['openclaw_gateway'] = None

    # 4) loadavg
    try:
        with open('/proc/loadavg', 'r') as f:
            la = f.read().strip().split()[:3]
            result['process']['loadavg'] = [float(x) for x in la]
    except Exception:
        result['process']['loadavg'] = None

    # 5) DB token summary
    token_info = query_token_table_summary(dbcfg)
    result['db']['token_table'] = token_info

    # 6) queue counts
    queue_info = query_queue_counts(dbcfg)
    result['queues'] = queue_info

    # 7) token budget evaluation
    max_tokens = args.max_daily_tokens or (int(env.get('MAX_DAILY_TOKENS')) if env.get('MAX_DAILY_TOKENS') else None)
    result['tokens']['budget'] = max_tokens
    tokens_today = token_info.get('tokens_today') if isinstance(token_info, dict) else None
    result['tokens']['today'] = tokens_today
    if max_tokens and tokens_today is not None:
        ratio = tokens_today / float(max_tokens) if max_tokens > 0 else None
        result['tokens']['ratio'] = ratio
        if ratio is not None:
            if ratio >= 1.0:
                result['tokens']['status'] = 'EXCEEDED'
            elif ratio >= 0.8:
                result['tokens']['status'] = 'HIGH'
            else:
                result['tokens']['status'] = 'OK'
    else:
        result['tokens']['status'] = 'UNKNOWN'

    # 8) UPBIT key presence (do NOT print keys)
    upbit_present = bool(env.get('UPBIT_ACCESS_KEY') and env.get('UPBIT_SECRET_KEY'))
    result['credentials'] = {'upbit_keys_present': upbit_present, 'drY_run': env.get('DRY_RUN')}

    # 9) logs
    findings = scan_logs_for_errors(args.log_dir, patterns=['ExpiredAccessKey', 'Traceback', 'ERROR', 'Exception'])
    result['logs']['recent_findings'] = findings

    # 10) memory info
    meminfo = get_meminfo_kb()
    result['system']['meminfo_kb'] = meminfo

    # 11) summary
    summary_lines = []
    if not result['gateway'].get('active'):
        summary_lines.append('openclaw-gateway is NOT active')
    else:
        summary_lines.append('openclaw-gateway running')
    if not result['ports']['gateway_18789']:
        summary_lines.append('gateway TCP port 18789 not reachable')
    if result['process'].get('openclaw_gateway'):
        p = result['process']['openclaw_gateway']
        summary_lines.append(f"gateway CPU={p['cpu_pct']}% MEM={p['mem_pct']}%")
    if result['tokens']['status'] == 'EXCEEDED':
        summary_lines.append('Token budget EXCEEDED for today')
    elif result['tokens']['status'] == 'HIGH':
        summary_lines.append('Token usage HIGH (>=80%)')
    # queue
    try:
        dr = int(result['queues'].get('decision_requests_awaiting') or 0)
        sigs = int(result['queues'].get('signals_pending') or 0)
        summary_lines.append(f'decision_requests.awaiting={dr}, signals.pending={sigs}')
    except Exception:
        pass

    if findings:
        summary_lines.append('recent log patterns found (see logs.recent_findings)')

    result['summary'] = ' ; '.join(summary_lines)

    # save JSON if requested
    if args.save_json:
        try:
            with open(args.save_json, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f'Failed to write JSON: {e}', file=sys.stderr)

    # print human friendly summary + JSON (unless suppressed)
    use_color = sys.stdout.isatty()
    human_summary(result, use_color=use_color)

    if not args.no_json:
        print('--- JSON result ---')
        print(json.dumps(result, indent=2, ensure_ascii=False))

    return 0


if __name__ == '__main__':
    rc = main()
    sys.exit(rc)
