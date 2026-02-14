#!/usr/bin/env python3
"""
check_env_and_connect.py

Quick environment check for bitcoin_trader_llm:
- loads app.config
- checks python packages availability
- attempts DB connection (pymysql) if DB_HOST is set, otherwise uses sqlite
- tries a public Upbit endpoint (get_current_price)
- if UPBIT_ACCESS_KEY and UPBIT_SECRET_KEY present, tries authenticated call get_balances() but will NOT print balances

Prints only high-level status messages (no secrets, no balances).
"""
from __future__ import annotations
import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import traceback

result = {
    'python_version': sys.version.split('\n')[0],
    'project': str(PROJECT_ROOT),
    'packages': {},
    'db': {},
    'upbit': {},
}

try:
    import app.config as config
    result['config_summary'] = config.summary()
except Exception as e:
    result['config_error'] = str(e)

# check packages
reqs = ['pymysql', 'pyupbit', 'python-dotenv']
for r in reqs:
    try:
        __import__(r)
        result['packages'][r] = 'ok'
    except Exception as e:
        result['packages'][r] = f'ERROR: {e}'

# DB connection test
try:
    if config.DB_HOST:
        # try pymysql
        import pymysql
        try:
            conn = pymysql.connect(host=config.DB_HOST, port=config.DB_PORT or 3306, user=config.DB_USER, password=config.DB_PASSWORD or '', database=config.DB_NAME or '', connect_timeout=5)
            cur = conn.cursor()
            cur.execute('SELECT 1')
            _ = cur.fetchone()
            cur.close()
            conn.close()
            result['db']['type'] = 'mysql'
            result['db']['status'] = 'ok'
        except Exception as e:
            result['db']['type'] = 'mysql'
            result['db']['status'] = 'ERROR'
            result['db']['error'] = str(e)
    else:
        # try sqlite file in project
        import sqlite3
        dbfile = PROJECT_ROOT / 'bitcoin_trader_llm.sqlite'
        if dbfile.exists():
            try:
                conn = sqlite3.connect(str(dbfile))
                cur = conn.cursor()
                cur.execute('SELECT count(*) FROM sqlite_master')
                _ = cur.fetchone()
                cur.close()
                conn.close()
                result['db']['type'] = 'sqlite'
                result['db']['status'] = 'ok'
                result['db']['path'] = str(dbfile)
            except Exception as e:
                result['db']['type'] = 'sqlite'
                result['db']['status'] = 'ERROR'
                result['db']['error'] = str(e)
        else:
            result['db']['status'] = 'no-db-config'
except Exception as e:
    result['db']['status'] = 'ERROR'
    result['db']['error'] = str(e)

# Upbit test
try:
    import pyupbit
    # public endpoint test
    try:
        price = pyupbit.get_current_price('KRW-BTC')
        if price is None:
            result['upbit']['public_status'] = 'ERROR: No data'
        else:
            result['upbit']['public_status'] = f'ok (KRW-BTC price fetched)'
    except Exception as e:
        result['upbit']['public_status'] = f'ERROR: {e}'
    # private endpoint test if keys exist
    ak = os.getenv('UPBIT_ACCESS_KEY')
    sk = os.getenv('UPBIT_SECRET_KEY')
    if ak and sk:
        try:
            up = pyupbit.Upbit(ak, sk)
            # call get_balances but do not print sensitive data; only count entries
            bals = up.get_balances()
            if isinstance(bals, list):
                result['upbit']['private_status'] = f'ok (balances count={len(bals)})'
            else:
                result['upbit']['private_status'] = f'ERROR or unexpected response: {type(bals)}'
        except Exception as e:
            result['upbit']['private_status'] = f'ERROR: {e}'
    else:
        result['upbit']['private_status'] = 'no-keys'
except Exception as e:
    result['upbit'] = {'error': str(e)}

# print a concise summary
print('CHECK_SUMMARY')
for k, v in result.items():
    if k in ('packages', 'db', 'upbit'):
        print(f'-- {k.upper()} --')
        for kk, vv in v.items():
            print(f'{kk}: {vv}')
    else:
        print(f'{k}: {v}')

# exit code
import json
# write detailed json to a file for inspection
outpath = PROJECT_ROOT / 'logs' / 'env_check.json'
try:
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    print(f'DETAILS_WRITTEN: {outpath}')
except Exception:
    pass
