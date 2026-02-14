#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime, timezone

# ensure project root on path
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from app import config
from app.db_logger import DBLogger


def safe_print(obj):
    try:
        print(json.dumps(obj, ensure_ascii=False))
    except Exception:
        print(str(obj))


def test_insert():
    db = DBLogger()
    now = datetime.now(timezone.utc)
    run_id = f"test_run_{now.strftime('%Y%m%dT%H%M%SZ')}"
    payload = {
        'run_id': run_id,
        'symbol': config.SYMBOL,
        'features': {'ma_short': 100.1, 'ma_long': 105.6},
        'recent_ohlcv': [],
        'news': [],
        'balances': {},
        'strategy': {'name': 'test_strategy', 'params': {}},
        'meta': {'timestamp': now.isoformat()}
    }
    try:
        req_id = db.insert_decision_request(run_id=run_id, payload=payload)
        safe_print({'insert_decision_request_id': req_id})
    except Exception as e:
        safe_print({'insert_failed': str(e)})


def test_upbit():
    try:
        import pyupbit
    except Exception:
        safe_print({'upbit': 'pyupbit_not_installed'})
        return

    # Read keys from environment (loaded by app.config earlier)
    access = os.getenv('UPBIT_ACCESS_KEY') or os.getenv('UPBIT_OPEN_API_KEY')
    secret = os.getenv('UPBIT_SECRET_KEY') or os.getenv('UPBIT_SECRET')
    if not access or not secret:
        safe_print({'upbit': 'no_keys_found'})
        return

    try:
        upbit = pyupbit.Upbit(access, secret)
        balances = upbit.get_balances()
        sanitized = []
        if isinstance(balances, list):
            for b in balances:
                # sanitized fields only
                sanitized.append({
                    'currency': b.get('currency'),
                    'balance': b.get('balance'),
                    'locked': b.get('locked'),
                    'avg_buy_price': b.get('avg_buy_price')
                })
        else:
            sanitized = balances
        safe_print({'upbit_balances': sanitized})
    except Exception as e:
        safe_print({'upbit_error': str(e)})


if __name__ == '__main__':
    test_insert()
    test_upbit()
