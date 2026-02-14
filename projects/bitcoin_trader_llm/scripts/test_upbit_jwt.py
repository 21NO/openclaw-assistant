#!/usr/bin/env python3
import os
import sys
import json
import uuid
from datetime import datetime

# ensure project root on path
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# import config to load .env if present
try:
    from app import config
except Exception:
    pass

access = os.getenv('UPBIT_ACCESS_KEY') or os.getenv('UPBIT_OPEN_API_KEY')
secret = os.getenv('UPBIT_SECRET_KEY') or os.getenv('UPBIT_SECRET')

# If not present in environment, try reading project .env directly
if not access or not secret:
    # project .env path
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    if os.path.exists(env_path):
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' not in line:
                        continue
                    k, v = line.split('=', 1)
                    k = k.strip()
                    v = v.strip()
                    # strip optional surrounding quotes
                    if v.startswith('"') and v.endswith('"'):
                        v = v[1:-1]
                    if v.startswith("'") and v.endswith("'"):
                        v = v[1:-1]
                    if k == 'UPBIT_ACCESS_KEY' and not access:
                        access = v
                    if k == 'UPBIT_SECRET_KEY' and not secret:
                        secret = v
        except Exception:
            pass

if not access or not secret:
    print(json.dumps({'upbit': 'no_keys_found'}))
    sys.exit(0)

try:
    import jwt
    import requests
except Exception as e:
    print(json.dumps({'upbit_error': f'missing_libs: {e}'}))
    sys.exit(0)

payload = {
    'access_key': access,
    'nonce': str(uuid.uuid4()),
}

try:
    token = jwt.encode(payload, secret, algorithm='HS256')
    if isinstance(token, bytes):
        token = token.decode('utf-8')
    headers = {'Authorization': 'Bearer ' + token}
    url = 'https://api.upbit.com/v1/accounts'
    resp = requests.get(url, headers=headers, timeout=10)
    try:
        resp.raise_for_status()
    except Exception:
        # include status code and text but avoid echoing keys
        print(json.dumps({'upbit_error': 'http_error', 'status_code': resp.status_code, 'text': resp.text}))
        sys.exit(0)
    balances = resp.json()
    sanitized = []
    if isinstance(balances, list):
        for b in balances:
            sanitized.append({
                'currency': b.get('currency'),
                'balance': b.get('balance'),
                'locked': b.get('locked'),
                'avg_buy_price': b.get('avg_buy_price')
            })
    else:
        sanitized = balances
    print(json.dumps({'upbit_balances': sanitized}, ensure_ascii=False))
except Exception as e:
    print(json.dumps({'upbit_error': str(e)}))
