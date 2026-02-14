"""
Configuration loader for bitcoin_trader_llm.
Loads environment variables from .env (if present) and exposes config constants.
"""
import os
from pathlib import Path

# try to import dotenv but continue if not available
try:
    from dotenv import load_dotenv
    _has_dotenv = True
except Exception:
    load_dotenv = None
    _has_dotenv = False

# Load .env from project root if present; if python-dotenv is missing, fall back to a minimal parser
ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / '.env'
if ENV_PATH.exists():
    if _has_dotenv:
        load_dotenv(dotenv_path=ENV_PATH)
    else:
        # minimal .env parser: KEY=VALUE pairs, strip quotes
        try:
            with open(ENV_PATH, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' not in line:
                        continue
                    k, v = line.split('=', 1)
                    k = k.strip()
                    v = v.strip()
                    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                        v = v[1:-1]
                    # only set env var if not already present to allow system env override
                    if k and os.getenv(k) is None:
                        os.environ[k] = v
        except Exception:
            # fallback: do nothing
            pass
else:
    # allow global envs
    if _has_dotenv:
        load_dotenv()

# Basic settings
SYMBOL = os.getenv('SYMBOL', 'KRW-BTC')
CANDLE_INTERVAL = os.getenv('CANDLE_INTERVAL', 'minutes30')  # pyupbit interval naming
LOOKBACK = int(os.getenv('LOOKBACK', '96'))  # number of candles to fetch (96*30min = 2 days)

# DRY_RUN and indexing defaults (from user approval)
DRY_RUN = os.getenv('DRY_RUN', 'true').lower() in ('1', 'true', 'yes')
INDEX_REFLECTIONS = os.getenv('INDEX_REFLECTIONS', 'true').lower() in ('1', 'true', 'yes')

# LLM / decision endpoint
# Default set to 'agent' to avoid external API calls. Modes:
#  - 'agent' : decisions are made by the OpenClaw agent (assistant) via polling/agent process (NO external API calls)
#  - 'local' : POST to a local LLM decision endpoint you operate (no external cloud)
#  - 'openai'|'mock' : external or local mock modes (not used by default)
LLM_MODE = os.getenv('LLM_MODE', 'agent')  # 'agent'|'local'|'openai'|'mock'
LLM_OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
LLM_ENDPOINT = os.getenv('LLM_DECISION_ENDPOINT', 'http://127.0.0.1:9000/decide')

# DB settings (prefer environment; fallback to sqlite)
DB_DRIVER = os.getenv('DB_DRIVER', 'pymysql')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT')) if os.getenv('DB_PORT') else None
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Risk & strategy defaults (attack profile -> aggressive defaults)
RISK_PER_TRADE_PCT = float(os.getenv('RISK_PER_TRADE_PCT', '3.0'))  # percent of equity risked per trade
MAX_DAILY_LOSS_PCT = float(os.getenv('MAX_DAILY_LOSS_PCT', '10.0'))
MAX_SINGLE_ORDER_PCT = float(os.getenv('MAX_SINGLE_ORDER_PCT', '20.0'))
MIN_ORDER_KRW = int(os.getenv('MIN_ORDER_KRW', '5000'))
# Execution safety: maximum acceptable slippage (fractional, e.g. 0.002 = 0.2%)
MAX_SLIPPAGE_PCT = float(os.getenv('MAX_SLIPPAGE_PCT', '0.002'))
# Reserved pool fraction (portion of equity to keep as emergency reserve) - default 5%
RESERVED_POOL_PCT = float(os.getenv('RESERVED_POOL_PCT', '0.05'))

# Paths
WORKSPACE = str(ROOT)
MEMORY_DIR = os.getenv('MEMORY_DIR', os.path.join(WORKSPACE, '..', 'memory'))
LOG_DIR = os.path.join(WORKSPACE, 'logs')

# Cron behavior
# default recommended schedule: at minute 5 and 35 each hour
CRON_SCHEDULE = os.getenv('CRON_SCHEDULE', '5,35 * * * *')

# Misc
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '20'))

# Ensure directories
os.makedirs(LOG_DIR, exist_ok=True)

# Quick config summary for logs
def summary():
    return {
        'SYMBOL': SYMBOL,
        'CANDLE_INTERVAL': CANDLE_INTERVAL,
        'LOOKBACK': LOOKBACK,
        'DRY_RUN': DRY_RUN,
        'INDEX_REFLECTIONS': INDEX_REFLECTIONS,
        'LLM_MODE': LLM_MODE,
        'DB_HOST': DB_HOST if DB_HOST else 'LOCAL_SQLITE',
    }
