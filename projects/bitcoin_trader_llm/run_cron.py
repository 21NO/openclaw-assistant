#!/usr/bin/env python3
"""
Entry point for cron execution (called by system cron or OpenClaw cron).
- Collects market data (30m candles), computes features
- Creates a decision request (to be processed by the OpenClaw agent). NO external AI API calls are made here.
- Persists the request into the DB (status='awaiting') for the OpenClaw agent to decide and later mark 'pending' for execution

This script is DRY_RUN-safe: it will not execute orders itself. Execution is done by OpenClaw agent executor which processes pending signals.

Usage (cron):
  5,35 * * * * /usr/bin/python3 /root/.openclaw/workspace/projects/bitcoin_trader_llm/run_cron.py >> /root/.openclaw/workspace/projects/bitcoin_trader_llm/logs/run_cron.log 2>&1

"""

import os
import sys
import logging
import uuid
import time
from datetime import datetime, timezone

# ensure project root is on path
PROJECT_ROOT = os.path.dirname(__file__)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from app import config
from app.data_fetcher import DataFetcher
from app.indicators import compute_features_from_ohlcv
from app.strategy_manager import StrategyManager
from app.db_logger import DBLogger

# logging
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'run_cron.log'))
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ')
# make asctime use UTC
formatter.converter = time.gmtime
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger('run_cron')


def main():
    now = datetime.now(timezone.utc)
    run_id = f"run_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
    logger.info(f"Starting run: {run_id}")

    # init helpers
    dfetch = DataFetcher()
    db = DBLogger()
    strat_mgr = StrategyManager(db=db)

    symbol = config.SYMBOL
    interval = config.CANDLE_INTERVAL  # e.g. 'minutes30'
    lookback = config.LOOKBACK

    try:
        # 1) fetch OHLCV
        ohlcv = dfetch.get_ohlcv(symbol=symbol, interval=interval, count=lookback)
        if ohlcv is None or ohlcv.empty:
            logger.error("No OHLCV data returned; aborting this run")
            return

        # 2) compute features
        features = compute_features_from_ohlcv(ohlcv)

        # 3) fetch balances, orderbook and recent news
        balances = dfetch.get_balances()
        news = dfetch.get_recent_news(limit=5)
        orderbook = dfetch.get_orderbook(symbol)

        # compute total equity estimate and cash balance
        total_equity = dfetch.estimate_total_krw(balances)
        cash_krw = 0.0
        try:
            raw = balances.get('raw') if isinstance(balances, dict) and balances.get('raw') is not None else balances
            if isinstance(raw, list):
                for b in raw:
                    currency = b.get('currency') or b.get('unit_currency') or b.get('currency_code')
                    bal = b.get('balance') or b.get('available') or 0
                    try:
                        bal = float(bal)
                    except Exception:
                        bal = 0.0
                    if currency and currency.upper() == 'KRW':
                        cash_krw += bal
        except Exception:
            logger.debug('Could not parse cash balance from balances payload')

        # reserved pool
        reserved_krw = float(total_equity) * float(config.RESERVED_POOL_PCT)

        # persist cron run metadata
        cron_row_id = db.insert_cron_run(run_id=run_id, started_at=now.isoformat(), finished_at=None, total_equity_krw=total_equity, cash_krw=cash_krw, reserved_krw=reserved_krw, summary=None, notes='')

        # persist positions (if balances provided)
        try:
            raw_bal = balances.get('raw') if isinstance(balances, dict) and balances.get('raw') is not None else balances
            if isinstance(raw_bal, list):
                for b in raw_bal:
                    try:
                        currency = b.get('currency') or b.get('unit_currency') or b.get('currency_code')
                        bal = b.get('balance') or b.get('available') or 0
                        bal = float(bal)
                    except Exception:
                        continue
                    if not currency:
                        continue
                    if currency.upper() == 'KRW':
                        continue
                    # estimate price
                    price = None
                    try:
                        import pyupbit
                        ticker = f"KRW-{currency}"
                        price = pyupbit.get_current_price(ticker) or 0.0
                    except Exception:
                        price = 0.0
                    notional = float(bal) * float(price)
                    db.insert_cron_position(run_id=run_id, symbol=f"KRW-{currency}", units=bal, avg_price=price, notional_krw=notional, side='long')
        except Exception:
            logger.exception('Failed to persist cron positions')

        # persist features snapshot
        try:
            # features may be a pandas Series or dict
            feats = features
            # serialize via db logger's helper
            db.insert_cron_feature(run_id=run_id, symbol=symbol, feature=feats)
        except Exception:
            logger.exception('Failed to persist features')

        # 4) select strategy (dynamic)
        active_strategy = strat_mgr.get_active_strategy()  # dict
        logger.info(f"Active strategy: {active_strategy.get('name')}")

        # 5) create a decision request for the OpenClaw agent (no external API call)
        decision_payload = {
            'run_id': run_id,
            'symbol': symbol,
            'features': features,
            'recent_ohlcv': ohlcv.tail(24).to_dict('records'),
            'news': news,
            'balances': balances,
            'orderbook': orderbook,
            'strategy': active_strategy,
            'meta': {
                'timestamp': now.isoformat()
            }
        }

        req_id = db.insert_decision_request(run_id=run_id, payload=decision_payload)
        logger.info(f"Inserted decision_request id={req_id} (awaiting agent decision)")

    except Exception as e:
        logger.exception(f"Unhandled error in run_cron: {e}")

    logger.info(f"Run {run_id} finished")


if __name__ == '__main__':
    main()
