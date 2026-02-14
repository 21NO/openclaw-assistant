"""
Data fetching utilities for market data, balances and news.
This module wraps pyupbit and the project's google_news module (if present).
"""
import logging
import time
from typing import List, Dict, Any

import pandas as pd

logger = logging.getLogger('data_fetcher')

from app import config


try:
    import pyupbit
except Exception:
    pyupbit = None

# optional news module path (project had a google_news module in the original copy)
try:
    import sys
    import os
    # attempt to import repo's google_news if available in sibling paths
    REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
    NEWS_PATH = os.path.join(REPO_ROOT, '..', 'google_news')
    if NEWS_PATH not in sys.path:
        sys.path.append(NEWS_PATH)
    import news_main  # type: ignore
except Exception:
    news_main = None


class DataFetcher:
    def __init__(self):
        self._client = None

    def get_ohlcv(self, symbol: str = 'KRW-BTC', interval: str = 'minutes30', count: int = 96) -> pd.DataFrame:
        """
        Fetch OHLCV as pandas DataFrame. Returns empty DataFrame on failure.
        interval is pyupbit-style: 'minutes30', 'minute60', 'day', etc.
        """
        try:
            if pyupbit is None:
                logger.warning('pyupbit not installed or import failed')
                return pd.DataFrame()

            # pyupbit.get_ohlcv supports interval strings like 'minute30'
            # convert 'minutes30' -> 'minute30' for backwards compat
            interval_arg = interval.replace('minutes', 'minute')
            df = pyupbit.get_ohlcv(symbol, interval=interval_arg, count=count)
            if df is None:
                return pd.DataFrame()
            df = df.reset_index()
            # ensure consistent columns: datetime, open, high, low, close, volume
            df.rename(columns={'index': 'timestamp'}, inplace=True)
            return df
        except Exception as e:
            logger.exception(f'get_ohlcv error: {e}')
            return pd.DataFrame()

    def get_balances(self) -> Dict[str, Any]:
        """
        Return balances dict using UPBIT keys from environment (if available).
        Returns minimal dict on failure.
        """
        try:
            # read keys from environment (config loads .env if present)
            access = os.getenv('UPBIT_ACCESS_KEY') or os.getenv('UPBIT_OPEN_API_KEY')
            secret = os.getenv('UPBIT_SECRET_KEY') or os.getenv('UPBIT_SECRET')

            # Try pyupbit if available and keys present
            if pyupbit is not None and access and secret:
                try:
                    upbit = pyupbit.Upbit(access, secret)
                    balances_list = upbit.get_balances()
                    return {'raw': balances_list}
                except Exception:
                    logger.exception('pyupbit get_balances failed; will fallback to HTTP method')

            # Fallback: use direct JWT+requests call if keys exist
            if access and secret:
                try:
                    import jwt
                    import requests
                    import uuid
                    payload = {'access_key': access, 'nonce': str(uuid.uuid4())}
                    token = jwt.encode(payload, secret, algorithm='HS256')
                    if isinstance(token, bytes):
                        token = token.decode('utf-8')
                    headers = {'Authorization': 'Bearer ' + token}
                    url = 'https://api.upbit.com/v1/accounts'
                    resp = requests.get(url, headers=headers, timeout=config.REQUEST_TIMEOUT)
                    resp.raise_for_status()
                    return {'raw': resp.json()}
                except Exception:
                    logger.exception('HTTP JWT Upbit get_balances failed')

            # no keys or all methods failed
            logger.info('No Upbit API keys found or unable to fetch balances')
            return {}
        except Exception as e:
            logger.exception(f'get_balances error: {e}')
            return {}

    def get_orderbook(self, symbol='KRW-BTC') -> Dict[str, Any]:
        try:
            if pyupbit is None:
                return {}
            ob = pyupbit.get_orderbook(symbol)
            return ob or {}
        except Exception as e:
            logger.exception(f'get_orderbook error: {e}')
            return {}

    def get_recent_news(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Use the project's news_main module if available to fetch recent news items.
        Falls back to empty list.
        """
        try:
            if news_main is None:
                return []
            # Attempt to reuse any exposed function in news_main
            if hasattr(news_main, 'fetch_recent'):
                return news_main.fetch_recent(limit=limit)
            if hasattr(news_main, 'get_latest_news'):
                return news_main.get_latest_news(limit=limit)
            return []
        except Exception as e:
            logger.exception(f'get_recent_news error: {e}')
            return []

    def estimate_total_krw(self, balances: Dict[str, Any]) -> float:
        """Estimate total equity in KRW from balances structure returned by get_balances().
        Returns SIM_EQUITY_KRW fallback when balances empty or estimation fails.
        """
        try:
            if not balances:
                return float(os.getenv('SIM_EQUITY_KRW', '1000000'))
            raw = balances.get('raw') if isinstance(balances, dict) and balances.get('raw') is not None else balances
            total_krw = 0.0
            if isinstance(raw, dict):
                # some fallback formats
                return float(os.getenv('SIM_EQUITY_KRW', '1000000'))
            for b in raw:
                try:
                    # pyupbit returns dicts with 'currency' and 'balance' keys
                    currency = b.get('currency') or b.get('unit_currency') or b.get('currency_code')
                    bal = b.get('balance') or b.get('available') or b.get('balance', 0)
                    bal = float(bal)
                except Exception:
                    continue
                if not currency:
                    continue
                if currency.upper() == 'KRW':
                    total_krw += bal
                else:
                    try:
                        ticker = f"KRW-{currency}"
                        price = 0.0
                        if pyupbit is not None:
                            price = pyupbit.get_current_price(ticker) or 0.0
                        total_krw += bal * float(price)
                    except Exception:
                        continue
            if total_krw <= 0:
                return float(os.getenv('SIM_EQUITY_KRW', '1000000'))
            return float(total_krw)
        except Exception:
            logger.exception('estimate_total_krw failed; using SIM_EQUITY_KRW')
            return float(os.getenv('SIM_EQUITY_KRW', '1000000'))
