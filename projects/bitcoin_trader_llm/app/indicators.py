"""
Indicator computations and feature builder.
Uses pandas and ta (ta-lib-like python package) when available.
Adds ADX-based regime detection and simple volume features (vol_ma20, vol_ratio).
"""
import logging
import pandas as pd

logger = logging.getLogger('indicators')

try:
    import ta
except Exception:
    ta = None


def compute_features_from_ohlcv(df: pd.DataFrame) -> dict:
    """
    Given DataFrame with columns at least: ['timestamp','open','high','low','close','volume']
    Returns a dict of computed indicators (latest values and some summary statistics).
    Adds:
      - adx14 (trend strength)
      - vol_ma20 (20-period volume MA)
      - vol_ratio = last_volume / vol_ma20
      - regime: 'trend'|'range'|'neutral'|'unknown'
    """
    if df is None or df.empty:
        return {}

    out = {}
    try:
        series_close = df['close'].astype(float)
        series_high = df['high'].astype(float)
        series_low = df['low'].astype(float)
        series_vol = df['volume'].astype(float)

        # moving averages
        out['ema9'] = float(series_close.ewm(span=9, adjust=False).mean().iloc[-1])
        out['ema50'] = float(series_close.ewm(span=50, adjust=False).mean().iloc[-1])
        out['sma20'] = float(series_close.rolling(window=20).mean().iloc[-1])

        # RSI
        if ta is not None:
            out['rsi14'] = float(ta.momentum.rsi(series_close, window=14).iloc[-1])
        else:
            # simple RSI fallback
            delta = series_close.diff().dropna()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            roll_up = up.ewm(com=13, adjust=False).mean()
            roll_down = down.ewm(com=13, adjust=False).mean()
            rs = roll_up / roll_down
            out['rsi14'] = float(100 - (100 / (1 + rs.iloc[-1])))

        # ATR
        if ta is not None:
            out['atr14'] = float(ta.volatility.average_true_range(series_high, series_low, series_close, window=14).iloc[-1])
        else:
            # naive ATR (fallback)
            tr = pd.concat([
                series_high - series_low,
                (series_high - series_close.shift()).abs(),
                (series_low - series_close.shift()).abs()
            ], axis=1).max(axis=1)
            out['atr14'] = float(tr.rolling(window=14).mean().iloc[-1])

        # bollinger
        if ta is not None:
            bb = ta.volatility.BollingerBands(close=series_close, window=20, window_dev=2)
            out['bb_mavg'] = float(bb.bollinger_mavg().iloc[-1])
            out['bb_hband'] = float(bb.bollinger_hband().iloc[-1])
            out['bb_lband'] = float(bb.bollinger_lband().iloc[-1])
        else:
            ma = series_close.rolling(window=20).mean()
            sd = series_close.rolling(window=20).std()
            out['bb_mavg'] = float(ma.iloc[-1])
            out['bb_hband'] = float((ma + 2 * sd).iloc[-1])
            out['bb_lband'] = float((ma - 2 * sd).iloc[-1])

        # recent returns / volatility
        out['return_1'] = float(series_close.pct_change().iloc[-1] if len(series_close) >= 2 else 0.0)
        out['vol_24'] = float(series_close.pct_change().rolling(window=24).std().iloc[-1])

        # last price and volume
        out['last_price'] = float(series_close.iloc[-1])
        out['last_volume'] = float(series_vol.iloc[-1])
        out['avg_vol_24'] = float(series_vol.rolling(window=24).mean().iloc[-1])

        # volume MA (short-term)
        try:
            out['vol_ma20'] = float(series_vol.rolling(window=20).mean().iloc[-1])
        except Exception:
            out['vol_ma20'] = float(series_vol.mean())

        # volume ratio
        try:
            vol_ma = out.get('vol_ma20') or 0.0
            out['vol_ratio'] = (out['last_volume'] / vol_ma) if vol_ma else None
        except Exception:
            out['vol_ratio'] = None

        # ADX (trend strength)
        if ta is not None:
            try:
                # prefer ADXIndicator if available
                try:
                    from ta.trend import ADXIndicator
                except Exception:
                    ADXIndicator = None
                if ADXIndicator is not None:
                    out['adx14'] = float(ADXIndicator(high=series_high, low=series_low, close=series_close, window=14).adx().iloc[-1])
                else:
                    # fallback to generic adx function if present
                    try:
                        out['adx14'] = float(ta.trend.adx(series_high, series_low, series_close, window=14).iloc[-1])
                    except Exception:
                        out['adx14'] = None
            except Exception:
                out['adx14'] = None
        else:
            out['adx14'] = None

        # regime detection based on ADX
        try:
            a = out.get('adx14')
            if a is None:
                out['regime'] = 'unknown'
            else:
                if a >= 25:
                    out['regime'] = 'trend'
                elif a <= 20:
                    out['regime'] = 'range'
                else:
                    out['regime'] = 'neutral'
        except Exception:
            out['regime'] = 'unknown'

    except Exception as e:
        logger.exception(f'compute_features error: {e}')

    return out
