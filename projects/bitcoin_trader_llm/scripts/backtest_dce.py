#!/usr/bin/env python3
"""
Simple backtester for the D+C+E strategy (Volatility breakout + RSI filter + optional LLM gate)
Runs on historical OHLCV from pyupbit and simulates trades with slippage and fees.

Usage:
  python3 scripts/backtest_dce.py --months 5 --vol-mult 2.0 --stop-rel 2.0 --risk-pct 1.0

Outputs:
  - prints brief summary
  - writes logs: logs/backtest_YYYYmmdd_HHMMSS.json and logs/backtest_trades.csv

Notes / simplifications:
  - single-position long-only simulation (no shorting)
  - execution at next bar open (market order), slippage applied to execution price
  - stop-loss honored intra-bar (low/high) on subsequent bars, executed at stop price adjusted for slippage
  - position sizing uses app.policy.size_from_risk with entry price estimate (close at decision time)
  - orderbook imbalance may be missing (treated as None) — agent_decider handles None
"""
from __future__ import annotations
import os
import sys
import time
import json
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# ensure project root on path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pyupbit
from app import config
from app.indicators import compute_features_from_ohlcv
from app.agent_decider import decide_from_payload
from app.policy import size_from_risk
from app.risk_engine import RiskEngine
from app.portfolio_manager import PortfolioManager, Signal

LOG_DIR = PROJECT_ROOT / 'logs'
os.makedirs(LOG_DIR, exist_ok=True)


def fetch_ohlcv_range(symbol: str, interval: str, start_dt: datetime, end_dt: datetime, batch_count: int = 200) -> pd.DataFrame:
    """Fetch OHLCV data between start_dt and end_dt (inclusive) by paging backwards using pyupbit.get_ohlcv.
    Timestamps are treated as naive UTC datetimes.
    """
    all_chunks = []
    to_dt = end_dt
    # pyupbit.get_ohlcv expects 'to' as a string like '2026-02-14 00:00:00'
    attempts = 0
    while True:
        attempts += 1
        if attempts > 200:
            break
        to_str = to_dt.strftime('%Y-%m-%d %H:%M:%S')
        try:
            df = pyupbit.get_ohlcv(symbol, interval=interval, count=batch_count, to=to_str)
        except Exception as e:
            print('pyupbit.get_ohlcv failed:', e)
            break
        if df is None or df.empty:
            break
        df = df.reset_index().rename(columns={'index': 'timestamp', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
        # ensure timestamp is datetime (pyupbit returns pandas.DatetimeIndex)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        all_chunks.append(df)
        earliest = df['timestamp'].iloc[0]
        # stop if we've reached or passed start_dt
        if earliest <= start_dt:
            break
        # set next 'to' to just before earliest
        to_dt = earliest - timedelta(seconds=1)
        time.sleep(0.1)  # be gentle on API

    if not all_chunks:
        return pd.DataFrame()
    big = pd.concat(all_chunks, ignore_index=True)
    big = big.drop_duplicates(subset='timestamp').sort_values('timestamp').reset_index(drop=True)
    # filter range
    mask = (big['timestamp'] >= start_dt) & (big['timestamp'] <= end_dt)
    big = big.loc[mask].reset_index(drop=True)
    return big


def run_backtest(months: int = 5, slippage_pct: float = 0.001, fee_pct: float = 0.0005, time_based_exit_min: int = 60, lookback_bars: int = None, vol_mult: float = 1.5, stop_rel_default: float = 1.5, risk_pct: float | None = None, mode: str = 'dce', vol_entry_mult: float = 1.5, tp_pct: float | None = None, sl_pct: float | None = None, disable_rsi_veto: bool = False, atr_trail: bool = False, gatekeeper_only: bool = False, early_abort: bool = False, early_abort_pct: float = 0.005, early_abort_bars: int = 2, adx_threshold: float = 25.0, htf_require: str = 'any', upper_wick_pct: float | None = None, enable_risk_engine: bool = False, risk_daily_loss_pct: float = 1.0, risk_max_dd_pct: float = 10.0, risk_consec_losses: int = 3, risk_consec_mult: float = 0.5, risk_initial_pct: float | None = None, risk_min_pct: float = 0.05, risk_max_reduction_steps: int = 5, risk_recovery_step_pct: float = 0.1, risk_recovery_consec_wins: int = 3, use_portfolio_manager: bool = False, pm_agent_weight: float = 0.6, pm_rule_weight: float = 0.2, agent_proposed_risk_mult: float = 2.0, risk_cooldown_days: int = 0, end_dt: datetime | None = None):
    if end_dt is None:
        end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=30 * months)
    symbol = getattr(config, 'SYMBOL', 'KRW-BTC')
    interval = getattr(config, 'CANDLE_INTERVAL', 'minutes30')
    lookback = lookback_bars or getattr(config, 'LOOKBACK', 96)

    print(f'Backtest: {months} months | symbol={symbol} interval={interval} start={start_dt} end={end_dt} lookback={lookback} vol_mult={vol_mult} stop_rel={stop_rel_default} risk_pct={risk_pct} disable_rsi_veto={disable_rsi_veto} atr_trail={atr_trail} gatekeeper_only={gatekeeper_only} early_abort={early_abort} early_abort_pct={early_abort_pct} early_abort_bars={early_abort_bars} adx_threshold={adx_threshold} htf_require={htf_require} upper_wick_pct={upper_wick_pct}')

    df = fetch_ohlcv_range(symbol, interval, start_dt, end_dt)
    if df is None or df.empty or len(df) < lookback + 10:
        print('Not enough OHLCV data fetched:', len(df) if df is not None else 0)
        return None

    # prepare simulation state
    initial_equity = float(os.getenv('SIM_EQUITY_KRW') or getattr(config, 'SIM_EQUITY_KRW', 1000000))
    cash = float(initial_equity)
    position = None  # dict(entry_price, amount, stop_price, entry_idx)
    trades = []
    nav_series = []
    # collect gatekeeper inspection logs (one entry per buy-signal evaluated)
    gate_logs = []

    bars_per_min = None

    # risk engine (optional)
    risk_engine = None
    if enable_risk_engine:
        try:
            initial = float(risk_initial_pct) if (risk_initial_pct is not None) else (float(risk_pct) if (risk_pct is not None) else float(config.RISK_PER_TRADE_PCT))
        except Exception:
            initial = float(config.RISK_PER_TRADE_PCT)
        risk_engine = RiskEngine(
            initial_risk_pct=initial,
            min_risk_pct=float(risk_min_pct),
            daily_loss_limit_pct=float(risk_daily_loss_pct),
            max_drawdown_limit_pct=float(risk_max_dd_pct),
            consecutive_losses_threshold=int(risk_consec_losses),
            consecutive_loss_multiplier=float(risk_consec_mult),
            max_reduction_steps=int(risk_max_reduction_steps),
            recovery_enabled=True,
            recovery_consec_wins=int(risk_recovery_consec_wins),
            recovery_step_pct_of_initial=float(risk_recovery_step_pct),
            cooldown_days=int(risk_cooldown_days)
        )
        # optional PortfolioManager integration
        pm = None
        if use_portfolio_manager:
            try:
                pm = PortfolioManager(risk_engine=risk_engine, agent_weight=pm_agent_weight, rule_weight=pm_rule_weight, dry_run=getattr(config, 'DRY_RUN', True))
            except Exception:
                pm = None
    else:
        pm = None
    current_day = None

    # determine minutes per bar from interval string
    if 'minute' in interval or 'minutes' in interval:
        # e.g. 'minutes30' or 'minute60'
        import re
        m = re.search(r'(\d+)', interval)
        if m:
            bar_minutes = int(m.group(1))
        else:
            bar_minutes = 30
    elif interval == 'day':
        bar_minutes = 24 * 60
    elif interval == 'week':
        bar_minutes = 7 * 24 * 60
    else:
        bar_minutes = 30

    # handle disabling time-based exit: if time_based_exit_min <= 0, disable time exits
    if time_based_exit_min and time_based_exit_min > 0:
        max_hold_bars = max(1, int(time_based_exit_min / bar_minutes))
    else:
        max_hold_bars = None

    # main loop: iterate so that decision at index i uses data up to i, execution at i+1
    for idx in range(lookback - 1, len(df) - 1):
        window = df.iloc[idx - (lookback - 1): idx + 1].copy()
        features = compute_features_from_ohlcv(window)

        # candidate execution bar (next bar after decision)
        next_bar = df.iloc[idx + 1]

        # volume entry filter
        try:
            last_vol = float(features.get('last_volume') or features.get('volume') or 0.0)
        except Exception:
            last_vol = 0.0
        try:
            vol_ma = float(features.get('vol_ma20') or features.get('vol_ma') or features.get('avg_vol_24') or 0.0)
        except Exception:
            vol_ma = 0.0
        vol_entry_allowed = False
        if vol_ma and vol_entry_mult and vol_entry_mult > 0:
            vol_entry_allowed = (last_vol >= (vol_ma * float(vol_entry_mult)))
        else:
            vol_entry_allowed = True

        # prepare holder for gatekeeper inspection for this decision
        current_gate_info = None

        strategy_name = 'dce_backtest' if mode == 'dce' else ('meanrev' if mode == 'meanrev' else 'regime_switch')
        payload = {
            'run_id': f'bt_{idx}',
            'symbol': symbol,
            'features': features,
            'recent_ohlcv': window.tail(24).to_dict('records'),
            'news': [],
            'balances': {},
            'orderbook': None,
            'strategy': {'name': strategy_name, 'params': {'vol_mult': vol_mult, 'stop_rel': stop_rel_default}},
            'params': {'vol_mult': vol_mult, 'stop_rel': stop_rel_default, 'risk_pct': risk_pct, 'rsi_veto': (not disable_rsi_veto), 'atr_trail': atr_trail}
        }

        decision = None
        # Regime switcher: trend (ADX>=25) -> D+C+E (decider), range (ADX<25) -> mean-reversion
        if mode == 'regime':
            adx_val = None
            try:
                adx_val = float(features.get('adx14') or features.get('adx') or 0.0)
            except Exception:
                adx_val = None
            if adx_val is not None and adx_val >= adx_threshold:
                # trend: use decider (breakout)
                decision = decide_from_payload(payload)
            else:
                # range: local mean-reversion logic
                rsi_val = None
                try:
                    rsi_val = float(features.get('rsi14') or features.get('rsi') or 0.0)
                except Exception:
                    rsi_val = None
                action = 'hold'
                pct = 0
                if rsi_val is not None and rsi_val <= 30:
                    action = 'buy'
                    pct = 30
                elif rsi_val is not None and rsi_val >= 70:
                    action = 'sell'
                    pct = 30
                decision = {'action': action, 'pct': pct, 'stop_rel': stop_rel_default, 'reason': f'regime_meanrev(adx={adx_val},rsi={rsi_val})'}
        else:
            # dce or meanrev modes
            if mode == 'meanrev':
                # replicate meanrev logic used earlier
                rsi_val = None
                adx_val = None
                try:
                    rsi_val = float(features.get('rsi14') or features.get('rsi') or 0.0)
                except Exception:
                    rsi_val = None
                try:
                    adx_val = float(features.get('adx14') or features.get('adx') or 0.0)
                except Exception:
                    adx_val = None
                action = 'hold'
                pct = 0
                if adx_val is not None and adx_val < adx_threshold:
                    if rsi_val is not None and rsi_val <= 30:
                        action = 'buy'
                        pct = 30
                    elif rsi_val is not None and rsi_val >= 70:
                        action = 'sell'
                        pct = 30
                decision = {'action': action, 'pct': pct, 'stop_rel': stop_rel_default, 'reason': f'meanrev(adx={adx_val},rsi={rsi_val})'}
            else:
                decision = decide_from_payload(payload)

        action = decision.get('action')
        pct = int(decision.get('pct') or 0)
        stop_rel = decision.get('stop_rel') or stop_rel_default

        # Gatekeeper-only enforcement: require 2-of-3 {HTF ema agree, volume filter, orderbook imbalance}
        if gatekeeper_only and action == 'buy':
            # compute gate components and record them for inspection
            htf1 = False
            htf4 = False
            try:
                df_up = df.iloc[:idx+1].copy()
                if not df_up.empty:
                    df_upi = df_up.set_index('timestamp')
                    close_1h = df_upi['close'].resample('60min', label='right', closed='right').last().dropna()
                    close_4h = df_upi['close'].resample('240min', label='right', closed='right').last().dropna()
                    if len(close_1h) >= 10:
                        ema9_1h = close_1h.ewm(span=9, adjust=False).mean().iloc[-1]
                        ema50_1h = close_1h.ewm(span=50, adjust=False).mean().iloc[-1]
                        htf1 = ema9_1h > ema50_1h
                    if len(close_4h) >= 5:
                        ema9_4h = close_4h.ewm(span=9, adjust=False).mean().iloc[-1]
                        ema50_4h = close_4h.ewm(span=50, adjust=False).mean().iloc[-1]
                        htf4 = ema9_4h > ema50_4h
            except Exception:
                htf1 = False
                htf4 = False

            if htf_require == 'both':
                htf_agree = bool(htf1 and htf4)
            else:
                htf_agree = bool(htf1 or htf4)

            # ADX regime filter
            adx_val = None
            try:
                adx_val = float(features.get('adx14') or features.get('adx') or 0.0)
            except Exception:
                adx_val = None
            adx_ok = (adx_val is not None and adx_val >= float(adx_threshold))

            # orderbook imbalance (best-effort)
            imbalance = None
            ob = payload.get('orderbook')
            if ob:
                try:
                    ob0 = ob[0] if isinstance(ob, list) and len(ob) > 0 else ob
                    total_bid = ob0.get('total_bid_size') or ob0.get('total_bid_qty')
                    total_ask = ob0.get('total_ask_size') or ob0.get('total_ask_qty')
                    if total_bid is None or total_ask is None:
                        units = ob0.get('orderbook_units') or []
                        tb = 0.0
                        ta = 0.0
                        for u in units:
                            tb += float(u.get('bid_size', 0) or 0)
                            ta += float(u.get('ask_size', 0) or 0)
                        total_bid = tb
                        total_ask = ta
                    if total_ask:
                        imbalance = float(total_bid) / float(total_ask)
                except Exception:
                    imbalance = None

            # interpret imbalance: None -> 'unknown', else boolean
            if imbalance is None:
                imbalance_status = 'unknown'
            else:
                imbalance_status = bool(imbalance > 1.1)

            vol_ok = vol_entry_allowed
            vol_ma20_val = vol_ma
            last_vol_val = last_vol
            vol_ratio_val = features.get('vol_ratio')

            # upper wick filter (entry candle quality) — compute on the actual execution/entry bar (next_bar)
            wick_pct_val = None
            upper_wick_ok = 'disabled'
            try:
                entry_candle = next_bar if 'next_bar' in locals() else window.iloc[-1]
                h_val = float(entry_candle['high'])
                l_val = float(entry_candle['low'])
                o_val = float(entry_candle['open'])
                c_val = float(entry_candle['close'])
                denom = (h_val - l_val)
                if denom > 0:
                    wick_pct_val = (h_val - max(o_val, c_val)) / denom
                else:
                    wick_pct_val = 0.0
            except Exception:
                wick_pct_val = None

            if upper_wick_pct is None:
                upper_wick_ok = 'disabled'
            else:
                try:
                    upper_wick_ok = (wick_pct_val is not None and wick_pct_val <= float(upper_wick_pct))
                except Exception:
                    upper_wick_ok = False

            # compute HTF OK according to htf_require
            if htf_require == 'both':
                htf_ok = bool(htf1 and htf4)
            else:
                htf_ok = bool(htf1 or htf4)

            # gate_count counts only available True checks (do not count unknown imbalance)
            gate_count = 0
            available_checks = 0
            # HTF considered available
            available_checks += 1
            if htf_ok:
                gate_count += 1
            # volume availability
            available_checks += 1
            if vol_ok:
                gate_count += 1
            # imbalance availability
            if imbalance_status != 'unknown':
                available_checks += 1
                if imbalance_status:
                    gate_count += 1
            # upper wick availability (only if configured)
            if upper_wick_pct is not None:
                available_checks += 1
                if upper_wick_ok is True:
                    gate_count += 1

            # decide pass logic
            if not adx_ok:
                pass_bool = False
                threshold = None
            else:
                if htf_require == 'both':
                    # recommended: HTF must be true, and at least one of vol or imbalance (if known) must be true
                    # additionally require upper_wick to be acceptable when configured
                    threshold = 'HTF_required AND (vol OR imbalance)'
                    # imbalance_status must be True to count; if unknown, rely on vol_ok only
                    if upper_wick_pct is None:
                        pass_bool = bool(htf_ok and (vol_ok or (imbalance_status is True)))
                    else:
                        pass_bool = bool(htf_ok and (vol_ok or (imbalance_status is True)) and (upper_wick_ok is True))
                else:
                    # default: require 2-of-N where N = available_checks
                    required = 2 if available_checks >= 2 else available_checks
                    threshold = f'{required}/{available_checks}'
                    pass_bool = bool(gate_count >= required)

            # reasons list
            reasons = []
            if htf_ok:
                reasons.append('htf_ok')
            if vol_ok:
                reasons.append('vol_ok')
            if imbalance_status is True:
                reasons.append('imbalance_ok')
            if upper_wick_ok is True:
                reasons.append('upper_wick_ok')

            current_gate_info = {
                'idx': idx,
                'timestamp': window['timestamp'].iloc[-1].isoformat() if not window.empty else None,
                'htf1': bool(htf1),
                'htf4': bool(htf4),
                'htf_ok': bool(htf_ok),
                'adx_val': adx_val,
                'adx_ok': bool(adx_ok),
                'vol_ma20': float(vol_ma20_val) if vol_ma20_val is not None else None,
                'last_vol': float(last_vol_val) if last_vol_val is not None else None,
                'vol_ratio': float(vol_ratio_val) if vol_ratio_val is not None else None,
                'vol_ok': bool(vol_ok),
                'imbalance': float(imbalance) if imbalance is not None else None,
                'imbalance_ok': (imbalance_status if imbalance_status in (True, False) else 'unknown'),
                'upper_wick_pct_threshold': (float(upper_wick_pct) if upper_wick_pct is not None else None),
                'upper_wick_pct': (float(wick_pct_val) if wick_pct_val is not None else None),
                'upper_wick_ok': (upper_wick_ok if upper_wick_ok in (True, False) else 'disabled'),
                'gate_count': gate_count,
                'available_checks': available_checks,
                'threshold': threshold,
                'reasons': reasons,
                'pass': bool(pass_bool)
            }

            gate_logs.append(current_gate_info.copy())

            if not pass_bool:
                # block this entry
                action = 'hold'
                pct = 0
                stop_rel = None

        # process existing position stop / time-based exit using the next bar (idx+1)
        next_bar = df.iloc[idx + 1]
        # update unrealized nav
        mark_price = next_bar['close']
        pos_value = (position['amount'] * mark_price) if position else 0.0
        nav = cash + pos_value
        nav_series.append({'timestamp': next_bar['timestamp'].isoformat(), 'nav': nav})

        # daily boundary detection for risk engine
        try:
            entry_day = pd.to_datetime(next_bar['timestamp']).date()
        except Exception:
            entry_day = None
        if enable_risk_engine and risk_engine is not None:
            if current_day is None or entry_day != current_day:
                current_day = entry_day
                # initialize start of day NAV
                try:
                    risk_engine.on_new_day(nav)
                except Exception:
                    pass

        # ATR trailing stop update (if enabled)
        if position is not None and atr_trail:
            try:
                atr = float(features.get('atr14') or 0.0)
            except Exception:
                atr = 0.0
            try:
                sr = float(stop_rel or stop_rel_default)
            except Exception:
                sr = float(stop_rel_default)
            if atr and atr > 0:
                # candidate stop based on this bar's high - atr*sr
                candidate_stop = next_bar['high'] - (atr * sr)
                if candidate_stop > position.get('stop_price', -1e9):
                    position['stop_price'] = candidate_stop

        # check TP/SL hit in next_bar (and early-abort if enabled)
        # early-abort: if enabled, exit early within first N bars when price moves beyond early_abort_pct against entry
        if position is not None and early_abort:
            try:
                held_bars = idx + 1 - position['entry_idx']
                if held_bars <= early_abort_bars:
                    if next_bar['low'] <= position['entry_price'] * (1.0 - float(early_abort_pct)):
                        exit_price = next_bar['open'] * (1.0 - slippage_pct)
                        exit_fee = exit_price * position['amount'] * fee_pct
                        cash += exit_price * position['amount'] - exit_fee
                        trade_pnl = (exit_price - position['entry_price']) * position['amount'] - exit_fee
                        trades.append({
                            'entry_time': position['entry_time'].isoformat(),
                            'exit_time': next_bar['timestamp'].isoformat(),
                            'entry_price': position['entry_price'],
                            'exit_price': exit_price,
                            'amount': position['amount'],
                            'pnl': trade_pnl,
                            'reason': 'early_abort'
                        })
                        # risk engine record
                        if enable_risk_engine and risk_engine is not None:
                            try:
                                final_nav = cash
                                risk_engine.record_trade_result(trade_pnl, final_nav, next_bar['timestamp'].isoformat())
                            except Exception:
                                pass
                        position = None
                        # after early abort, skip new signals on this bar
                        continue
            except Exception:
                # ignore early-abort failures
                pass

        if position is not None:
            sl_price = position.get('sl_price') or position.get('stop_price')
            tp_price = position.get('tp_price')
            sl_hit = False
            tp_hit = False
            if sl_price is not None and next_bar['low'] <= sl_price:
                sl_hit = True
            if tp_price is not None and next_bar['high'] >= tp_price:
                tp_hit = True

            # if both hit in same bar, treat as STOP hit (conservative)
            if sl_hit:
                exit_price = sl_price * (1.0 - slippage_pct)
                exit_fee = exit_price * position['amount'] * fee_pct
                cash += exit_price * position['amount'] - exit_fee
                trade_pnl = (exit_price - position['entry_price']) * position['amount'] - exit_fee
                trades.append({
                    'entry_time': position['entry_time'].isoformat(),
                    'exit_time': next_bar['timestamp'].isoformat(),
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'amount': position['amount'],
                    'pnl': trade_pnl,
                    'reason': 'stop'
                })
                if enable_risk_engine and risk_engine is not None:
                    try:
                        final_nav = cash
                        risk_engine.record_trade_result(trade_pnl, final_nav, next_bar['timestamp'].isoformat())
                    except Exception:
                        pass
                position = None
                # after stop, we skip processing new signals on this bar (conservative)
                continue
            elif tp_hit:
                exit_price = tp_price * (1.0 - slippage_pct)
                exit_fee = exit_price * position['amount'] * fee_pct
                cash += exit_price * position['amount'] - exit_fee
                trade_pnl = (exit_price - position['entry_price']) * position['amount'] - exit_fee
                trades.append({
                    'entry_time': position['entry_time'].isoformat(),
                    'exit_time': next_bar['timestamp'].isoformat(),
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'amount': position['amount'],
                    'pnl': trade_pnl,
                    'reason': 'take_profit'
                })
                if enable_risk_engine and risk_engine is not None:
                    try:
                        final_nav = cash
                        risk_engine.record_trade_result(trade_pnl, final_nav, next_bar['timestamp'].isoformat())
                    except Exception:
                        pass
                position = None
                continue

            # time-based exit (if enabled)
            if max_hold_bars is not None:
                held_bars = idx + 1 - position['entry_idx']
                if held_bars >= max_hold_bars:
                    exit_price = next_bar['open'] * (1.0 - slippage_pct)
                    exit_fee = exit_price * position['amount'] * fee_pct
                    cash += exit_price * position['amount'] - exit_fee
                    trade_pnl = (exit_price - position['entry_price']) * position['amount'] - exit_fee
                    trades.append({
                        'entry_time': position['entry_time'].isoformat(),
                        'exit_time': next_bar['timestamp'].isoformat(),
                        'entry_price': position['entry_price'],
                        'exit_price': exit_price,
                        'amount': position['amount'],
                        'pnl': trade_pnl,
                        'reason': 'time_exit'
                    })
                    if enable_risk_engine and risk_engine is not None:
                        try:
                            final_nav = cash
                            risk_engine.record_trade_result(trade_pnl, final_nav, next_bar['timestamp'].isoformat())
                        except Exception:
                            pass
                    position = None
                    # continue to next bar
                    continue

        # if decision is sell and we have a position -> exit at next_bar open
        if action == 'sell' and position is not None:
            exec_price = next_bar['open'] * (1.0 - slippage_pct)
            exec_fee = exec_price * position['amount'] * fee_pct
            cash += exec_price * position['amount'] - exec_fee
            trade_pnl = (exec_price - position['entry_price']) * position['amount'] - exec_fee
            trades.append({
                'entry_time': position['entry_time'].isoformat(),
                'exit_time': next_bar['timestamp'].isoformat(),
                'entry_price': position['entry_price'],
                'exit_price': exec_price,
                'amount': position['amount'],
                'pnl': trade_pnl,
                'reason': 'sell_signal'
            })
            if enable_risk_engine and risk_engine is not None:
                try:
                    final_nav = cash
                    risk_engine.record_trade_result(trade_pnl, final_nav, next_bar['timestamp'].isoformat())
                except Exception:
                    pass
            position = None
            continue

        # if decision is buy and no position, open a new position
        if action == 'buy' and position is None and pct > 0:
            # enforce volume entry filter
            if not vol_entry_allowed:
                # skip entry due to volume filter
                continue

            # estimate entry price and stop using features (use last close as estimate)
            entry_price_est = float(features.get('last_price') or window['close'].iloc[-1])
            atr = float(features.get('atr14') or 0.0)
            # if fixed sl_pct provided, use that for stop estimate
            if sl_pct is not None:
                stop_price_est = entry_price_est * (1.0 - float(sl_pct))
            else:
                if atr and atr > 0:
                    stop_price_est = entry_price_est - (atr * float(stop_rel))
                else:
                    stop_price_est = entry_price_est * (1.0 - 0.03)  # fallback 3% stop

            equity_for_risk = cash + (position['amount'] * mark_price if position else 0.0)

            # If PortfolioManager integration requested, use it for sizing & risk evaluation
            if use_portfolio_manager and 'pm' in locals() and pm is not None:
                # block new entries if daily loss limit reached
                try:
                    if enable_risk_engine and risk_engine is not None and not risk_engine.allow_entry():
                        continue
                except Exception:
                    pass

                # derive suggested risk for agent signal (agent may be more aggressive)
                try:
                    base_initial = float(risk_engine.initial_risk_pct) if (risk_engine is not None) else float(getattr(config, 'RISK_PER_TRADE_PCT', 1.0))
                except Exception:
                    base_initial = float(getattr(config, 'RISK_PER_TRADE_PCT', 1.0))
                # interpret agent_proposed_risk_mult (e.g., 2.0 -> double)
                try:
                    agent_prop_risk = float(base_initial) * float(agent_proposed_risk_mult)
                except Exception:
                    agent_prop_risk = float(base_initial) * 2.0

                # compute stop pct (percent)
                try:
                    stop_pct = ((entry_price_est - float(stop_price_est)) / float(entry_price_est)) * 100.0
                except Exception:
                    stop_pct = float(1.0)

                # build agent signal
                agent_signal = Signal(
                    id=f'sig-agent-{idx}',
                    source='agent',
                    publish_ts=datetime.utcnow().isoformat(),
                    symbol=symbol,
                    side='long',
                    score=float(decision.get('confidence') or 0.5),
                    p_win=float(decision.get('confidence') or 0.5),
                    confidence=float(decision.get('confidence') or 0.5),
                    suggested_risk_pct=float(agent_prop_risk),
                    suggested_stop_pct=float(stop_pct),
                    horizon_minutes=int(decision.get('meta', {}).get('horizon_minutes') or 720),
                    model_version=decision.get('meta', {}).get('decider') or 'agent_dce',
                    meta={'entry_price': entry_price_est}
                )

                # simple rule signal (auxiliary) - use a modest score and conservative risk
                rule_signal = Signal(
                    id=f'sig-rule-{idx}',
                    source='rule',
                    publish_ts=datetime.utcnow().isoformat(),
                    symbol=symbol,
                    side='long',
                    score=max(0.1, float(decision.get('confidence') or 0.2) * 0.5),
                    p_win=None,
                    confidence=None,
                    suggested_risk_pct=float(base_initial),
                    suggested_stop_pct=float(stop_pct),
                    horizon_minutes=720,
                    model_version='rule-simple',
                    meta={}
                )

                pm.ingest_signals([agent_signal, rule_signal])
                proposals = pm.propose_positions(equity=equity_for_risk, market_state={'price': entry_price_est})
                if not proposals:
                    continue
                decisions_pm = pm.apply_risk_engine(proposals, portfolio_snapshot={'equity': equity_for_risk}, market_state={'price': entry_price_est})
                orders = pm.finalize_orders(decisions_pm, market_state={'price': entry_price_est}, equity=equity_for_risk)
                if not orders:
                    continue

                # execute first order (single-position system)
                ord0 = orders[0]
                exec_price = next_bar['open'] * (1.0 + slippage_pct)
                units = float(ord0.units)
                entry_fee = exec_price * units * fee_pct
                cash -= exec_price * units + entry_fee

                # compute actual stop and tp based on executed price
                if sl_pct is not None:
                    actual_sl = exec_price * (1.0 - float(sl_pct))
                else:
                    actual_sl = exec_price - (atr * float(stop_rel)) if atr and atr > 0 else exec_price * (1.0 - 0.03)
                tp_price = exec_price * (1.0 + float(tp_pct)) if tp_pct is not None else None

                position = {
                    'entry_price': exec_price,
                    'amount': units,
                    'sl_price': actual_sl,
                    'tp_price': tp_price,
                    'entry_idx': idx + 1,
                    'entry_time': next_bar['timestamp']
                }
                trades.append({
                    'entry_time': position['entry_time'].isoformat(),
                    'exit_time': None,
                    'entry_price': position['entry_price'],
                    'exit_price': None,
                    'amount': position['amount'],
                    'pnl': None,
                    'reason': 'entry_pm',
                    'gate_info': current_gate_info.copy() if current_gate_info is not None else None
                })
                continue

            # fallback: original sizing logic
            equity_for_risk = cash + (position['amount'] * mark_price if position else 0.0)
            # risk engine enforcement (optional)
            eff_risk_pct = risk_pct
            if enable_risk_engine and risk_engine is not None:
                # block new entries if daily loss limit reached
                try:
                    if not risk_engine.allow_entry():
                        continue
                except Exception:
                    pass
                try:
                    eff_risk_pct = risk_engine.get_effective_risk_pct(risk_pct)
                except Exception:
                    eff_risk_pct = risk_pct
            sizing = size_from_risk(equity_for_risk, entry_price_est, stop_price_est, eff_risk_pct)
            position_krw = float(sizing.get('position_krw') or 0.0)
            if position_krw <= 0:
                # too small
                continue

            # execute at next bar open with slippage
            exec_price = next_bar['open'] * (1.0 + slippage_pct)
            amount = position_krw / exec_price
            entry_fee = exec_price * amount * fee_pct
            cash -= exec_price * amount + entry_fee
            # compute actual stop and tp based on executed price
            if sl_pct is not None:
                actual_sl = exec_price * (1.0 - float(sl_pct))
            else:
                actual_sl = exec_price - (atr * float(stop_rel)) if atr and atr > 0 else exec_price * (1.0 - 0.03)
            tp_price = exec_price * (1.0 + float(tp_pct)) if tp_pct is not None else None

            position = {
                'entry_price': exec_price,
                'amount': amount,
                'sl_price': actual_sl,
                'tp_price': tp_price,
                'entry_idx': idx + 1,
                'entry_time': next_bar['timestamp']
            }
            trades.append({
                'entry_time': position['entry_time'].isoformat(),
                'exit_time': None,
                'entry_price': position['entry_price'],
                'exit_price': None,
                'amount': position['amount'],
                'pnl': None,
                'reason': 'entry',
                'gate_info': current_gate_info.copy() if current_gate_info is not None else None
            })
            continue

    # finalize: if position still open, close at last close
    if position is not None:
        last_bar = df.iloc[-1]
        exit_price = last_bar['close'] * (1.0 - slippage_pct)
        exit_fee = exit_price * position['amount'] * fee_pct
        cash += exit_price * position['amount'] - exit_fee
        trade_pnl = (exit_price - position['entry_price']) * position['amount'] - exit_fee
        trades.append({
            'entry_time': position['entry_time'].isoformat(),
            'exit_time': last_bar['timestamp'].isoformat(),
            'entry_price': position['entry_price'],
            'exit_price': exit_price,
            'amount': position['amount'],
            'pnl': trade_pnl,
            'reason': 'final_exit'
        })
        if enable_risk_engine and risk_engine is not None:
            try:
                final_nav = cash
                risk_engine.record_trade_result(trade_pnl, final_nav, last_bar['timestamp'].isoformat())
            except Exception:
                pass
        position = None

    final_nav = cash
    total_pnl = final_nav - initial_equity
    wins = [t for t in trades if t.get('pnl') is not None and t.get('pnl') > 0]
    losses = [t for t in trades if t.get('pnl') is not None and t.get('pnl') <= 0]
    win_rate = (len(wins) / len([t for t in trades if t.get('pnl') is not None])) if trades else 0.0
    gross_profit = sum([t['pnl'] for t in trades if t.get('pnl') and t['pnl'] > 0])
    gross_loss = -sum([t['pnl'] for t in trades if t.get('pnl') and t['pnl'] < 0])
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None

    # compute max drawdown from nav_series
    nav_df = pd.DataFrame(nav_series)
    nav_df['nav'] = nav_df['nav'].astype(float)
    nav_df['cummax'] = nav_df['nav'].cummax()
    nav_df['drawdown'] = (nav_df['cummax'] - nav_df['nav']) / nav_df['cummax']
    max_dd = float(nav_df['drawdown'].max()) if not nav_df.empty else 0.0

    result = {
        'start': start_dt.isoformat(),
        'end': end_dt.isoformat(),
        'symbol': symbol,
        'interval': interval,
        'months': months,
        'initial_equity': initial_equity,
        'final_nav': final_nav,
        'total_pnl': total_pnl,
        'num_trades': len([t for t in trades if t.get('pnl') is not None]),
        'win_rate': win_rate,
        'profit_factor': profit_factor,
        'max_drawdown': max_dd,
        'slippage_pct': slippage_pct,
        'fee_pct': fee_pct,
        'params': {
            'lookback': lookback,
            'time_based_exit_min': time_based_exit_min,
            'vol_mult': vol_mult,
            'stop_rel': stop_rel_default,
            'risk_pct': risk_pct,
            'mode': mode,
            'vol_entry_mult': vol_entry_mult,
            'tp_pct': tp_pct,
            'sl_pct': sl_pct,
            'time_exit_disabled': (max_hold_bars is None),
            'adx_threshold': adx_threshold,
            'htf_require': htf_require,
            'upper_wick_pct': (float(upper_wick_pct) if upper_wick_pct is not None else None)
        }
    }

    # attach risk engine summary if enabled
    if enable_risk_engine and risk_engine is not None:
        try:
            result['risk_events'] = risk_engine.summary()
        except Exception:
            result['risk_events'] = {}

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    outpath = LOG_DIR / f'backtest_{timestamp}.json'
    trades_path = LOG_DIR / f'backtest_trades_{timestamp}.csv'
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump({'result': result, 'trades': trades, 'gate_logs': gate_logs}, f, indent=2, ensure_ascii=False)
    # write trades
    if trades:
        df_trades = pd.DataFrame(trades)
        df_trades.to_csv(trades_path, index=False)

    print('Backtest finished:')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print('Trades written to', trades_path)
    print('Details written to', outpath)
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--months', type=int, default=5)
    p.add_argument('--slippage', type=float, default=0.001)
    p.add_argument('--fee', type=float, default=0.0005)
    p.add_argument('--time-exit-min', type=int, default=60)
    p.add_argument('--vol-mult', type=float, default=1.5)
    p.add_argument('--stop-rel', type=float, default=1.5)
    p.add_argument('--risk-pct', type=float, default=None)
    p.add_argument('--mode', choices=['dce','meanrev','regime'], default='dce', help='Backtest mode: dce (breakout) or meanrev (RSI-based) or regime (switch)')
    p.add_argument('--vol-entry-mult', type=float, default=1.5, help='Minimum volume multiplier (last_vol >= vol_ma * vol_entry_mult) required for entry')
    p.add_argument('--tp-pct', type=float, default=None, help='Fixed take-profit percent (e.g. 0.015 for 1.5%)')
    p.add_argument('--sl-pct', type=float, default=None, help='Fixed stop-loss percent (e.g. 0.008 for 0.8%)')
    p.add_argument('--disable-rsi-veto', action='store_true', help='Disable RSI overbought veto in decider')
    p.add_argument('--atr-trail', action='store_true', help='Enable ATR-based trailing stop (updates stop_price each bar)')
    p.add_argument('--disable-time-exit', action='store_true', help='Explicitly disable time-based exit')
    p.add_argument('--gatekeeper-only', action='store_true', help='Enable strict gatekeeper gating (2-of-3) before entries')
    p.add_argument('--early-abort', action='store_true', help='Enable early-abort rule to exit early when immediate adverse movement occurs')
    p.add_argument('--early-abort-pct', type=float, default=0.005, help='Early abort threshold percent (e.g. 0.005 for 0.5%)')
    p.add_argument('--early-abort-bars', type=int, default=2, help='Number of bars after entry to consider early-abort')
    p.add_argument('--adx-threshold', type=float, default=25.0, help='Minimum ADX to consider trend (regime detection and gatekeeper).')
    p.add_argument('--htf-require', choices=['any','both'], default='any', help='HTF requirement for gatekeeper: any (1h OR 4h) or both (1h AND 4h).')
    p.add_argument('--upper-wick-pct', type=float, default=None, help='Max allowed upper wick percent for entry candle (0-1). If omitted, filter disabled.')
    p.add_argument('--enable-risk-engine', action='store_true', help='Enable portfolio-level risk engine')
    p.add_argument('--risk-daily-loss-pct', type=float, default=1.0, help='Daily realized loss percent threshold to block entries (e.g., 1.0 for 1%)')
    p.add_argument('--risk-max-dd-pct', type=float, default=10.0, help='Max drawdown percent to trigger risk reduction (e.g., 10.0)')
    p.add_argument('--risk-consec-losses', type=int, default=3, help='Consecutive losing trades threshold')
    p.add_argument('--risk-consec-mult', type=float, default=0.5, help='Multiplier to apply to risk_pct on consecutive loss/DD triggers (e.g., 0.5)')
    p.add_argument('--risk-initial-pct', type=float, default=None, help='Initial per-trade risk pct for risk engine (overrides RISK_PER_TRADE_PCT)')
    p.add_argument('--risk-min-pct', type=float, default=0.05, help='Minimum allowed risk pct floor (absolute, e.g. 0.05)')
    p.add_argument('--risk-max-reductions', type=int, default=5, help='Maximum number of multiplicative reduction steps allowed')
    p.add_argument('--risk-recovery-step-pct', type=float, default=0.1, help='Recovery step as fraction of initial risk (e.g. 0.1 = 10% of initial)')
    p.add_argument('--risk-recovery-consec-wins', type=int, default=3, help='Consecutive winning trades required to trigger a recovery step')
    p.add_argument('--use-portfolio-manager', action='store_true', help='Use PortfolioManager for sizing and decision aggregation (agent+rule merging)')
    p.add_argument('--pm-agent-weight', type=float, default=0.6, help='Agent weight in PortfolioManager')
    p.add_argument('--pm-rule-weight', type=float, default=0.2, help='Rule weight in PortfolioManager')
    p.add_argument('--agent-proposed-risk-mult', type=float, default=2.0, help='Agent proposed risk as multiplier of initial_risk_pct (e.g., 2.0 -> agent proposes 2x initial)')
    p.add_argument('--risk-cooldown-days', type=int, default=0, help='Cooldown days between risk reductions')
    args = p.parse_args()
    time_exit_value = 0 if args.disable_time_exit else args.time_exit_min
    run_backtest(months=args.months, slippage_pct=args.slippage, fee_pct=args.fee, time_based_exit_min=time_exit_value, vol_mult=args.vol_mult, stop_rel_default=args.stop_rel, risk_pct=args.risk_pct, mode=args.mode, vol_entry_mult=args.vol_entry_mult, tp_pct=args.tp_pct, sl_pct=args.sl_pct, disable_rsi_veto=args.disable_rsi_veto, atr_trail=args.atr_trail, gatekeeper_only=args.gatekeeper_only, early_abort=args.early_abort, early_abort_pct=args.early_abort_pct, early_abort_bars=args.early_abort_bars, adx_threshold=args.adx_threshold, htf_require=args.htf_require, upper_wick_pct=args.upper_wick_pct, enable_risk_engine=args.enable_risk_engine, risk_daily_loss_pct=args.risk_daily_loss_pct, risk_max_dd_pct=args.risk_max_dd_pct, risk_consec_losses=args.risk_consec_losses, risk_consec_mult=args.risk_consec_mult, risk_initial_pct=args.risk_initial_pct, risk_min_pct=args.risk_min_pct, risk_max_reduction_steps=args.risk_max_reductions, risk_recovery_step_pct=args.risk_recovery_step_pct, risk_recovery_consec_wins=args.risk_recovery_consec_wins, use_portfolio_manager=args.use_portfolio_manager, pm_agent_weight=args.pm_agent_weight, pm_rule_weight=args.pm_rule_weight, agent_proposed_risk_mult=args.agent_proposed_risk_mult, risk_cooldown_days=args.risk_cooldown_days)
