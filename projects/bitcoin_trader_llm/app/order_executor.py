"""TWAP order executor

Provides a simple, safe TWAP (time-weighted average price) order executor used by Executor.

Design goals:
- Run in DRY_RUN mode without sending real orders; record child executions in DB for traceability
- Use orderbook-based depth checks (policy.compute_max_notional_for_slippage) to cap per-slice notional
- Support configurable params via payload['twap_params'] or environment/config defaults
- Be conservative by default and provide clear logging/trace for later analysis

Note: This is an execution helper. Integration with risk engine / portfolio manager should be
performed at the proposal stage; this module focuses on order-splitting and safe sending.
"""
from __future__ import annotations

import logging
import time
import math
import os
from typing import Optional, Dict, Any

from app import config
from app.db_logger import DBLogger
from app.policy import compute_max_notional_for_slippage, estimate_slippage_for_notional

try:
    import pyupbit
except Exception:
    pyupbit = None

logger = logging.getLogger('twap')


class TWAPExecutor:
    def __init__(self, db: DBLogger, dry_run: bool = True, exchange_client: Optional[Any] = None):
        self.db = db
        self.dry_run = bool(dry_run)
        # exchange_client expected to be pyupbit module or a compatible wrapper
        self.exchange_client = exchange_client or (pyupbit if pyupbit is not None else None)

    def _default_params(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        p = params.copy() if isinstance(params, dict) else {}
        # total duration in seconds (default 5 minutes)
        p.setdefault('duration_sec', int(os.getenv('TWAP_DURATION_SEC', '300')))
        # number of slices - default 6
        p.setdefault('slices', int(os.getenv('TWAP_SLICES', '6')))
        # per-slice limit order price offset (fractional). For buys, price = best_ask*(1+offset).
        p.setdefault('limit_offset_pct', float(os.getenv('TWAP_LIMIT_OFFSET_PCT', '0.001')))
        # maximum acceptable slippage fraction per slice
        p.setdefault('max_slippage_pct', float(os.getenv('MAX_SLIPPAGE_PCT', str(config.MAX_SLIPPAGE_PCT))))
        # minimum KRW per slice
        p.setdefault('min_slice_krw', int(os.getenv('TWAP_MIN_SLICE_KRW', str(config.MIN_ORDER_KRW))))
        # timeout (seconds) to wait for a passive limit fill before falling back
        p.setdefault('limit_timeout_sec', int(os.getenv('TWAP_LIMIT_TIMEOUT_SEC', '8')))
        # whether to fallback to market order when limit not filled (last slice only if True)
        p.setdefault('market_fallback', True)
        # whether to use orderbook depth checks
        p.setdefault('use_orderbook_depth', True)
        return p

    def execute_twap(self, signal_id: int, side: str, total_notional_krw: float, entry_price: float, total_units: float, params: Optional[Dict[str, Any]] = None, market_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute TWAP split for a signal.

        Args:
            signal_id: parent signal id for DB linkage
            side: 'buy'|'sell'
            total_notional_krw: desired KRW notional to execute
            entry_price: price hint used as best_price fallback
            total_units: units corresponding to total_notional_krw (for reference)
            params: TWAP customization dict
            market_state: optional pre-fetched market_state (e.g., {'orderbook':..., 'best_price': ...})

        Returns summary dict with per-slice records and final status.
        """
        p = self._default_params(params)
        duration = int(p['duration_sec'])
        slices = int(p['slices']) if int(p['slices']) > 0 else 1
        slice_interval = float(duration) / float(slices)
        limit_offset = float(p['limit_offset_pct'])
        max_slippage = float(p['max_slippage_pct'])
        min_slice_krw = int(p['min_slice_krw'])
        limit_timeout = int(p['limit_timeout_sec'])
        market_fallback = bool(p['market_fallback'])
        use_depth = bool(p['use_orderbook_depth'])

        logger.info('TWAP start signal=%s side=%s notional=%s slices=%s dur=%ss', signal_id, side, total_notional_krw, slices, duration)

        remaining = float(total_notional_krw)
        per_slice_target = float(total_notional_krw) / float(slices)
        slices_executed = []

        start_ts = time.time()
        for i in range(slices):
            if remaining <= 0:
                break
            slice_no = i + 1
            # default planned slice (equal-weight)
            planned = per_slice_target if i < slices - 1 else remaining
            planned = min(planned, remaining)

            # fetch orderbook preference: market_state -> pyupbit
            orderbook = None
            best_price = float(entry_price or 0.0)
            try:
                if market_state and isinstance(market_state, dict):
                    if 'orderbook' in market_state:
                        orderbook = market_state.get('orderbook')
                    if 'best_price' in market_state:
                        best_price = float(market_state.get('best_price') or best_price)
                if orderbook is None and self.exchange_client is not None:
                    try:
                        orderbook = pyupbit.get_orderbook(config.SYMBOL) if pyupbit is not None else None
                    except Exception:
                        orderbook = None
                # try to get a best price from orderbook
                if orderbook and isinstance(orderbook, dict):
                    asks = orderbook.get('asks') or []
                    bids = orderbook.get('bids') or []
                    if side.lower().startswith('b') and len(asks) > 0:
                        best_price = float(asks[0].get('price') or best_price)
                    elif side.lower().startswith('s') and len(bids) > 0:
                        best_price = float(bids[0].get('price') or best_price)
            except Exception:
                logger.debug('TWAP: failed to fetch orderbook or best_price, using entry_price')

            # compute allowed notional under slippage cap
            allowed = None
            if use_depth:
                try:
                    allowed = compute_max_notional_for_slippage(orderbook, side, max_slippage, best_price)
                except Exception:
                    allowed = None

            # decide actual slice notional
            if allowed is None:
                # no depth estimate - be conservative and reduce planned by half
                actual_notional = min(planned * 0.5, remaining)
                reduced_reason = 'no_depth_info_reduced_50pct'
            else:
                # can execute up to `allowed` without exceeding max_slippage
                actual_notional = min(planned, allowed, remaining)
                reduced_reason = None

            # if actual_notional is too small -> block or skip
            if actual_notional < min_slice_krw:
                logger.info('TWAP slice %s: actual_notional %s below min_slice %s - stopping', slice_no, actual_notional, min_slice_krw)
                slices_executed.append({'slice': slice_no, 'status': 'skipped_too_small', 'requested': planned, 'allowed': allowed, 'actual': 0, 'reason': 'too_small'})
                break

            # choose order type and limit price
            if side.lower().startswith('b'):
                limit_price = best_price * (1.0 + limit_offset)
            else:
                limit_price = best_price * (1.0 - limit_offset)

            # Execution simulation / actual placement
            slice_units = None
            slice_price = None
            slice_status = 'failed'
            slippage_note = None

            # DRY_RUN path: simulate realistic fill using estimate_slippage_for_notional
            if self.dry_run or self.exchange_client is None:
                try:
                    slippage_frac = estimate_slippage_for_notional(orderbook, side, actual_notional, best_price)
                except Exception:
                    slippage_frac = None
                if slippage_frac is None:
                    # assume executed at limit price
                    slice_price = float(limit_price)
                    slippage_note = 'no_slippage_estimate'
                else:
                    if side.lower().startswith('b'):
                        slice_price = float(best_price * (1.0 + float(slippage_frac)))
                    else:
                        slice_price = float(best_price * (1.0 - float(slippage_frac)))
                    slippage_note = f'estimated_slippage_{slippage_frac:.6f}'

                slice_units = float(actual_notional) / float(slice_price) if slice_price > 0 else 0.0
                child_res = {
                    'simulated': True,
                    'twap_slice': slice_no,
                    'slice_notional_krw': actual_notional,
                    'price': slice_price,
                    'amount': slice_units,
                    'side': side,
                    'slippage_note': slippage_note,
                    'timestamp': time.time()
                }
                try:
                    exec_id = self.db.insert_execution(signal_id=signal_id, side=side, price=slice_price, amount=slice_units, fee=0.0, status='simulated', result=child_res)
                    # mark parent signal as simulated/in-progress on first slice
                    if slice_no == 1:
                        self.db.update_signal_status(signal_id, 'simulated')
                except Exception:
                    logger.exception('TWAP: failed to insert simulated execution')
                    exec_id = None
                slice_status = 'simulated'
            else:
                # real execution path: submit limit order, wait up to limit_timeout, then optionally fallback
                try:
                    upbit = None
                    if self.exchange_client is not None:
                        upbit = self.exchange_client.Upbit(os.getenv('UPBIT_ACCESS_KEY'), os.getenv('UPBIT_SECRET_KEY'))
                    if upbit is None:
                        raise RuntimeError('exchange client not configured')

                    # For buys, pyupbit.buy_limit_order(ticker, price, volume) expects volume in units
                    vol = actual_notional / limit_price if limit_price > 0 else 0.0
                    order = None
                    if side.lower().startswith('b'):
                        order = upbit.buy_limit_order(config.SYMBOL, limit_price, vol)
                    else:
                        order = upbit.sell_limit_order(config.SYMBOL, limit_price, vol)

                    # record submission
                    child_res = {'simulated': False, 'twap_slice': slice_no, 'requested_notional_krw': actual_notional, 'limit_price': limit_price, 'order': order, 'timestamp': time.time()}
                    exec_id = self.db.insert_execution(signal_id=signal_id, side=side, price=limit_price, amount=vol, fee=0.0, status='submitted', result=child_res)
                    # naive wait-loop to check fill (short polling)
                    waited = 0
                    filled = False
                    while waited < limit_timeout:
                        time.sleep(1)
                        waited += 1
                        # TODO: check order status via upbit.get_order or similar -> not implemented here
                        # assume not filled; in production, poll order status
                        pass
                    if not filled and market_fallback and (slice_no == slices or p.get('market_fallback', True)):
                        # fallback to market order for remainder of this slice
                        if side.lower().startswith('b'):
                            order2 = upbit.buy_market_order(config.SYMBOL, actual_notional)
                            child_res['fallback_market'] = order2
                            # record as executed
                            exec_id = self.db.insert_execution(signal_id=signal_id, side=side, price=entry_price, amount=actual_notional/entry_price if entry_price>0 else 0.0, fee=0.0, status='submitted', result=child_res)
                        else:
                            # sell market order
                            order2 = upbit.sell_market_order(config.SYMBOL, actual_notional/entry_price if entry_price>0 else 0.0)
                            child_res['fallback_market'] = order2
                            exec_id = self.db.insert_execution(signal_id=signal_id, side=side, price=entry_price, amount=actual_notional/entry_price if entry_price>0 else 0.0, fee=0.0, status='submitted', result=child_res)
                    slice_status = 'submitted'
                except Exception:
                    logger.exception('TWAP live execution failed')
                    slice_status = 'failed'

            slices_executed.append({'slice': slice_no, 'status': slice_status, 'requested': planned, 'actual': actual_notional, 'price': slice_price, 'units': slice_units, 'reason': reduced_reason})

            remaining -= actual_notional

            # if remaining is tiny - stop
            if remaining <= 0 or remaining < float(min_slice_krw):
                break

            # sleep until next slice
            if i < slices - 1:
                try:
                    time.sleep(max(0.0, float(slice_interval)))
                except Exception:
                    pass

        total_executed = float(total_notional_krw) - float(remaining)
        summary = {
            'signal_id': signal_id,
            'requested_notional_krw': total_notional_krw,
            'executed_notional_krw': total_executed,
            'slices_planned': slices,
            'slices_executed': len([s for s in slices_executed if s.get('status') not in ('skipped_too_small', 'failed')]),
            'slices_detail': slices_executed,
            'remaining_notional_krw': remaining,
            'duration_sec': time.time() - start_ts
        }
        logger.info('TWAP complete signal=%s executed=%s remaining=%s', signal_id, total_executed, remaining)
        return summary


__all__ = ['TWAPExecutor']
