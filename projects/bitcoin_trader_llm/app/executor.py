"""
Executor module — processes pending signals and executes them.
This module is intended to be invoked by the OpenClaw agent (executor agent) which has permission to perform trade actions.

Behavior:
 - Read pending signals from DB
 - For each signal: validate policy, compute sizing via policy.size_from_risk, then either simulate (DRY_RUN) or place order via pyupbit
 - Record execution results in llm_executions and update llm_signals status

NOTE: This module does not auto-run on import.
"""
import logging
import time
import json
import os
from typing import Dict, Any

from app import config
from app.db_logger import DBLogger
from app.policy import apply_policy_caps, is_safe_to_execute, size_from_risk

logger = logging.getLogger('executor')

try:
    import pyupbit
except Exception:
    pyupbit = None


class Executor:
    def __init__(self, db: DBLogger = None):
        self.db = db or DBLogger()

    def process_pending(self, limit: int = 10):
        pending = self.db.get_pending_signals(limit=limit)
        if not pending:
            logger.info('No pending signals')
            return
        for sig in pending:
            try:
                self._process_single(sig)
            except Exception:
                logger.exception('Error processing signal id=%s', sig.get('id'))

    def _process_single(self, sig: Dict[str, Any]):
        signal_id = sig.get('id')
        run_id = sig.get('run_id')
        payload = sig.get('payload_json')
        # payload may be JSON string or already parsed
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        action = payload.get('action', 'hold')
        pct = int(payload.get('pct', 0) or 0)
        confidence = float(payload.get('confidence', 0.0) or 0.0)

        # get equity estimate (best-effort)
        equity = self._get_equity_estimate()
        daily_loss_pct = self._get_daily_loss_pct()

        ok, reason = is_safe_to_execute(equity, daily_loss_pct)
        if not ok:
            logger.warning('Execution blocked by policy: %s', reason)
            self.db.update_signal_status(signal_id, 'blocked')
            return

        # preserve agent intent and other metadata from the stored payload
        if not isinstance(payload, dict):
            payload = {}
        payload['action'] = action
        payload['pct'] = pct
        payload['confidence'] = confidence

        # apply caps (this returns a safe copy)
        payload = apply_policy_caps(payload, equity)

        if payload.get('action') == 'hold' or payload.get('pct', 0) == 0:
            logger.info('Signal is hold or pct 0 - marking as ignored')
            self.db.update_signal_status(signal_id, 'ignored')
            return

        # compute sizing - naive: assume stop is derived from payload stop_rel (ATR multiple) or use small buffer
        entry_price = self._get_current_price()
        stop_rel = payload.get('stop_rel')
        if isinstance(stop_rel, (int, float)):
            # interpret as ATR multiple if provided
            atr = self._get_atr()
            stop_price = entry_price - (atr * float(stop_rel))
        else:
            stop_price = entry_price * 0.97  # default 3% stop if not provided

        sizing = size_from_risk(equity, entry_price, stop_price, None)
        if sizing['position_krw'] <= 0:
            logger.info('Computed position size too small - ignoring signal')
            self.db.update_signal_status(signal_id, 'ignored')
            return

        # --- slippage / orderbook-based cap (safety) ---
        slippage_note = None
        try:
            # attempt to fetch fresh orderbook if pyupbit available
            orderbook = None
            try:
                if pyupbit is not None:
                    orderbook = pyupbit.get_orderbook(config.SYMBOL)
            except Exception:
                orderbook = None

            # compute max allowed notional under slippage constraint
            max_allowed = None
            try:
                from app.policy import compute_max_notional_for_slippage
                max_allowed = compute_max_notional_for_slippage(orderbook, payload.get('action', 'buy'), float(config.MAX_SLIPPAGE_PCT), best_price=entry_price)
            except Exception:
                max_allowed = None

            desired_notional = float(sizing['position_krw'])
            # if not enough depth or cap smaller than desired, scale down
            if max_allowed is None:
                # no depth information -> be conservative: reduce to half of computed position to limit market impact
                reduced_notional = desired_notional * 0.5
                if reduced_notional < config.MIN_ORDER_KRW:
                    logger.warning('Insufficient depth estimate and reduced notional below MIN_ORDER_KRW -> ignoring')
                    self.db.update_signal_status(signal_id, 'blocked')
                    return
                slippage_note = 'reduced_due_to_unknown_depth'
                sizing['position_krw'] = reduced_notional
                sizing['amount'] = reduced_notional / entry_price
            else:
                # max_allowed is KRW allowed under slippage thresh
                if max_allowed < desired_notional:
                    if max_allowed < config.MIN_ORDER_KRW:
                        logger.warning('Max allowed notional under slippage limit below MIN_ORDER_KRW -> blocking signal')
                        self.db.update_signal_status(signal_id, 'blocked')
                        return
                    # scale down to max_allowed
                    slippage_note = f'slippage_capped_to_{int(max_allowed)}'
                    sizing['position_krw'] = float(max_allowed)
                    sizing['amount'] = float(max_allowed) / entry_price
        except Exception:
            logger.exception('Slippage cap check failed; proceeding with original sizing')

        # If TWAP execution requested, delegate to TWAPExecutor
        if payload.get('execution') == 'twap' or payload.get('use_twap', False):
            try:
                from app.order_executor import TWAPExecutor
                te = TWAPExecutor(self.db, dry_run=config.DRY_RUN, exchange_client=pyupbit)
                twap_result = te.execute_twap(signal_id=signal_id, side=payload.get('action'), total_notional_krw=sizing['position_krw'], entry_price=entry_price, total_units=sizing['amount'], params=payload.get('twap_params', {}), market_state={'orderbook': orderbook, 'best_price': entry_price})
                logger.info('TWAP result summary: %s', twap_result)
                # record twap summary
                try:
                    self.db.insert_twap_run(run_id=run_id, signal_id=signal_id, total_requested_krw=sizing['position_krw'], executed_krw=twap_result.get('executed_notional_krw', 0.0), slices_planned=twap_result.get('slices_planned', 0), slices_executed=twap_result.get('slices_executed', 0), summary=twap_result)
                except Exception:
                    logger.exception('Failed to insert twap_run summary')
                # update parent signal status to indicate handled
                if config.DRY_RUN:
                    self.db.update_signal_status(signal_id, 'simulated')
                else:
                    self.db.update_signal_status(signal_id, 'submitted')
                return
            except Exception:
                logger.exception('TWAP execution failed; falling back to normal execution')

        # simulate or execute
        if config.DRY_RUN:
            logger.info(f"DRY_RUN: would execute {payload.get('action')} size_krw={sizing['position_krw']:.0f}")
            result = {
                'simulated': True,
                'side': payload.get('action'),
                'price': entry_price,
                'amount': sizing['amount'],
                'fee': 0.0,
                'executed_at': None,
                'slippage_note': slippage_note,
                'agent_intent': payload.get('agent_intent')
            }
            exec_id = self.db.insert_execution(signal_id=signal_id, side=payload.get('action'), price=entry_price, amount=sizing['amount'], fee=0.0, status='simulated', result=result)
            self.db.update_signal_status(signal_id, 'simulated')
        else:
            # real execution path
            if pyupbit is None:
                logger.error('pyupbit not available - cannot execute')
                self.db.update_signal_status(signal_id, 'failed')
                return
            try:
                # assume Upbit client keys available in env
                upbit = pyupbit.Upbit(os.getenv('UPBIT_ACCESS_KEY'), os.getenv('UPBIT_SECRET_KEY'))
                if payload.get('action') == 'buy':
                    krw_amount = sizing['position_krw']
                    order = upbit.buy_market_order(config.SYMBOL, krw_amount)
                else:
                    # sell - amount in units
                    amount = sizing['amount']
                    order = upbit.sell_market_order(config.SYMBOL, amount)
                # record order
                res_obj = order or {}
                try:
                    if isinstance(res_obj, dict):
                        res_obj['agent_intent'] = payload.get('agent_intent')
                    else:
                        res_obj = {'order': res_obj, 'agent_intent': payload.get('agent_intent')}
                except Exception:
                    res_obj = {'order': str(res_obj), 'agent_intent': payload.get('agent_intent')}
                exec_id = self.db.insert_execution(signal_id=signal_id, side=payload.get('action'), price=entry_price, amount=sizing['amount'], fee=0.0, status='submitted', result=res_obj)
                self.db.update_signal_status(signal_id, 'submitted')
            except Exception:
                logger.exception('Order placement failed')
                self.db.update_signal_status(signal_id, 'failed')

    # helper stubs - best-effort implementations
    def _get_equity_estimate(self) -> float:
        """
        Estimate equity in KRW.
        Priority:
          1) Use UPBIT keys to fetch balances and convert non-KRW assets to KRW using current market prices.
          2) Fallback to SIM_EQUITY_KRW environment variable.
        """
        try:
            access = os.getenv('UPBIT_ACCESS_KEY')
            secret = os.getenv('UPBIT_SECRET_KEY')
            if access and secret and pyupbit is not None:
                try:
                    upbit = pyupbit.Upbit(access, secret)
                    balances = upbit.get_balances() or []
                    total_krw = 0.0
                    for b in balances:
                        # upbit balance entries typically have 'currency' and 'balance' keys
                        currency = b.get('currency') or b.get('unit_currency') or None
                        bal = b.get('balance') or b.get('available') or 0
                        try:
                            bal = float(bal)
                        except Exception:
                            bal = 0.0
                        if not currency:
                            continue
                        if currency == 'KRW':
                            total_krw += bal
                        else:
                            ticker = f"KRW-{currency}"
                            price = pyupbit.get_current_price(ticker) or 0.0
                            try:
                                total_krw += bal * float(price)
                            except Exception:
                                # skip if price unavailable
                                continue
                    if total_krw > 0:
                        return float(total_krw)
                except Exception:
                    logger.exception('Failed to fetch balances from Upbit; falling back to SIM_EQUITY_KRW')
            # fallback
            return float(os.getenv('SIM_EQUITY_KRW', '1000000'))
        except Exception:
            logger.exception('get_equity_estimate failed; using SIM_EQUITY_KRW')
            return float(os.getenv('SIM_EQUITY_KRW', '1000000'))

    def _get_daily_loss_pct(self) -> float:
        # TODO: compute using trades in DB
        return 0.0

    def _get_current_price(self) -> float:
        # lightweight current price lookup - fallback
        try:
            import pyupbit
            p = pyupbit.get_current_price(config.SYMBOL)
            return float(p) if p else 0.0
        except Exception:
            return 0.0

    def _get_atr(self) -> float:
        # best-effort fetch last ATR from indicators if available; fallback constant
        return float(os.getenv('SIM_ATR', '50000'))


if __name__ == '__main__':
    # quick runner for local testing - will process pending signals
    logging.basicConfig(level=logging.DEBUG)
    ex = Executor()
    ex.process_pending(limit=10)
