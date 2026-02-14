"""
Allocation evaluator utilities.
Provides evaluate_allocation(...) which computes desired/capped notional, additional needed vs current exposure,
and estimates slippage/fees/capital cost using orderbook data.
"""
from __future__ import annotations

import logging
from typing import Dict, Any, Optional

from app import config
from app.db_logger import DBLogger
from app.policy import compute_max_notional_for_slippage, estimate_slippage_for_notional

logger = logging.getLogger('allocation')

# fee estimate (exchange taker fee approx)
FEE_PCT = float(getattr(config, 'ESTIMATED_FEE_PCT', 0.001))  # default 0.1%


def evaluate_allocation(db: DBLogger, run_id: str, symbol: str, suggested_risk_pct: float, stop_pct: float, entry_price: float, orderbook: Optional[dict] = None) -> Dict[str, Any]:
    """Evaluate allocation proposal and return diagnostic dict.

    Returns keys:
      total_equity, invested, available, desired_notional, capped_notional, final_notional,
      additional_needed, scale, est_slippage, est_fees, capital_cost
    """
    try:
        cron = db.get_cron_run(run_id) or {}
        total_equity = float(cron.get('total_equity_krw') or 0.0)
        if total_equity <= 0:
            # fallback to env
            total_equity = float(getattr(config, 'SIM_EQUITY_KRW', 1000000))
        invested = float(db.get_invested(run_id) or 0.0)
        reserved = float(cron.get('reserved_krw') or 0.0)
        available = max(0.0, total_equity - invested - reserved)

        desired_dollar_risk = float(total_equity) * (float(suggested_risk_pct) / 100.0)
        stop_frac = max(float(stop_pct) / 100.0, 1e-6)
        desired_notional = desired_dollar_risk / stop_frac if stop_frac > 0 else 0.0

        cap = float(total_equity) * (float(config.MAX_SINGLE_ORDER_PCT) / 100.0)
        capped_notional = min(desired_notional, cap)

        current_sym = float(db.get_symbol_exposure(run_id, symbol) or 0.0)
        additional_needed = max(0.0, capped_notional - current_sym)

        # orderbook depth cap
        max_allowed = None
        if orderbook is not None:
            try:
                max_allowed = compute_max_notional_for_slippage(orderbook, 'buy', float(config.MAX_SLIPPAGE_PCT), best_price=entry_price)
            except Exception:
                max_allowed = None
        if max_allowed is not None and max_allowed < capped_notional:
            capped_notional = float(max_allowed)
            additional_needed = max(0.0, capped_notional - current_sym)

        if additional_needed <= available:
            scale = 1.0
            final_notional = capped_notional
        else:
            scale = (available / capped_notional) if capped_notional > 0 else 0.0
            final_notional = capped_notional * scale
            additional_needed = max(0.0, final_notional - current_sym)

        est_slippage = 0.0
        try:
            est_slippage = float(estimate_slippage_for_notional(orderbook, 'buy', final_notional, best_price=entry_price) or 0.0)
        except Exception:
            est_slippage = 0.0
        est_fees = final_notional * float(FEE_PCT)
        capital_cost = final_notional * est_slippage + est_fees

        return {
            'total_equity': total_equity,
            'invested': invested,
            'available': available,
            'desired_notional': desired_notional,
            'capped_notional': capped_notional,
            'final_notional': final_notional,
            'additional_needed': additional_needed,
            'scale': scale,
            'est_slippage': est_slippage,
            'est_fees': est_fees,
            'capital_cost': capital_cost
        }
    except Exception:
        logger.exception('evaluate_allocation failed')
        return {}
