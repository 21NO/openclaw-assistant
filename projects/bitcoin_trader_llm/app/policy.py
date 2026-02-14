"""
Policy and sizing utilities.
Provides functions to compute position sizing, apply policy caps, and validate suggested signals against safety rules.
"""
import logging
from typing import Dict, Any

from app import config

logger = logging.getLogger('policy')


def size_from_risk(equity_krw: float, entry_price: float, stop_price: float, risk_pct: float = None) -> Dict[str, Any]:
    """
    Calculate position size (KRW and amount) given equity, entry and stop levels and a risk percentage.
    Returns dict with keys: position_krw, amount, actual_risk_pct
    """
    if risk_pct is None:
        risk_pct = config.RISK_PER_TRADE_PCT
    risk_krw = equity_krw * (risk_pct / 100.0)
    # avoid division by zero
    unit_risk = max(entry_price - stop_price, 1e-6)
    position_krw = risk_krw / unit_risk * entry_price if unit_risk > 0 else 0
    # position_krw cannot exceed MAX_SINGLE_ORDER_PCT of equity
    cap = equity_krw * (config.MAX_SINGLE_ORDER_PCT / 100.0)
    if position_krw > cap:
        position_krw = cap
    # enforce minimum
    if position_krw < config.MIN_ORDER_KRW:
        return {'position_krw': 0, 'amount': 0, 'actual_risk_pct': 0}
    amount = position_krw / entry_price
    actual_risk_pct = (position_krw * unit_risk / entry_price) / equity_krw * 100 if equity_krw>0 else 0
    return {'position_krw': position_krw, 'amount': amount, 'actual_risk_pct': actual_risk_pct}


def apply_policy_caps(suggestion: Dict[str, Any], equity_krw: float) -> Dict[str, Any]:
    """
    Apply policy caps to the suggested signal dict (mutates a copy and returns it).
    Ensures suggested pct and order size are within configured limits.
    suggestion keys expected: action, pct (0-100), stop_rel (optionally)
    """
    out = suggestion.copy()
    try:
        pct = int(out.get('pct', 0))
        pct = max(0, min(100, pct))
        out['pct'] = pct
        # cap single order as pct of equity
        max_pct = int(config.MAX_SINGLE_ORDER_PCT)
        if pct > max_pct:
            out['pct'] = max_pct
            out['policy_note'] = f'capped_pct_to_{max_pct}'
    except Exception:
        logger.exception('apply_policy_caps error parsing pct')
    return out


def is_safe_to_execute(equity_krw: float, daily_loss_pct: float) -> (bool, str):
    """
    Quick check whether the system is allowed to execute trades.
    Returns (allowed, reason)
    """
    if daily_loss_pct >= config.MAX_DAILY_LOSS_PCT:
        return False, f'daily_loss_limit_reached ({daily_loss_pct:.2f}%)'
    return True, 'ok'


# --- slippage / orderbook helpers ---

def _normalize_orderbook(ob: Any) -> dict:
    """Normalize orderbook input into a dict with 'asks' and 'bids' lists.
    Supports multiple input shapes:
      - pyupbit.get_orderbook() structure (list with 'orderbook_units' containing ask_price/ask_size/bid_price/bid_size)
      - already-normalized dict with 'asks' and 'bids' lists of {price,size}
      - fallback: empty lists

    Each returned entry contains keys: price (float), size (float), krw (price*size)
    """
    if not ob:
        return {'asks': [], 'bids': []}
    ob0 = ob[0] if isinstance(ob, list) and len(ob) > 0 else ob
    asks = []
    bids = []

    # pyupbit style: 'orderbook_units' list with ask_price/ask_size/bid_price/bid_size
    if isinstance(ob0, dict) and ob0.get('orderbook_units'):
        units = ob0.get('orderbook_units')
        for u in units:
            try:
                if 'ask_price' in u and u.get('ask_price') is not None:
                    p = float(u.get('ask_price') or 0)
                    s = float(u.get('ask_size') or 0)
                    asks.append({'price': p, 'size': s, 'krw': p * s})
                if 'bid_price' in u and u.get('bid_price') is not None:
                    p = float(u.get('bid_price') or 0)
                    s = float(u.get('bid_size') or 0)
                    bids.append({'price': p, 'size': s, 'krw': p * s})
            except Exception:
                continue
        asks = sorted(asks, key=lambda x: x['price'])
        bids = sorted(bids, key=lambda x: -x['price'])
        return {'asks': asks, 'bids': bids}

    # already normalized dict provided (expects 'asks' and/or 'bids')
    if isinstance(ob0, dict) and (ob0.get('asks') or ob0.get('bids')):
        try:
            raw_asks = ob0.get('asks', []) or []
            raw_bids = ob0.get('bids', []) or []
            for a in raw_asks:
                try:
                    p = float(a.get('price') if isinstance(a, dict) else a[0])
                    s = float(a.get('size') if isinstance(a, dict) else a[1])
                    asks.append({'price': p, 'size': s, 'krw': p * s})
                except Exception:
                    continue
            for b in raw_bids:
                try:
                    p = float(b.get('price') if isinstance(b, dict) else b[0])
                    s = float(b.get('size') if isinstance(b, dict) else b[1])
                    bids.append({'price': p, 'size': s, 'krw': p * s})
                except Exception:
                    continue
            asks = sorted(asks, key=lambda x: x['price'])
            bids = sorted(bids, key=lambda x: -x['price'])
            return {'asks': asks, 'bids': bids}
        except Exception:
            return {'asks': [], 'bids': []}

    # unknown format
    return {'asks': [], 'bids': []}


def estimate_slippage_for_notional(orderbook: Any, side: str, notional_krw: float, best_price: float = None) -> float | None:
    """Estimate expected slippage (fraction) when executing `notional_krw` against the provided orderbook.
    Returns slippage fraction (e.g. 0.002) or None if not enough depth.
    """
    try:
        nb = _normalize_orderbook(orderbook)
        if side.lower().startswith('b'):
            levels = nb['asks']
            if not levels:
                return None
            best = levels[0]['price'] if best_price is None else float(best_price)
            remaining = float(notional_krw)
            sum_amount = 0.0
            sum_krw = 0.0
            for lvl in levels:
                p = float(lvl['price'])
                krw_available = float(lvl['krw'])
                if krw_available <= 0:
                    continue
                if krw_available >= remaining:
                    amount_needed = remaining / p
                    sum_amount += amount_needed
                    sum_krw += amount_needed * p
                    remaining = 0.0
                    break
                else:
                    sum_amount += lvl['size']
                    sum_krw += krw_available
                    remaining -= krw_available
            if remaining > 0:
                # not enough depth
                return None
            if sum_amount <= 0:
                return None
            weighted_avg = float(sum_krw) / float(sum_amount)
            slippage = (weighted_avg / best) - 1.0
            return float(slippage)
        else:
            # sell side -> consume bids
            levels = _normalize_orderbook(orderbook)['bids']
            if not levels:
                return None
            best = levels[0]['price'] if best_price is None else float(best_price)
            remaining = float(notional_krw)
            sum_amount = 0.0
            sum_krw = 0.0
            for lvl in levels:
                p = float(lvl['price'])
                krw_available = float(lvl['krw'])
                if krw_available <= 0:
                    continue
                if krw_available >= remaining:
                    amount_needed = remaining / p
                    sum_amount += amount_needed
                    sum_krw += amount_needed * p
                    remaining = 0.0
                    break
                else:
                    sum_amount += lvl['size']
                    sum_krw += krw_available
                    remaining -= krw_available
            if remaining > 0:
                return None
            if sum_amount <= 0:
                return None
            weighted_avg = float(sum_krw) / float(sum_amount)
            slippage = abs((weighted_avg - best) / best)
            return float(slippage)
    except Exception:
        logger.exception('estimate_slippage_for_notional failed')
        return None


def compute_max_notional_for_slippage(orderbook: Any, side: str, max_slippage_pct: float, best_price: float = None) -> float | None:
    """Compute the maximum KRW notional that can be executed given the orderbook without exceeding max_slippage_pct.
    Returns krw amount allowed (float) or None if insufficient depth even for smallest level.
    """
    try:
        nb = _normalize_orderbook(orderbook)
        if side.lower().startswith('b'):
            levels = nb['asks']
            if not levels:
                return None
            best = levels[0]['price'] if best_price is None else float(best_price)
            target = float(best) * (1.0 + float(max_slippage_pct))
            cum_amount = 0.0
            cum_krw = 0.0
            # iterate levels and add until weighted_avg > target
            for lvl in levels:
                p = float(lvl['price'])
                sz = float(lvl['size'])
                krw = p * sz
                if cum_amount + sz <= 0:
                    # nothing yet, add full
                    cum_amount += sz
                    cum_krw += krw
                else:
                    # compute weighted avg if adding full level
                    new_cum_amount = cum_amount + sz
                    new_cum_krw = cum_krw + krw
                    new_avg = new_cum_krw / new_cum_amount
                    if new_avg <= target:
                        cum_amount = new_cum_amount
                        cum_krw = new_cum_krw
                        continue
                    else:
                        # partial fill of this level allowed - solve for x amount
                        # want (cum_krw + p*x) / (cum_amount + x) <= target
                        denom = (p - target)
                        if denom <= 0:
                            # adding this level will not exceed target (p <= target), include all
                            cum_amount = new_cum_amount
                            cum_krw = new_cum_krw
                            continue
                        # solve x <= (target*cum_amount - cum_krw) / (p - target)
                        numer = (target * cum_amount) - cum_krw
                        x = numer / denom
                        if x <= 0:
                            # cannot add any amount from this level
                            return float(cum_krw) if cum_krw > 0 else None
                        allowed = min(x, sz)
                        cum_amount += allowed
                        cum_krw += allowed * p
                        return float(cum_krw)
            # finished all levels without exceeding target
            return float(cum_krw) if cum_krw > 0 else None
        else:
            levels = nb['bids']
            if not levels:
                return None
            best = levels[0]['price'] if best_price is None else float(best_price)
            target = float(best) * (1.0 - float(max_slippage_pct))
            cum_amount = 0.0
            cum_krw = 0.0
            for lvl in levels:
                p = float(lvl['price'])
                sz = float(lvl['size'])
                krw = p * sz
                new_cum_amount = cum_amount + sz
                new_cum_krw = cum_krw + krw
                new_avg = new_cum_krw / new_cum_amount if new_cum_amount>0 else p
                if new_avg >= target:
                    cum_amount = new_cum_amount
                    cum_krw = new_cum_krw
                    continue
                else:
                    denom = (target - p)
                    if denom <= 0:
                        cum_amount = new_cum_amount
                        cum_krw = new_cum_krw
                        continue
                    numer = (target * cum_amount) - cum_krw
                    x = numer / denom
                    if x <= 0:
                        return float(cum_krw) if cum_krw>0 else None
                    allowed = min(x, sz)
                    cum_amount += allowed
                    cum_krw += allowed * p
                    return float(cum_krw)
            return float(cum_krw) if cum_krw>0 else None
    except Exception:
        logger.exception('compute_max_notional_for_slippage failed')
        return None
