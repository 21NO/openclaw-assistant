"""
Agent decider module — processes awaiting decision requests and converts them into llm_signals.
This represents the OpenClaw agent's decision logic (no external API calls).

Implements a D+C+E style heuristic (Volatility Breakout + RSI filter + optional LLM gating).
"""
import logging
import json
import os
import uuid
from typing import List, Dict, Any

from app.db_logger import DBLogger
from app import config

logger = logging.getLogger('agent_decider')

# optional LLM gate (lazy import)
try:
    from app.llm_agent import LLMAgent
except Exception:
    LLMAgent = None


def _compute_orderbook_imbalance(ob) -> float | None:
    """Compute a simple bid/ask imbalance from pyupbit-like orderbook response."""
    try:
        if not ob:
            return None
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
        if not total_ask:
            return None
        return float(total_bid) / float(total_ask)
    except Exception:
        return None


def decide_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    features = payload.get('features') or {}
    orderbook = payload.get('orderbook')

    # parameters: allow override via payload.params or strategy.params or environment variables
    params = payload.get('params') or {}
    strat_params = (payload.get('strategy') or {}).get('params') or {}
    try:
        vol_mult = float(params.get('vol_mult') or strat_params.get('vol_mult') or os.getenv('VOL_MULT') or 1.5)
    except Exception:
        vol_mult = 1.5
    try:
        default_stop_rel = float(params.get('stop_rel') or strat_params.get('stop_rel') or os.getenv('STOP_REL') or 1.5)
    except Exception:
        default_stop_rel = 1.5

    def _num(x):
        try:
            return float(x)
        except Exception:
            return None

    close = _num(features.get('last_price'))
    vol = _num(features.get('last_volume'))
    vol_ma20 = _num(features.get('vol_ma20') or features.get('vol_ma') or features.get('avg_vol_24'))
    bb_h = _num(features.get('bb_hband'))
    bb_l = _num(features.get('bb_lband'))
    adx = _num(features.get('adx14'))
    rsi = _num(features.get('rsi14'))
    ema9 = _num(features.get('ema9'))
    ema50 = _num(features.get('ema50'))
    ret1 = _num(features.get('return_1')) or 0.0
    regime = features.get('regime') or ('trend' if (adx and adx >= 25) else ('range' if (adx and adx <= 20) else 'neutral'))

    imbalance = _compute_orderbook_imbalance(orderbook)

    buy_score = 0
    sell_score = 0
    reasons: List[str] = []

    # D: Volatility breakout (buy)
    if close is not None and bb_h is not None and vol_ma20 is not None:
        try:
            vol_threshold = vol_ma20 * float(vol_mult)
        except Exception:
            vol_threshold = vol_ma20 * 1.5
        if close > bb_h and vol is not None and vol > vol_threshold and (regime == 'trend' or (adx and adx >= 25)):
            if imbalance is None or imbalance > 1.1:
                buy_score += 2
                reasons.append('breakout+vol+imbalance')
            else:
                buy_score += 1
                reasons.append('breakout+vol')

    # symmetric breakdown (sell)
    if close is not None and bb_l is not None and vol_ma20 is not None:
        try:
            vol_threshold = vol_ma20 * float(vol_mult)
        except Exception:
            vol_threshold = vol_ma20 * 1.5
        if close < bb_l and vol is not None and vol > vol_threshold and (regime == 'trend' or (adx and adx >= 25)):
            if imbalance is None or imbalance < 0.9:
                sell_score += 2
                reasons.append('breakdown+vol+imbalance')
            else:
                sell_score += 1
                reasons.append('breakdown+vol')

    # C: mean-reversion buy
    if rsi is not None and bb_l is not None and close is not None:
        if rsi < 25 and close < bb_l * 1.0:
            buy_score += 1
            reasons.append('rsi_meanrev')

    # EMA trend filter
    if ema9 is not None and ema50 is not None:
        if ema9 > ema50:
            buy_score += 1
            reasons.append('ema_up')
        elif ema9 < ema50:
            sell_score += 1
            reasons.append('ema_down')

    # short-term momentum
    if ret1 is not None:
        if ret1 > 0.002:
            buy_score += 1
            reasons.append('short_mom')
        elif ret1 < -0.002:
            sell_score += 1
            reasons.append('short_mom_neg')

    # RSI overbought handling — contextual veto: allow when strong trend + high volume
    try:
        overbought_thresh = float(params.get('rsi_overbought_thresh') or strat_params.get('rsi_overbought_thresh') or os.getenv('RSI_OVERBOUGHT_THRESH') or 70)
    except Exception:
        overbought_thresh = 70
    try:
        adx_allow_thresh = float(params.get('rsi_adx_allow') or strat_params.get('rsi_adx_allow') or os.getenv('RSI_ADX_ALLOW') or 30)
    except Exception:
        adx_allow_thresh = 30
    try:
        vol_allow_thresh = float(params.get('rsi_vol_allow') or strat_params.get('rsi_vol_allow') or os.getenv('RSI_VOL_ALLOW') or 1.2)
    except Exception:
        vol_allow_thresh = 1.2

    # rsi_veto_setting can be bool False/True or 'contextual' (default)
    rsi_veto_setting = params.get('rsi_veto') if params.get('rsi_veto') is not None else strat_params.get('rsi_veto', 'contextual')

    if rsi is not None and rsi >= overbought_thresh:
        # explicit disable
        if isinstance(rsi_veto_setting, bool) and not rsi_veto_setting:
            # do not veto
            reasons.append('rsi_overbought_no_veto')
        else:
            # contextual allow: require ADX high and recent volume > vol_allow_thresh * vol_ma20
            allow_in_trend = False
            try:
                if adx is not None and adx >= adx_allow_thresh:
                    if vol_ma20 is not None and vol is not None and vol_ma20 > 0:
                        if (vol / vol_ma20) >= vol_allow_thresh:
                            allow_in_trend = True
                    else:
                        # if no volume MA info, allow based on ADX alone
                        allow_in_trend = True
            except Exception:
                allow_in_trend = False

            if not allow_in_trend:
                buy_score = max(0, buy_score - 2)
                reasons.append('rsi_overbought_veto')
            else:
                reasons.append('rsi_overbought_allowed_by_trend')

    # pick action
    action = 'hold'
    pct = 0
    confidence = 0.2
    stop_rel = None

    if buy_score > sell_score and buy_score >= 2:
        action = 'buy'
        pct = min(50, int(10 * buy_score))
        confidence = min(0.99, buy_score / 6.0)
        stop_rel = default_stop_rel
    elif sell_score > buy_score and sell_score >= 2:
        action = 'sell'
        pct = min(50, int(10 * sell_score))
        confidence = min(0.99, sell_score / 6.0)
        stop_rel = default_stop_rel
    else:
        action = 'hold'
        pct = 0
        confidence = 0.2
        stop_rel = None

    # E: optional LLM gating (only if configured to use local/openai)
    llm_mode = getattr(config, 'LLM_MODE', None)
    if llm_mode and llm_mode in ('local', 'openai') and LLMAgent is not None and action in ('buy', 'sell'):
        try:
            gate_agent = LLMAgent()
            run_id = payload.get('run_id')
            symbol = payload.get('symbol') or payload.get('market') or 'KRW-BTC'
            recent_ohlcv = payload.get('recent_ohlcv', [])
            news = payload.get('news', [])
            balances = payload.get('balances', {})
            strategy = payload.get('strategy', {})
            gate_decision = gate_agent.decide(run_id or 'gate', symbol, features, recent_ohlcv, news, balances, strategy)
            gate_action = gate_decision.get('action', 'hold')
            gate_conf = float(gate_decision.get('confidence') or 0.0)
            if gate_action != action or gate_conf < 0.75:
                reasons.append(f"llm_gate_block(action={gate_action},conf={gate_conf})")
                action = 'hold'
                pct = 0
                confidence = float(min(confidence, gate_conf))
                stop_rel = None
            else:
                reasons.append(f"llm_gate_allow(conf={gate_conf})")
                confidence = min(0.99, (confidence + gate_conf) / 2.0)
        except Exception:
            logger.exception('LLM gating failed; proceeding without gate')

    # prepare agent_intent for audit and later reflection linking
    try:
        suggested_risk_pct = float(params.get('suggested_risk_pct') or strat_params.get('suggested_risk_pct') or getattr(config, 'RISK_PER_TRADE_PCT', 1.0))
    except Exception:
        suggested_risk_pct = float(getattr(config, 'RISK_PER_TRADE_PCT', 1.0))

    intent = {
        'intent_id': f"{payload.get('run_id','')}_{uuid.uuid4().hex[:8]}",
        'intent': 'buy' if action == 'buy' else ('sell' if action == 'sell' else 'hold'),
        'reason': ','.join(reasons) or 'no_signal',
        'suggested_risk_pct': suggested_risk_pct,
        'stop_rel': stop_rel,
        'target_horizon': 'short',
        'meta': {'regime': regime, 'adx': adx}
    }

    return {
        'action': action,
        'pct': pct,
        'confidence': float(confidence),
        'stop_rel': stop_rel,
        'reason': intent['reason'],
        'agent_intent': intent,
        'meta': {'decider': 'dce_v1', 'adx': adx, 'imbalance': imbalance, 'vol_mult': vol_mult, 'stop_rel_used': stop_rel}
    }


def process_pending_requests(limit: int = 10) -> List[int]:
    db = DBLogger()
    processed_signals = []
    reqs = db.get_pending_decision_requests(limit=limit)
    if not reqs:
        logger.info('No pending decision requests')
        return []

    # lazy import for allocation evaluator to avoid heavy deps at module import
    try:
        from app.allocation import evaluate_allocation
    except Exception:
        evaluate_allocation = None

    for r in reqs:
        try:
            req_id = r.get('id')
            payload = r.get('payload_json')
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}

            # mark assigned
            db.update_decision_request_status(req_id, 'assigned', assigned_to='assistant')

            # make a decision
            suggestion = decide_from_payload(payload)

            strategy_name = (payload.get('strategy') or {}).get('name') if payload else 'agent_default'
            run_id = payload.get('run_id') if payload else None

            # persist as signal
            sig_id = db.insert_signal(run_id=run_id or f'agent_{req_id}', strategy_name=strategy_name or 'agent_default', payload=suggestion, suggested_pct=suggestion.get('pct'), confidence=suggestion.get('confidence'))
            logger.info(f'Created signal id={sig_id} for request id={req_id} suggestion={suggestion}')
            processed_signals.append(sig_id)

            # attempt to create allocation proposal snapshot if evaluator available
            try:
                if evaluate_allocation is not None and run_id:
                    symbol = payload.get('symbol') or 'KRW-BTC'
                    entry_price = float(suggestion.get('meta', {}).get('last_price') or payload.get('features', {}).get('last_price') or 0.0)
                    suggested_risk_pct = suggestion.get('agent_intent', {}).get('suggested_risk_pct') if suggestion.get('agent_intent') else None
                    stop_rel = suggestion.get('stop_rel')
                    stop_pct = None
                    if stop_rel is not None and entry_price > 0:
                        # interpret stop_rel as ATR multiple if features contain ATR else keep as percent
                        atr = payload.get('features', {}).get('atr') or payload.get('features', {}).get('atr14')
                        try:
                            if atr:
                                stop_price = entry_price - float(atr) * float(stop_rel)
                                stop_pct = max(0.0, (entry_price - stop_price) / entry_price * 100.0)
                            else:
                                stop_pct = float(payload.get('stop_pct') or (stop_rel * 1.0))
                        except Exception:
                            stop_pct = None
                    if suggested_risk_pct is None:
                        suggested_risk_pct = float(getattr(config, 'RISK_PER_TRADE_PCT', 1.0))
                    alloc = evaluate_allocation(db, run_id, symbol, suggested_risk_pct, stop_pct or 1.0, entry_price, orderbook=payload.get('orderbook'))
                    # persist allocation proposal
                    try:
                        db.insert_allocation_proposal(run_id=run_id, signal_id=sig_id, strategy_name=strategy_name or 'agent_default', symbol=symbol, suggested_risk_pct=suggested_risk_pct, stop_pct=stop_pct or 1.0, desired_notional_krw=alloc.get('desired_notional', 0.0), capped_notional_krw=alloc.get('capped_notional', 0.0), additional_needed_krw=alloc.get('additional_needed', 0.0), scale=alloc.get('scale', 0.0), params=payload.get('params') or {}, reason='auto_alloc')
                    except Exception:
                        logger.exception('Failed to persist allocation proposal')
            except Exception:
                logger.debug('Allocation evaluation skipped or failed')

            # mark request done
            db.update_decision_request_status(req_id, 'done', assigned_to='assistant')
        except Exception:
            logger.exception('Failed to process decision request id=%s', r.get('id'))
    return processed_signals


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    s = process_pending_requests(limit=10)
    print('processed_signals=', s)
