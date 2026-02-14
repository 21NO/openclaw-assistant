"""Portfolio Manager skeleton

Responsibilities:
- Accept Signal objects from Agent and Rule modules
- Aggregate signals (weights: agent=0.6, rule=0.2 by default)
- Produce ProposedPosition objects with sizing suggestions
- Call RiskEngine.evaluate_proposal(...) for each proposal
- Produce final Orders (DRY_RUN only: no execution)
- Emit decision trace JSON to logs/decision_traces/<trace_id>.json

This is a lightweight, well-documented skeleton intended for unit-testing and
integration into the existing backtester / live DRY_RUN environment.
"""
from __future__ import annotations

import os
import uuid
import json
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any
from datetime import datetime

from . import risk_engine as _risk_engine_mod
from .risk_engine import RiskEngine
from .config import LOG_DIR
from . import config as _global_config

# ensure decision trace dir exists
DECISION_TRACES_DIR = os.path.join(LOG_DIR, 'decision_traces')
os.makedirs(DECISION_TRACES_DIR, exist_ok=True)


@dataclass
class Signal:
    id: str
    source: str  # 'agent' | 'rule'
    publish_ts: str
    symbol: str
    side: str  # 'long' | 'short'
    score: float
    p_win: Optional[float] = None
    confidence: Optional[float] = None
    suggested_risk_pct: Optional[float] = None  # percent (e.g. 2.0)
    suggested_stop_pct: Optional[float] = None  # percent
    suggested_takeprofit_pct: Optional[float] = None
    horizon_minutes: Optional[int] = None
    model_version: Optional[str] = None
    explain: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = field(default_factory=dict)


@dataclass
class ProposedPosition:
    proposal_id: str
    symbol: str
    side: str
    entry_price_hint: float
    suggested_notional: float
    suggested_risk_pct: float
    suggested_stop_pct: float
    units: float
    combined_score: float
    components: List[Dict[str, Any]]  # list of signal dicts
    timestamp: str


@dataclass
class Order:
    order_id: str
    symbol: str
    side: str
    units: float
    notional: float
    entry_type: str = 'market'
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    meta: Optional[Dict[str, Any]] = field(default_factory=dict)


class PortfolioManager:
    def __init__(self, risk_engine: RiskEngine, agent_weight: float = 0.6, rule_weight: float = 0.2, dry_run: bool = _global_config.DRY_RUN):
        self.risk_engine = risk_engine
        self.agent_weight = float(agent_weight)
        self.rule_weight = float(rule_weight)
        self.dry_run = bool(dry_run)
        self.signals: List[Signal] = []

    def ingest_signals(self, signals: List[Signal]) -> None:
        self.signals = list(signals)

    def _group_by_symbol(self) -> Dict[str, List[Signal]]:
        groups: Dict[str, List[Signal]] = {}
        for s in self.signals:
            groups.setdefault(s.symbol, []).append(s)
        return groups

    def propose_positions(self, equity: float, market_state: Optional[Dict[str, Any]] = None) -> List[ProposedPosition]:
        market_state = market_state or {}
        price_hint = market_state.get('price')
        groups = self._group_by_symbol()
        proposals: List[ProposedPosition] = []

        for symbol, sigs in groups.items():
            agent_sigs = [s for s in sigs if s.source == 'agent']
            rule_sigs = [s for s in sigs if s.source == 'rule']

            # Representative signals (take highest confidence or first)
            agent_rep = None
            rule_rep = None
            if agent_sigs:
                # choose by highest confidence, fallback to max score
                agent_rep = sorted(agent_sigs, key=lambda x: (x.confidence or 0.0, x.score), reverse=True)[0]
            if rule_sigs:
                rule_rep = sorted(rule_sigs, key=lambda x: (x.confidence or 0.0, x.score), reverse=True)[0]

            # compute combined_score
            total_weight = 0.0
            weighted = 0.0
            if agent_rep is not None:
                weighted += self.agent_weight * (agent_rep.score)
                total_weight += self.agent_weight
            if rule_rep is not None:
                weighted += self.rule_weight * (rule_rep.score)
                total_weight += self.rule_weight

            if total_weight == 0.0:
                # nothing to propose
                continue
            combined_score = weighted / total_weight

            # determine suggested risk pct: prefer agent, else rule, else config default
            if agent_rep is not None and agent_rep.suggested_risk_pct is not None:
                suggested_risk_pct = float(agent_rep.suggested_risk_pct)
            elif rule_rep is not None and rule_rep.suggested_risk_pct is not None:
                suggested_risk_pct = float(rule_rep.suggested_risk_pct)
            else:
                suggested_risk_pct = float(getattr(_global_config, 'RISK_PER_TRADE_PCT', 1.0))

            # stop pct preference
            if agent_rep is not None and agent_rep.suggested_stop_pct is not None:
                stop_pct = float(agent_rep.suggested_stop_pct)
            elif rule_rep is not None and rule_rep.suggested_stop_pct is not None:
                stop_pct = float(rule_rep.suggested_stop_pct)
            else:
                stop_pct = 1.0  # conservative default 1%

            # entry price hint
            entry_price = price_hint or (agent_rep.meta.get('entry_price') if agent_rep and agent_rep.meta else None) or 1.0
            try:
                entry_price = float(entry_price)
            except Exception:
                entry_price = 1.0

            # compute sizing: desired dollar risk = suggested_risk_pct% * equity
            desired_dollar_risk = (float(suggested_risk_pct) / 100.0) * float(equity)
            stop_frac = max(float(stop_pct) / 100.0, 1e-6)
            suggested_notional = desired_dollar_risk / stop_frac
            units = suggested_notional / entry_price

            # create proposal
            proposal = ProposedPosition(
                proposal_id=str(uuid.uuid4()),
                symbol=symbol,
                side=(agent_rep.side if agent_rep else (rule_rep.side if rule_rep else 'long')),
                entry_price_hint=float(entry_price),
                suggested_notional=float(suggested_notional),
                suggested_risk_pct=float(suggested_risk_pct),
                suggested_stop_pct=float(stop_pct),
                units=float(units),
                combined_score=float(combined_score),
                components=[asdict(x) for x in ([agent_rep] if agent_rep else []) + ([rule_rep] if rule_rep else [])],
                timestamp=datetime.utcnow().isoformat()
            )
            proposals.append(proposal)

        return proposals

    def apply_risk_engine(self, proposals: List[ProposedPosition], portfolio_snapshot: Optional[Dict[str, Any]] = None, market_state: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        decisions: List[Dict[str, Any]] = []
        for p in proposals:
            # risk_engine.evaluate_proposal accepts dict or object; pass proposal as dict for safety
            proposal_dict = asdict(p)
            dec = self.risk_engine.evaluate_proposal(proposal_dict, portfolio_snapshot=portfolio_snapshot, market_state=market_state, timestamp=datetime.utcnow().isoformat())
            # attach proposal for downstream
            dec['proposal'] = proposal_dict
            decisions.append(dec)
        return decisions

    def finalize_orders(self, decisions: List[Dict[str, Any]], market_state: Optional[Dict[str, Any]] = None, equity: Optional[float] = None) -> List[Order]:
        orders: List[Order] = []
        for dec in decisions:
            proposal = dec.get('proposal')
            if not proposal:
                continue
            allow = bool(dec.get('allow', False))
            scale = float(dec.get('scale', 0.0)) if allow else 0.0
            if not allow or scale <= 0.0:
                # vetoed
                continue
            # compute final units
            units = float(proposal.get('units', 0.0)) * float(scale)
            entry_price = float(proposal.get('entry_price_hint', 1.0))
            notional = units * entry_price
            order = Order(
                order_id=str(uuid.uuid4()),
                symbol=proposal.get('symbol'),
                side=proposal.get('side'),
                units=units,
                notional=notional,
                entry_type='market',
                limit_price=None,
                stop_price=None,
                meta={'decision_reason': dec.get('reason')}
            )
            orders.append(order)

        # write trace and return orders
        trace_path = self.audit_trace(decisions, orders, equity=equity)
        return orders

    def audit_trace(self, decisions: List[Dict[str, Any]], orders: List[Order], equity: Optional[float] = None) -> str:
        trace_id = str(uuid.uuid4())
        payload = {
            'trace_id': trace_id,
            'timestamp': datetime.utcnow().isoformat(),
            'equity': equity,
            'signals': [asdict(s) for s in self.signals],
            'proposals': [d.get('proposal') for d in decisions],
            'risk_decisions': decisions,
            'orders': [asdict(o) for o in orders]
        }
        path = os.path.join(DECISION_TRACES_DIR, f"{trace_id}.json")
        try:
            with open(path, 'w') as f:
                json.dump(payload, f, indent=2, default=str)
        except Exception:
            # best-effort
            path = ''
        return path


__all__ = ['Signal', 'ProposedPosition', 'Order', 'PortfolioManager']
