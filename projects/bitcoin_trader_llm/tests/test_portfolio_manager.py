import os
import time
import json
import math
from datetime import datetime
import sys

# insert project root into sys.path so imports like 'app.*' succeed when running tests
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.portfolio_manager import PortfolioManager, Signal
from app.risk_engine import RiskEngine
from app import portfolio_manager as pm_mod
from app import config as app_config

DECISION_TRACES_DIR = os.path.join(app_config.LOG_DIR, 'decision_traces')


def newest_trace_path():
    files = []
    if not os.path.isdir(DECISION_TRACES_DIR):
        return None
    for fn in os.listdir(DECISION_TRACES_DIR):
        if fn.endswith('.json'):
            files.append(os.path.join(DECISION_TRACES_DIR, fn))
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def test_agent_2pct_and_risk_1pct_scales_to_half():
    # setup
    equity = 100_000_000.0
    entry_price = 50_000_000.0

    risk = RiskEngine(initial_risk_pct=1.0, min_risk_pct=0.05)
    risk.on_new_day(equity)

    # create agent signal proposing 2% risk with 1% stop
    s = Signal(
        id='sig-1',
        source='agent',
        publish_ts=datetime.utcnow().isoformat(),
        symbol='KRW-BTC',
        side='long',
        score=1.0,
        p_win=0.6,
        confidence=0.6,
        suggested_risk_pct=2.0,
        suggested_stop_pct=1.0,
        horizon_minutes=720,
        model_version='agent-test',
        meta={'entry_price': entry_price}
    )

    pm = PortfolioManager(risk_engine=risk, agent_weight=0.6, rule_weight=0.2, dry_run=True)
    pm.ingest_signals([s])

    # propose
    proposals = pm.propose_positions(equity=equity, market_state={'price': entry_price})
    assert len(proposals) == 1
    prop = proposals[0]
    assert math.isclose(prop.suggested_risk_pct, 2.0, rel_tol=1e-9)

    # apply risk engine
    decisions = pm.apply_risk_engine(proposals, portfolio_snapshot={'equity': equity}, market_state={'price': entry_price})
    assert len(decisions) == 1
    dec = decisions[0]
    assert dec['allow'] is True
    # scale should be 0.5 since base risk 2.0 and current cap 1.0
    assert math.isclose(dec['scale'], 0.5, rel_tol=1e-6)

    # finalize orders (also writes trace)
    orders = pm.finalize_orders(decisions, market_state={'price': entry_price}, equity=equity)
    assert len(orders) == 1

    # units after scale is half of proposed units
    assert math.isclose(orders[0].units, prop.units * 0.5, rel_tol=1e-6)

    # trace created
    path = newest_trace_path()
    assert path is not None
    with open(path, 'r') as f:
        data = json.load(f)
    assert 'risk_decisions' in data
    assert len(data['risk_decisions']) >= 1


def test_daily_loss_block_vetoes_entries():
    equity = 100_000_000.0
    entry_price = 50_000_000.0

    # risk engine with 1% daily loss limit
    risk = RiskEngine(initial_risk_pct=1.0, min_risk_pct=0.05, daily_loss_limit_pct=1.0)
    risk.on_new_day(equity)
    # cause a loss greater than 1% -> block
    risk.record_trade_result(pnl=-2_000_000.0, nav_after=98_000_000.0)
    assert risk.blocked_for_day is True

    s = Signal(
        id='sig-2',
        source='agent',
        publish_ts=datetime.utcnow().isoformat(),
        symbol='KRW-BTC',
        side='long',
        score=1.0,
        p_win=0.6,
        confidence=0.6,
        suggested_risk_pct=2.0,
        suggested_stop_pct=1.0,
        horizon_minutes=720,
        model_version='agent-test',
        meta={'entry_price': entry_price}
    )

    pm = PortfolioManager(risk_engine=risk, agent_weight=0.6, rule_weight=0.2, dry_run=True)
    pm.ingest_signals([s])
    proposals = pm.propose_positions(equity=equity, market_state={'price': entry_price})
    decisions = pm.apply_risk_engine(proposals, portfolio_snapshot={'equity': equity}, market_state={'price': entry_price})

    assert len(decisions) == 1
    dec = decisions[0]
    assert dec['allow'] is False

    orders = pm.finalize_orders(decisions, market_state={'price': entry_price}, equity=equity)
    assert len(orders) == 0

    # trace contains risk_decisions showing veto
    path = newest_trace_path()
    assert path is not None
    with open(path, 'r') as f:
        data = json.load(f)
    assert data['risk_decisions'][0]['allow'] is False
