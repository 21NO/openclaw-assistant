import os
import json
import math
import unittest
from datetime import datetime
import sys

# insert project root into sys.path so imports like 'app.*' succeed when running tests
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.portfolio_manager import PortfolioManager, Signal
from app.risk_engine import RiskEngine
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


class TestPortfolioManager(unittest.TestCase):
    def test_agent_2pct_and_risk_1pct_scales_to_half(self):
        equity = 100_000_000.0
        entry_price = 50_000_000.0

        risk = RiskEngine(initial_risk_pct=1.0, min_risk_pct=0.05)
        risk.on_new_day(equity)

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

        proposals = pm.propose_positions(equity=equity, market_state={'price': entry_price})
        self.assertEqual(len(proposals), 1)
        prop = proposals[0]
        self.assertAlmostEqual(prop.suggested_risk_pct, 2.0)

        decisions = pm.apply_risk_engine(proposals, portfolio_snapshot={'equity': equity}, market_state={'price': entry_price})
        self.assertEqual(len(decisions), 1)
        dec = decisions[0]
        self.assertTrue(dec['allow'])
        self.assertAlmostEqual(dec['scale'], 0.5)

        orders = pm.finalize_orders(decisions, market_state={'price': entry_price}, equity=equity)
        self.assertEqual(len(orders), 1)
        self.assertAlmostEqual(orders[0].units, prop.units * 0.5)

        path = newest_trace_path()
        self.assertIsNotNone(path)
        with open(path, 'r') as f:
            data = json.load(f)
        self.assertIn('risk_decisions', data)

    def test_daily_loss_block_vetoes_entries(self):
        equity = 100_000_000.0
        entry_price = 50_000_000.0

        risk = RiskEngine(initial_risk_pct=1.0, min_risk_pct=0.05, daily_loss_limit_pct=1.0)
        risk.on_new_day(equity)
        risk.record_trade_result(pnl=-2_000_000.0, nav_after=equity - 2_000_000.0)
        self.assertTrue(risk.blocked_for_day)

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

        self.assertEqual(len(decisions), 1)
        dec = decisions[0]
        self.assertFalse(dec['allow'])

        orders = pm.finalize_orders(decisions, market_state={'price': entry_price}, equity=equity)
        self.assertEqual(len(orders), 0)

        path = newest_trace_path()
        self.assertIsNotNone(path)
        with open(path, 'r') as f:
            data = json.load(f)
        self.assertFalse(data['risk_decisions'][0]['allow'])


if __name__ == '__main__':
    unittest.main()
