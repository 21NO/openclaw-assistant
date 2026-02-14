#!/usr/bin/env python3
"""
End-to-end dry-run test:
- Ensures DB is available (sqlite fallback)
- Inserts a synthetic decision request if none pending
- Runs agent_decider to convert requests -> signals
- Runs executor to process pending signals (DRY_RUN so simulated)
- Prints summary counts and recent rows for verification
"""
import logging
import json
from datetime import datetime

from app.db_logger import DBLogger
from app.agent_decider import process_pending_requests
from app.executor import Executor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('test_e2e')


def make_sample_request(db: DBLogger):
    run_id = f"test_run_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    payload = {
        'run_id': run_id,
        'symbol': 'KRW-BTC',
        'features': {'last_price': 60000000, 'return_1': 0.005},
        'recent_ohlcv': [],
        'news': [],
        'balances': {},
        'strategy': {'name': 'momentum_breakout_30m', 'params': {}},
        'meta': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
    }
    rid = db.insert_decision_request(run_id=run_id, payload=payload)
    logger.info(f'Inserted synthetic decision_request id={rid}')
    return rid


def print_summary(db: DBLogger):
    pending_reqs = db.get_pending_decision_requests(limit=20)
    pending_sigs = db.get_pending_signals(limit=50)
    print('\nSUMMARY:')
    print('pending decision requests:', len(pending_reqs))
    print('pending signals:', len(pending_sigs))
    if pending_reqs:
        print('\nlatest decision request sample:')
        print(pending_reqs[0])
    if pending_sigs:
        print('\nlatest signal sample:')
        print(pending_sigs[0])


if __name__ == '__main__':
    db = DBLogger()

    # ensure we have a decision request
    reqs = db.get_pending_decision_requests(limit=5)
    if not reqs:
        make_sample_request(db)

    # 1) agent decides
    processed = process_pending_requests(limit=10)
    logger.info(f'Agent created signals: {processed}')

    # 2) executor processes pending signals (DRY_RUN by config)
    ex = Executor(db=db)
    ex.process_pending(limit=10)

    # 3) print summary
    print_summary(db)
