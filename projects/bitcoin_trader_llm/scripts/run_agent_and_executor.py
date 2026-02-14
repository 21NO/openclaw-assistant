#!/usr/bin/env python3
"""Run agent_decider to process awaiting decision requests, then run Executor to process pending signals.
Intended for cron scheduling.
"""
import logging
import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.agent_decider import process_pending_requests
from app.executor import Executor

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('run_agent_and_executor')

if __name__ == '__main__':
    logger.info('Starting agent_decider -> executor run')
    try:
        processed = process_pending_requests(limit=50)
        logger.info(f'agent_decider processed signals: {processed}')
    except Exception:
        logger.exception('agent_decider run failed')

    try:
        ex = Executor()
        ex.process_pending(limit=50)
    except Exception:
        logger.exception('Executor run failed')

    logger.info('Run finished')
