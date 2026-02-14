"""
Reflection/post-mortem generator.
- Creates a textual post-mortem for an execution and stores into DB and (optionally) writes to memory files for LanceDB ingestion.
"""
import logging
import os
from datetime import datetime
from typing import Dict, Any, List

from app import config
from app.llm_agent import LLMAgent

logger = logging.getLogger('reflection')


class Reflector:
    def __init__(self, db=None):
        self.db = db
        self.llm = LLMAgent()

    def create_and_store(self, execution_record: Dict[str, Any], metrics: Dict[str, Any], recent_trades: List[Dict[str, Any]]):
        try:
            text = self.llm.generate_reflection(execution_record, metrics, recent_trades)
            # store to DB
            exec_id = execution_record.get('id')
            created = self.db.insert_reflection(execution_id=exec_id,
                                                strategy_name=execution_record.get('strategy_name'),
                                                entry_ts=execution_record.get('entry_ts'),
                                                exit_ts=execution_record.get('exit_ts'),
                                                pnl_krw=metrics.get('pnl_krw'),
                                                pnl_pct=metrics.get('pnl_pct'),
                                                duration_sec=metrics.get('duration_sec'),
                                                metrics=metrics,
                                                text=text)
            logger.info(f'Reflection stored id={created}')

            # optionally write to memory dir for LanceDB ingestion
            if config.INDEX_REFLECTIONS:
                self._write_memory_file(exec_id, text)

            return text
        except Exception:
            logger.exception('create_and_store failed')
            return None

    def _write_memory_file(self, exec_id, text):
        try:
            memory_dir = config.MEMORY_DIR
            os.makedirs(memory_dir, exist_ok=True)
            ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
            fname = os.path.join(memory_dir, f'reflection_{exec_id or "tmp"}_{ts}.md')
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(f'# reflection {exec_id}\n')
                f.write(text)
            logger.info(f'Wrote reflection to memory file: {fname}')
            return fname
        except Exception:
            logger.exception('failed to write memory file')
            return None
