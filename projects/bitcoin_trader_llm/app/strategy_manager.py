"""
Strategy manager: holds available strategies, scoring, and dynamic selection logic.
- get_active_strategy() returns the currently active strategy (from DB or default)
- evaluate_and_maybe_switch() can be invoked to consider switching strategies based on recent performance

This is a simple, extensible implementation which you can enhance with more sophisticated metrics.
"""
import logging
import json
from typing import Dict, Any

logger = logging.getLogger('strategy_manager')

DEFAULT_STRATEGIES = [
    {
        'name': 'momentum_breakout_30m',
        'params': {
            'breakout_lookback': 24,
            'ema_short': 9,
            'ema_long': 50,
            'rsi_min': 50,
            'vol_mult': 1.2,
            'atr_k': 1.5,
            'rr': 2.5
        }
    },
    {
        'name': 'mean_reversion_15m_small',
        'params': {
            'rsi_low': 25,
            'rsi_high': 75,
            'size_pct': 0.5
        }
    }
]


class StrategyManager:
    def __init__(self, db=None):
        self.db = db
        # naive active strategy selection: read from DB active flag
        self._active = None
        try:
            if db is not None and hasattr(db, 'get_active_strategy'):
                s = db.get_active_strategy()
                if s:
                    self._active = s
        except Exception:
            logger.exception('Could not load active strategy from DB; falling back to default')

        if not self._active:
            self._active = DEFAULT_STRATEGIES[0]

    def get_active_strategy(self) -> Dict[str, Any]:
        return self._active

    def evaluate_and_maybe_switch(self, recent_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Given recent_metrics (e.g., strategy_stats), decide whether to switch strategies.
        Returns the selected strategy dict. This function currently implements a simple rule-based check:
        - If active strategy expectancy < 0 and another strategy has positive expectancy over the lookback window, switch.
        """
        try:
            active = self._active
            # look for candidate with better expectancy
            best = active
            for cand in DEFAULT_STRATEGIES:
                # fetch metrics from recent_metrics if available
                m = recent_metrics.get(cand['name'], {})
                expectancy = m.get('expectancy') if isinstance(m, dict) else None
                if expectancy is not None and expectancy > (recent_metrics.get(active['name'], {}).get('expectancy') or 0):
                    best = cand
            if best['name'] != active['name']:
                logger.info(f"Switching active strategy {active['name']} -> {best['name']}")
                self._active = best
                # persist to DB if available
                if self.db and hasattr(self.db, 'record_strategy_version'):
                    self.db.record_strategy_version(best['name'], best['params'], reason='auto_switch')
            return self._active
        except Exception:
            logger.exception('Error in evaluate_and_maybe_switch')
            return self._active
