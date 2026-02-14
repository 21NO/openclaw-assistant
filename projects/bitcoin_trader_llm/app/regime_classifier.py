"""Regime classifier module

Provides a small, dependency-light RegimeClassifier that labels market regime
based on ADX + EMA structure with a hysteresis (confirmation) period to avoid
flip-flopping.

API:
- RegimeClassifier(...)
- classify_point(features: dict) -> str
- classify_series(points: list[dict]) -> list[str]

features dict expected keys (flexible lookup):
- adx14 (or adx)
- ema9 (or ema_short)
- ema50 (or ema_long)

Regimes: 'trend_up', 'trend_down', 'range', 'transition', 'unknown'

This is intentionally lightweight so it can be integrated into the backtest
pipeline and iterated on.
"""
from __future__ import annotations
from typing import List, Dict, Optional


class RegimeClassifier:
    def __init__(
        self,
        adx_trend_threshold: float = 25.0,
        adx_range_threshold: float = 20.0,
        hysteresis_period: int = 2,
    ):
        """Initialize the classifier.

        Args:
            adx_trend_threshold: ADX >= this is considered "trend" region.
            adx_range_threshold: ADX <= this is considered "range" region.
            hysteresis_period: number of consecutive confirmations required to
                change regime (prevents flip-flop).
        """
        if hysteresis_period < 1:
            raise ValueError("hysteresis_period must be >= 1")
        self.adx_trend_threshold = float(adx_trend_threshold)
        self.adx_range_threshold = float(adx_range_threshold)
        self.hysteresis_period = int(hysteresis_period)

    def _get_number(self, d: Dict, *keys) -> Optional[float]:
        for k in keys:
            v = d.get(k)
            if v is None:
                continue
            try:
                return float(v)
            except Exception:
                continue
        return None

    def _raw_label(self, point: Dict) -> str:
        """Produce a raw regime label from a single feature snapshot (no hysteresis)."""
        adx = self._get_number(point, 'adx14', 'adx', 'adx_val')
        ema9 = self._get_number(point, 'ema9', 'ema_short')
        ema50 = self._get_number(point, 'ema50', 'ema_long')

        if adx is None:
            return 'unknown'

        # Trend region
        if adx >= self.adx_trend_threshold:
            # if EMA info available, decide direction
            if ema9 is not None and ema50 is not None:
                if ema9 > ema50:
                    return 'trend_up'
                elif ema9 < ema50:
                    return 'trend_down'
                else:
                    return 'transition'
            # no EMA -> generic trend
            return 'transition'

        # Range region
        if adx <= self.adx_range_threshold:
            return 'range'

        # In-between -> transition/mixed
        return 'transition'

    def classify_point(self, point: Dict) -> str:
        """Classify a single snapshot without hysteresis.

        Useful for inline checks; for production use classify_series to apply
        hysteresis.
        """
        return self._raw_label(point)

    def classify_series(self, points: List[Dict]) -> List[str]:
        """Classify an ordered sequence of snapshots and apply hysteresis.

        Hysteresis logic: a new candidate regime must appear consecutively for
        `hysteresis_period` points before the active regime is switched.
        """
        if not points:
            return []

        labels: List[str] = []
        current_label: Optional[str] = None
        candidate: Optional[str] = None
        candidate_count = 0

        for idx, p in enumerate(points):
            raw = self._raw_label(p)
            if current_label is None:
                # first sample -> accept raw immediately
                current_label = raw
                candidate = None
                candidate_count = 0
                labels.append(current_label)
                continue

            if raw == current_label:
                # continuing same regime, reset any candidate
                candidate = None
                candidate_count = 0
                labels.append(current_label)
                continue

            # raw != current_label -> treat as candidate
            if candidate is None or raw != candidate:
                candidate = raw
                candidate_count = 1
            else:
                candidate_count += 1

            if candidate_count >= self.hysteresis_period:
                # accept the candidate
                current_label = candidate
                candidate = None
                candidate_count = 0

            labels.append(current_label)

        return labels


__all__ = ['RegimeClassifier']
