"""Risk engine for portfolio-level risk controls.

Provides a lightweight RiskEngine that:
- Tracks daily realized PnL and start-of-day NAV
- Tracks peak NAV and drawdown
- Tracks consecutive losing trades
- Applies reductions to per-trade risk_pct when triggers activate
- Provides allow_entry() and get_effective_risk_pct() interfaces used by backtester

This is intentionally simple and stateful so it can be used in backtests and
live (DRY_RUN) simulations.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict


@dataclass
class RiskEngine:
    initial_risk_pct: float = 1.0  # percent
    daily_loss_limit_pct: float = 1.0  # percent (if realized loss >= this percent of start_of_day_nav -> block entries)
    max_drawdown_limit_pct: float = 10.0  # percent (if drawdown >= this percent -> reduce risk)
    consecutive_losses_threshold: int = 3
    consecutive_loss_multiplier: float = 0.5  # multiply risk_pct when threshold exceeded

    # runtime state
    start_of_day_nav: Optional[float] = None
    daily_realized_pnl: float = 0.0
    peak_nav: Optional[float] = None
    current_risk_pct: float = field(init=False)
    consecutive_losses: int = 0
    blocked_for_day: bool = False

    # counters for reporting
    daily_loss_triggers: int = 0
    dd_triggers: int = 0
    consecutive_loss_triggers: int = 0
    events: list = field(default_factory=list)

    def __post_init__(self):
        self.current_risk_pct = float(self.initial_risk_pct)

    def on_new_day(self, nav: float):
        """Call at start of a new trading day with current NAV.
        Sets the reference start_of_day_nav and resets daily realized PnL tracking.
        """
        self.start_of_day_nav = float(nav)
        self.daily_realized_pnl = 0.0
        self.blocked_for_day = False
        # update peak
        if self.peak_nav is None or nav > self.peak_nav:
            self.peak_nav = float(nav)

    def allow_entry(self) -> bool:
        """Return True if new entries are allowed under current daily limits."""
        return not self.blocked_for_day

    def get_effective_risk_pct(self, base_risk_pct: Optional[float]) -> float:
        """Return an effective risk percent to use for sizing (min of base and current cap).
        If base_risk_pct is None, use current_risk_pct.
        """
        if base_risk_pct is None:
            return float(self.current_risk_pct)
        try:
            br = float(base_risk_pct)
        except Exception:
            br = float(self.current_risk_pct)
        # risk engine enforces an upper cap
        return float(min(br, self.current_risk_pct))

    def record_trade_result(self, pnl: float, nav_after: float, timestamp=None):
        """Record realized trade result immediately after trade exit.

        Args:
            pnl: realized PnL in quote currency (KRW)
            nav_after: NAV after applying this trade's PnL
            timestamp: optional timestamp (for event logs)
        """
        # update daily realized PnL
        self.daily_realized_pnl += float(pnl)

        # update consecutive losses
        if pnl <= 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

        # update peak/nav drawdown tracking
        if self.peak_nav is None:
            self.peak_nav = float(nav_after)
        else:
            if nav_after > self.peak_nav:
                self.peak_nav = float(nav_after)

        drawdown = 0.0
        if self.peak_nav and self.peak_nav > 0:
            drawdown = (self.peak_nav - float(nav_after)) / float(self.peak_nav) * 100.0

        # check daily loss trigger (compare absolute realized loss to start_of_day_nav)
        if self.start_of_day_nav and self.start_of_day_nav > 0:
            daily_loss_pct = (-self.daily_realized_pnl) / float(self.start_of_day_nav) * 100.0 if self.daily_realized_pnl < 0 else 0.0
            if daily_loss_pct >= float(self.daily_loss_limit_pct) and not self.blocked_for_day:
                self.blocked_for_day = True
                self.daily_loss_triggers += 1
                self.events.append({'type': 'daily_loss_limit', 'daily_loss_pct': daily_loss_pct, 'timestamp': timestamp})

        # check consecutive losses trigger
        if self.consecutive_losses >= int(self.consecutive_losses_threshold):
            # reduce risk
            old = self.current_risk_pct
            self.current_risk_pct = max(0.01, float(self.current_risk_pct) * float(self.consecutive_loss_multiplier))
            self.consecutive_loss_triggers += 1
            self.events.append({'type': 'consecutive_losses', 'count': self.consecutive_losses, 'old_risk_pct': old, 'new_risk_pct': self.current_risk_pct, 'timestamp': timestamp})
            # reset consecutive losses counter after applying reduction to avoid repeated immediate triggers
            self.consecutive_losses = 0

        # check drawdown trigger
        if drawdown >= float(self.max_drawdown_limit_pct):
            old = self.current_risk_pct
            self.current_risk_pct = max(0.01, float(self.current_risk_pct) * float(self.consecutive_loss_multiplier))
            self.dd_triggers += 1
            self.events.append({'type': 'max_drawdown', 'drawdown_pct': drawdown, 'old_risk_pct': old, 'new_risk_pct': self.current_risk_pct, 'timestamp': timestamp})

    def summary(self) -> Dict:
        return {
            'initial_risk_pct': self.initial_risk_pct,
            'current_risk_pct': self.current_risk_pct,
            'daily_loss_triggers': self.daily_loss_triggers,
            'consecutive_loss_triggers': self.consecutive_loss_triggers,
            'dd_triggers': self.dd_triggers,
            'blocked_for_day': self.blocked_for_day,
            'events': list(self.events)
        }


__all__ = ['RiskEngine']
