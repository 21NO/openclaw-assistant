"""Risk engine for portfolio-level risk controls.

Provides a lightweight RiskEngine that:
- Tracks daily realized PnL and start-of-day NAV
- Tracks peak NAV and drawdown
- Tracks consecutive losing trades
- Applies reductions to per-trade risk_pct when triggers activate
- Limits depth of reductions and supports a conservative recovery/ramp-up
- Provides allow_entry() and get_effective_risk_pct() interfaces used by backtester

This is intentionally simple and stateful so it can be used in backtests and
live (DRY_RUN) simulations.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict


@dataclass
class RiskEngine:
    # configuration
    initial_risk_pct: float = 1.0  # percent
    min_risk_pct: float = 0.05  # do not reduce below this fraction (absolute, e.g. 0.05 -> 5% of initial equity)
    daily_loss_limit_pct: float = 1.0  # percent (if realized loss >= this percent of start_of_day_nav -> block entries)
    max_drawdown_limit_pct: float = 10.0  # percent (if drawdown >= this percent -> reduce risk)
    consecutive_losses_threshold: int = 3
    consecutive_loss_multiplier: float = 0.5  # multiply risk_pct when threshold exceeded
    max_reduction_steps: int = 5  # maximum number of multiplicative reductions allowed

    # recovery configuration
    recovery_enabled: bool = True
    recovery_consec_wins: int = 3  # number of consecutive winning trades to trigger a partial recovery
    recovery_step_pct_of_initial: float = 0.1  # recover by this fraction of initial_risk_pct (absolute)

    # runtime state
    start_of_day_nav: Optional[float] = None
    daily_realized_pnl: float = 0.0
    peak_nav: Optional[float] = None
    current_risk_pct: float = field(init=False)
    consecutive_losses: int = 0
    consecutive_wins: int = 0
    blocked_for_day: bool = False

    # counters for reporting
    daily_loss_triggers: int = 0
    dd_triggers: int = 0
    consecutive_loss_triggers: int = 0
    reduction_steps: int = 0
    events: list = field(default_factory=list)

    def __post_init__(self):
        # ensure sensible bounds
        self.initial_risk_pct = float(self.initial_risk_pct)
        self.min_risk_pct = float(self.min_risk_pct)
        if self.min_risk_pct < 0.0:
            self.min_risk_pct = 0.01
        # cap min to initial
        if self.min_risk_pct > self.initial_risk_pct:
            self.min_risk_pct = float(self.initial_risk_pct)
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

    def _apply_reduction(self, reason: str, timestamp=None):
        """Apply a multiplicative reduction to current_risk_pct honoring min and max steps."""
        if self.reduction_steps >= int(self.max_reduction_steps):
            # already at max reductions; do not reduce further
            self.events.append({'type': 'reduction_limited', 'reason': reason, 'reduction_steps': self.reduction_steps, 'timestamp': timestamp})
            return
        old = float(self.current_risk_pct)
        new = float(self.current_risk_pct) * float(self.consecutive_loss_multiplier)
        # enforce floor
        if new < float(self.min_risk_pct):
            new = float(self.min_risk_pct)
        # only record if change
        if new != old:
            self.current_risk_pct = new
            self.reduction_steps += 1
            self.consecutive_loss_triggers += 1
            self.events.append({'type': 'risk_reduction', 'reason': reason, 'old_risk_pct': old, 'new_risk_pct': new, 'timestamp': timestamp, 'reduction_steps': self.reduction_steps})

    def _attempt_recovery(self, timestamp=None):
        """Attempt to recover risk_pct toward initial_risk_pct in small steps when conditions met."""
        if not self.recovery_enabled:
            return
        if self.current_risk_pct >= self.initial_risk_pct:
            return
        # increase by a fraction of initial risk (absolute)
        step = float(self.initial_risk_pct) * float(self.recovery_step_pct_of_initial)
        candidate = float(self.current_risk_pct) + step
        if candidate > self.initial_risk_pct:
            candidate = float(self.initial_risk_pct)
        old = float(self.current_risk_pct)
        if candidate > old:
            self.current_risk_pct = candidate
            self.events.append({'type': 'risk_recovery', 'old_risk_pct': old, 'new_risk_pct': self.current_risk_pct, 'timestamp': timestamp})

    def record_trade_result(self, pnl: float, nav_after: float, timestamp=None):
        """Record realized trade result immediately after trade exit.

        Args:
            pnl: realized PnL in quote currency (KRW)
            nav_after: NAV after applying this trade's PnL
            timestamp: optional timestamp (for event logs)
        """
        # update daily realized PnL
        self.daily_realized_pnl += float(pnl)

        # update consecutive trackers
        if pnl <= 0:
            self.consecutive_losses += 1
            self.consecutive_wins = 0
        else:
            self.consecutive_wins += 1
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
            self._apply_reduction('consecutive_losses', timestamp=timestamp)
            # reset consecutive losses counter after applying reduction to avoid repeated immediate triggers
            self.consecutive_losses = 0

        # check drawdown trigger
        if drawdown >= float(self.max_drawdown_limit_pct):
            self._apply_reduction('max_drawdown', timestamp=timestamp)

        # attempt recovery on consecutive wins
        if self.consecutive_wins >= int(self.recovery_consec_wins):
            self._attempt_recovery(timestamp=timestamp)
            # reset wins counter after recovery step
            self.consecutive_wins = 0

    def evaluate_proposal(self, proposal: Optional[dict], portfolio_snapshot: Optional[dict] = None, market_state: Optional[dict] = None, timestamp=None) -> Dict:
        """Evaluate a proposed position and return a decision dict.

        Decision format:
            {
                'allow': bool,
                'scale': float,  # 0..1 multiplier to apply to proposed units
                'adjusted_risk_pct': float,  # effective per-trade risk pct after scaling
                'reason': str,
                'events': list
            }
        The method enforces daily blocks and caps the proposal's suggested_risk_pct by the current_risk_pct.
        """
        # If entries are blocked for the day, veto immediately
        if self.blocked_for_day:
            evt = {'type': 'risk_vetoed', 'reason': 'daily_loss_blocked', 'timestamp': timestamp}
            self.events.append(evt)
            return {'allow': False, 'scale': 0.0, 'adjusted_risk_pct': 0.0, 'reason': 'daily_loss_blocked', 'events': [evt]}

        # Extract base risk from proposal
        base_risk = None
        if proposal and isinstance(proposal, dict):
            base_risk = proposal.get('suggested_risk_pct') or proposal.get('suggested_risk')
        # allow objects with attributes by trying getattr
        if base_risk is None and proposal is not None:
            try:
                base_risk = getattr(proposal, 'suggested_risk_pct', None) or getattr(proposal, 'suggested_risk', None)
            except Exception:
                base_risk = None

        try:
            base_risk = float(base_risk) if base_risk is not None else float(self.initial_risk_pct)
        except Exception:
            base_risk = float(self.initial_risk_pct)

        # If base_risk is zero or negative, allow by default
        if base_risk <= 0.0:
            return {'allow': True, 'scale': 1.0, 'adjusted_risk_pct': float(self.initial_risk_pct), 'reason': 'no_base_risk', 'events': []}

        # Compute scale as ratio of current cap to base risk
        try:
            scale = float(self.current_risk_pct) / float(base_risk) if float(base_risk) > 0 else 1.0
        except Exception:
            scale = 1.0

        # Clamp scale
        if scale <= 0.0:
            evt = {'type': 'risk_vetoed', 'reason': 'scale_zero', 'timestamp': timestamp}
            self.events.append(evt)
            return {'allow': False, 'scale': 0.0, 'adjusted_risk_pct': 0.0, 'reason': 'scale_zero', 'events': [evt]}
        if scale > 1.0:
            scale = 1.0

        adjusted = float(base_risk) * float(scale)
        # Record a scaling event if scale < 1
        if scale < 1.0:
            evt = {'type': 'risk_scaled', 'reason': 'cap_enforced_current_risk', 'base_risk_pct': float(base_risk), 'new_risk_pct': adjusted, 'scale': scale, 'timestamp': timestamp}
            self.events.append(evt)
            return {'allow': True, 'scale': float(scale), 'adjusted_risk_pct': adjusted, 'reason': 'cap_enforced_current_risk', 'events': [evt]}

        # otherwise allow as-is
        return {'allow': True, 'scale': 1.0, 'adjusted_risk_pct': adjusted, 'reason': 'ok', 'events': []}

    def summary(self) -> Dict:
        return {
            'initial_risk_pct': self.initial_risk_pct,
            'min_risk_pct': self.min_risk_pct,
            'current_risk_pct': self.current_risk_pct,
            'daily_loss_triggers': self.daily_loss_triggers,
            'consecutive_loss_triggers': self.consecutive_loss_triggers,
            'dd_triggers': self.dd_triggers,
            'reduction_steps': self.reduction_steps,
            'blocked_for_day': self.blocked_for_day,
            'events': list(self.events)
        }


__all__ = ['RiskEngine']
