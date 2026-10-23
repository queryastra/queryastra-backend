"""Planos/assinatura do sistema — define limites de instancias e historico por plano.

`PlanService` resolve o plano efetivo, considerando expiracao de trial e de assinaturas pagas.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class PlanConfig:
    plan_id: str
    label: str
    max_instances: int | None  # None = ilimitado
    history_days: int
    trial_days: int | None  # None = nao e plano trial


PLANS: dict[str, PlanConfig] = {
    "trial": PlanConfig("trial", "Trial", max_instances=None, history_days=30, trial_days=30),
    "free":  PlanConfig("free",  "Free",  max_instances=1,    history_days=14, trial_days=None),
    "pro":   PlanConfig("pro",   "Pro",   max_instances=5,    history_days=90,  trial_days=None),
    "team":  PlanConfig("team",  "Team",  max_instances=20,   history_days=365, trial_days=None),
}


class PlanService:
    """Resolve o plano efetivo do sistema, tratando expiracao de trial e de assinaturas."""

    def __init__(
        self,
        trial_started_at: datetime,
        plan_id: str,
        subscription_expires_at: datetime | None,
    ) -> None:
        self._trial_started_at = trial_started_at
        self._plan_id = plan_id
        self._subscription_expires_at = subscription_expires_at

    def effective_plan(self) -> PlanConfig:
        now = datetime.now(UTC)
        if self._plan_id == "trial":
            trial_end = self._trial_started_at + timedelta(days=PLANS["trial"].trial_days)  # type: ignore[arg-type]
            if now > trial_end:
                return PLANS["free"]
            return PLANS["trial"]
        if self._plan_id in ("pro", "team"):
            if self._subscription_expires_at and now > self._subscription_expires_at:
                return PLANS["free"]
        return PLANS.get(self._plan_id, PLANS["free"])

    def trial_days_remaining(self) -> int | None:
        if self._plan_id != "trial":
            return None
        trial_end = self._trial_started_at + timedelta(days=PLANS["trial"].trial_days)  # type: ignore[arg-type]
        remaining = (trial_end - datetime.now(UTC)).days
        return max(0, remaining)

    def clamp_from(self, from_dt: datetime) -> datetime:
        """Limita `from_dt` ao historico permitido pelo plano efetivo."""
        plan = self.effective_plan()
        min_from = datetime.now(UTC) - timedelta(days=plan.history_days)
        if from_dt.tzinfo is None:
            from_dt = from_dt.replace(tzinfo=UTC)
        return max(from_dt, min_from)
