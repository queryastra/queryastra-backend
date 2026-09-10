"""Repositorio de `AlertRule`/`Alert`/`AlertNotification`/`AlertSilence` — stub.

Ver `app/models/alert.py` e `Docs/database-schema.md` §4. Supressao por estado e lembrete por
tempo (ARCHITECTURE_SPEC.md Q8) sao responsabilidade do `app.workers.alerting_worker`, que usa
este repositorio para ler/gravar estado — a logica de decisao fica no worker/service, nao aqui.
"""

from __future__ import annotations

from uuid import UUID

from app.models.alert import Alert, AlertRule
from app.repositories.base import BaseRepository


class AlertsRepository(BaseRepository):
    """Acesso a dados de regras de alerta e alertas disparados."""

    async def list_active_rules(self) -> list[AlertRule]:
        raise NotImplementedError("AlertsRepository.list_active_rules ainda nao implementado.")

    async def get_open_alert(self, alert_rule_id: UUID, target_id: UUID) -> Alert | None:
        raise NotImplementedError("AlertsRepository.get_open_alert ainda nao implementado.")

    async def create_alert(self, alert: Alert) -> Alert:
        raise NotImplementedError("AlertsRepository.create_alert ainda nao implementado.")
