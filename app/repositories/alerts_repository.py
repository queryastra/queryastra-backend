"""Repositorio de `AlertRule`/`Alert`/`AlertNotification`/`AlertSilence`.

Ver `app/models/alert.py` e `Docs/database-schema.md` §4. Supressao por estado e lembrete por
tempo (ARCHITECTURE_SPEC.md Q8) sao responsabilidade do `app.workers.alerting_worker`, que usa
este repositorio para ler/gravar estado — a logica de decisao fica no worker/service, nao aqui.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import select

from app.models.alert import Alert, AlertNotification, AlertRule, AlertSilence
from app.models.instance import Instance
from app.models.server import Server
from app.repositories.base import BaseRepository


@dataclass(frozen=True, slots=True)
class AlertWithNames:
    """Um Alert com o nome da regra e do alvo ja resolvidos (denormalizados para a API)."""

    alert: Alert
    alert_name: str
    target_name: str


class AlertsRepository(BaseRepository):
    """Acesso a dados de regras de alerta, alertas disparados, notificacoes e silenciamentos."""

    async def list_active_rules(self) -> list[AlertRule]:
        result = await self._session.execute(select(AlertRule).where(AlertRule.active.is_(True)))
        return list(result.scalars().all())

    async def list_rules(self) -> list[AlertRule]:
        """Todas as regras, ativas ou nao — usado por GET /alerts/rules (gestao)."""
        result = await self._session.execute(select(AlertRule))
        return list(result.scalars().all())

    async def create_alert_rule(self, rule: AlertRule) -> AlertRule:
        self._session.add(rule)
        await self._session.commit()
        await self._session.refresh(rule)
        return rule

    async def get_open_alert(self, alert_rule_id: UUID, target_id: UUID) -> Alert | None:
        """Episodio ainda nao fechado (state != 'closed') — inclui 'acknowledged', que
        continua sendo o mesmo episodio (reconhecimento nao fecha o alerta)."""
        stmt = (
            select(Alert)
            .where(
                Alert.alert_rule_id == alert_rule_id,
                Alert.target_id == target_id,
                Alert.state != "closed",
            )
            .order_by(Alert.triggered_at.desc())
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_alert_by_id(self, alert_id: UUID) -> Alert | None:
        return await self._session.get(Alert, alert_id)

    async def create_alert(self, alert: Alert) -> Alert:
        self._session.add(alert)
        await self._session.commit()
        await self._session.refresh(alert)
        return alert

    async def update_alert(self, alert: Alert) -> Alert:
        """`alert` ja deve estar anexado a sessao (obtido via get_open_alert/get_alert_by_id
        na mesma sessao) — so persiste as mutacoes feitas em memoria."""
        await self._session.commit()
        await self._session.refresh(alert)
        return alert

    async def list_alerts(self, state: str | None, limit: int) -> list[AlertWithNames]:
        stmt = (
            select(Alert, AlertRule.name)
            .join(AlertRule, AlertRule.id == Alert.alert_rule_id)
            .order_by(Alert.triggered_at.desc())
            .limit(limit)
        )
        if state is not None:
            stmt = stmt.where(Alert.state == state)
        rows = (await self._session.execute(stmt)).all()
        alerts = [row[0] for row in rows]
        names = await self._resolve_target_names(alerts)
        result: list[AlertWithNames] = []
        for alert, rule_name in rows:
            target_name = names[(alert.target_type, alert.target_id)]
            result.append(
                AlertWithNames(alert=alert, alert_name=rule_name, target_name=target_name)
            )
        return result

    async def get_alert_with_names(self, alert_id: UUID) -> AlertWithNames | None:
        alert = await self.get_alert_by_id(alert_id)
        if alert is None:
            return None
        rule = await self._session.get(AlertRule, alert.alert_rule_id)
        rule_name = rule.name if rule is not None else f"regra-removida:{alert.alert_rule_id}"
        names = await self._resolve_target_names([alert])
        target_name = names[(alert.target_type, alert.target_id)]
        return AlertWithNames(alert=alert, alert_name=rule_name, target_name=target_name)

    async def _resolve_target_names(self, alerts: list[Alert]) -> dict[tuple[str, UUID], str]:
        """2 queries em lote (nunca N+1): instancia e servidor nao tem FK polimorfica real,
        target_type decide a qual tabela olhar."""
        instance_ids = {a.target_id for a in alerts if a.target_type == "instance"}
        server_ids = {a.target_id for a in alerts if a.target_type == "server"}
        names: dict[tuple[str, UUID], str] = {}

        if instance_ids:
            rows = await self._session.execute(
                select(Instance.id, Instance.name).where(Instance.id.in_(instance_ids))
            )
            for target_id, name in rows:
                names[("instance", target_id)] = name

        if server_ids:
            rows = await self._session.execute(
                select(Server.id, Server.hostname).where(Server.id.in_(server_ids))
            )
            for target_id, hostname in rows:
                names[("server", target_id)] = hostname

        for a in alerts:
            names.setdefault((a.target_type, a.target_id), f"{a.target_type}:{a.target_id}")
        return names

    async def create_alert_notification(self, notification: AlertNotification) -> AlertNotification:
        self._session.add(notification)
        await self._session.commit()
        await self._session.refresh(notification)
        return notification

    async def get_active_silence(
        self, target_type: str, target_id: UUID, alert_rule_id: UUID
    ) -> AlertSilence | None:
        matches_rule = sa.or_(
            AlertSilence.alert_rule_id.is_(None), AlertSilence.alert_rule_id == alert_rule_id
        )
        still_active = sa.or_(
            AlertSilence.silenced_until.is_(None), AlertSilence.silenced_until > sa.func.now()
        )
        stmt = (
            select(AlertSilence)
            .where(
                AlertSilence.target_type == target_type,
                AlertSilence.target_id == target_id,
                matches_rule,
                still_active,
            )
            .order_by(AlertSilence.silenced_at.desc())
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def create_alert_silence(self, silence: AlertSilence) -> AlertSilence:
        self._session.add(silence)
        await self._session.commit()
        await self._session.refresh(silence)
        return silence

    async def list_alert_silences(self, target_id: UUID | None) -> list[AlertSilence]:
        stmt = select(AlertSilence).order_by(AlertSilence.silenced_at.desc())
        if target_id is not None:
            stmt = stmt.where(AlertSilence.target_id == target_id)
        return list((await self._session.execute(stmt)).scalars().all())

    async def delete_alert_silence(self, silence_id: UUID) -> bool:
        silence = await self._session.get(AlertSilence, silence_id)
        if silence is None:
            return False
        await self._session.delete(silence)
        await self._session.commit()
        return True
