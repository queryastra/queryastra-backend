"""Repositorio de `AlertRule`/`Alert`/`AlertNotification`/`AlertSilence`.

Ver `app/models/alert.py` e `Docs/database-schema.md` §4. Supressao por estado e lembrete por
tempo (ARCHITECTURE_SPEC.md Q8) sao responsabilidade do `app.workers.alerting_worker`, que usa
este repositorio para ler/gravar estado — a logica de decisao fica no worker/service, nao aqui.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import select

from app.models.alert import Alert, AlertEscalationLevel, AlertNotification, AlertRule, AlertSilence
from app.models.instance import Instance
from app.models.server import Server
from app.repositories.base import BaseRepository


@dataclass(frozen=True, slots=True)
class AlertWithNames:
    """Um Alert com o nome da regra e do alvo ja resolvidos (denormalizados para a API).

    `rule_type` (Fase 2) tambem denormalizado aqui — a UI usa pra decidir o badge/rotulo certo
    (ex.: "Recorrencia de bloqueio" para `blocking_recurrence`) sem precisar de uma 2a consulta
    por alerta."""

    alert: Alert
    alert_name: str
    rule_type: str
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

    async def get_rule_by_id(self, rule_id: UUID) -> AlertRule | None:
        return await self._session.get(AlertRule, rule_id)

    async def get_rule_by_name(self, name: str) -> AlertRule | None:
        """Usado por `PlanCompileTimeoutSupervisor` (Fase 2, item 5) para achar/criar a regra
        auto-gerada "uma por instancia" antes de criar um `Alert` — nao ha coluna dedicada pra
        essa associacao, o nome (unico por instancia, montado pelo supervisor) e a chave."""
        stmt = select(AlertRule).where(AlertRule.name == name)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def list_open_alerts(self, alert_rule_id: UUID, target_id: UUID) -> list[Alert]:
        """Todos os episodios ainda nao fechados desse par (regra, alvo) — ao contrario de
        `get_open_alert` (so o mais recente), usado quando mais de um `Alert` simultaneo faz
        sentido pro mesmo par (ex.: `plan_compile_timeout`, um por `sql_hash` distinto)."""
        stmt = select(Alert).where(
            Alert.alert_rule_id == alert_rule_id,
            Alert.target_id == target_id,
            Alert.state != "closed",
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def count_recent_alerts(self, rule_id: UUID, target_id: UUID, since: datetime) -> int:
        """Quantos `Alert` (episodios, nao notificacoes) desse par (regra, alvo) foram
        disparados desde `since` — base do escalonamento por recorrencia (Fase 2, item 4):
        conta o proprio `triggered_at`, entao um episodio que ainda esta aberto conta uma vez so
        (nao cresce enquanto continua aberto), so novos disparos aumentam a contagem."""
        stmt = select(sa.func.count()).where(
            Alert.alert_rule_id == rule_id,
            Alert.target_id == target_id,
            Alert.triggered_at >= since,
        )
        return (await self._session.execute(stmt)).scalar_one()

    async def count_open_alerts_by_target(self, target_ids: list[UUID]) -> dict[UUID, int]:
        """Quantos alertas ainda nao fechados (`state != 'closed'`, mesmo criterio de
        `get_open_alert`) cada alvo tem — usado por `GET /instances` (coluna "Saude" do resumo).
        Uma unica query agregada para todos os alvos da lista, evitando N+1. Alvos sem nenhum
        alerta aberto simplesmente nao aparecem no dict retornado (contagem implicita 0)."""
        if not target_ids:
            return {}
        # Label "open_count", nao "count": `Row` e tuple-like e ja tem um metodo `.count()`
        # herdado — um label "count" colidiria com esse metodo em vez de virar atributo.
        stmt = (
            select(Alert.target_id, sa.func.count().label("open_count"))
            .where(Alert.target_id.in_(target_ids), Alert.state != "closed")
            .group_by(Alert.target_id)
        )
        result = await self._session.execute(stmt)
        return {row.target_id: row.open_count for row in result.all()}

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
            select(Alert, AlertRule.name, AlertRule.rule_type)
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
        for alert, rule_name, rule_type in rows:
            target_name = names[(alert.target_type, alert.target_id)]
            result.append(
                AlertWithNames(
                    alert=alert, alert_name=rule_name, rule_type=rule_type, target_name=target_name
                )
            )
        return result

    async def get_alert_with_names(self, alert_id: UUID) -> AlertWithNames | None:
        alert = await self.get_alert_by_id(alert_id)
        if alert is None:
            return None
        rule = await self._session.get(AlertRule, alert.alert_rule_id)
        rule_name = rule.name if rule is not None else f"regra-removida:{alert.alert_rule_id}"
        rule_type = rule.rule_type if rule is not None else "threshold"
        names = await self._resolve_target_names([alert])
        target_name = names[(alert.target_type, alert.target_id)]
        return AlertWithNames(
            alert=alert, alert_name=rule_name, rule_type=rule_type, target_name=target_name
        )

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

    # --- Escalonamento em niveis (Fase 2, item 3) ------------------------------------------------

    async def create_escalation_levels(
        self, rule_id: UUID, levels: list[AlertEscalationLevel]
    ) -> list[AlertEscalationLevel]:
        for level in levels:
            level.alert_rule_id = rule_id
            self._session.add(level)
        await self._session.commit()
        for level in levels:
            await self._session.refresh(level)
        return levels

    async def list_escalation_levels(self, rule_id: UUID) -> list[AlertEscalationLevel]:
        stmt = (
            select(AlertEscalationLevel)
            .where(AlertEscalationLevel.alert_rule_id == rule_id)
            .order_by(AlertEscalationLevel.level_number)
        )
        return list((await self._session.execute(stmt)).scalars().all())
