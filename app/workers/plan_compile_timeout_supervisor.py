"""Supervisor de deteccao de timeout de compilacao de plano — Fase 2, item 5.

Mesmo padrao de baixa frequencia de `MissingIndexSupervisor`/`BackupStatusSupervisor`: um unico
loop acorda no intervalo configurado (`plan_compile_timeout_check_interval_seconds`, default 1
dia), relista instancias ativas e varre o cache de planos de cada uma (so SQL Server —
`collector.scan_plan_compile_timeouts()` ja devolve lista vazia sem tentar conectar no
PostgreSQL, mas este supervisor pula a chamada de proposito pra nao abrir conexao a toa).

Cada achado (`NormalizedPlanCompileTimeoutFinding`, um por `sql_hash` distinto com
`StatementOptmEarlyAbortReason`) vira um `Alert` contra uma `AlertRule` auto-criada,
`rule_type="plan_compile_timeout"`, UMA POR INSTANCIA (nome
`f"Timeout de compilacao de plano - {instance.name}"` - nao ha coluna dedicada de associacao
regra-instancia no schema, o nome e a chave de busca/criacao). Dedupe simples (mesmo racional de
`DeadlockEventsRepository.insert_new`): so cria um `Alert` novo para um `sql_hash` que ainda nao
tem um alerta aberto com esse hash na descricao — um achado que persiste entre varreduras nao
gera alerta duplicado, so o primeiro.

Mesma politica de resiliencia dos demais supervisores: erro em uma instancia nunca derruba outra
instancia nem o supervisor inteiro.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.collectors.base import Collector
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.models.alert import Alert, AlertRule
from app.normalizers.models import NormalizedPlanCompileTimeoutFinding
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.instances_repository import InstancesRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_AlertsRepositoryFactory = Callable[[AsyncSession], AlertsRepository]


def _rule_name_for_instance(instance_name: str) -> str:
    return f"Timeout de compilacao de plano - {instance_name}"


class PlanCompileTimeoutSupervisor:
    """Roda `collector.scan_plan_compile_timeouts()` para cada instancia SQL Server ativa, no
    intervalo configurado — worker de baixa frequencia, separado do `PollingSupervisor`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        alerts_repository_factory: _AlertsRepositoryFactory = AlertsRepository,
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._alerts_repository_factory = alerts_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "PlanCompileTimeoutSupervisor iniciado check_interval=%ss", self._check_interval_seconds
        )
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001 - um ciclo nunca derruba o supervisor
                logger.exception(
                    "Erro inesperado em PlanCompileTimeoutSupervisor._run_all_instances"
                )

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("PlanCompileTimeoutSupervisor finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_all_instances(self) -> None:
        async with self._session_factory() as session:
            instances = await self._instances_repository_factory(session).list_active()

        for instance in instances:
            await self._run_one_instance(instance.id)

    async def _run_one_instance(self, instance_id: UUID) -> None:
        try:
            async with self._session_factory() as session:
                instances_repository = self._instances_repository_factory(session)
                instance = await instances_repository.get_by_id(instance_id)

                if instance is None or not instance.active:
                    logger.info(
                        "Instancia instance_id=%s inativa/removida - varredura de plano pulada",
                        instance_id,
                    )
                    return

                if instance.db_type != "sqlserver":
                    # Sem tentar conectar - PostgreSQL nao tem cache de plano exposto do mesmo
                    # jeito (mesmo racional de `Collector.scan_plan_compile_timeouts`).
                    return

                username: str | None = None
                secret_value: str | None = None
                if instance.auth_method == "encrypted_fallback":
                    if instance.encrypted_credential_ref is None:
                        logger.error(
                            "encrypted_credential_ref ausente para instance_id=%s "
                            "(auth_method=encrypted_fallback) - varredura pulada",
                            instance_id,
                        )
                        return
                    try:
                        resolved = await self._credential_resolver.resolve(
                            CredentialReference(instance.encrypted_credential_ref)
                        )
                    except CredentialResolutionError:
                        logger.exception(
                            "Falha ao resolver credencial de instance_id=%s - varredura pulada",
                            instance_id,
                        )
                        return
                    username, secret_value = resolved.username, resolved.secret_value

                connection = ConnectionParams(
                    host=instance.host,
                    port=instance.port,
                    auth_method=instance.auth_method,
                    username=username,
                    secret_value=secret_value,
                )
                collector = self._create_collector(
                    instance.db_type, connection=connection, instance_id=instance.id
                )
                try:
                    findings = await collector.scan_plan_compile_timeouts()
                    if findings:
                        alerts_repository = self._alerts_repository_factory(session)
                        await self._create_alerts_for_findings(
                            alerts_repository, instance.id, instance.name, findings
                        )
                    logger.info(
                        "Varredura de plano concluida instance_id=%s finding_count=%s",
                        instance_id,
                        len(findings),
                    )
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - varredura de 1 instancia nunca derruba o supervisor
            logger.exception("Erro inesperado na varredura de plano de instance_id=%s", instance_id)

    async def _create_alerts_for_findings(
        self,
        alerts_repository: AlertsRepository,
        instance_id: UUID,
        instance_name: str,
        findings: list[NormalizedPlanCompileTimeoutFinding],
    ) -> None:
        rule_name = _rule_name_for_instance(instance_name)
        rule = await alerts_repository.get_rule_by_name(rule_name)
        if rule is None:
            rule = await alerts_repository.create_alert_rule(
                AlertRule(
                    name=rule_name,
                    target_type="instance",
                    rule_type="plan_compile_timeout",
                    active=True,
                )
            )

        open_alerts = await alerts_repository.list_open_alerts(rule.id, instance_id)
        already_alerted_hashes = {
            finding.sql_hash
            for finding in findings
            for alert in open_alerts
            if alert.description is not None and finding.sql_hash in alert.description
        }

        for finding in findings:
            if finding.sql_hash in already_alerted_hashes:
                continue
            preview = finding.statement_text_masked[:200]
            await alerts_repository.create_alert(
                Alert(
                    alert_rule_id=rule.id,
                    target_type="instance",
                    target_id=instance_id,
                    severity="warning",
                    description=(
                        f"Timeout de compilacao de plano ({finding.reason}) - "
                        f"sql_hash={finding.sql_hash} - {preview}"
                    ),
                )
            )
