"""Worker de polling por instancia.

Contrato (Docs/specs/01-collection-contract.md, Docs/rules/02-safe-collection.md):
    - cada instancia e polada no seu proprio `polling_interval_seconds`
      (`app/models/instance.py`, default 10s — ARCHITECTURE_SPEC.md Q3: 5s de default de
      produto recomendado, configuravel ate 1s em instancia critica; o default de schema fica
      conservador ate o throughput do pipeline ser validado);
    - o loop checa `instance_maintenance_windows` antes de cada ciclo — se `suppress_polling`
      e o horario atual (UTC) cai dentro da janela, o ciclo inteiro e pulado (nenhuma amostra
      coletada nem persistida, nenhuma telemetria de ciclo gravada);
    - erro de coleta (rede, timeout, qualquer `Exception`) e capturado por ciclo — nunca
      derruba o loop da instancia inteira; ainda assim grava telemetria de duracao/erro;
    - o loop nunca roda mais rapido que `polling_interval_seconds` nem acumula ciclos
      atrasados (skip, nao enfileira);
    - quando `system_settings_repository` e injetado, bloqueios com `wait_time_ms` abaixo do
      limite minimo sao filtrados ANTES de persistir em `blocking_events`/`blocking_incidents` —
      lido a cada ciclo (nao cacheado), pra uma mudanca valer no proximo ciclo sem restart. O
      limite efetivo prioriza, do mais especifico pro mais generico: (1) override ad-hoc por
      BANCO DE DADOS (`instance_database_lock_overrides_repository`, so se o `blocked_database_
      name` do evento bater com um banco configurado); (2) override ad-hoc por INSTANCIA
      (`instance.min_lock_time_ms`); (3) valor global (`system_settings.blocking_min_duration_ms`,
      tela "Configuracoes -> Geral") — permite afinar temporariamente uma investigacao num
      unico banco, ou numa instancia inteira, sem afetar o resto.

Convencao de `day_of_week` (`InstanceMaintenanceWindow`, `app/models/instance.py`): 0 = domingo,
conforme o comentario explicito no modelo (`# 0 = domingo`) e a CHECK `BETWEEN 0 AND 6` — NAO e
a convencao de `datetime.weekday()` do Python (Monday=0..Sunday=6). A conversao usada aqui e
`(weekday() + 1) % 7`. A janela e comparada em UTC (`datetime.now(UTC)`) e assume que
`start_time <= end_time` no mesmo dia — janelas que cruzam meia-noite (`end_time < start_time`)
NAO sao tratadas (corte de escopo desta feature, documentado em `_is_within_maintenance_window`).

TODO (fora de escopo desta feature, ver plano): nenhum supervisor auto-inicia `PollingWorker`
por instancia ativa em `app/main.py` — o worker so precisa ser instanciavel/testavel
isoladamente; orquestracao (ler `instances.active`, criar/parar workers dinamicamente) fica
para uma feature futura dedicada.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from uuid import UUID

from app.collectors.base import Collector
from app.core.events import event_bus
from app.core.logging import get_logger
from app.models.instance import Instance
from app.models.system_settings import SystemSettings
from app.normalizers.metric_names import (
    COLLECTOR_CYCLE_DURATION_MS,
    COLLECTOR_CYCLE_ERROR_COUNT,
    SQLSERVER_DEADLOCKS_TOTAL,
)
from app.normalizers.models import NormalizedBlockingEvent, NormalizedMetricSample
from app.repositories.blocking_events_repository import BlockingEventsRepository
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.deadlock_events_repository import DeadlockEventsRepository
from app.repositories.instance_database_lock_overrides_repository import (
    InstanceDatabaseLockOverridesRepository,
)
from app.repositories.instance_database_sleeping_overrides_repository import (
    InstanceDatabaseSleepingOverridesRepository,
)
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.repositories.query_samples_repository import QuerySamplesRepository
from app.repositories.system_settings_repository import SystemSettingsRepository
from app.repositories.transaction_events_repository import TransactionEventsRepository

# Cap defensivo pro numero de registros "pendentes" criados num unico ciclo, mesmo que o
# contador cumulativo salte um valor muito maior (ex.: apos um restart do SQL Server que zera o
# contador e o proximo ciclo o ve "subir" de 0 pra um valor alto de outra fonte - cenario nao
# esperado na pratica, mas evita uma inundacao de linhas pendentes por engano).
_MAX_PENDING_DEADLOCKS_PER_CYCLE = 10

logger = get_logger(__name__)


def _is_within_maintenance_window(instance: Instance, now: datetime) -> bool:
    """True se `now` (UTC) cai dentro de uma janela de manutencao com `suppress_polling=True`.

    Assume UTC e `start_time <= end_time` no mesmo dia — NAO trata cruzamento de meia-noite
    (`end_time < start_time`), corte de escopo desta feature.
    """
    schema_day_of_week = (now.weekday() + 1) % 7  # Python Monday=0 -> schema domingo=0
    current_time = now.time()  # datetime.time() ja descarta tzinfo, comparavel a Time (naive)
    for window in instance.maintenance_windows:
        if not window.suppress_polling:
            continue
        if window.day_of_week != schema_day_of_week:
            continue
        if window.start_time <= current_time <= window.end_time:
            return True
    return False


class PollingWorker:
    """Loop de polling de uma unica instancia.

    Uso pretendido: uma instancia deste worker por `Instance` ativa, orquestrado por um
    supervisor (ainda nao implementado, fora de escopo) que le `instances.active` e
    `instances.polling_interval_seconds` para criar/parar workers dinamicamente.
    """

    def __init__(
        self,
        instance_id: UUID,
        collector: Collector,
        interval_seconds: int,
        metric_samples_repository: MetricSamplesRepository,
        query_samples_repository: QuerySamplesRepository,
        blocking_events_repository: BlockingEventsRepository,
        instances_repository: InstancesRepository,
        transaction_events_repository: TransactionEventsRepository | None = None,
        blocking_incidents_repository: BlockingIncidentsRepository | None = None,
        *,
        system_settings_repository: SystemSettingsRepository | None = None,
        instance_database_lock_overrides_repository: (
            InstanceDatabaseLockOverridesRepository | None
        ) = None,
        deadlock_events_repository: DeadlockEventsRepository | None = None,
        instance_database_sleeping_overrides_repository: (
            InstanceDatabaseSleepingOverridesRepository | None
        ) = None,
    ) -> None:
        self._instance_id = instance_id
        self._collector = collector
        self._interval_seconds = interval_seconds
        self._metric_samples_repository = metric_samples_repository
        self._query_samples_repository = query_samples_repository
        self._blocking_events_repository = blocking_events_repository
        self._instances_repository = instances_repository
        # Opcionais (default None) — testes/chamadores existentes que nao passam esses
        # repositorios continuam funcionando sem persistir transacoes/incidentes de bloqueio
        # (so ficam ativos quando o chamador injeta o repositorio de verdade).
        self._transaction_events_repository = transaction_events_repository
        self._blocking_incidents_repository = blocking_incidents_repository
        # Opcional, keyword-only (default None = sem filtro, todo bloqueio e persistido, mesmo
        # comportamento de antes desta feature) — lido A CADA CICLO (nao cacheado), pra uma
        # mudanca na tela "Configuracoes -> Geral" valer a partir do proximo ciclo sem restart.
        self._system_settings_repository = system_settings_repository
        # Opcional, keyword-only (default None) — override ad-hoc por BANCO DE DADOS dentro
        # desta instancia (mais granular que `instance.min_lock_time_ms`), tambem lido A CADA
        # CICLO, mesma filosofia de nunca cachear este tipo de configuracao.
        self._instance_database_lock_overrides_repository = (
            instance_database_lock_overrides_repository
        )
        # Opcional, keyword-only (default None) — deteccao instantanea de deadlock (bug real
        # relatado ao vivo pelo usuario: o `system_health` demorou 25-35min pra publicar o
        # relatorio completo). So ativa quando o chamador injeta o repositorio de verdade, mesmo
        # padrao dos demais repositorios opcionais acima.
        self._deadlock_events_repository = deadlock_events_repository
        # Opcional, keyword-only (default None) — mesmo padrao de
        # `instance_database_lock_overrides_repository`, para "Transacoes em Sleeping" (novas
        # tarefas do usuario, itens 1/2).
        self._instance_database_sleeping_overrides_repository = (
            instance_database_sleeping_overrides_repository
        )
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Loop principal: coleta, dorme `interval_seconds`, repete ate `stop()`."""
        logger.info(
            "PollingWorker iniciado instance_id=%s interval=%ss",
            self._instance_id,
            self._interval_seconds,
        )
        while not self._stop_event.is_set():
            await self._run_cycle()

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass  # intervalo normal entre ciclos

    async def _run_cycle(self) -> None:
        """Executa um unico ciclo: checagem de janela de manutencao, coleta e persistencia.

        Publico o suficiente (nome com `_` mas sem underscore duplo) para ser chamado
        diretamente em teste/smoke-test sem esperar `interval_seconds`.
        """
        instance = await self._instances_repository.get_by_id(self._instance_id)
        now = datetime.now(UTC)

        if instance is not None and _is_within_maintenance_window(instance, now):
            logger.info(
                "Ciclo suprimido por janela de manutencao (suppress_polling) instance_id=%s",
                self._instance_id,
            )
            return

        cycle_start = time.perf_counter()
        error_count = 0
        try:
            # Linha de `system_settings` lida NO MAXIMO 1x por ciclo (nao 1x por parametro) —
            # reaproveitada tanto pelo limite de "sleeping" (resolvido abaixo, ANTES de coletar)
            # quanto pelo de bloqueio (resolvido mais adiante, DEPOIS de coletar).
            system_settings_row: SystemSettings | None = None

            async def _get_system_settings_row() -> SystemSettings | None:
                nonlocal system_settings_row
                if system_settings_row is None and self._system_settings_repository is not None:
                    system_settings_row = await self._system_settings_repository.get()
                return system_settings_row

            # Limite efetivo de "Transacoes em Sleeping" (novas tarefas do usuario, itens 1/2) —
            # resolvido ANTES de coletar (mesma precedencia banco > instancia > global de
            # `blocking_min_duration_ms`) porque o coletor decide, DURANTE a coleta, se cada
            # transacao aberta e "digna de nota" o suficiente pra virar uma linha em
            # `transaction_events` (flag_reason) — diferente de bloqueio, que e filtrado DEPOIS
            # de coletar (ver abaixo).
            sleeping_min_duration_ms: int | None = (
                instance.min_sleeping_time_ms if instance is not None else None
            )
            if sleeping_min_duration_ms is None:
                settings_row = await _get_system_settings_row()
                if settings_row is not None:
                    sleeping_min_duration_ms = settings_row.sleeping_min_duration_ms

            sleeping_database_overrides_ms: dict[str, int] = {}
            if self._instance_database_sleeping_overrides_repository is not None:
                sleeping_database_overrides_ms = {
                    override.database_name: override.min_sleeping_time_ms
                    for override in (
                        await (
                            self._instance_database_sleeping_overrides_repository
                        ).list_for_instance(self._instance_id)
                    )
                }

            result = await self._collector.collect(
                sleeping_min_duration_ms=sleeping_min_duration_ms,
                sleeping_database_overrides_ms=sleeping_database_overrides_ms,
            )

            # Deteccao instantanea de deadlock (bug real relatado ao vivo pelo usuario, ver
            # `DeadlockEventsRepository`) — a leitura da metrica PRECEDE o `insert_batch` abaixo
            # (que grava o valor NOVO), senao `get_latest_value` devolveria o valor que estamos
            # prestes a inserir agora, nao o do ciclo anterior.
            previous_deadlock_count: float | None = None
            if self._deadlock_events_repository is not None:
                previous_row = await self._metric_samples_repository.get_latest_value(
                    "instance", self._instance_id, SQLSERVER_DEADLOCKS_TOTAL
                )
                previous_deadlock_count = previous_row.value if previous_row is not None else None

            await self._metric_samples_repository.insert_batch(result.metric_samples)
            await self._query_samples_repository.insert_batch(result.query_samples)

            if self._deadlock_events_repository is not None and previous_deadlock_count is not None:
                new_deadlock_sample = next(
                    (
                        sample
                        for sample in result.metric_samples
                        if sample.metric_name == SQLSERVER_DEADLOCKS_TOTAL
                    ),
                    None,
                )
                if (
                    new_deadlock_sample is not None
                    and new_deadlock_sample.value > previous_deadlock_count
                ):
                    increment = int(new_deadlock_sample.value - previous_deadlock_count)
                    for _ in range(min(increment, _MAX_PENDING_DEADLOCKS_PER_CYCLE)):
                        await self._deadlock_events_repository.insert_pending(
                            self._instance_id, now
                        )
                    logger.info(
                        "Deadlock detectado na hora (contador incrementou) "
                        "instance_id=%s increment=%s",
                        self._instance_id,
                        increment,
                    )

            blocking_events = result.blocking_events
            # Override ad-hoc por instancia (`instance.min_lock_time_ms`, a pedido explicito do
            # usuario — investigar um caso especifico sem afetar as demais instancias) tem
            # prioridade sobre o valor global (`system_settings.blocking_min_duration_ms`); só
            # cai pro global quando o override esta NULL (padrao).
            instance_min_duration_ms: int | None = (
                instance.min_lock_time_ms if instance is not None else None
            )
            if instance_min_duration_ms is None:
                settings_row = await _get_system_settings_row()
                if settings_row is not None:
                    instance_min_duration_ms = settings_row.blocking_min_duration_ms

            # Override ad-hoc por BANCO DE DADOS (mais granular que o da instancia inteira) —
            # tem prioridade MAIOR AINDA, mas só se aplica ao evento cujo `blocked_database_name`
            # bate com o banco configurado; eventos de outros bancos (ou sem banco resolvido)
            # caem pro limite da instancia/global normalmente.
            database_overrides_ms: dict[str, int] = {}
            if self._instance_database_lock_overrides_repository is not None:
                database_overrides_ms = {
                    override.database_name: override.min_lock_time_ms
                    for override in (
                        await self._instance_database_lock_overrides_repository.list_for_instance(
                            self._instance_id
                        )
                    )
                }

            def _passes_threshold(event: NormalizedBlockingEvent) -> bool:
                threshold = instance_min_duration_ms
                if (
                    event.blocked_database_name is not None
                    and event.blocked_database_name in database_overrides_ms
                ):
                    threshold = database_overrides_ms[event.blocked_database_name]
                if threshold is None or threshold <= 0:
                    return True
                return event.wait_time_ms >= threshold

            blocking_events = [event for event in blocking_events if _passes_threshold(event)]

            await self._blocking_events_repository.insert_batch(blocking_events)
            if self._blocking_incidents_repository is not None:
                await self._blocking_incidents_repository.upsert_batch(
                    self._instance_id, blocking_events, now
                )
            if self._transaction_events_repository is not None:
                await self._transaction_events_repository.upsert_batch(
                    self._instance_id,
                    result.transaction_events,
                    set(result.open_transaction_identifiers),
                    now,
                )
            if result.engine_version is not None and result.engine_version_major is not None:
                await self._instances_repository.update_engine_version(
                    self._instance_id, result.engine_version, result.engine_version_major
                )
        except Exception:  # noqa: BLE001 — worker nunca deve morrer por erro de 1 ciclo
            error_count = 1
            logger.exception(
                "Erro inesperado no ciclo de polling de instance_id=%s", self._instance_id
            )

        cycle_duration_ms = (time.perf_counter() - cycle_start) * 1000
        telemetry_time = datetime.now(UTC)
        await self._metric_samples_repository.insert_batch(
            [
                NormalizedMetricSample(
                    time=telemetry_time,
                    target_type="instance",
                    target_id=self._instance_id,
                    metric_name=COLLECTOR_CYCLE_DURATION_MS,
                    value=cycle_duration_ms,
                ),
                NormalizedMetricSample(
                    time=telemetry_time,
                    target_type="instance",
                    target_id=self._instance_id,
                    metric_name=COLLECTOR_CYCLE_ERROR_COUNT,
                    value=float(error_count),
                ),
            ]
        )

        # Push em tempo real (`/ws/live`): notifica que um ciclo de coleta terminou para esta
        # instancia — nao publicado quando o ciclo e suprimido por janela de manutencao (return
        # antecipado acima, antes deste ponto).
        event_bus.publish(
            {
                "type": "instance_collected",
                "instance_id": str(self._instance_id),
                "error_count": error_count,
                "duration_ms": int(cycle_duration_ms),
            }
        )

    def stop(self) -> None:
        """Sinaliza o loop para parar no proximo ponto de checagem."""
        self._stop_event.set()
