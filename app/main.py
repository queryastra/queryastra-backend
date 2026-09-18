"""FastAPI app factory — `Docs/specs/02-backend-contract.md`.

"O backend FastAPI expoe cadastro, health-check, leitura de metricas, consultas de detalhe e
configuracao de alertas. Jobs de coleta rodam fora das rotas HTTP e usam adaptadores por SGBD."

O lifespan liga/desliga o `PollingSupervisor` (`app.workers.polling_supervisor`) quando
`POLLING_SUPERVISOR_ENABLED=true` e o `AlertingWorker` (`app.workers.alerting_worker`) quando
`ALERTING_WORKER_ENABLED=true`; caso contrario cada um e no-op (default seguro — nunca ligar
coleta/avaliacao real sem configuracao explicita).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_v1_router
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import async_session_factory
from app.workers.alerting_worker import AlertingWorker, EmailSettings
from app.workers.backup_status_supervisor import BackupStatusSupervisor
from app.workers.deadlock_supervisor import DeadlockSupervisor
from app.workers.extended_health_metrics_supervisor import ExtendedHealthMetricsSupervisor
from app.workers.missing_index_supervisor import MissingIndexSupervisor
from app.workers.plan_compile_timeout_supervisor import PlanCompileTimeoutSupervisor
from app.workers.plan_history_supervisor import PlanHistorySupervisor
from app.workers.polling_supervisor import PollingSupervisor
from app.workers.problem_events_retention_worker import ProblemEventsRetentionWorker

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Liga PollingSupervisor/AlertingWorker conforme os respectivos flags; no-op caso contrario."""
    settings = get_settings()

    # Substitui o executor padrao do event loop (`min(32, os.cpu_count() + 4)`, baixo demais
    # para varias instancias reais coletando em paralelo via pymssql/pyodbc sincrono) por um
    # dimensionado explicitamente (`BACKEND_EXECUTOR_MAX_WORKERS`) - ver docstring da config.
    # Precisa ser feito com o loop ja rodando (dentro do lifespan), antes de qualquer worker
    # iniciar, senao workers que ja chamaram `asyncio.to_thread` pegam o executor antigo.
    executor = ThreadPoolExecutor(max_workers=settings.backend_executor_max_workers)
    asyncio.get_running_loop().set_default_executor(executor)
    logger.info(
        "Executor padrao do event loop dimensionado para %s threads (era min(32, cpu_count+4))",
        settings.backend_executor_max_workers,
    )

    supervisor: PollingSupervisor | None = None
    supervisor_task: asyncio.Task[None] | None = None
    alerting_worker: AlertingWorker | None = None
    alerting_task: asyncio.Task[None] | None = None
    missing_index_supervisor: MissingIndexSupervisor | None = None
    missing_index_task: asyncio.Task[None] | None = None
    backup_status_supervisor: BackupStatusSupervisor | None = None
    backup_status_task: asyncio.Task[None] | None = None
    deadlock_supervisor: DeadlockSupervisor | None = None
    deadlock_task: asyncio.Task[None] | None = None
    problem_events_retention_worker: ProblemEventsRetentionWorker | None = None
    problem_events_retention_task: asyncio.Task[None] | None = None
    extended_health_metrics_supervisor: ExtendedHealthMetricsSupervisor | None = None
    extended_health_metrics_task: asyncio.Task[None] | None = None
    plan_compile_timeout_supervisor: PlanCompileTimeoutSupervisor | None = None
    plan_compile_timeout_task: asyncio.Task[None] | None = None
    plan_history_supervisor: PlanHistorySupervisor | None = None
    plan_history_task: asyncio.Task[None] | None = None

    if settings.polling_supervisor_enabled:
        supervisor = PollingSupervisor(
            async_session_factory, settings.supervisor_discovery_interval_seconds
        )
        supervisor_task = asyncio.create_task(supervisor.run())
        logger.info(
            "PollingSupervisor habilitado (discovery_interval=%ss)",
            settings.supervisor_discovery_interval_seconds,
        )
    else:
        logger.info("PollingSupervisor desabilitado (POLLING_SUPERVISOR_ENABLED=false)")

    if settings.alerting_worker_enabled:
        email_settings: EmailSettings | None = None
        if settings.alert_smtp_host:
            email_settings = EmailSettings(
                host=settings.alert_smtp_host,
                port=settings.alert_smtp_port,
                username=settings.alert_smtp_username,
                password=settings.alert_smtp_password,
                sender=settings.alert_smtp_from or "alerts@ab-xql-pm.local",
                recipients=[
                    address.strip()
                    for address in (settings.alert_email_to or "").split(",")
                    if address.strip()
                ],
            )
        alerting_worker = AlertingWorker(
            async_session_factory,
            settings.alerting_evaluation_interval_seconds,
            settings.alert_critical_reminder_interval_seconds,
            webhook_url=settings.alert_webhook_url,
            email_settings=email_settings,
            custom_sql_timeout_seconds=settings.custom_sql_alert_timeout_seconds,
        )
        alerting_task = asyncio.create_task(alerting_worker.run())
        logger.info(
            "AlertingWorker habilitado (interval=%ss)",
            settings.alerting_evaluation_interval_seconds,
        )
    else:
        logger.info("AlertingWorker desabilitado (ALERTING_WORKER_ENABLED=false)")

    if settings.missing_index_supervisor_enabled:
        missing_index_supervisor = MissingIndexSupervisor(
            async_session_factory, settings.missing_index_check_interval_seconds
        )
        missing_index_task = asyncio.create_task(missing_index_supervisor.run())
        logger.info(
            "MissingIndexSupervisor habilitado (check_interval=%ss)",
            settings.missing_index_check_interval_seconds,
        )
    else:
        logger.info("MissingIndexSupervisor desabilitado (MISSING_INDEX_SUPERVISOR_ENABLED=false)")

    if settings.backup_status_supervisor_enabled:
        backup_status_supervisor = BackupStatusSupervisor(
            async_session_factory, settings.backup_status_check_interval_seconds
        )
        backup_status_task = asyncio.create_task(backup_status_supervisor.run())
        logger.info(
            "BackupStatusSupervisor habilitado (check_interval=%ss)",
            settings.backup_status_check_interval_seconds,
        )
    else:
        logger.info("BackupStatusSupervisor desabilitado (BACKUP_STATUS_SUPERVISOR_ENABLED=false)")

    if settings.deadlock_supervisor_enabled:
        deadlock_supervisor = DeadlockSupervisor(
            async_session_factory, settings.deadlock_check_interval_seconds
        )
        deadlock_task = asyncio.create_task(deadlock_supervisor.run())
        logger.info(
            "DeadlockSupervisor habilitado (check_interval=%ss)",
            settings.deadlock_check_interval_seconds,
        )
    else:
        logger.info("DeadlockSupervisor desabilitado (DEADLOCK_SUPERVISOR_ENABLED=false)")

    if settings.problem_events_retention_worker_enabled:
        problem_events_retention_worker = ProblemEventsRetentionWorker(
            async_session_factory,
            settings.problem_events_retention_days,
            settings.problem_events_retention_check_interval_seconds,
        )
        problem_events_retention_task = asyncio.create_task(problem_events_retention_worker.run())
        logger.info(
            "ProblemEventsRetentionWorker habilitado (retention_days=%s, check_interval=%ss)",
            settings.problem_events_retention_days,
            settings.problem_events_retention_check_interval_seconds,
        )
    else:
        logger.info(
            "ProblemEventsRetentionWorker desabilitado "
            "(PROBLEM_EVENTS_RETENTION_WORKER_ENABLED=false)"
        )

    if settings.extended_health_metrics_supervisor_enabled:
        extended_health_metrics_supervisor = ExtendedHealthMetricsSupervisor(
            async_session_factory, settings.extended_health_metrics_check_interval_seconds
        )
        extended_health_metrics_task = asyncio.create_task(extended_health_metrics_supervisor.run())
        logger.info(
            "ExtendedHealthMetricsSupervisor habilitado (check_interval=%ss)",
            settings.extended_health_metrics_check_interval_seconds,
        )
    else:
        logger.info(
            "ExtendedHealthMetricsSupervisor desabilitado "
            "(EXTENDED_HEALTH_METRICS_SUPERVISOR_ENABLED=false)"
        )

    if settings.plan_compile_timeout_supervisor_enabled:
        plan_compile_timeout_supervisor = PlanCompileTimeoutSupervisor(
            async_session_factory, settings.plan_compile_timeout_check_interval_seconds
        )
        plan_compile_timeout_task = asyncio.create_task(plan_compile_timeout_supervisor.run())
        logger.info(
            "PlanCompileTimeoutSupervisor habilitado (check_interval=%ss)",
            settings.plan_compile_timeout_check_interval_seconds,
        )
    else:
        logger.info(
            "PlanCompileTimeoutSupervisor desabilitado "
            "(PLAN_COMPILE_TIMEOUT_SUPERVISOR_ENABLED=false)"
        )

    if settings.plan_history_supervisor_enabled:
        plan_history_supervisor = PlanHistorySupervisor(
            async_session_factory, settings.plan_history_check_interval_seconds
        )
        plan_history_task = asyncio.create_task(plan_history_supervisor.run())
        logger.info(
            "PlanHistorySupervisor habilitado (check_interval=%ss)",
            settings.plan_history_check_interval_seconds,
        )
    else:
        logger.info(
            "PlanHistorySupervisor desabilitado (PLAN_HISTORY_SUPERVISOR_ENABLED=false)"
        )

    yield

    if supervisor is not None and supervisor_task is not None:
        supervisor.stop()
        await supervisor_task
    if alerting_worker is not None and alerting_task is not None:
        alerting_worker.stop()
        await alerting_task
    if missing_index_supervisor is not None and missing_index_task is not None:
        missing_index_supervisor.stop()
        await missing_index_task
    if backup_status_supervisor is not None and backup_status_task is not None:
        backup_status_supervisor.stop()
        await backup_status_task
    if deadlock_supervisor is not None and deadlock_task is not None:
        deadlock_supervisor.stop()
        await deadlock_task
    if problem_events_retention_worker is not None and problem_events_retention_task is not None:
        problem_events_retention_worker.stop()
        await problem_events_retention_task
    if extended_health_metrics_supervisor is not None and extended_health_metrics_task is not None:
        extended_health_metrics_supervisor.stop()
        await extended_health_metrics_task
    if plan_compile_timeout_supervisor is not None and plan_compile_timeout_task is not None:
        plan_compile_timeout_supervisor.stop()
        await plan_compile_timeout_task
    if plan_history_supervisor is not None and plan_history_task is not None:
        plan_history_supervisor.stop()
        await plan_history_task
    executor.shutdown(wait=False, cancel_futures=True)
    logger.info("Lifespan finalizado")


def create_app() -> FastAPI:
    """Factory da aplicacao FastAPI."""
    configure_logging()
    settings = get_settings()

    app = FastAPI(
        title="AB-XQL-PM Backend",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_v1_router, prefix=settings.api_v1_prefix)

    return app


app = create_app()
