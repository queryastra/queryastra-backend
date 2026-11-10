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
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.api.v1.router import api_v1_router
from app.core import instance_executors
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import async_session_factory
from app.models.system_settings import SYSTEM_SETTINGS_SINGLETON_ID, SystemSettings
from app.repositories.instance_database_sleeping_overrides_repository import (
    InstanceDatabaseSleepingOverridesRepository,
)
from app.repositories.instances_repository import InstancesRepository
from app.repositories.system_license_repository import SystemLicenseRepository
from app.repositories.users_repository import UsersRepository
from app.services.auth_service import hash_password
from app.workers.alerting_worker import AlertingWorker, EmailSettings
from app.workers.anomaly_detection_worker import AnomalyDetectionWorker
from app.workers.backup_status_supervisor import BackupStatusSupervisor
from app.workers.deadlock_supervisor import DeadlockSupervisor
from app.workers.extended_health_metrics_supervisor import ExtendedHealthMetricsSupervisor
from app.workers.missing_index_supervisor import MissingIndexSupervisor
from app.workers.plan_compile_timeout_supervisor import PlanCompileTimeoutSupervisor
from app.workers.plan_history_supervisor import PlanHistorySupervisor
from app.workers.polling_supervisor import PollingSupervisor
from app.workers.problem_events_retention_worker import ProblemEventsRetentionWorker
from app.workers.query_stats_supervisor import QueryStatsSupervisor
from app.workers.report_worker import ReportWorker
from app.workers.update_checker import UpdateChecker
from app.workers.transaction_error_supervisor import TransactionErrorSupervisor
from app.workers.trial_expiry_worker import TrialExpiryWorker

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

    # Seed: cria o usuario admin inicial se ainda nao existir.
    async with async_session_factory() as session:
        users_repo = UsersRepository(session)
        existing = await users_repo.get_by_username(settings.admin_username)
        if existing is None:
            hashed = hash_password(settings.admin_password)
            await users_repo.create(settings.admin_username, hashed, "admin")
            await session.commit()
            logger.info("Usuario admin seed criado: %s", settings.admin_username)

    # Seed: grava system_settings com os valores das variaveis de ambiente se a linha ainda nao
    # existir. ON CONFLICT (id) DO NOTHING — banco recem-criado recebe os valores do .env;
    # uma linha ja existente (usuario ja configurou pela UI) NUNCA e sobrescrita por restart.
    async with async_session_factory() as session:
        stmt = (
            pg_insert(SystemSettings)
            .values(
                id=SYSTEM_SETTINGS_SINGLETON_ID,
                blocking_min_duration_ms=settings.blocking_min_duration_ms,
                sleeping_min_duration_ms=settings.sleeping_min_duration_ms,
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(stmt)
        await session.commit()
        logger.info(
            "system_settings seed verificado (blocking_ms=%s, sleeping_ms=%s)",
            settings.blocking_min_duration_ms,
            settings.sleeping_min_duration_ms,
        )

    # Purge de instâncias soft-deletadas cujo período de carência (30 dias) expirou.
    async with async_session_factory() as session:
        purged = await InstancesRepository(session).purge_expired_soft_deleted()
        if purged:
            logger.info("Purge de instancias soft-deletadas expiradas: %s removidas", purged)

    # Seed: garante que a linha de licenca do sistema existe (singleton).
    # ON CONFLICT nao aplicavel aqui — `get_or_create` faz o INSERT apenas se necessario.
    async with async_session_factory() as session:
        license_repo = SystemLicenseRepository(session)
        await license_repo.get_or_create()
        await session.commit()
        logger.info("system_license seed verificado")

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
    transaction_error_supervisor: TransactionErrorSupervisor | None = None
    transaction_error_task: asyncio.Task[None] | None = None
    problem_events_retention_worker: ProblemEventsRetentionWorker | None = None
    problem_events_retention_task: asyncio.Task[None] | None = None
    extended_health_metrics_supervisor: ExtendedHealthMetricsSupervisor | None = None
    extended_health_metrics_task: asyncio.Task[None] | None = None
    plan_compile_timeout_supervisor: PlanCompileTimeoutSupervisor | None = None
    plan_compile_timeout_task: asyncio.Task[None] | None = None
    plan_history_supervisor: PlanHistorySupervisor | None = None
    plan_history_task: asyncio.Task[None] | None = None
    query_stats_supervisor: QueryStatsSupervisor | None = None
    query_stats_task: asyncio.Task[None] | None = None
    report_worker: ReportWorker | None = None
    report_worker_task: asyncio.Task[None] | None = None
    update_checker: UpdateChecker | None = None
    update_checker_task: asyncio.Task[None] | None = None
    trial_expiry_worker: TrialExpiryWorker | None = None
    trial_expiry_task: asyncio.Task[None] | None = None
    anomaly_worker: AnomalyDetectionWorker | None = None
    anomaly_task: asyncio.Task[None] | None = None

    if settings.polling_supervisor_enabled:
        supervisor = PollingSupervisor(
            async_session_factory,
            settings.supervisor_discovery_interval_seconds,
            instance_database_sleeping_overrides_repository_factory=InstanceDatabaseSleepingOverridesRepository,
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
                sender=settings.alert_smtp_from or "alerts@queryastra.local",
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

    if settings.transaction_error_supervisor_enabled:
        transaction_error_supervisor = TransactionErrorSupervisor(
            async_session_factory, settings.transaction_error_check_interval_seconds
        )
        transaction_error_task = asyncio.create_task(transaction_error_supervisor.run())
        logger.info(
            "TransactionErrorSupervisor habilitado (check_interval=%ss)",
            settings.transaction_error_check_interval_seconds,
        )
    else:
        logger.info(
            "TransactionErrorSupervisor desabilitado (TRANSACTION_ERROR_SUPERVISOR_ENABLED=false)"
        )

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
        logger.info("PlanHistorySupervisor desabilitado (PLAN_HISTORY_SUPERVISOR_ENABLED=false)")

    if settings.query_stats_supervisor_enabled:
        query_stats_supervisor = QueryStatsSupervisor(
            async_session_factory, settings.query_stats_check_interval_seconds
        )
        query_stats_task = asyncio.create_task(query_stats_supervisor.run())
        logger.info(
            "QueryStatsSupervisor habilitado (check_interval=%ss)",
            settings.query_stats_check_interval_seconds,
        )
    else:
        logger.info("QueryStatsSupervisor desabilitado (QUERY_STATS_SUPERVISOR_ENABLED=false)")

    if settings.report_worker_enabled:
        report_worker = ReportWorker(
            async_session_factory, settings.report_worker_check_interval_seconds
        )
        report_worker_task = asyncio.create_task(report_worker.run())
        logger.info(
            "ReportWorker habilitado (check_interval=%ss)",
            settings.report_worker_check_interval_seconds,
        )
    else:
        logger.info("ReportWorker desabilitado (REPORT_WORKER_ENABLED=false)")

    if settings.update_check_enabled:
        update_checker = UpdateChecker(settings.update_check_interval_seconds)
        update_checker_task = asyncio.create_task(update_checker.run())
        logger.info(
            "UpdateChecker habilitado (check_interval=%ss)",
            settings.update_check_interval_seconds,
        )
    else:
        logger.info("UpdateChecker desabilitado (UPDATE_CHECK_ENABLED=false)")

    if settings.trial_expiry_worker_enabled:
        trial_expiry_worker = TrialExpiryWorker(
            async_session_factory,
            settings,
            settings.trial_expiry_check_interval_seconds,
        )
        trial_expiry_task = asyncio.create_task(trial_expiry_worker.run())
        logger.info(
            "TrialExpiryWorker habilitado (check_interval=%ss)",
            settings.trial_expiry_check_interval_seconds,
        )
    else:
        logger.info("TrialExpiryWorker desabilitado (TRIAL_EXPIRY_WORKER_ENABLED=false)")

    if settings.anomaly_detection_enabled:
        anomaly_worker = AnomalyDetectionWorker(
            async_session_factory,
            settings.anomaly_detection_interval_seconds,
            baseline_weeks=settings.anomaly_baseline_weeks,
            warning_z=settings.anomaly_warning_z,
            critical_z=settings.anomaly_critical_z,
            notifier=alerting_worker.send_notification if alerting_worker is not None else None,
        )
        anomaly_task = asyncio.create_task(anomaly_worker.run())
        logger.info(
            "AnomalyDetectionWorker habilitado (interval=%ss)",
            settings.anomaly_detection_interval_seconds,
        )
    else:
        logger.info("AnomalyDetectionWorker desabilitado (ANOMALY_DETECTION_ENABLED=false)")

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
    if transaction_error_supervisor is not None and transaction_error_task is not None:
        transaction_error_supervisor.stop()
        await transaction_error_task
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
    if query_stats_supervisor is not None and query_stats_task is not None:
        query_stats_supervisor.stop()
        await query_stats_task
    if report_worker is not None and report_worker_task is not None:
        report_worker.stop()
        await report_worker_task
    if update_checker is not None and update_checker_task is not None:
        update_checker.stop()
        await update_checker_task
    if trial_expiry_worker is not None and trial_expiry_task is not None:
        trial_expiry_worker.stop()
        await trial_expiry_task
    if anomaly_worker is not None and anomaly_task is not None:
        anomaly_worker.stop()
        await anomaly_task
    executor.shutdown(wait=False, cancel_futures=True)
    instance_executors.shutdown_all()
    logger.info("Lifespan finalizado")


def create_app() -> FastAPI:
    """Factory da aplicacao FastAPI."""
    configure_logging()
    settings = get_settings()

    # Aviso de segurança para defaults inseguros — impede expor em rede sem configuração mínima
    _INSECURE_JWT_DEFAULT = "change-me-in-production-use-a-long-random-string"
    if settings.jwt_secret_key == _INSECURE_JWT_DEFAULT:
        logger.warning(
            "SEGURANÇA: JWT_SECRET_KEY ainda é o valor padrão inseguro. "
            "Defina JWT_SECRET_KEY no .env com uma string aleatória longa antes de expor em rede."
        )
    if settings.admin_password == "admin":
        logger.warning(
            "SEGURANÇA: ADMIN_PASSWORD ainda é 'admin'. "
            "Troque a senha do administrador antes de expor o sistema em rede."
        )

    is_dev = settings.environment == "development"
    app = FastAPI(
        title="QueryAstra Backend",
        version="0.1.0",
        lifespan=lifespan,
        # Swagger UI e ReDoc só ficam ativos em development — expor em produção
        # revela schema completo da API para qualquer visitante sem autenticação.
        docs_url="/docs" if is_dev else None,
        redoc_url="/redoc" if is_dev else None,
        openapi_url="/openapi.json" if is_dev else None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Accept"],
    )

    app.include_router(api_v1_router, prefix=settings.api_v1_prefix)

    return app


app = create_app()
