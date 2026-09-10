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
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_v1_router
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import async_session_factory
from app.workers.alerting_worker import AlertingWorker
from app.workers.polling_supervisor import PollingSupervisor

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Liga PollingSupervisor/AlertingWorker conforme os respectivos flags; no-op caso contrario."""
    settings = get_settings()
    supervisor: PollingSupervisor | None = None
    supervisor_task: asyncio.Task[None] | None = None
    alerting_worker: AlertingWorker | None = None
    alerting_task: asyncio.Task[None] | None = None

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
        alerting_worker = AlertingWorker(
            async_session_factory,
            settings.alerting_evaluation_interval_seconds,
            settings.alert_critical_reminder_interval_seconds,
            webhook_url=settings.alert_webhook_url,
        )
        alerting_task = asyncio.create_task(alerting_worker.run())
        logger.info(
            "AlertingWorker habilitado (interval=%ss)",
            settings.alerting_evaluation_interval_seconds,
        )
    else:
        logger.info("AlertingWorker desabilitado (ALERTING_WORKER_ENABLED=false)")

    yield

    if supervisor is not None and supervisor_task is not None:
        supervisor.stop()
        await supervisor_task
    if alerting_worker is not None and alerting_task is not None:
        alerting_worker.stop()
        await alerting_task
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
