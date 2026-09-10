"""FastAPI app factory — `Docs/specs/02-backend-contract.md`.

"O backend FastAPI expoe cadastro, health-check, leitura de metricas, consultas de detalhe e
configuracao de alertas. Jobs de coleta rodam fora das rotas HTTP e usam adaptadores por SGBD."

O lifespan liga/desliga os workers (`app.workers`) — neste scaffold, como no-op, ja que os
workers ainda nao tem logica de coleta real (ver `Docs/collector-queries.md`, adaptadores
stub em `app/collectors`).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_v1_router
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start/stop dos workers de polling/agregacao/alertas.

    Stub: nao inicia nenhum worker de verdade ainda — so registra a intencao no log. Quando os
    adaptadores de coleta (`app/collectors`) e o supervisor de `PollingWorker` existirem, este
    lifespan passa a criar/parar as tasks `asyncio` correspondentes.
    """
    logger.info("Lifespan iniciado — workers ainda nao implementados (no-op neste scaffold)")
    yield
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
