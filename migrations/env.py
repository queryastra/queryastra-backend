"""Ambiente Alembic — engine assincrono (asyncpg), padrao oficial de "async migrations".

`target_metadata` aponta para `app.db.base.Base.metadata`, que agrega todos os modelos
importados em `app/models/__init__.py`. `DATABASE_URL` vem de `app.core.config.get_settings()`
(nunca hardcoded em `alembic.ini` — Docs/rules/01-credential-security.md).

Cobre apenas as tabelas relacionais simples do MVP (Instance, InstanceMaintenanceWindow,
Server, InstanceServer, AlertRule, Alert, AlertNotification, AlertSilence). Hypertables e
continuous aggregates (`metric_samples`, `query_samples`, `blocking_events`, `deadlock_events`,
`sql_exclusions`, `*_hourly`, `*_daily`) exigem TimescaleDB de verdade e ficam fora do
autogenerate — ver `Docs/database-schema.md` §2/§3 e a nota em
`app/repositories/metric_samples_repository.py`.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Import agregador — garante que todos os modelos (app/models/__init__.py) sejam registrados em
# Base.metadata antes do autogenerate rodar.
import app.models  # noqa: F401
from app.core.config import get_settings
from app.db.base import Base

# Objeto de configuracao do Alembic, le `alembic.ini`.
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Sobrescreve `sqlalchemy.url` com o valor de app.core.config (fonte unica de verdade da conexao).
config.set_main_option("sqlalchemy.url", get_settings().database_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Gera SQL sem abrir conexao (`alembic upgrade --sql`)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Abre um engine assincrono e delega a migration sincrona via `run_sync`."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Ponto de entrada padrao do Alembic — despacha para o runner assincrono."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
