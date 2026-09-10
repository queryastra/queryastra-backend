"""Fixtures de banco real compartilhadas pelos testes de integracao de API.

Mesmo padrao de `tests/unit/repositories/conftest.py` (banco real `ab_xql_pm_dev`, `DATABASE_URL`
de `.env`). Duplicado aqui deliberadamente em vez de importado de
`tests/unit/repositories/conftest.py`: os dois conjuntos de teste vivem em arvores `pytest`
diferentes (`tests/unit` vs `tests/integration`) sem um pacote comum "obvio" para hospedar um
conftest partilhado sem subir para `tests/conftest.py` (que hoje documenta explicitamente "nenhuma
fixture aqui depende de banco real" — mudar essa premissa ali afetaria todo o resto do scaffold).
Duplicar as ~30 linhas de fixture e mais simples e local do que reestruturar esse contrato para
economizar a duplicacao.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import async_session_factory, engine
from app.models.instance import Instance
from app.repositories._tables import (
    blocking_events_table,
    metric_samples_table,
    query_samples_table,
    sql_exclusions_table,
)


@pytest.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    # Ver nota identica em `tests/unit/repositories/conftest.py`: descarta o pool de conexoes
    # `asyncpg` ao final de cada teste para nao prender uma conexao a um event loop ja fechado.
    async with async_session_factory() as session:
        yield session
    await engine.dispose()


@pytest.fixture
async def test_instance(db_session: AsyncSession) -> AsyncIterator[Instance]:
    instance = Instance(
        name=f"test-instance-{uuid4()}",
        db_type="postgresql",
        host="127.0.0.1",
        port=5432,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
    )
    db_session.add(instance)
    await db_session.commit()
    await db_session.refresh(instance)

    yield instance

    await db_session.execute(
        sa.delete(query_samples_table).where(query_samples_table.c.instance_id == instance.id)
    )
    await db_session.execute(
        sa.delete(blocking_events_table).where(blocking_events_table.c.instance_id == instance.id)
    )
    await db_session.execute(
        sa.delete(sql_exclusions_table).where(sql_exclusions_table.c.instance_id == instance.id)
    )
    await db_session.execute(
        sa.delete(metric_samples_table).where(metric_samples_table.c.target_id == instance.id)
    )
    await db_session.commit()
    await db_session.delete(instance)
    await db_session.commit()
