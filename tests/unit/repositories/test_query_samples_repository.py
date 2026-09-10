"""Testes de `QuerySamplesRepository.insert_batch` contra o banco real `ab_xql_pm_dev`.

Caso principal: uma regra em `sql_exclusions` (`match_type='program_name'`,
`match_value='meu_programa'`) descarta so as amostras daquele programa — o resto do batch e
inserido normalmente (`Docs/rules/03-sql-privacy.md`).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedQuerySample
from app.repositories._tables import query_samples_table, sql_exclusions_table
from app.repositories.query_samples_repository import QuerySamplesRepository


async def test_insert_batch_excludes_matching_program_name(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    await db_session.execute(
        sa.insert(sql_exclusions_table),
        [
            {
                "id": uuid4(),
                "instance_id": test_instance.id,
                "match_type": "program_name",
                "match_value": "meu_programa",
                "reason": "teste",
                "created_at": datetime.now(UTC),
            }
        ],
    )
    await db_session.commit()

    excluded_sample = NormalizedQuerySample(
        time=datetime.now(UTC),
        instance_id=test_instance.id,
        engine="postgresql",
        sql_hash="hash-excluded",
        sql_text_masked="SELECT ?",
        program="meu_programa",
    )
    kept_sample = NormalizedQuerySample(
        time=datetime.now(UTC),
        instance_id=test_instance.id,
        engine="postgresql",
        sql_hash="hash-kept",
        sql_text_masked="SELECT ?",
        program="outro_programa",
    )

    repo = QuerySamplesRepository(db_session)
    await repo.insert_batch([excluded_sample, kept_sample])

    result = await db_session.execute(
        sa.select(query_samples_table.c.sql_hash).where(
            query_samples_table.c.instance_id == test_instance.id
        )
    )
    hashes = {row.sql_hash for row in result.fetchall()}
    assert hashes == {"hash-kept"}


async def test_insert_batch_without_exclusions_inserts_all(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    sample = NormalizedQuerySample(
        time=datetime.now(UTC),
        instance_id=test_instance.id,
        engine="sqlserver",
        sql_hash="hash-no-exclusion",
        sql_text_masked="SELECT ?",
    )
    repo = QuerySamplesRepository(db_session)
    await repo.insert_batch([sample])

    result = await db_session.execute(
        sa.select(query_samples_table.c.sql_hash).where(
            query_samples_table.c.instance_id == test_instance.id
        )
    )
    hashes = {row.sql_hash for row in result.fetchall()}
    assert hashes == {"hash-no-exclusion"}


async def test_insert_batch_noop_for_empty_list(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    await repo.insert_batch([])
