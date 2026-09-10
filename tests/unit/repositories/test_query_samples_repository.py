"""Testes de `QuerySamplesRepository.insert_batch` contra o banco real `ab_xql_pm_dev`.

Caso principal: uma regra em `sql_exclusions` (`match_type='program_name'`,
`match_value='meu_programa'`) descarta so as amostras daquele programa — o resto do batch e
inserido normalmente (`Docs/rules/03-sql-privacy.md`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedQuerySample
from app.normalizers.wait_taxonomy import WaitCategory
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


async def test_list_active_filters_by_window(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-in-window",
                sql_text_masked="SELECT ?",
            ),
            NormalizedQuerySample(
                time=now - timedelta(hours=2),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-out-of-window",
                sql_text_masked="SELECT ?",
            ),
        ]
    )

    rows = await repo.list_active(
        test_instance.id,
        since=now - timedelta(minutes=5),
        until=now + timedelta(minutes=5),
        limit=100,
    )

    hashes = {row.sql_hash for row in rows}
    assert hashes == {"hash-in-window"}


async def test_top_sql_aggregates_by_hash_with_predominant_wait_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_name="query-a",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_name="query-a",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=200,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=2),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_name="query-a",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.IO,
                wait_time_ms=50,
            ),
        ]
    )

    rows = await repo.top_sql(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.sql_hash == "hash-a"
    assert row.execution_count == 3
    assert row.total_wait_time_ms == 350
    assert row.predominant_wait_type == WaitCategory.LOCK.value


async def test_wait_summary_groups_by_wait_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=50,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-c",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.IO,
                wait_time_ms=30,
            ),
        ]
    )

    rows = await repo.wait_summary(
        test_instance.id, since=now - timedelta(minutes=1), until=now + timedelta(minutes=1)
    )

    summary = {row.wait_type: row.total_wait_time_ms for row in rows}
    assert summary == {WaitCategory.LOCK.value: 150, WaitCategory.IO.value: 30}


async def test_latest_by_session_returns_most_recent_row(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now - timedelta(seconds=10),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-old",
                sql_text_masked="SELECT ? FROM old",
                session_id="session-1",
                db_user="old_user",
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-new",
                sql_text_masked="SELECT ? FROM new",
                session_id="session-1",
                db_user="new_user",
            ),
        ]
    )

    result = await repo.latest_by_session(
        test_instance.id, ["session-1"], since=now - timedelta(minutes=1)
    )

    assert result["session-1"].sql_hash == "hash-new"
    assert result["session-1"].db_user == "new_user"
