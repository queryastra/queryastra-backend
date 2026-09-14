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


async def test_top_sql_by_wait_type_filters_to_queries_with_that_native_wait(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Drill-down a partir da folha da dimensao "Waits" — so as queries com amostras do
    `wait_event_native` filtrado aparecem, e o total refletido e so a contribuicao DESSE wait
    (nao o total da query somando todos os tipos)."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-lck",
                sql_name="query-lck",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.LOCK,
                wait_event_native="LCK_M_S",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-lck",
                sql_name="query-lck",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.IO,
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=999,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-other",
                sql_name="query-other",
                sql_text_masked="SELECT ? FROM other",
                wait_type=WaitCategory.IO,
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=50,
            ),
        ]
    )

    rows = await repo.top_sql_by_wait_type(
        test_instance.id,
        "LCK_M_S",
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.sql_hash == "hash-lck"
    assert row.execution_count == 1
    assert row.total_wait_time_ms == 100  # so a amostra LCK_M_S, nao soma a PAGEIOLATCH_SH
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


async def test_top_sql_by_database_groups_by_database_name(
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
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=50,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-c",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=30,
            ),
        ]
    )

    rows = await repo.top_sql_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_database = {row.database_name: row for row in rows}
    assert set(by_database) == {"db_a", "db_b"}
    assert by_database["db_a"].execution_count == 2
    assert by_database["db_a"].total_wait_time_ms == 150
    assert by_database["db_b"].execution_count == 1
    assert by_database["db_b"].total_wait_time_ms == 30


async def test_top_wait_types_groups_by_wait_event_native(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`top_wait_types` agrupa `query_samples` por `wait_event_native` — dimensao "Waits" do
    drill-down, folha da propria dimensao (sem `wait_breakdown`)."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=50,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-c",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=30,
            ),
        ]
    )

    rows = await repo.top_wait_types(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_type = {row.wait_event_native: row for row in rows}
    assert set(by_type) == {"ASYNC_NETWORK_IO", "PAGEIOLATCH_SH"}
    assert by_type["ASYNC_NETWORK_IO"].execution_count == 2
    assert by_type["ASYNC_NETWORK_IO"].total_wait_time_ms == 150
    assert by_type["PAGEIOLATCH_SH"].total_wait_time_ms == 30


async def test_top_wait_types_includes_synthetic_cpu_bucket_when_wait_time_is_zero(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Regressao real: o SQL Server de verdade reporta `wait_time = 0` (nao NULL) quando nao ha
    espera ativa (confirmado ao vivo contra `Autklinux20`) — so testar `wait_time_ms IS NULL`
    deixaria de fora a maioria das amostras genuinamente sem espera."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-cpu-zero",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native=None,
                wait_time_ms=0,
                cpu_time_ms=700,
            ),
        ]
    )

    rows = await repo.top_wait_types(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_type = {row.wait_event_native: row for row in rows}
    assert set(by_type) == {"Memory/CPU"}
    assert by_type["Memory/CPU"].total_wait_time_ms == 700


async def test_top_wait_types_includes_synthetic_cpu_bucket(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Fecha um gap real confirmado comparando com o SolarWinds DPA (categoria "Memory/CPU"):
    amostras sem NENHUMA espera ativa (`wait_event_native`/`wait_time_ms` ambos None) agora
    aparecem como um bucket sintetico 'CPU', usando `cpu_time_ms`."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-cpu",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native=None,
                wait_time_ms=None,
                cpu_time_ms=500,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-lock",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="LCK_M_S",
                wait_time_ms=200,
            ),
        ]
    )

    rows = await repo.top_wait_types(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_type = {row.wait_event_native: row for row in rows}
    assert set(by_type) == {"Memory/CPU", "LCK_M_S"}
    assert by_type["Memory/CPU"].total_wait_time_ms == 500
    assert by_type["LCK_M_S"].total_wait_time_ms == 200


async def test_top_wait_types_cpu_bucket_sums_effective_delta_not_raw_cpu_time(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`cpu_time_ms` e cumulativo por REQUEST (como `wait_time_ms` e cumulativo por espera) — o
    bucket 'CPU' precisa somar o INCREMENTO real entre amostras da mesma sessao, nao o valor
    bruto de cada amostra (que re-contaria a mesma CPU varias vezes, exatamente o bug que
    `effective_wait_ms` ja corrigia pra waits)."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-cpu",
                sql_text_masked="SELECT ? FROM t",
                session_id="session-cpu",
                wait_event_native=None,
                wait_time_ms=None,
                cpu_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=10),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-cpu",
                sql_text_masked="SELECT ? FROM t",
                session_id="session-cpu",
                wait_event_native=None,
                wait_time_ms=None,
                cpu_time_ms=250,
            ),
        ]
    )

    rows = await repo.top_wait_types(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_type = {row.wait_event_native: row for row in rows}
    # 100 (primeira amostra, valor cheio) + (250 - 100) = 250, nao 100 + 250 = 350
    assert by_type["Memory/CPU"].total_wait_time_ms == 250


async def test_wait_time_by_bucket_and_wait_type_groups_by_utc_day(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`wait_time_by_bucket_and_wait_type` (bucket="day", default) agrupa por dia calendario UTC
    + tipo de espera nativo — visao historica "Tudo" da dimensao "Waits"."""
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=day1 + timedelta(hours=1),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=20,
            ),
        ]
    )

    rows = await repo.wait_time_by_bucket_and_wait_type(
        test_instance.id,
        since=day1 - timedelta(days=1),
        until=day1 + timedelta(days=1),
        limit=10,
    )

    assert all(row.bucket_start.tzinfo is not None for row in rows)
    by_type = {row.wait_event_native: row.total_wait_time_ms for row in rows}
    assert by_type["ASYNC_NETWORK_IO"] == 100
    assert by_type["PAGEIOLATCH_SH"] == 20


async def test_top_sql_by_database_groups_null_database_name_as_desconhecido(
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
                sql_hash="hash-no-db",
                sql_text_masked="SELECT ?",
                database_name=None,
                wait_time_ms=10,
            )
        ]
    )

    rows = await repo.top_sql_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    assert len(rows) == 1
    assert rows[0].database_name == "desconhecido"
    assert rows[0].execution_count == 1


async def test_wait_breakdown_by_database_groups_by_native_wait_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`wait_breakdown_by_database` decompoe o wait time de cada banco pelo tipo de espera NATIVO
    (`wait_event_native`) — granularidade maior que o bucket canonico de 9 categorias usado em
    `wait_summary()` (ex.: `PAGEIOLATCH_SH` continua `PAGEIOLATCH_SH`, nao vira so `IO`)."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=50,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-c",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=30,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-d",
                sql_text_masked="SELECT ?",
                database_name="db_b",
                wait_event_native=None,
                wait_time_ms=10,
            ),
        ]
    )

    rows = await repo.wait_breakdown_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_key = {(row.database_name, row.wait_event_native): row.total_wait_time_ms for row in rows}
    assert by_key[("db_a", "PAGEIOLATCH_SH")] == 150
    assert by_key[("db_a", "ASYNC_NETWORK_IO")] == 30
    assert by_key[("db_b", "desconhecido")] == 10


async def test_wait_breakdown_by_sql_hash_groups_by_native_wait_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`wait_breakdown_by_sql_hash` decompoe o wait time de cada query pelo tipo de espera NATIVO
    — mesma ideia de `wait_breakdown_by_database`, granularidade `sql_hash`."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=30,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=10,
            ),
        ]
    )

    rows = await repo.wait_breakdown_by_sql_hash(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_key = {(row.sql_hash, row.wait_event_native): row.total_wait_time_ms for row in rows}
    assert by_key[("hash-a", "PAGEIOLATCH_SH")] == 100
    assert by_key[("hash-a", "ASYNC_NETWORK_IO")] == 30
    assert by_key[("hash-b", "PAGEIOLATCH_SH")] == 10

    # Filtro opcional por database_name (nivel 2 do drill-down so quer as queries daquele banco).
    filtered = await repo.wait_breakdown_by_sql_hash(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
        database_name="db_b",
    )
    assert {(row.sql_hash, row.wait_event_native) for row in filtered} == {
        ("hash-b", "PAGEIOLATCH_SH")
    }


async def test_wait_time_by_bucket_and_database_groups_by_utc_day(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`wait_time_by_bucket_and_database` (bucket="day", default) agrupa por dia calendario UTC +
    banco — visao historica "Tudo" do nivel 1 (um dia por barra, empilhada por banco)."""
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    day2 = datetime(2026, 9, 2, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=day1 + timedelta(hours=2),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=20,
            ),
            NormalizedQuerySample(
                time=day2,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=50,
            ),
        ]
    )

    rows = await repo.wait_time_by_bucket_and_database(
        test_instance.id,
        since=day1 - timedelta(days=1),
        until=day2 + timedelta(days=1),
        limit=10,
    )

    by_key = {(row.bucket_start.date(), row.database_name): row.total_wait_time_ms for row in rows}
    assert by_key[(day1.date(), "db_a")] == 100
    assert by_key[(day1.date(), "db_b")] == 20
    assert by_key[(day2.date(), "db_a")] == 50


async def test_wait_time_by_bucket_and_sql_hash_groups_by_utc_day(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`wait_time_by_bucket_and_sql_hash` (bucket="day", default) agrupa por dia calendario UTC +
    `sql_hash` — visao historica "Tudo" do nivel 2 (um dia por barra, empilhada por query), com
    filtro opcional de `database_name` (mesmo padrao de `top_sql()`)."""
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=day1 + timedelta(hours=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=20,
            ),
        ]
    )

    rows = await repo.wait_time_by_bucket_and_sql_hash(
        test_instance.id,
        since=day1 - timedelta(days=1),
        until=day1 + timedelta(days=1),
        limit=10,
    )
    assert {(row.bucket_start.date(), row.sql_hash, row.total_wait_time_ms) for row in rows} == {
        (day1.date(), "hash-a", 100),
        (day1.date(), "hash-b", 20),
    }

    filtered = await repo.wait_time_by_bucket_and_sql_hash(
        test_instance.id,
        since=day1 - timedelta(days=1),
        until=day1 + timedelta(days=1),
        limit=10,
        database_name="db_b",
    )
    assert {(row.sql_hash, row.total_wait_time_ms) for row in filtered} == {("hash-b", 20)}


async def test_wait_time_by_bucket_and_database_groups_by_utc_hour(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`wait_time_by_bucket_and_database(bucket="hour")` agrupa por hora UTC + banco —
    detalhamento por hora dentro de um dia (passo intermediario entre "Tudo" e o grafico de um
    dia inteiro)."""
    repo = QuerySamplesRepository(db_session)
    hour10 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    hour11 = datetime(2026, 9, 1, 11, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=hour10,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=hour10 + timedelta(minutes=30),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=20,
            ),
            NormalizedQuerySample(
                time=hour11,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=50,
            ),
        ]
    )

    rows = await repo.wait_time_by_bucket_and_database(
        test_instance.id,
        since=hour10 - timedelta(hours=1),
        until=hour11 + timedelta(hours=1),
        limit=10,
        bucket="hour",
    )

    # Regressao: `bucket_start` tem que ser timezone-aware, senao o FastAPI serializa sem `Z` e o
    # navegador interpreta o instante como fuso local (bug real ja encontrado nesta feature).
    assert all(row.bucket_start.tzinfo is not None for row in rows)

    # `bucket_start` volta como timestamptz aware (UTC) — o `AT TIME ZONE 'UTC'` externo da query
    # reconverte o timestamp naive intermediario, garantindo que o JSON saia com `Z`/offset (sem
    # isso, `new Date(...)` no navegador interpretaria o instante como fuso local).
    by_key = {(row.bucket_start, row.database_name): row.total_wait_time_ms for row in rows}
    assert by_key[(hour10, "db_a")] == 120
    assert by_key[(hour11, "db_b")] == 50


async def test_top_sql_filters_by_database_name(
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
                sql_hash="hash-shared",
                sql_name="query-shared",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-shared",
                sql_name="query-shared",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=200,
            ),
        ]
    )

    unfiltered = await repo.top_sql(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )
    assert len(unfiltered) == 1
    assert unfiltered[0].sql_hash == "hash-shared"
    # Sem filtro: agrega os dois bancos juntos sob o mesmo sql_hash (comportamento historico).
    assert unfiltered[0].execution_count == 2
    assert unfiltered[0].total_wait_time_ms == 300

    filtered = await repo.top_sql(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
        database_name="db_a",
    )
    assert len(filtered) == 1
    assert filtered[0].sql_hash == "hash-shared"
    assert filtered[0].database_name == "db_a"
    assert filtered[0].execution_count == 1
    assert filtered[0].total_wait_time_ms == 100


async def test_query_detail_returns_raw_samples_for_hash(
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
                sql_hash="hash-detail",
                sql_name="query-detail",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                session_id="1",
                wait_time_ms=10,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-detail",
                sql_name="query-detail",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                session_id="2",
                wait_time_ms=20,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-other",
                sql_text_masked="SELECT ? FROM other",
                database_name="db_a",
            ),
        ]
    )

    rows = await repo.query_detail(
        test_instance.id,
        "hash-detail",
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    assert len(rows) == 2
    session_ids = {row.session_id for row in rows}
    assert session_ids == {"1", "2"}
    # Ordenado por time desc — o mais recente (session_id="2") vem primeiro.
    assert rows[0].session_id == "2"


async def test_top_sql_sums_effective_wait_delta_for_continuous_wait_same_session(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Regressao do bug real encontrado ao vivo (instancia `Autklinux20`, sessao 797,
    `AB_APLICAUT_AX`): amostras sucessivas da MESMA sessao, no MESMO `wait_event_native`, com
    `wait_time_ms` crescendo monotonicamente, sao a mesma espera continua sendo fotografada
    varias vezes — `total_wait_time_ms` deve somar so os incrementos (deltas), nao os valores
    brutos (que somados dariam 100+250+400=750, quase 2x o valor real da espera de 400ms)."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-continuous",
                sql_text_masked="SELECT ? FROM t",
                session_id="sess-1",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=10),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-continuous",
                sql_text_masked="SELECT ? FROM t",
                session_id="sess-1",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=250,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=20),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-continuous",
                sql_text_masked="SELECT ? FROM t",
                session_id="sess-1",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=400,
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
    # 100 (1a amostra, espera nova) + 150 (250-100) + 150 (400-250) = 400 — nao 750.
    assert rows[0].total_wait_time_ms == 400


async def test_top_sql_counts_full_value_when_wait_resets_or_changes_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Quando o `wait_time_ms` cai (a espera anterior terminou e uma nova comecou) ou o
    `wait_event_native` muda entre amostras da mesma sessao, cada amostra conta o valor cheio —
    nao ha "delta negativo" nem mistura de deltas entre esperas diferentes."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-reset",
                sql_text_masked="SELECT ? FROM t",
                session_id="sess-2",
                wait_event_native="LCK_M_S",
                wait_time_ms=9000,
            ),
            # Wait_time_ms caiu — a espera de 9000ms terminou, uma nova comecou do zero.
            NormalizedQuerySample(
                time=now + timedelta(seconds=10),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-reset",
                sql_text_masked="SELECT ? FROM t",
                session_id="sess-2",
                wait_event_native="LCK_M_S",
                wait_time_ms=500,
            ),
            # Mesmo crescendo, o tipo de espera mudou — nao e continuacao da anterior.
            NormalizedQuerySample(
                time=now + timedelta(seconds=20),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-reset",
                sql_text_masked="SELECT ? FROM t",
                session_id="sess-2",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=800,
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
    # 9000 (nova) + 500 (nova, valor caiu) + 800 (nova, tipo mudou) = 10300.
    assert rows[0].total_wait_time_ms == 10300


async def test_wait_summary_sums_effective_wait_delta_for_continuous_wait(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Mesma regressao de `test_top_sql_sums_effective_wait_delta_for_continuous_wait_same_session`,
    validada em `wait_summary` — usada pela barra de severidade de wait da lista de instancias."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ?",
                session_id="sess-3",
                wait_type=WaitCategory.LOCK,
                wait_event_native="LCK_M_S",
                wait_time_ms=1000,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=10),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ?",
                session_id="sess-3",
                wait_type=WaitCategory.LOCK,
                wait_event_native="LCK_M_S",
                wait_time_ms=2200,
            ),
        ]
    )

    rows = await repo.wait_summary(
        test_instance.id, since=now - timedelta(minutes=1), until=now + timedelta(minutes=1)
    )

    summary = {row.wait_type: row.total_wait_time_ms for row in rows}
    # 1000 (1a amostra) + 1200 (2200-1000) = 2200 — nao 3200.
    assert summary == {WaitCategory.LOCK.value: 2200}


async def test_wait_breakdown_by_database_sums_effective_wait_delta(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Mesma regressao validada em `wait_breakdown_by_database` — a decomposicao por wait nativo
    usada no grafico "Bancos de dados" do drill-down."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ?",
                database_name="db_a",
                session_id="sess-4",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=3000,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=10),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ?",
                database_name="db_a",
                session_id="sess-4",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=6500,
            ),
        ]
    )

    rows = await repo.wait_breakdown_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_key = {(row.database_name, row.wait_event_native): row.total_wait_time_ms for row in rows}
    # 3000 (1a amostra) + 3500 (6500-3000) = 6500 — nao 9500.
    assert by_key[("db_a", "PAGEIOLATCH_SH")] == 6500


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


async def test_top_wait_types_excludes_benign_wait_from_group_by(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`WAITFOR` (job de polling do SQL Agent em loop) e um wait benigno/de background — nao deve
    aparecer no resultado (nem como linha "WAITFOR: 0ms"), so o wait real."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="WAITFOR",
                wait_time_ms=999_999,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=30,
            ),
        ]
    )

    rows = await repo.top_wait_types(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_type = {row.wait_event_native: row for row in rows}
    assert set(by_type) == {"PAGEIOLATCH_SH"}


async def test_top_sql_zeroes_wait_time_for_benign_wait_but_keeps_execution_count(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Uma amostra com wait benigno ainda conta em `execution_count` (a query realmente
    executou) — so nao contribui pro total de wait time."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-benign",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="WAITFOR",
                wait_time_ms=500_000,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-benign",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=42,
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
    assert rows[0].execution_count == 2
    assert rows[0].total_wait_time_ms == 42


async def test_wait_breakdown_by_database_excludes_benign_wait(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ?",
                database_name="db_a",
                wait_event_native="BROKER_RECEIVE_WAITFOR",
                wait_time_ms=123_456,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ?",
                database_name="db_a",
                wait_event_native="WRITELOG",
                wait_time_ms=77,
            ),
        ]
    )

    rows = await repo.wait_breakdown_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_key = {(row.database_name, row.wait_event_native): row.total_wait_time_ms for row in rows}
    assert set(by_key) == {("db_a", "WRITELOG")}
