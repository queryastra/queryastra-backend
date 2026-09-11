"""Testes de `BackupStatusRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.backup_status import BackupStatus
from app.models.instance import Instance
from app.normalizers.models import NormalizedBackupStatus
from app.repositories.backup_status_repository import BackupStatusRepository


async def _cleanup(db_session: AsyncSession, instance_id) -> None:  # noqa: ANN001 - helper de teste
    await db_session.execute(delete(BackupStatus).where(BackupStatus.instance_id == instance_id))
    await db_session.commit()


async def test_upsert_batch_inserts_sql_server_row_per_database(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BackupStatusRepository(db_session)
    checked_at = datetime.now(UTC)
    row = NormalizedBackupStatus(
        instance_id=test_instance.id,
        database_name="AB_COBRANCA",
        last_backup_at=checked_at - timedelta(days=1),
        detail={"backup_type": "D"},
    )

    await repo.upsert_batch([row], checked_at)

    rows = await repo.list_for_instance(test_instance.id)
    assert len(rows) == 1
    assert rows[0].database_name == "AB_COBRANCA"
    assert rows[0].last_backup_at == checked_at - timedelta(days=1)
    assert rows[0].detail == {"backup_type": "D"}
    assert rows[0].checked_at == checked_at

    await _cleanup(db_session, test_instance.id)


async def test_upsert_batch_updates_existing_sql_server_row_by_database(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Segunda checagem do MESMO banco atualiza a linha existente, nao cria uma nova (conflito
    no indice parcial `uq_backup_status_instance_database`)."""
    repo = BackupStatusRepository(db_session)
    first_checked_at = datetime.now(UTC) - timedelta(hours=1)
    second_checked_at = datetime.now(UTC)

    await repo.upsert_batch(
        [
            NormalizedBackupStatus(
                instance_id=test_instance.id,
                database_name="AB_COBRANCA",
                last_backup_at=first_checked_at - timedelta(days=2),
                detail={"backup_type": "D"},
            )
        ],
        first_checked_at,
    )
    await repo.upsert_batch(
        [
            NormalizedBackupStatus(
                instance_id=test_instance.id,
                database_name="AB_COBRANCA",
                last_backup_at=second_checked_at - timedelta(hours=6),
                detail={"backup_type": "D"},
            )
        ],
        second_checked_at,
    )

    rows = await repo.list_for_instance(test_instance.id)
    assert len(rows) == 1
    assert rows[0].last_backup_at == second_checked_at - timedelta(hours=6)
    assert rows[0].checked_at == second_checked_at

    await _cleanup(db_session, test_instance.id)


async def test_upsert_batch_inserts_postgresql_instance_level_row(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """PostgreSQL: `database_name=None`, uma unica linha por instancia."""
    repo = BackupStatusRepository(db_session)
    checked_at = datetime.now(UTC)
    row = NormalizedBackupStatus(
        instance_id=test_instance.id,
        database_name=None,
        last_backup_at=checked_at - timedelta(hours=3),
        detail={"archived_count": 42},
    )

    await repo.upsert_batch([row], checked_at)

    rows = await repo.list_for_instance(test_instance.id)
    assert len(rows) == 1
    assert rows[0].database_name is None
    assert rows[0].last_backup_at == checked_at - timedelta(hours=3)

    await _cleanup(db_session, test_instance.id)


async def test_upsert_batch_updates_existing_postgresql_instance_level_row(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Segunda checagem do Postgres atualiza a MESMA linha (`database_name IS NULL`), nao cria
    uma segunda — confirma que o indice parcial `uq_backup_status_instance_only` funciona (NULL
    normalmente NAO seria tratado como repetivel num UNIQUE comum)."""
    repo = BackupStatusRepository(db_session)
    first_checked_at = datetime.now(UTC) - timedelta(hours=1)
    second_checked_at = datetime.now(UTC)

    await repo.upsert_batch(
        [
            NormalizedBackupStatus(
                instance_id=test_instance.id,
                database_name=None,
                last_backup_at=first_checked_at - timedelta(hours=2),
                detail={"archived_count": 10},
            )
        ],
        first_checked_at,
    )
    await repo.upsert_batch(
        [
            NormalizedBackupStatus(
                instance_id=test_instance.id,
                database_name=None,
                last_backup_at=second_checked_at - timedelta(minutes=30),
                detail={"archived_count": 20},
            )
        ],
        second_checked_at,
    )

    rows = await repo.list_for_instance(test_instance.id)
    assert len(rows) == 1
    assert rows[0].detail == {"archived_count": 20}
    assert rows[0].checked_at == second_checked_at

    await _cleanup(db_session, test_instance.id)


async def test_upsert_batch_noop_for_empty_list(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BackupStatusRepository(db_session)
    await repo.upsert_batch([], datetime.now(UTC))
    rows = await repo.list_for_instance(test_instance.id)
    assert rows == []
