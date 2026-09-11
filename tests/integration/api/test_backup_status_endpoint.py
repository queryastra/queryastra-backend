"""Teste de integracao de `GET /api/v1/backup-status/{instance_id}` contra o banco real
`ab_xql_pm_dev` — ARCHITECTURE_SPEC.md Q16."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedBackupStatus
from app.repositories.backup_status_repository import BackupStatusRepository


async def test_list_backup_status_returns_empty_list_when_none(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/backup-status/{test_instance.id}")

    assert response.status_code == 200
    assert response.json() == []


async def test_list_backup_status_returns_sql_server_rows_per_database(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BackupStatusRepository(db_session)
    checked_at = datetime.now(UTC)
    last_backup = checked_at - timedelta(days=1)
    await repo.upsert_batch(
        [
            NormalizedBackupStatus(
                instance_id=test_instance.id,
                database_name="AB_COBRANCA",
                last_backup_at=last_backup,
                detail={"backup_type": "D"},
            ),
            NormalizedBackupStatus(
                instance_id=test_instance.id,
                database_name="master",
                last_backup_at=None,
                detail={"backup_type": "D"},
            ),
        ],
        checked_at,
    )

    response = await client.get(f"/api/v1/backup-status/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    by_database = {row["database_name"]: row for row in body}
    assert by_database["AB_COBRANCA"]["last_backup_at"] is not None
    assert by_database["master"]["last_backup_at"] is None
    assert by_database["AB_COBRANCA"]["instance_id"] == str(test_instance.id)


async def test_list_backup_status_returns_postgresql_instance_level_row(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = BackupStatusRepository(db_session)
    checked_at = datetime.now(UTC)
    await repo.upsert_batch(
        [
            NormalizedBackupStatus(
                instance_id=test_instance.id,
                database_name=None,
                last_backup_at=checked_at - timedelta(hours=2),
                detail={"archived_count": 42},
            )
        ],
        checked_at,
    )

    response = await client.get(f"/api/v1/backup-status/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["database_name"] is None
    assert body[0]["detail"] == {"archived_count": 42}


async def test_list_backup_status_for_unknown_instance_returns_empty_list(
    client: AsyncClient,
) -> None:
    response = await client.get(f"/api/v1/backup-status/{uuid4()}")

    assert response.status_code == 200
    assert response.json() == []
