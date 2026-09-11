"""Repositorio de `backup_status` — ARCHITECTURE_SPEC.md Q16.

Tabela relacional comum (nao hypertable): cada linha e o estado mais recente de backup/archiving
de um banco (SQL Server, `database_name` preenchido) ou da instancia inteira (PostgreSQL,
`database_name` NULL) — atualizada via UPSERT a cada ciclo do `BackupStatusSupervisor`, nunca
inserida como serie temporal bruta.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.backup_status import BackupStatus
from app.normalizers.models import NormalizedBackupStatus
from app.repositories.base import BaseRepository


class BackupStatusRepository(BaseRepository):
    async def upsert_batch(
        self,
        rows: list[NormalizedBackupStatus],
        checked_at: datetime,
    ) -> None:
        """Insere ou atualiza cada linha — alvo de conflito depende de `database_name` ser
        `None` (Postgres: 1 linha por instancia, indice parcial `uq_backup_status_instance_only`)
        ou preenchido (SQL Server: 1 linha por banco, indice parcial
        `uq_backup_status_instance_database`) — ver docstring do modelo. Loop por linha em vez de
        um `INSERT` multi-linha: os 2 alvos de conflito diferentes nao podem ser combinados numa
        unica instrucao, e o volume por ciclo e pequeno (1 linha no Postgres, algumas dezenas de
        bancos no SQL Server no maximo)."""
        if not rows:
            return

        for row in rows:
            values = {
                "instance_id": row.instance_id,
                "database_name": row.database_name,
                "last_backup_at": row.last_backup_at,
                "detail": row.detail,
                "checked_at": checked_at,
            }
            stmt = pg_insert(BackupStatus).values(**values)
            if row.database_name is None:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["instance_id"],
                    index_where=BackupStatus.database_name.is_(None),
                    set_={
                        "last_backup_at": stmt.excluded.last_backup_at,
                        "detail": stmt.excluded.detail,
                        "checked_at": stmt.excluded.checked_at,
                    },
                )
            else:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["instance_id", "database_name"],
                    index_where=BackupStatus.database_name.is_not(None),
                    set_={
                        "last_backup_at": stmt.excluded.last_backup_at,
                        "detail": stmt.excluded.detail,
                        "checked_at": stmt.excluded.checked_at,
                    },
                )
            await self._session.execute(stmt)
        await self._session.commit()

    async def list_for_instance(self, instance_id: UUID) -> list[BackupStatus]:
        """Estado de backup/archiving de uma instancia — 1 linha (Postgres, `database_name`
        NULL) ou 1 linha por banco (SQL Server), ordenado por nome de banco (`NULL` primeiro)."""
        stmt = (
            sa.select(BackupStatus)
            .where(BackupStatus.instance_id == instance_id)
            .order_by(sa.nulls_first(BackupStatus.database_name))
        )
        return list((await self._session.execute(stmt)).scalars().all())
