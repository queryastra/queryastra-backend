"""Repositorio de `annotations` — anotacoes de eventos exibidas sobre os graficos.

`build_list_statement` e uma funcao pura (sem sessao) para que o filtro "anotacoes da instancia
+ globais, dentro da janela" seja testavel sem banco real.
"""

from __future__ import annotations

from collections.abc import Collection
from datetime import datetime
from typing import Any
from uuid import UUID

import sqlalchemy as sa

from app.models.annotation import Annotation
from app.repositories.base import BaseRepository

MAX_ANNOTATIONS = 1000


def build_list_statement(
    *,
    instance_id: UUID | None,
    since: datetime | None,
    until: datetime | None,
    include_global: bool = True,
    accessible_instance_ids: Collection[UUID] | None = None,
    category: str | None = None,
    limit: int = MAX_ANNOTATIONS,
) -> sa.Select[tuple[Annotation]]:
    """Monta o SELECT de listagem.

    - `instance_id` informado: anotacoes daquela instancia (+ globais se `include_global`).
    - `instance_id` ausente: globais + anotacoes das instancias em `accessible_instance_ids`
      (`None` = sem restricao, caso do admin).
    """
    stmt = sa.select(Annotation)
    if instance_id is not None:
        scope = Annotation.instance_id == instance_id
        if include_global:
            scope = sa.or_(scope, Annotation.instance_id.is_(None))
        stmt = stmt.where(scope)
    elif accessible_instance_ids is not None:
        ids = list(accessible_instance_ids)
        conditions: list[Any] = [Annotation.instance_id.is_(None)]
        if ids:
            conditions.append(Annotation.instance_id.in_(ids))
        stmt = stmt.where(sa.or_(*conditions))
    if since is not None:
        stmt = stmt.where(Annotation.occurred_at >= since)
    if until is not None:
        stmt = stmt.where(Annotation.occurred_at <= until)
    if category is not None:
        stmt = stmt.where(Annotation.category == category)
    return stmt.order_by(Annotation.occurred_at.desc()).limit(min(limit, MAX_ANNOTATIONS))


class AnnotationsRepository(BaseRepository):
    async def list(
        self,
        *,
        instance_id: UUID | None,
        since: datetime | None,
        until: datetime | None,
        include_global: bool = True,
        accessible_instance_ids: Collection[UUID] | None = None,
        category: str | None = None,
        limit: int = MAX_ANNOTATIONS,
    ) -> list[Annotation]:
        stmt = build_list_statement(
            instance_id=instance_id,
            since=since,
            until=until,
            include_global=include_global,
            accessible_instance_ids=accessible_instance_ids,
            category=category,
            limit=limit,
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get(self, annotation_id: UUID) -> Annotation | None:
        return await self._session.get(Annotation, annotation_id)

    async def create(
        self,
        *,
        instance_id: UUID | None,
        occurred_at: datetime,
        title: str,
        description: str | None,
        category: str,
        created_by_user_id: int | None,
        created_by_username: str | None,
    ) -> Annotation:
        row = Annotation(
            instance_id=instance_id,
            occurred_at=occurred_at,
            title=title,
            description=description,
            category=category,
            created_by_user_id=created_by_user_id,
            created_by_username=created_by_username,
        )
        self._session.add(row)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def update(self, row: Annotation, changes: dict[str, Any]) -> Annotation:
        for key, value in changes.items():
            setattr(row, key, value)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def delete(self, row: Annotation) -> None:
        await self._session.delete(row)
        await self._session.commit()
