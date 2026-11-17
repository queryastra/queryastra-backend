"""Anotacoes de eventos (deploy, config, incidente, manutencao) — CRUD.

Leitura: qualquer usuario autenticado, restrito as instancias a que tem acesso (anotacoes
globais — `instance_id` nulo — sao visiveis para todos). Escrita (criar/editar/excluir): operator
ou admin; anotacao global so pode ser criada/alterada por admin (afeta todas as instancias).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_db_session, require_operator
from app.models.annotation import Annotation
from app.repositories.annotations_repository import MAX_ANNOTATIONS, AnnotationsRepository
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository
from app.services.audit_service import record_audit

router = APIRouter()

AnnotationCategory = Literal["deploy", "config", "incident", "maintenance", "other"]


class AnnotationResponse(BaseModel):
    id: UUID
    instance_id: UUID | None
    occurred_at: datetime
    title: str
    description: str | None
    category: str
    created_by_user_id: int | None
    created_by_username: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class AnnotationCreateRequest(BaseModel):
    instance_id: UUID | None = None
    occurred_at: datetime
    title: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=4000)
    category: AnnotationCategory = "other"

    @field_validator("title")
    @classmethod
    def _strip_title(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("title must not be blank")
        return stripped


class AnnotationUpdateRequest(BaseModel):
    occurred_at: datetime | None = None
    title: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=4000)
    category: AnnotationCategory | None = None

    @field_validator("title")
    @classmethod
    def _strip_title(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("title must not be blank")
        return stripped


async def _ensure_instance_access(
    session: AsyncSession, user: CurrentUser, instance_id: UUID | None, *, write: bool
) -> None:
    if user.is_admin:
        return
    if instance_id is None:
        if write:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Somente administradores podem gerenciar anotacoes globais.",
            )
        return
    if not await UserInstanceAccessRepository(session).has_access(user.user_id, instance_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acesso nao autorizado a esta instancia.",
        )


async def _get_or_404(repo: AnnotationsRepository, annotation_id: UUID) -> Annotation:
    row = await repo.get(annotation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Anotacao nao encontrada.")
    return row


@router.get("", response_model=list[AnnotationResponse])
async def list_annotations(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    instance_id: Annotated[UUID | None, Query()] = None,
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    include_global: Annotated[bool, Query()] = True,
    category: Annotated[AnnotationCategory | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_ANNOTATIONS)] = 500,
) -> list[AnnotationResponse]:
    """Anotacoes no periodo. Com `instance_id`: daquela instancia + globais (`include_global`).
    Sem `instance_id`: globais + das instancias acessiveis ao usuario."""
    accessible: set[UUID] | None = None
    if instance_id is not None:
        await _ensure_instance_access(session, current_user, instance_id, write=False)
    elif not current_user.is_admin:
        accessible = await UserInstanceAccessRepository(session).get_instance_ids_for_user(
            current_user.user_id
        )
    rows = await AnnotationsRepository(session).list(
        instance_id=instance_id,
        since=since,
        until=until,
        include_global=include_global,
        accessible_instance_ids=accessible,
        category=category,
        limit=limit,
    )
    return [AnnotationResponse.model_validate(r) for r in rows]


@router.get("/{annotation_id}", response_model=AnnotationResponse)
async def get_annotation(
    annotation_id: UUID,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AnnotationResponse:
    row = await _get_or_404(AnnotationsRepository(session), annotation_id)
    await _ensure_instance_access(session, current_user, row.instance_id, write=False)
    return AnnotationResponse.model_validate(row)


@router.post("", response_model=AnnotationResponse, status_code=status.HTTP_201_CREATED)
async def create_annotation(
    body: AnnotationCreateRequest,
    request: Request,
    current_user: Annotated[CurrentUser, Depends(require_operator)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AnnotationResponse:
    await _ensure_instance_access(session, current_user, body.instance_id, write=True)
    await record_audit(
        session,
        actor=current_user.username,
        action="annotation.created",
        ip_address=request.client.host if request.client else None,
        target_type="instance" if body.instance_id else "global",
        target_id=str(body.instance_id) if body.instance_id else None,
        target_name=body.title,
        detail={"category": body.category, "occurred_at": body.occurred_at.isoformat()},
    )
    row = await AnnotationsRepository(session).create(
        instance_id=body.instance_id,
        occurred_at=body.occurred_at,
        title=body.title,
        description=body.description,
        category=body.category,
        created_by_user_id=current_user.user_id,
        created_by_username=current_user.username,
    )
    return AnnotationResponse.model_validate(row)


@router.patch("/{annotation_id}", response_model=AnnotationResponse)
async def update_annotation(
    annotation_id: UUID,
    body: AnnotationUpdateRequest,
    request: Request,
    current_user: Annotated[CurrentUser, Depends(require_operator)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AnnotationResponse:
    repo = AnnotationsRepository(session)
    row = await _get_or_404(repo, annotation_id)
    await _ensure_instance_access(session, current_user, row.instance_id, write=True)
    changes = body.model_dump(exclude_unset=True)
    # `title`/`occurred_at`/`category` sao NOT NULL — um `null` explicito e ignorado.
    for key in ("title", "occurred_at", "category"):
        if key in changes and changes[key] is None:
            changes.pop(key)
    if not changes:
        return AnnotationResponse.model_validate(row)
    await record_audit(
        session,
        actor=current_user.username,
        action="annotation.updated",
        ip_address=request.client.host if request.client else None,
        target_type="instance" if row.instance_id else "global",
        target_id=str(row.instance_id) if row.instance_id else None,
        target_name=row.title,
        detail={"annotation_id": str(row.id), "fields": sorted(changes.keys())},
    )
    row = await repo.update(row, changes)
    return AnnotationResponse.model_validate(row)


@router.delete("/{annotation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_annotation(
    annotation_id: UUID,
    request: Request,
    current_user: Annotated[CurrentUser, Depends(require_operator)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    repo = AnnotationsRepository(session)
    row = await _get_or_404(repo, annotation_id)
    await _ensure_instance_access(session, current_user, row.instance_id, write=True)
    await record_audit(
        session,
        actor=current_user.username,
        action="annotation.deleted",
        ip_address=request.client.host if request.client else None,
        target_type="instance" if row.instance_id else "global",
        target_id=str(row.instance_id) if row.instance_id else None,
        target_name=row.title,
        detail={"annotation_id": str(row.id)},
    )
    await repo.delete(row)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
