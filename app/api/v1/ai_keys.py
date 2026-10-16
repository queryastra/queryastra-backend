"""CRUD de chaves de API de IA (BYOK multi-key) — tela "Config → IA / Diagnóstico" no frontend.

Cada chave tem prioridade, data de expiração opcional, limite de tokens e contagem de consumo.
O advisor usa as chaves habilitadas em ordem de prioridade, com failover automático em caso de
erros 429/quota (cooldown de 5 min via `last_error_at`).
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Annotated, Sequence

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session, require_admin
from app.models.ai_api_key import AiApiKey
from app.repositories.ai_api_key_repository import AiApiKeyRepository

router = APIRouter()


# ── schemas ───────────────────────────────────────────────────────────────────

class AiApiKeyResponse(BaseModel):
    id: uuid.UUID
    name: str
    provider: str
    base_url: str | None
    model: str | None
    is_enabled: bool
    priority: int
    expiration_date: date | None
    token_limit: int | None
    tokens_used: int
    last_used_at: str | None
    last_error_at: str | None
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class AiApiKeyCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    provider: str = Field(default="openai")
    api_key: str = Field(..., min_length=1)
    base_url: str | None = None
    model: str | None = None
    priority: int = Field(default=0, ge=0)
    expiration_date: date | None = None
    token_limit: int | None = Field(default=None, ge=1)


class AiApiKeyUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    provider: str | None = None
    api_key: str | None = Field(default=None, min_length=1)
    base_url: str | None = None
    model: str | None = None
    is_enabled: bool | None = None
    priority: int | None = Field(default=None, ge=0)
    expiration_date: date | None = None
    token_limit: int | None = Field(default=None, ge=1)
    clear_expiration: bool = False
    clear_token_limit: bool = False


# ── helpers ───────────────────────────────────────────────────────────────────

def _to_response(row: AiApiKey) -> AiApiKeyResponse:
    return AiApiKeyResponse(
        id=row.id,
        name=row.name,
        provider=row.provider,
        base_url=row.base_url,
        model=row.model,
        is_enabled=row.is_enabled,
        priority=row.priority,
        expiration_date=row.expiration_date,
        token_limit=row.token_limit,
        tokens_used=row.tokens_used,
        last_used_at=row.last_used_at.isoformat() if row.last_used_at else None,
        last_error_at=row.last_error_at.isoformat() if row.last_error_at else None,
        created_at=row.created_at.isoformat(),
        updated_at=row.updated_at.isoformat(),
    )


async def _get_or_404(repo: AiApiKeyRepository, key_id: uuid.UUID) -> AiApiKey:
    row = await repo.get(key_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="AI API key not found")
    return row


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.get("", response_model=list[AiApiKeyResponse], dependencies=[Depends(require_admin)])
async def list_ai_keys(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[AiApiKeyResponse]:
    repo = AiApiKeyRepository(session)
    rows = await repo.list_all()
    return [_to_response(r) for r in rows]


@router.post("", response_model=AiApiKeyResponse, status_code=status.HTTP_201_CREATED,
             dependencies=[Depends(require_admin)])
async def create_ai_key(
    body: AiApiKeyCreateRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AiApiKeyResponse:
    repo = AiApiKeyRepository(session)
    row = await repo.create(
        name=body.name,
        provider=body.provider,
        raw_api_key=body.api_key,
        base_url=body.base_url,
        model=body.model,
        priority=body.priority,
        expiration_date=body.expiration_date,
        token_limit=body.token_limit,
    )
    return _to_response(row)


@router.patch("/{key_id}", response_model=AiApiKeyResponse, dependencies=[Depends(require_admin)])
async def update_ai_key(
    key_id: uuid.UUID,
    body: AiApiKeyUpdateRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AiApiKeyResponse:
    repo = AiApiKeyRepository(session)
    row = await _get_or_404(repo, key_id)
    row = await repo.update(
        row,
        name=body.name,
        provider=body.provider,
        raw_api_key=body.api_key,
        base_url=body.base_url,
        model=body.model,
        is_enabled=body.is_enabled,
        priority=body.priority,
        expiration_date=body.expiration_date,
        token_limit=body.token_limit,
        _clear_expiration=body.clear_expiration,
        _clear_token_limit=body.clear_token_limit,
    )
    return _to_response(row)


@router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(require_admin)])
async def delete_ai_key(
    key_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    repo = AiApiKeyRepository(session)
    row = await _get_or_404(repo, key_id)
    await repo.delete(row)


@router.post("/{key_id}/reset-tokens", response_model=AiApiKeyResponse,
             dependencies=[Depends(require_admin)])
async def reset_ai_key_tokens(
    key_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AiApiKeyResponse:
    repo = AiApiKeyRepository(session)
    row = await _get_or_404(repo, key_id)
    row = await repo.reset_tokens(row)
    return _to_response(row)


@router.post("/{key_id}/reset-error", response_model=AiApiKeyResponse,
             dependencies=[Depends(require_admin)])
async def reset_ai_key_error(
    key_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AiApiKeyResponse:
    repo = AiApiKeyRepository(session)
    row = await _get_or_404(repo, key_id)
    row = await repo.reset_error(row)
    return _to_response(row)
