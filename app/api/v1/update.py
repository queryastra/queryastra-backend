"""Endpoints de verificacao e instalacao de atualizacoes — admin only.

GET  /update/check   — retorna o status da ultima verificacao (versao atual, disponivel, etc.)
POST /update/install — dispara o download + substituicao + reinicio do processo em background
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.api.deps import CurrentUser, require_admin
from app.workers.update_checker import UpdateStatus, get_update_status, perform_install

router = APIRouter()


class UpdateCheckResponse(BaseModel):
    current_version: str
    latest_version: str | None
    update_available: bool
    download_url: str | None
    release_notes: str | None
    check_error: str | None


@router.get("/check", response_model=UpdateCheckResponse)
async def check_update(
    _admin: Annotated[CurrentUser, Depends(require_admin)],
) -> UpdateCheckResponse:
    """Retorna o status da ultima verificacao de atualizacao. Requer papel admin."""
    s: UpdateStatus = get_update_status()
    return UpdateCheckResponse(
        current_version=s.current_version,
        latest_version=s.latest_version,
        update_available=s.update_available,
        download_url=s.download_url,
        release_notes=s.release_notes,
        check_error=s.check_error,
    )


@router.post("/install", status_code=status.HTTP_202_ACCEPTED)
async def install_update(
    _admin: Annotated[CurrentUser, Depends(require_admin)],
) -> dict[str, str]:
    """Baixa e instala a atualizacao, reiniciando o servico. Requer papel admin.

    Responde 202 imediatamente; download e substituicao ocorrem em background.
    Em modo desenvolvimento (nao-frozen) retorna 202 com aviso no log, sem reiniciar.
    """
    s = get_update_status()
    if not s.update_available or not s.download_url:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Nenhuma atualizacao disponivel para instalar.",
        )
    asyncio.create_task(perform_install(s.download_url))
    return {"detail": "Instalacao iniciada. O servico sera reiniciado em instantes."}
