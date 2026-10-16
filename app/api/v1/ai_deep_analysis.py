"""Análise profunda de instância via IA com acesso ao banco ao vivo.

POST /instances/{instance_id}/ai/deep-analysis

Coleta diagnóstico em tempo real (índices não usados, I/O de tabela, queries lentas,
sugestões de índice) usando a conexão já gerenciada pelo QueryAstra, e envia ao modelo
de IA configurado para gerar análise acionável.

Requer o mesmo nível de acesso de endpoints de vacuum/health — verifica via
`check_instance_access`. A IA nunca recebe credenciais; o backend executa as queries.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_db_session
from app.api.v1.queries import get_ai_advisor
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.repositories.instances_repository import InstancesRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)
from app.services.ai_advisor import AiAdvisor, MultiKeyAiAdvisor
from app.services.deep_analysis_service import DeepAnalysisService

logger = get_logger(__name__)
router = APIRouter()


class DeepAnalysisResponse(BaseModel):
    configured: bool
    narrative: str | None = None
    data_collected: list[str] = []
    tokens_consumed: int = 0


@router.post("/{instance_id}/ai/deep-analysis", response_model=DeepAnalysisResponse)
async def run_deep_analysis(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: Annotated[CredentialResolver, Depends(CredentialResolver)],
    ai_advisor: Annotated[AiAdvisor | MultiKeyAiAdvisor, Depends(get_ai_advisor)],
) -> DeepAnalysisResponse:
    if not ai_advisor.is_configured:
        return DeepAnalysisResponse(configured=False)

    instance = await InstancesRepository(session).get_by_id(instance_id)
    if instance is None or not instance.active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instância não encontrada ou inativa.",
        )

    username: str | None = None
    secret_value: str | None = None
    if instance.auth_method == "encrypted_fallback":
        if instance.encrypted_credential_ref is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Referência de credencial ausente na instância.",
            )
        try:
            resolved = await credential_resolver.resolve(
                CredentialReference(instance.encrypted_credential_ref)
            )
            username, secret_value = resolved.username, resolved.secret_value
        except CredentialResolutionError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Falha ao resolver credencial da instância.",
            ) from exc

    connection = ConnectionParams(
        host=instance.host,
        port=instance.port,
        auth_method=instance.auth_method,
        username=username,
        secret_value=secret_value,
    )
    collector = create_collector(instance.db_type, connection=connection, instance_id=instance.id)
    try:
        service = DeepAnalysisService(ai_advisor)
        result = await service.run(collector, instance)
    except Exception as exc:
        logger.warning("Erro na análise profunda da instância %s: %s", instance_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Falha ao coletar diagnóstico da instância.",
        ) from exc
    finally:
        await collector.close()

    return DeepAnalysisResponse(
        configured=result.configured,
        narrative=result.narrative,
        data_collected=result.data_collected,
        tokens_consumed=result.tokens_consumed,
    )
