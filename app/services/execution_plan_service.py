"""Busca sob demanda de plano de execucao — Etapa 11.

Diferente dos demais servicos deste modulo, isto NAO e parte do ciclo de coleta (nunca chamado
por um worker/supervisor) — e uma acao pontual disparada pelo usuario na tela de detalhe de query
(nivel 3 do drill-down), buscando o plano em cache do SQL Server no momento da chamada. So SQL
Server: PostgreSQL nao expoe nenhum plano de execucao historico via SQL (`pg_stat_statements` nao
guarda plano; `auto_explain` so escreve no log do servidor, sem funcao SQL-acessivel) — devolve
`available=False` direto, sem tentar conectar.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.collectors.connection import ConnectionParams
from app.collectors.sqlserver.adapter import SQLServerCollector
from app.masking.sql_masking import mask_showplan_xml_literals
from app.models.instance import Instance
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)


@dataclass(frozen=True, slots=True)
class ExecutionPlanResult:
    """Resultado da busca — `reason` sempre preenchido quando `available=False`, nunca inventa
    dado quando o plano nao foi encontrado."""

    available: bool
    plan_xml: str | None
    reason: str | None


_POSTGRESQL_NOT_SUPPORTED_REASON = (
    "PostgreSQL nao expoe plano de execucao via SQL — pg_stat_statements nao guarda plano, e "
    "auto_explain so escreve no log do servidor (sem funcao SQL-acessivel)."
)
_PLAN_NOT_FOUND_REASON = (
    "Plano não encontrado no cache — pode ter sido removido (memória, recompilação) desde a "
    "última execução, ou empurrado para fora da janela de busca por atividade de fundo muito "
    "intensa nesta instância (CDC, replicação, o próprio monitoramento). Mais provável de "
    "encontrar logo após ver a query rodando ao vivo."
)
_CREDENTIAL_MISSING_REASON = "Credencial da instancia nao configurada."
_CREDENTIAL_RESOLUTION_FAILED_REASON = "Falha ao resolver a credencial da instancia."


async def fetch_execution_plan(
    instance: Instance, sql_hash: str, credential_resolver: CredentialResolver
) -> ExecutionPlanResult:
    """Busca o plano de execucao em cache do SQL Server que corresponde a `sql_hash`, mascarando
    valores de parametro embutidos no XML antes de devolver (`mask_showplan_xml_literals`)."""
    if instance.db_type != "sqlserver":
        return ExecutionPlanResult(
            available=False, plan_xml=None, reason=_POSTGRESQL_NOT_SUPPORTED_REASON
        )

    username: str | None = None
    secret_value: str | None = None
    if instance.auth_method == "encrypted_fallback":
        if instance.encrypted_credential_ref is None:
            return ExecutionPlanResult(
                available=False, plan_xml=None, reason=_CREDENTIAL_MISSING_REASON
            )
        try:
            resolved = await credential_resolver.resolve(
                CredentialReference(instance.encrypted_credential_ref)
            )
        except CredentialResolutionError:
            return ExecutionPlanResult(
                available=False, plan_xml=None, reason=_CREDENTIAL_RESOLUTION_FAILED_REASON
            )
        username, secret_value = resolved.username, resolved.secret_value

    connection = ConnectionParams(
        host=instance.host,
        port=instance.port,
        auth_method=instance.auth_method,
        username=username,
        secret_value=secret_value,
    )
    collector = SQLServerCollector(connection=connection, instance_id=instance.id)
    try:
        plan_xml = await collector.fetch_execution_plan(sql_hash)
    finally:
        await collector.close()

    if plan_xml is None:
        return ExecutionPlanResult(available=False, plan_xml=None, reason=_PLAN_NOT_FOUND_REASON)
    return ExecutionPlanResult(
        available=True, plan_xml=mask_showplan_xml_literals(plan_xml), reason=None
    )
