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
    dado quando o plano nao foi encontrado. `source` (`available=True` apenas) distingue
    "actual" (contagens reais de execucao, so quando a sessao original ainda esta rodando) de
    "cached" (plano cacheado/estimado, o caso comum) — a tela SEMPRE informa qual dos dois esta
    mostrando, nunca apresenta um como se fosse o outro."""

    available: bool
    plan_xml: str | None
    reason: str | None
    source: str | None = None


_POSTGRESQL_NOT_SUPPORTED_REASON = (
    "PostgreSQL does not expose execution plans via SQL — pg_stat_statements does not store plans, "
    "and auto_explain only writes to the server log (no SQL-accessible function)."
)
_PLAN_NOT_FOUND_REASON = (
    "Plan not found in cache or Query Store — it may have been evicted (memory pressure, "
    "recompilation, Query Store retention limit) since the last execution. Most likely to be found "
    "immediately after observing the query running live."
)
_CREDENTIAL_MISSING_REASON = "Instance credential not configured."
_CREDENTIAL_RESOLUTION_FAILED_REASON = "Failed to resolve the instance credential."


async def fetch_execution_plan(
    instance: Instance,
    sql_hash: str,
    credential_resolver: CredentialResolver,
    database_names: list[str] | None = None,
) -> ExecutionPlanResult:
    """Busca o plano de execucao do SQL Server que corresponde a `sql_hash`, mascarando valores
    de parametro embutidos no XML antes de devolver. Fluxo de 3 etapas:
      1. Plano REAL via sessao ativa (`fetch_actual_execution_plan`) — contagens reais, so
         possivel enquanto a sessao original ainda executa.
      2. Cache de plano (`fetch_execution_plan`) — plano estimado/cacheado, o caso comum.
      3. Query Store (`fetch_plan_from_query_store`) — Item 10: persiste planos em disco mesmo
         apos recompilacao/pressao de memoria; requer `database_names` para restringir a busca
         aos bancos onde o sql_hash foi visto.
    PostgreSQL: `available=False` direto, sem tentar conectar."""
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
        actual_plan_xml = await collector.fetch_actual_execution_plan(sql_hash)
        if actual_plan_xml is not None:
            return ExecutionPlanResult(
                available=True,
                plan_xml=mask_showplan_xml_literals(actual_plan_xml),
                reason=None,
                source="actual",
            )
        plan_xml = await collector.fetch_execution_plan(sql_hash)
        if plan_xml is not None:
            return ExecutionPlanResult(
                available=True,
                plan_xml=mask_showplan_xml_literals(plan_xml),
                reason=None,
                source="cached",
            )
        # Item 10: fallback pro Query Store quando o cache de plano nao tem o plano.
        qs_plan_xml = await collector.fetch_plan_from_query_store(sql_hash, database_names or [])
    finally:
        await collector.close()

    if qs_plan_xml is None:
        return ExecutionPlanResult(available=False, plan_xml=None, reason=_PLAN_NOT_FOUND_REASON)
    return ExecutionPlanResult(
        available=True,
        plan_xml=mask_showplan_xml_literals(qs_plan_xml),
        reason=None,
        source="query_store",
    )
