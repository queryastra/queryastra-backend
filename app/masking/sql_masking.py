"""Mascaramento de literais em texto SQL — `Docs/rules/03-sql-privacy.md`.

"Mascarar literais antes de persistir ou exibir texto SQL." Este e o unico modulo de coleta com
logica real e completa neste scaffold (o resto e stub) — mascaramento e obrigatorio no MVP, nao
configuravel (ARCHITECTURE_SPEC.md, decisao Q7).

Cobre os dois tipos de literal mais comuns entre PostgreSQL e SQL Server:
    - string entre aspas simples, incluindo `''` (aspas simples escapada dentro do literal);
    - numero (inteiro ou decimal, com sinal opcional).

Não e um parser SQL completo (nao lida com dollar-quoting do Postgres, comentarios, ou
identificadores entre aspas duplas contendo caracteres especiais) — e uma normalizacao razoavel
o suficiente para o texto exibido/persistido em `query_samples.sql_text_masked`, não uma
ferramenta de reescrita de SQL.
"""

from __future__ import annotations

import re

# String literal: aspas simples, permitindo '' como escape de aspas simples dentro do literal
# (padrão SQL ANSI, valido tanto em PostgreSQL quanto em SQL Server / T-SQL).
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")

# Numero: inteiro ou decimal, sinal opcional, isolado por fronteira de palavra — evita casar
# dentro de um identificador como `coluna_123` (o \b antes do sinal/digito cobre isso).
_NUMERIC_LITERAL_RE = re.compile(r"(?<![\w.])[-+]?\d+(?:\.\d+)?\b")

MASK_STRING = "?"
MASK_NUMBER = "?"


def mask_sql_literals(sql: str) -> str:
    """Substitui literais de string e numero em ``sql`` por um placeholder ``?``.

    Args:
        sql: texto de SQL capturado do coletor (ex.: `pg_stat_activity.query`,
            `sys.dm_exec_sql_text`), ainda nao mascarado.

    Returns:
        O mesmo texto, com todo literal de string entre aspas simples e todo literal numerico
        substituido por ``?``. Nomes de coluna, tabela, palavras-chave e estrutura da query
        permanecem intactos.

    Example:
        >>> mask_sql_literals("SELECT * FROM t WHERE id = 42 AND name = 'joão''s'")
        "SELECT * FROM t WHERE id = ? AND name = ?"
    """
    masked = _STRING_LITERAL_RE.sub(MASK_STRING, sql)
    masked = _NUMERIC_LITERAL_RE.sub(MASK_NUMBER, masked)
    return masked


# Etapa 11 (plano de execucao sob demanda) — o `ShowPlanXML` devolvido por
# `sys.dm_exec_query_plan` pode conter valores literais reais nos atributos `ParameterCompiledValue`
# e `ParameterRuntimeValue` de `<ParameterList>` (o valor com que o plano foi compilado/executado).
# Mascaramento por regex sobre esses dois atributos especificos — nao um parser XML completo (mesma
# filosofia de `mask_sql_literals`) — e obrigatorio antes de devolver o XML pra fora do backend.
_PARAMETER_VALUE_ATTR_RE = re.compile(
    r'(ParameterCompiledValue|ParameterRuntimeValue)="(?:[^"]|"")*"'
)


def mask_showplan_xml_literals(plan_xml: str) -> str:
    """Mascara valores de parametro embutidos no XML de um plano de execucao (`ShowPlanXML`).

    Args:
        plan_xml: XML bruto devolvido por `sys.dm_exec_query_plan(plan_handle)`.

    Returns:
        O mesmo XML, com todo atributo `ParameterCompiledValue`/`ParameterRuntimeValue`
        substituido por ``"?"`` — o resto da estrutura do plano (operadores, custos, indices)
        permanece intacto.

    Example:
        >>> mask_showplan_xml_literals('<ColumnReference ParameterCompiledValue="(42)" />')
        '<ColumnReference ParameterCompiledValue="?" />'
    """

    def _mask(match: re.Match[str]) -> str:
        return f'{match.group(1)}="?"'

    return _PARAMETER_VALUE_ATTR_RE.sub(_mask, plan_xml)
