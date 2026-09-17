"""Mascaramento de literais em texto SQL — `Docs/rules/03-sql-privacy.md`.

"Mascarar literais antes de persistir ou exibir texto SQL." Este e o unico modulo de coleta com
logica real e completa neste scaffold (o resto e stub) — mascaramento e obrigatorio no MVP, nao
configuravel (ARCHITECTURE_SPEC.md, decisao Q7) — nenhum literal de string/numero passa pra
`sql_text_masked` sem passar por aqui, mesmo que "pareça" um valor inofensivo (ver docstring de
`mask_sql_literals` sobre por que não existe distinção confiável entre "valor fixo no código" e
"valor vindo de variável" a partir só do texto do SQL).

Cobre os dois tipos de literal mais comuns entre PostgreSQL e SQL Server:
    - string entre aspas simples, incluindo `''` (aspas simples escapada dentro do literal);
    - numero (inteiro ou decimal, com sinal opcional).

Não e um parser SQL completo (nao lida com dollar-quoting do Postgres, comentarios, ou
identificadores entre aspas duplas contendo caracteres especiais) — e uma normalizacao razoavel
o suficiente para o texto exibido/persistido em `query_samples.sql_text_masked`, não uma
ferramenta de reescrita de SQL.
"""

from __future__ import annotations

import itertools
import re

# String literal: aspas simples, permitindo '' como escape de aspas simples dentro do literal
# (padrão SQL ANSI, valido tanto em PostgreSQL quanto em SQL Server / T-SQL). Grupo de captura
# pra poder mascarar só o CONTEUDO (preservando `%` — ver `_mask_string_content`), mantendo as
# aspas externas na saida (item explicito do usuário: string mascarada mostra `'?'`, não um `?`
# bruto igual a um número mascarado — o formato já deixa claro que era uma string).
_STRING_LITERAL_RE = re.compile(r"'((?:[^']|'')*)'")

# Numero: inteiro ou decimal, sinal opcional, isolado por fronteira de palavra — evita casar
# dentro de um identificador como `coluna_123` (o \b antes do sinal/digito cobre isso). `$` no
# lookbehind evita casar o `1`/`2` de um parametro posicional `$1`/`$2` do protocolo estendido do
# PostgreSQL (não é um literal, é uma REFERÊNCIA a parâmetro — não deveria virar `$?`).
_NUMERIC_LITERAL_RE = re.compile(r"(?<![\w.$])[-+]?\d+(?:\.\d+)?\b")

MASK_STRING = "?"
MASK_NUMBER = "?"


def _mask_string_content(content: str) -> str:
    """Mascara o CONTEUDO de um literal de string (sem as aspas externas), preservando `%` na
    posição original — a pedido explícito do usuário: um padrão `LIKE` como `'%valor%'`/
    `'%valor'`/`'valor%'` continua mostrando onde estava o curinga (`'%?%'`/`'%?'`/`'?%'`), só o
    valor de fato entre eles vira `?`. Qualquer sequência de caracteres SEM `%` vira um único
    `?` (comportamento anterior, ainda o caso comum). Um literal que é só `%` (ex.: o padrão
    `'%' + @valor + '%'`, muito comum pra montar `LIKE` com concatenação) não tem nada pra
    mascarar — fica `%` mesmo, sem `?` (não é um valor, é só o marcador de curinga). Content
    vazio (literal `''`) permanece vazio.
    """
    pieces = []
    for is_percent, group in itertools.groupby(content, key=lambda c: c == "%"):
        run = "".join(group)
        pieces.append(run if is_percent else MASK_STRING)
    return "".join(pieces)


def _mask_string_literal(match: re.Match[str]) -> str:
    return f"'{_mask_string_content(match.group(1))}'"


def mask_sql_literals(sql: str) -> str:
    """Substitui literais de string e numero em ``sql`` por um placeholder.

    Args:
        sql: texto de SQL capturado do coletor (ex.: `pg_stat_activity.query`,
            `sys.dm_exec_sql_text`), ainda nao mascarado.

    Returns:
        O mesmo texto, com todo literal numerico substituido por ``?`` (sem aspas — mesmo
        formato de antes) e todo literal de string substituido por ``'?'`` (com as aspas simples
        originais preservadas, e qualquer `%` mantido na posição original dentro delas — ver
        `_mask_string_content`). Nomes de coluna, tabela, palavras-chave, referências a variável/
        parâmetro (`@nome`, `$1`, `:nome`) e a estrutura da query permanecem intactos.

    Example:
        >>> mask_sql_literals("SELECT * FROM t WHERE id = 42 AND name = 'joão''s'")
        "SELECT * FROM t WHERE id = ? AND name = '?'"
        >>> mask_sql_literals("SELECT * FROM t WHERE nome LIKE '%silva%'")
        "SELECT * FROM t WHERE nome LIKE '%?%'"

    Nota sobre "valor fixo no código" vs. "valor vindo de variável" (pedido explícito do
    usuário, não implementado): não existe marcador sintático confiável, no texto de SQL já
    capturado, que distinga um literal escrito à mão pelo desenvolvedor (ex.: uma constante de
    negócio como `'ATIVO'` dentro do corpo de uma stored procedure) de um literal que chegou por
    concatenação de uma variável em SQL dinâmico (ex.: um valor de cliente) — os dois têm
    exatamente a mesma forma (aspas + texto, ou um número) no momento em que `sys.dm_exec_sql_text`/
    `pg_stat_activity.query` os expõe; a informação de "de onde veio" já não existe mais nesse
    ponto. O que É possível, e já acontece hoje (nenhuma mudança necessária): uma referência a
    variável/parâmetro de verdade (`@nome` no T-SQL, `$1`/`$2` no protocolo estendido do
    PostgreSQL, `:nome` em PL/pgSQL) nunca é mascarada, porque nunca é um literal — não bate com
    nenhuma das duas regras acima, então passa direto. Mascarar TODO literal de string/número
    (independente de "parecer" uma constante inofensiva) é a postura deliberada e obrigatória do
    projeto (Q7, `ARCHITECTURE_SPEC.md`) — relaxar isso por uma heurística de "parece hardcoded"
    arriscaria deixar passar dado sensível real (ex.: uma constante de negócio E um CPF real têm
    a mesma forma sintática), então não foi implementado sem uma decisão explícita do usuário
    sobre esse trade-off de privacidade.
    """
    masked = _STRING_LITERAL_RE.sub(_mask_string_literal, sql)
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
