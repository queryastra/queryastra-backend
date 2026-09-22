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

from app.masking.pii_classification import is_pii_column

# String literal: aspas simples, permitindo '' como escape de aspas simples dentro do literal
# (padrão SQL ANSI, valido tanto em PostgreSQL quanto em SQL Server / T-SQL). Grupo de captura
# pra poder mascarar só o CONTEUDO (preservando `%` — ver `_mask_string_content`), mantendo as
# aspas externas na saida (item explicito do usuário: string mascarada mostra `'?'`, não um `?`
# bruto igual a um número mascarado — o formato já deixa claro que era uma string).
_STRING_LITERAL_RE = re.compile(r"'((?:[^']|'')*)'")

# Numero: inteiro ou decimal, sinal opcional, isolado por fronteira de palavra — evita casar
# dentro de um identificador como `coluna_123` (o \b antes do sinal/digito cobre isso). `$` no
# lookbehind evita casar o `1`/`2` de um parametro posicional `$1`/`$2` do protocolo estendido do
# PostgreSQL (não é um literal, é uma REFERÊNCIA a parâmetro — não deveria virar `$?`). `@` cobre
# o mesmo caso no T-SQL: o cabeçalho de auto-parametrização do SQL Server nomeia os parâmetros
# como `@0`/`@1`/`@2` (sem prefixo de letra) quando o motor parametriza automaticamente uma query
# ad-hoc — sem essa exclusão, `@0`/`@1` virava `@?`/`@?` (bug real relatado pelo usuário ao vivo:
# `(@0 int,@1 tinyint)UPDATE ...` saía como `(@? int,@? tinyint)UPDATE ...`, apagando a distinção
# entre os parâmetros e parecendo um erro de mascaramento).
_NUMERIC_LITERAL_RE = re.compile(r"(?<![\w.$@])[-+]?\d+(?:\.\d+)?\b")

MASK_STRING = "?"
MASK_NUMBER = "?"


def _strip_nul_bytes(sql: str) -> str:
    """Remove bytes NUL (`\\x00`) do texto antes de qualquer outro processamento.

    Bug real confirmado ao vivo (2026-09-22, instancia Autklinux20, driver jTDS): o SQL Server
    as vezes devolve texto de query com um `\\x00` sobrando no final (ex.:
    `"FETCH API_CURSOR0000000000000496\\x00"`) - artefato de buffer de string do driver/motor,
    nao um literal de verdade. O PostgreSQL rejeita categoricamente `\\x00` em qualquer coluna de
    texto (`CharacterNotInRepertoireError`, nao e uma questao real de encoding UTF-8), entao um
    NUL sobrevivendo ate a persistencia derruba o `insert_batch` inteiro daquele ciclo de
    polling. Removido aqui (nao substituido por outro caractere) porque nunca carrega informacao
    de negocio - e sempre um artefato de transporte.
    """
    return sql.replace("\x00", "")


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

    Decisão do usuário revertendo o parágrafo acima (mascaramento seletivo, "mascare somente
    dados sensíveis ou pessoais dos clientes"): ver `mask_sql_literals_for_display`, abaixo.
    Esta função (`mask_sql_literals`) permanece INALTERADA e continua sendo a única usada para
    calcular `sql_hash` (a identidade estável usada para agrupar execuções da "mesma query" com
    valores de literal diferentes — Top SQL, agregação de wait time). Se o mascaramento usado
    pra `sql_hash` passasse a mostrar alguns valores, duas execuções da mesma query com valores
    não-PII diferentes gerariam hashes diferentes, fragmentando esse agrupamento em todo o
    produto — por isso a separação em duas funções, não uma flag na mesma função.
    """
    sql = _strip_nul_bytes(sql)
    masked = _STRING_LITERAL_RE.sub(_mask_string_literal, sql)
    masked = _NUMERIC_LITERAL_RE.sub(MASK_NUMBER, masked)
    return masked


# Literal de data/hora ISO-like (`YYYY-MM-DD`, opcionalmente com hora e fuso) — pedido explícito
# do usuário: "os parâmetros de data devem ser gravados e exibidos, pois não são dados pessoais
# ou sensíveis". Checado incondicionalmente (antes de qualquer classificação por coluna) porque o
# pedido do usuário não distingue coluna (ex.: uma data de nascimento continua sendo mostrada) —
# se essa nuance for necessária depois, precisa vir de uma decisão explícita nova do usuário.
_DATE_LITERAL_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)

# Literal isolado de hora/duração (`HH:MM:SS[.fff]`, ex. `WAITFOR DELAY '00:00:10'`) — mesma
# lógica da data acima, generalizada: um intervalo de tempo não é dado pessoal/sensível, mostrado
# incondicionalmente independente de coluna. Bug real relatado pelo usuário ao vivo: o parâmetro
# de `WAITFOR DELAY` (sem nenhuma coluna associada — não é `coluna = literal`) caía no padrão
# seguro (mascarar) por não ter coluna identificável, quando devia ser mostrado como a data.
_TIME_LITERAL_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}(\.\d+)?$")

# Nome de coluna imediatamente antes de um operador de comparação, sem espaço entre o operador e
# o literal que vem a seguir (ex.: `cpf = `, `c.nome LIKE `, `[dbo].[Clientes].[Email]<>`) — cobre
# o caso `WHERE coluna op literal` e `SET coluna = literal`, de longe o mais comum e o mais seguro
# de reconhecer sem um parser SQL real. Não cobre listas `IN (...)` — deliberadamente fora do
# escopo desta primeira versão (cai no padrão seguro: mascarar). Posições de
# `INSERT INTO tabela (col1, col2, ...) VALUES (...)` são cobertas separadamente, abaixo
# (`_find_insert_value_column_ranges`) — só quando a lista de colunas é EXPLÍCITA; sem ela (ex.:
# `INSERT INTO tabela VALUES (...)`, sem lista) não há como saber a ordem sem o schema real da
# tabela, então continua caindo no padrão seguro.
_COLUMN_BEFORE_LITERAL_RE = re.compile(
    r'([A-Za-z_][\w.\[\]"]*)\s*(?:=|<>|!=|<=|>=|<|>|LIKE)\s*$',
    re.IGNORECASE,
)

# Janela de texto olhada pra trás de cada literal ao procurar `coluna <op>` — suficiente pra um
# nome de coluna qualificado por schema/tabela/alias, sem custo de regex sobre a query inteira.
_COLUMN_LOOKBEHIND_WINDOW = 100


def _find_preceding_column(text: str, literal_start: int) -> str | None:
    window_start = max(0, literal_start - _COLUMN_LOOKBEHIND_WINDOW)
    match = _COLUMN_BEFORE_LITERAL_RE.search(text[window_start:literal_start])
    return match.group(1) if match else None


# `INSERT INTO tabela (col1, col2, ...) VALUES (v1, v2), (v3, v4), ...` — pedido explícito do
# usuário ("no comando INSERT INTO, você está mascarando tudo. Tem como mascarar somente os
# campos sensíveis?"). Só reconhece a lista de colunas quando ela é EXPLÍCITA logo depois do nome
# da tabela — é a única forma de saber a ordem das colunas a partir só do texto do SQL, sem
# acesso ao schema real (que este módulo nunca tem).
_INSERT_INTO_COLUMNS_RE = re.compile(
    r"INSERT\s+INTO\s+[\w.\[\]\"]+\s*\(([^()]*)\)\s*VALUES",
    re.IGNORECASE,
)


def _parse_column_list(raw: str) -> list[str]:
    return [part.strip().strip('[]"') for part in raw.split(",") if part.strip()]


def _find_insert_value_column_ranges(sql: str) -> list[tuple[int, int, str]]:
    """Varre cada `INSERT INTO tabela (col1, col2, ...) VALUES (...), (...), ...` encontrado e
    devolve um range de posições `(inicio, fim, nome_da_coluna)` por VALOR dentro de cada tupla —
    usado como classificação PII PRIORITÁRIA sobre `_find_preceding_column` (que não bate com
    nada dentro de `VALUES`, já que não há `coluna =` ali). Ignora vírgulas/parênteses dentro de
    um literal de string (ex.: `'Rua A, 123'`) ao contar posições."""
    ranges: list[tuple[int, int, str]] = []
    for insert_match in _INSERT_INTO_COLUMNS_RE.finditer(sql):
        columns = _parse_column_list(insert_match.group(1))
        if not columns:
            continue
        pos = insert_match.end()
        while pos < len(sql):
            while pos < len(sql) and sql[pos] in " \t\r\n,":
                pos += 1
            if pos >= len(sql) or sql[pos] != "(":
                break
            pos += 1
            depth = 1
            col_index = 0
            item_start = pos
            while pos < len(sql) and depth > 0:
                ch = sql[pos]
                if ch == "'":
                    pos += 1
                    while pos < len(sql):
                        if sql[pos : pos + 2] == "''":
                            pos += 2
                            continue
                        if sql[pos] == "'":
                            pos += 1
                            break
                        pos += 1
                    continue
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        if col_index < len(columns):
                            ranges.append((item_start, pos, columns[col_index]))
                        pos += 1
                        break
                elif ch == "," and depth == 1:
                    if col_index < len(columns):
                        ranges.append((item_start, pos, columns[col_index]))
                    col_index += 1
                    item_start = pos + 1
                pos += 1
    return ranges


def _column_for_insert_value(ranges: list[tuple[int, int, str]], pos: int) -> str | None:
    for start, end, column in ranges:
        if start <= pos < end:
            return column
    return None


def _resolve_column_for_literal(
    text: str, pos: int, insert_ranges: list[tuple[int, int, str]]
) -> str | None:
    return _column_for_insert_value(insert_ranges, pos) or _find_preceding_column(text, pos)


def _mask_string_literal_for_display(
    match: re.Match[str], insert_ranges: list[tuple[int, int, str]]
) -> str:
    content = match.group(1)
    if _DATE_LITERAL_RE.match(content) or _TIME_LITERAL_RE.match(content):
        return match.group(0)
    column = _resolve_column_for_literal(match.string, match.start(), insert_ranges)
    if column is not None and not is_pii_column(column):
        return match.group(0)
    return _mask_string_literal(match)


def _mask_numeric_literal_for_display(
    match: re.Match[str], insert_ranges: list[tuple[int, int, str]]
) -> str:
    column = _resolve_column_for_literal(match.string, match.start(), insert_ranges)
    if column is not None and not is_pii_column(column):
        return match.group(0)
    return MASK_NUMBER


def mask_sql_literals_for_display(sql: str) -> str:
    """Mascaramento SELETIVO pra texto exibido ao usuário (`sql_text_masked`/
    `sql_text_full_masked`) — pedido explícito do usuário: "mascare somente dados sensíveis ou
    pessoais dos clientes. Os outros parâmetros informados devem ser exibidos nas queries."

    Diferente de `mask_sql_literals` (que mascara TUDO, sem exceção, e continua sendo a única
    função usada para `sql_hash` — ver nota na docstring dela), esta função decide, literal por
    literal, se ele deve ser mostrado ou mascarado:

    1. Literal de string com conteúdo em formato de data (`_DATE_LITERAL_RE`) ou de hora/duração
       isolada (`_TIME_LITERAL_RE`, ex. `WAITFOR DELAY '00:00:10'`) → sempre MOSTRADO,
       incondicionalmente (data/hora não é dado pessoal/sensível).
    2. Qualquer outro literal (string ou número) associado a um nome de coluna — seja pelo padrão
       `nome_de_coluna <operador> literal` (`_find_preceding_column`) ou pela posição dentro de um
       `INSERT INTO tabela (col1, col2, ...) VALUES (...)` com lista de colunas EXPLÍCITA
       (`_find_insert_value_column_ranges`) → mostrado se a coluna NÃO bate com
       `pii_classification.is_pii_column`, mascarado se bater.
    3. Literal sem nenhuma coluna identificável (ex.: dentro de `IN (...)`, ou em
       `INSERT INTO tabela VALUES (...)` SEM lista de colunas — a ordem só existe no schema real
       da tabela, que este módulo nunca tem acesso) → MASCARADO por padrão seguro — nunca mostra
       um valor cuja coluna não foi possível determinar.

    Args:
        sql: texto de SQL capturado do coletor, ainda não mascarado.

    Returns:
        O mesmo texto, com cada literal individualmente mostrado ou mascarado conforme as
        regras acima.

    Example:
        >>> mask_sql_literals_for_display("SELECT * FROM t WHERE status = 'ATIVO' AND cpf = '123'")
        "SELECT * FROM t WHERE status = 'ATIVO' AND cpf = '?'"
        >>> mask_sql_literals_for_display("... WHERE data_criacao = '2024-01-15'")
        "... WHERE data_criacao = '2024-01-15'"
        >>> mask_sql_literals_for_display("SELECT * FROM t WHERE id IN (1, 2, 3)")
        "SELECT * FROM t WHERE id IN (?, ?, ?)"
        >>> mask_sql_literals_for_display("INSERT INTO t (id, cpf) VALUES (1, '123')")
        "INSERT INTO t (id, cpf) VALUES (1, '?')"

    Nota de implementação: ao contrário de `mask_sql_literals` (que pode aplicar as duas regras em
    sequência, string primeiro e depois número, porque toda string já virou `'?'` na primeira
    passada), esta função localiza os dois tipos de literal de uma vez sobre o texto ORIGINAL e
    resolve cada um independentemente — necessário porque mostrar uma string sem mascarar (ex.:
    uma data) deixa os dígitos originais no texto, e uma segunda passada do regex numérico sobre
    esse resultado voltaria a "encontrar" e mascarar esses dígitos como se fossem números soltos
    (bug real encontrado ao escrever os testes desta função).
    """
    sql = _strip_nul_bytes(sql)
    insert_ranges = _find_insert_value_column_ranges(sql)
    string_matches = list(_STRING_LITERAL_RE.finditer(sql))
    string_spans = [(m.start(), m.end()) for m in string_matches]

    def _inside_a_string_literal(pos: int) -> bool:
        return any(start <= pos < end for start, end in string_spans)

    numeric_matches = [
        match
        for match in _NUMERIC_LITERAL_RE.finditer(sql)
        if not _inside_a_string_literal(match.start())
    ]

    replacements = sorted(
        [
            (m.start(), m.end(), _mask_string_literal_for_display(m, insert_ranges))
            for m in string_matches
        ]
        + [
            (m.start(), m.end(), _mask_numeric_literal_for_display(m, insert_ranges))
            for m in numeric_matches
        ],
        key=lambda replacement: replacement[0],
    )

    pieces = []
    cursor = 0
    for start, end, replacement in replacements:
        pieces.append(sql[cursor:start])
        pieces.append(replacement)
        cursor = end
    pieces.append(sql[cursor:])
    return "".join(pieces)


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

    return _PARAMETER_VALUE_ATTR_RE.sub(_mask, _strip_nul_bytes(plan_xml))
