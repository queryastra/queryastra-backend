"""Análise estática de SQL — regras de performance e boas práticas.

Motor local (sem chamadas externas, sem IA), derivado das regras do ab-sql-rules-checker com
adição de severidade e sugestões de reescrita. Opera sobre sql_text_masked (valores substituídos
por '?'), suficiente para detectar estrutura.

Regras universais (todos os bancos) têm prefixo R; SQL Server-específicas têm prefixo S.
Ordenação do resultado: critical → warning → info.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

Severity = Literal["critical", "warning", "info"]


@dataclass(frozen=True, slots=True)
class AdvisorFinding:
    rule_id: str
    severity: Severity
    description: str
    suggestion: str


def analyze_sql(sql_text: str, db_type: str = "sqlserver") -> list[AdvisorFinding]:
    """Retorna lista de achados ordenados por severidade (critical primeiro)."""
    if not sql_text or not sql_text.strip():
        return []

    findings: list[AdvisorFinding] = []
    F = AdvisorFinding

    # ── Helpers internos ──────────────────────────────────────────────────────────────

    def find(pattern: str, flags: int = re.IGNORECASE) -> bool:
        return bool(re.search(pattern, sql_text, flags))

    def find_ds(pattern: str) -> bool:
        return bool(re.search(pattern, sql_text, re.IGNORECASE | re.DOTALL))

    # ── Regras universais (todos os bancos) ───────────────────────────────────────────

    # R001 — SELECT *
    if find(r"\bSELECT\b\s*(?:DISTINCT\s+)?\*"):
        findings.append(F(
            rule_id="R001",
            severity="warning",
            description="SELECT * traz todas as colunas, incluindo colunas desnecessárias.",
            suggestion=(
                "Substitua por lista explícita de colunas. Reduz I/O de rede e disco, "
                "e evita key lookups causados por colunas fora do índice."
            ),
        ))

    # R002 — Subquery no WHERE com IN (SELECT ...)
    if find(r"\bIN\s*\(\s*SELECT\b"):
        findings.append(F(
            rule_id="R002",
            severity="warning",
            description="Subquery correlacionada no WHERE com IN (SELECT …).",
            suggestion=(
                "Reescreva como INNER JOIN ou EXISTS. O otimizador pode executar "
                "a subquery repetidamente para cada linha da tabela externa."
            ),
        ))

    # R003 — LIKE com wildcard à esquerda
    if find(r"\bLIKE\s*'%"):
        findings.append(F(
            rule_id="R003",
            severity="warning",
            description="LIKE com wildcard à esquerda impede uso de índice (Full Scan inevitável).",
            suggestion=(
                "Wildcard à esquerda (LIKE '%texto') força varredura completa. "
                "Considere Full-Text Search ou reformule para usar prefixo (LIKE 'texto%')."
            ),
        ))

    # R004 — Função aplicada em coluna do WHERE (non-sargable)
    fn_pat = (
        r"\bWHERE\b.*?"
        r"\b(CONVERT|CAST|LEFT|RIGHT|SUBSTRING|RTRIM|LTRIM|ISNULL|COALESCE|"
        r"DATEPART|DATEADD|YEAR|MONTH|DAY|LOWER|UPPER|TRIM|NVL|TO_DATE|TO_CHAR)\s*\("
    )
    if find_ds(fn_pat):
        findings.append(F(
            rule_id="R004",
            severity="critical",
            description="Função aplicada sobre coluna no WHERE impede uso de índice (non-sargable).",
            suggestion=(
                "Mova a função para o lado do literal, não da coluna. "
                "Ex.: em vez de YEAR(data) = 2024, use data >= '2024-01-01' AND data < '2025-01-01'. "
                "Ou crie um índice computado sobre a expressão."
            ),
        ))

    # R005 — OR no WHERE
    if find_ds(r"\bWHERE\b.*?\bOR\b"):
        findings.append(F(
            rule_id="R005",
            severity="warning",
            description="OR no WHERE pode impedir uso eficiente de índice.",
            suggestion=(
                "Considere reescrever como UNION ALL — cada ramo usa seu próprio índice. "
                "O otimizador raramente escolhe dois índices distintos com OR."
            ),
        ))

    # R006 — NOT IN
    if find(r"\bNOT\s+IN\s*\("):
        findings.append(F(
            rule_id="R006",
            severity="warning",
            description="NOT IN tem comportamento inesperado com NULLs e desempenho inferior a NOT EXISTS.",
            suggestion=(
                "Substitua por NOT EXISTS ou LEFT JOIN … WHERE coluna IS NULL. "
                "NOT IN retorna zero linhas se a subquery contiver qualquer NULL."
            ),
        ))

    # R007 — NOT EXISTS
    if find(r"\bNOT\s+EXISTS\s*\("):
        findings.append(F(
            rule_id="R007",
            severity="info",
            description="NOT EXISTS é mais seguro que NOT IN com NULLs, mas avalie se LEFT JOIN seria mais eficiente.",
            suggestion=(
                "LEFT JOIN … WHERE t2.chave IS NULL pode ser mais eficiente quando "
                "o conjunto de dados for grande e houver índice na chave de join."
            ),
        ))

    # R008 — DISTINCT
    if find(r"\bSELECT\s+DISTINCT\b"):
        findings.append(F(
            rule_id="R008",
            severity="info",
            description="DISTINCT geralmente indica duplicatas inesperadas — possível JOIN incorreto.",
            suggestion=(
                "Investigue a causa das duplicatas. Se o JOIN gera duplicatas, corrija-o. "
                "DISTINCT sobre resultado grande gera operação de Sort/Hash implícita."
            ),
        ))

    # R009 — <> ou !=
    if re.search(r"(<>|!=)", sql_text):
        findings.append(F(
            rule_id="R009",
            severity="info",
            description="<> / != geralmente força Full Scan — o otimizador não usa índice de range.",
            suggestion=(
                "Se possível, reformule com range positivo (< x OR > x) ou "
                "filtre por exclusão de outra forma para permitir seek no índice."
            ),
        ))

    # R010 — UNION sem ALL
    if find(r"\bUNION\b(?!\s+ALL\b)"):
        findings.append(F(
            rule_id="R010",
            severity="info",
            description="UNION sem ALL elimina duplicatas via Sort/Hash implícito.",
            suggestion=(
                "Use UNION ALL quando duplicatas entre os conjuntos forem impossíveis "
                "ou aceitáveis. Evita o passo de deduplicação."
            ),
        ))

    # R011 — Mais de 5 JOINs
    join_count = len(re.findall(r"\bJOIN\b", sql_text, re.IGNORECASE))
    if join_count > 5:
        findings.append(F(
            rule_id="R011",
            severity="info",
            description=f"Query com {join_count} JOINs — alta complexidade aumenta custo de otimização do plano.",
            suggestion=(
                "Considere decompor em CTEs ou tabelas temporárias. "
                "Queries com muitos JOINs tendem a ter planos instáveis com variação de cardinalidade."
            ),
        ))

    # R012 — CURSOR
    if find(r"\bCURSOR\b"):
        findings.append(F(
            rule_id="R012",
            severity="warning",
            description="CURSOR processa uma linha por vez — muito mais lento que operações set-based.",
            suggestion=(
                "Reescreva como UPDATE/INSERT/DELETE com JOIN ou CTE. "
                "CURSORs podem ser 10-100x mais lentos para volumes médios e altos."
            ),
        ))

    # R013 — Subquery correlacionada no SELECT (executada por linha)
    from_pos = sql_text.upper().find(" FROM ")
    select_part = sql_text[:from_pos] if from_pos > 0 else sql_text
    if re.search(r"\(\s*SELECT\b", select_part, re.IGNORECASE):
        findings.append(F(
            rule_id="R013",
            severity="info",
            description="Subquery na lista do SELECT é executada uma vez por linha do resultado.",
            suggestion=(
                "Substitua por LEFT JOIN ou OUTER APPLY. Subqueries escalares na lista "
                "SELECT escalam linearmente com o número de linhas retornadas."
            ),
        ))

    # ── Regras SQL Server-específicas ─────────────────────────────────────────────────

    if db_type == "sqlserver":

        # S001 — JOIN implícito estilo vírgula (FROM T1, T2 WHERE T1.x = T2.y)
        has_explicit_join = find(r"\bJOIN\b")
        # FROM seguido de identificador, vírgula, outro identificador, WHERE
        if not has_explicit_join and find_ds(r"\bFROM\s+\w[\w.]*\s*,\s*\w[\w.]*[^)]*\bWHERE\b"):
            findings.append(F(
                rule_id="S001",
                severity="warning",
                description="JOIN implícito (vírgula no FROM) — sintaxe ANSI-86 obsoleta.",
                suggestion=(
                    "Substitua por INNER JOIN explícito (ANSI-92). Além de mais legível, "
                    "evita produto cartesiano acidental quando a condição de JOIN for omitida."
                ),
            ))

        # S002 — Prefixo SP_ em procedure
        if find(r"\b(?:CREATE|ALTER)\s+PROC(?:EDURE)?\s+(?:\w+\.)?SP_"):
            findings.append(F(
                rule_id="S002",
                severity="info",
                description="Prefixo SP_ é reservado pelo SQL Server para procedures do sistema.",
                suggestion=(
                    "Use outro prefixo (ex.: USP_, APP_). O SQL Server pesquisa em master..sys "
                    "antes do banco atual quando o nome começa com SP_, adicionando overhead."
                ),
            ))

        # S003 — INSERT sem lista de colunas explícita
        if find(r"\bINSERT\s+INTO\s+[\w.\[\]]+\s+(?:SELECT|VALUES)\b"):
            findings.append(F(
                rule_id="S003",
                severity="warning",
                description="INSERT sem especificar colunas explicitamente.",
                suggestion=(
                    "Especifique sempre a lista: INSERT INTO tabela (col1, col2) VALUES … "
                    "Sem lista, qualquer alteração na ordem das colunas da tabela quebra silenciosamente."
                ),
            ))

        # S004 — ORDER BY posicional (ORDER BY 1, 2)
        if find(r"\bORDER\s+BY\s+\d+"):
            findings.append(F(
                rule_id="S004",
                severity="info",
                description="ORDER BY com número de posição de coluna é frágil.",
                suggestion=(
                    "Use ORDER BY com nome de coluna explícito. "
                    "Alterações no SELECT podem reordenar resultados silenciosamente."
                ),
            ))

        # S005 — DELETE sem WHERE
        delete_match = re.search(r"\bDELETE\b.*", sql_text, re.IGNORECASE | re.DOTALL)
        if delete_match and not find_ds(r"\bDELETE\b.*?\bWHERE\b"):
            findings.append(F(
                rule_id="S005",
                severity="critical",
                description="DELETE sem WHERE — apaga todas as linhas da tabela.",
                suggestion=(
                    "Adicione WHERE. Para limpar tabela inteira, use TRUNCATE TABLE "
                    "(mais rápido, gera menos log de transação)."
                ),
            ))

        # S006 — Hint de índice no FROM
        if find(r"\(INDEX\s*(?:=|\s+)"):
            findings.append(F(
                rule_id="S006",
                severity="info",
                description="INDEX hint hardcoded ignora atualizações de estatísticas.",
                suggestion=(
                    "Hints fixos podem ser piores que a escolha do otimizador quando "
                    "os dados mudam. Prefira atualizar estatísticas e deixar o otimizador decidir."
                ),
            ))

        # S007 — EXEC() com string (risco de SQL Injection + sem reuso de plano)
        if find(r"\bEXEC(?:UTE)?\s*\("):
            findings.append(F(
                rule_id="S007",
                severity="warning",
                description="EXEC() com string não reutiliza plano de execução e abre risco de SQL Injection.",
                suggestion=(
                    "Use sp_executeSQL com parâmetros tipados: "
                    "EXEC sp_executeSQL N'SELECT … WHERE id = @id', N'@id INT', @id = valor"
                ),
            ))

        # S008 — GOTO
        if find(r"\bGOTO\b"):
            findings.append(F(
                rule_id="S008",
                severity="info",
                description="GOTO torna o fluxo difícil de seguir e manter.",
                suggestion="Substitua por IF/WHILE/TRY-CATCH. GOTO é considerado má prática em T-SQL moderno.",
            ))

        # S009 — DROP PROCEDURE (perde permissões)
        if find(r"\bDROP\s+PROC(?:EDURE)?\b"):
            findings.append(F(
                rule_id="S009",
                severity="info",
                description="DROP PROCEDURE remove todas as permissões concedidas.",
                suggestion="Use ALTER PROCEDURE — preserva permissões existentes sem precisar recriá-las.",
            ))

        # S010 — XP_CMDSHELL / OPENROWSET
        if find(r"\b(?:XP_CMDSHELL|OPENROWSET)\b"):
            findings.append(F(
                rule_id="S010",
                severity="warning",
                description="XP_CMDSHELL / OPENROWSET são operações de alto privilégio.",
                suggestion=(
                    "Verifique se o uso é intencional. XP_CMDSHELL executa comandos do SO "
                    "com as permissões do serviço SQL Server."
                ),
            ))

        # S011 — ROW_NUMBER para paginação com OFFSET alto
        if find(r"\bROW_NUMBER\s*\("):
            findings.append(F(
                rule_id="S011",
                severity="info",
                description="ROW_NUMBER para paginação degrada em páginas distantes (OFFSET alto).",
                suggestion=(
                    "Para tabelas grandes, prefira keyset pagination: "
                    "WHERE id > @ultimo_id ORDER BY id FETCH NEXT @n ROWS ONLY"
                ),
            ))

    # Ordena: critical → warning → info
    _order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: _order[f.severity])
    return findings
