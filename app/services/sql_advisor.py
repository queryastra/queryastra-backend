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
    params: dict | None = None


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
            description="SELECT * fetches all columns, including unnecessary ones.",
            suggestion=(
                "Replace with an explicit column list. Reduces network and disk I/O, "
                "and avoids key lookups caused by columns outside the index."
            ),
        ))

    # R002 — Subquery no WHERE com IN (SELECT ...)
    if find(r"\bIN\s*\(\s*SELECT\b"):
        findings.append(F(
            rule_id="R002",
            severity="warning",
            description="Correlated subquery in WHERE with IN (SELECT …).",
            suggestion=(
                "Rewrite as INNER JOIN or EXISTS. The optimizer may execute "
                "the subquery repeatedly for each row of the outer table."
            ),
        ))

    # R003 — LIKE com wildcard à esquerda
    if find(r"\bLIKE\s*'%"):
        findings.append(F(
            rule_id="R003",
            severity="warning",
            description="LIKE with leading wildcard prevents index use (Full Scan inevitable).",
            suggestion=(
                "A leading wildcard (LIKE '%text') forces a full table scan. "
                "Consider Full-Text Search, or rewrite to use a prefix (LIKE 'text%')."
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
            description="Function applied to a column in WHERE prevents index use (non-sargable).",
            suggestion=(
                "Move the function to the literal side, not the column. "
                "E.g., instead of YEAR(date) = 2024, use date >= '2024-01-01' AND date < '2025-01-01'. "
                "Or create a computed index on the expression."
            ),
        ))

    # R005 — OR no WHERE
    if find_ds(r"\bWHERE\b.*?\bOR\b"):
        findings.append(F(
            rule_id="R005",
            severity="warning",
            description="OR in WHERE may prevent efficient index use.",
            suggestion=(
                "Consider rewriting as UNION ALL — each branch can use its own index. "
                "The optimizer rarely chooses two distinct indexes with OR."
            ),
        ))

    # R006 — NOT IN
    if find(r"\bNOT\s+IN\s*\("):
        findings.append(F(
            rule_id="R006",
            severity="warning",
            description="NOT IN has unexpected behavior with NULLs and is slower than NOT EXISTS.",
            suggestion=(
                "Replace with NOT EXISTS or LEFT JOIN … WHERE column IS NULL. "
                "NOT IN returns zero rows if the subquery contains any NULL."
            ),
        ))

    # R007 — NOT EXISTS
    if find(r"\bNOT\s+EXISTS\s*\("):
        findings.append(F(
            rule_id="R007",
            severity="info",
            description="NOT EXISTS is safer than NOT IN with NULLs, but evaluate whether LEFT JOIN would be more efficient.",
            suggestion=(
                "LEFT JOIN … WHERE t2.key IS NULL may be more efficient when "
                "the dataset is large and there is an index on the join key."
            ),
        ))

    # R008 — DISTINCT
    if find(r"\bSELECT\s+DISTINCT\b"):
        findings.append(F(
            rule_id="R008",
            severity="info",
            description="DISTINCT usually indicates unexpected duplicates — possibly an incorrect JOIN.",
            suggestion=(
                "Investigate the source of duplicates. If the JOIN produces duplicates, fix it. "
                "DISTINCT over a large result triggers an implicit Sort/Hash operation."
            ),
        ))

    # R009 — <> ou !=
    if re.search(r"(<>|!=)", sql_text):
        findings.append(F(
            rule_id="R009",
            severity="info",
            description="<> / != generally forces a Full Scan — the optimizer does not use a range index.",
            suggestion=(
                "If possible, rewrite using a positive range (< x OR > x) or "
                "filter by exclusion in another way to allow an index seek."
            ),
        ))

    # R010 — UNION sem ALL
    if find(r"\bUNION\b(?!\s+ALL\b)"):
        findings.append(F(
            rule_id="R010",
            severity="info",
            description="UNION without ALL eliminates duplicates via an implicit Sort/Hash.",
            suggestion=(
                "Use UNION ALL when duplicates between the sets are impossible "
                "or acceptable. Avoids the deduplication step."
            ),
        ))

    # R011 — Mais de 5 JOINs
    join_count = len(re.findall(r"\bJOIN\b", sql_text, re.IGNORECASE))
    if join_count > 5:
        findings.append(F(
            rule_id="R011",
            severity="info",
            description=f"Query with {join_count} JOINs — high complexity increases plan optimization cost.",
            suggestion=(
                "Consider decomposing into CTEs or temp tables. "
                "Queries with many JOINs tend to have unstable plans with cardinality variance."
            ),
            params={"joinCount": join_count},
        ))

    # R012 — CURSOR
    if find(r"\bCURSOR\b"):
        findings.append(F(
            rule_id="R012",
            severity="warning",
            description="CURSOR processes one row at a time — much slower than set-based operations.",
            suggestion=(
                "Rewrite as UPDATE/INSERT/DELETE with JOIN or CTE. "
                "CURSORs can be 10-100x slower for medium and large volumes."
            ),
        ))

    # R013 — Subquery correlacionada no SELECT (executada por linha)
    from_pos = sql_text.upper().find(" FROM ")
    select_part = sql_text[:from_pos] if from_pos > 0 else sql_text
    if re.search(r"\(\s*SELECT\b", select_part, re.IGNORECASE):
        findings.append(F(
            rule_id="R013",
            severity="info",
            description="Subquery in the SELECT list is executed once per result row.",
            suggestion=(
                "Replace with LEFT JOIN or OUTER APPLY. Scalar subqueries in the SELECT list "
                "scale linearly with the number of returned rows."
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
                description="Implicit JOIN (comma in FROM) — deprecated ANSI-86 syntax.",
                suggestion=(
                    "Replace with explicit INNER JOIN (ANSI-92). More readable and avoids "
                    "accidental cross joins when the JOIN condition is omitted."
                ),
            ))

        # S002 — Prefixo SP_ em procedure
        if find(r"\b(?:CREATE|ALTER)\s+PROC(?:EDURE)?\s+(?:\w+\.)?SP_"):
            findings.append(F(
                rule_id="S002",
                severity="info",
                description="SP_ prefix is reserved by SQL Server for system procedures.",
                suggestion=(
                    "Use another prefix (e.g., USP_, APP_). SQL Server searches in master..sys "
                    "before the current database when the name starts with SP_, adding overhead."
                ),
            ))

        # S003 — INSERT sem lista de colunas explícita
        if find(r"\bINSERT\s+INTO\s+[\w.\[\]]+\s+(?:SELECT|VALUES)\b"):
            findings.append(F(
                rule_id="S003",
                severity="warning",
                description="INSERT without an explicit column list.",
                suggestion=(
                    "Always specify the list: INSERT INTO table (col1, col2) VALUES … "
                    "Without a list, any change in column order silently breaks the insert."
                ),
            ))

        # S004 — ORDER BY posicional (ORDER BY 1, 2)
        if find(r"\bORDER\s+BY\s+\d+"):
            findings.append(F(
                rule_id="S004",
                severity="info",
                description="ORDER BY with positional column number is fragile.",
                suggestion=(
                    "Use ORDER BY with explicit column names. "
                    "Changes to the SELECT list may silently reorder results."
                ),
            ))

        # S005 — DELETE sem WHERE
        delete_match = re.search(r"\bDELETE\b.*", sql_text, re.IGNORECASE | re.DOTALL)
        if delete_match and not find_ds(r"\bDELETE\b.*?\bWHERE\b"):
            findings.append(F(
                rule_id="S005",
                severity="critical",
                description="DELETE without WHERE — deletes all rows in the table.",
                suggestion=(
                    "Add a WHERE clause. To clear an entire table, use TRUNCATE TABLE "
                    "(faster, generates less transaction log)."
                ),
            ))

        # S006 — Hint de índice no FROM
        if find(r"\(INDEX\s*(?:=|\s+)"):
            findings.append(F(
                rule_id="S006",
                severity="info",
                description="Hardcoded INDEX hint ignores statistics updates.",
                suggestion=(
                    "Fixed hints may be worse than the optimizer's choice when data changes. "
                    "Prefer updating statistics and letting the optimizer decide."
                ),
            ))

        # S007 — EXEC() com string (risco de SQL Injection + sem reuso de plano)
        if find(r"\bEXEC(?:UTE)?\s*\("):
            findings.append(F(
                rule_id="S007",
                severity="warning",
                description="EXEC() with a string does not reuse the execution plan and opens SQL Injection risk.",
                suggestion=(
                    "Use sp_executeSQL with typed parameters: "
                    "EXEC sp_executeSQL N'SELECT … WHERE id = @id', N'@id INT', @id = value"
                ),
            ))

        # S008 — GOTO
        if find(r"\bGOTO\b"):
            findings.append(F(
                rule_id="S008",
                severity="info",
                description="GOTO makes control flow hard to follow and maintain.",
                suggestion="Replace with IF/WHILE/TRY-CATCH. GOTO is considered bad practice in modern T-SQL.",
            ))

        # S009 — DROP PROCEDURE (perde permissões)
        if find(r"\bDROP\s+PROC(?:EDURE)?\b"):
            findings.append(F(
                rule_id="S009",
                severity="info",
                description="DROP PROCEDURE removes all granted permissions.",
                suggestion="Use ALTER PROCEDURE — preserves existing permissions without needing to recreate them.",
            ))

        # S010 — XP_CMDSHELL / OPENROWSET
        if find(r"\b(?:XP_CMDSHELL|OPENROWSET)\b"):
            findings.append(F(
                rule_id="S010",
                severity="warning",
                description="XP_CMDSHELL / OPENROWSET are high-privilege operations.",
                suggestion=(
                    "Verify the usage is intentional. XP_CMDSHELL executes OS commands "
                    "with the SQL Server service account permissions."
                ),
            ))

        # S011 — ROW_NUMBER para paginação com OFFSET alto
        if find(r"\bROW_NUMBER\s*\("):
            findings.append(F(
                rule_id="S011",
                severity="info",
                description="ROW_NUMBER for pagination degrades on distant pages (high OFFSET).",
                suggestion=(
                    "For large tables, prefer keyset pagination: "
                    "WHERE id > @last_id ORDER BY id FETCH NEXT @n ROWS ONLY"
                ),
            ))

    # Ordena: critical → warning → info
    _order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: _order[f.severity])
    return findings
