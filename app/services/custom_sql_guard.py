"""Bloqueio por palavra-chave para `AlertRule.custom_sql` (Fase 2, item 2).

Decisao do usuario, ja confirmada: "melhor esforco, nao uma sandbox real" — rejeita a query no
CADASTRO da regra (antes de qualquer execucao periodica contra producao) se ela contiver, fora de
literais de string, alguma palavra-chave de escrita/DDL/controle de acesso. Nao e uma garantia
absoluta de somente-leitura (ex.: uma funcao/procedure chamada por SELECT poderia escrever por
baixo) — cobre o caso comum de erro/abuso direto, documentado honestamente como tal.
"""

from __future__ import annotations

import re

_BLOCKED_KEYWORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "EXEC",
    "EXECUTE",
    "TRUNCATE",
    "MERGE",
    "CREATE",
    "GRANT",
    "REVOKE",
)

# Remove literais de string antes de checar palavra-chave, pra nao rejeitar uma query legitima
# só porque uma dessas palavras aparece dentro de um valor literal (ex.: `WHERE status = 'DELETE'`).
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")

_BLOCKED_KEYWORD_RE = re.compile(r"\b(" + "|".join(_BLOCKED_KEYWORDS) + r")\b", re.IGNORECASE)


class CustomSqlRejectedError(ValueError):
    """`custom_sql` contem uma palavra-chave bloqueada — rejeitado antes de qualquer execucao."""

    def __init__(self, keyword: str) -> None:
        self.keyword = keyword
        super().__init__(
            f"Comando SQL customizado rejeitado: contem a palavra-chave '{keyword}', nao "
            "permitida em alertas (regra so aceita leitura). Bloqueio por palavra-chave e "
            "melhor esforco, nao uma sandbox real — evite qualquer construcao que escreva ou "
            "altere dado/schema."
        )


def validate_custom_sql(sql: str) -> None:
    """Levanta `CustomSqlRejectedError` se `sql` contiver uma palavra-chave bloqueada fora de
    literais de string. Nao valida mais nada alem disso (sintaxe, plano, permissao real do
    usuario configurado ficam por conta da execucao em si)."""
    stripped = _STRING_LITERAL_RE.sub("", sql)
    match = _BLOCKED_KEYWORD_RE.search(stripped)
    if match is not None:
        raise CustomSqlRejectedError(match.group(1).upper())
