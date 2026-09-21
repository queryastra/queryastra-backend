"""Classificacao de coluna SQL como dado pessoal/sensivel (PII) — Fase de mascaramento seletivo
(pedido explicito do usuario: "mascare somente dados sensiveis ou pessoais dos clientes... os
outros parametros devem ser exibidos").

`mask_sql_literals_for_display` (`app/masking/sql_masking.py`) opera sobre texto SQL bruto via
regex, sem um parser SQL real — a unica informacao disponivel pra decidir se um literal e PII e o
NOME DA COLUNA imediatamente associado a ele (o padrao `coluna <operador> literal`, ex.:
`WHERE cpf = '123'`, `SET nome = 'Joao'`). Este modulo so mantem o dicionario de padroes de nome
de coluna que indicam dado pessoal/sensivel — a decisao de MASCARAR quando a coluna nao pode ser
determinada (ex.: literal dentro de `IN (...)` ou de `INSERT ... VALUES (...)`) fica no modulo de
mascaramento, nao aqui.
"""

from __future__ import annotations

import re

# Cada padrao casa como SUBSTRING (case-insensitive) do nome de coluna, entao cobre variacoes
# comuns de prefixo/sufixo em portugues (ex.: `cliente_cpf`, `cpf_titular`, `nm_cliente`) sem
# precisar enumerar cada combinacao. Falso positivo aqui so causa mascaramento de um valor que
# talvez nao precisasse (direcao segura); falso negativo mostraria um valor pessoal — por isso a
# lista inclui tanto o termo completo em portugues quanto abreviacoes muito comuns.
_PII_COLUMN_PATTERNS = (
    r"cpf",
    r"cnpj",
    r"\brg\b",
    r"nome",
    r"sobrenome",
    r"e-?mail",
    r"email",
    r"telefone",
    r"celular",
    r"endere[cç]o",
    r"endereco",
    r"logradouro",
    r"bairro",
    r"\bcep\b",
    r"senha",
    r"password",
    r"cart[aã]o",
    r"\bcvv\b",
    r"conta",
    r"\biban\b",
    r"nascimento",
    r"matr[ií]cula",
    r"renda",
    r"sal[aá]rio",
    r"titular",
)

_PII_COLUMN_RE = re.compile("|".join(_PII_COLUMN_PATTERNS), re.IGNORECASE)


def is_pii_column(column_name: str) -> bool:
    """`True` se `column_name` corresponde a um padrao de dado pessoal/sensivel conhecido.

    Args:
        column_name: nome de coluna extraido do texto SQL (pode vir com prefixo de
            tabela/schema/alias, ex. `"c.cpf"`, `"[dbo].[Clientes].[Nome]"` — a busca e por
            substring, entao o prefixo nao precisa ser removido antes de chamar esta funcao).

    Example:
        >>> is_pii_column("cpf")
        True
        >>> is_pii_column("c.cliente_nome")
        True
        >>> is_pii_column("status")
        False
    """
    return bool(_PII_COLUMN_RE.search(column_name))
