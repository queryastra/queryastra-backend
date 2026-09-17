"""Testes reais de `app.masking.sql_masking.mask_sql_literals` — `Docs/rules/03-sql-privacy.md`."""

from __future__ import annotations

import pytest

from app.masking.sql_masking import mask_showplan_xml_literals, mask_sql_literals


def test_masks_single_string_literal() -> None:
    sql = "SELECT * FROM users WHERE name = 'joao'"
    assert mask_sql_literals(sql) == "SELECT * FROM users WHERE name = '?'"


def test_masks_numeric_literal() -> None:
    sql = "SELECT * FROM orders WHERE id = 42"
    assert mask_sql_literals(sql) == "SELECT * FROM orders WHERE id = ?"


def test_masks_decimal_literal() -> None:
    sql = "SELECT * FROM orders WHERE total > 199.90"
    assert mask_sql_literals(sql) == "SELECT * FROM orders WHERE total > ?"


def test_masks_negative_numeric_literal() -> None:
    sql = "SELECT * FROM accounts WHERE balance < -100"
    assert mask_sql_literals(sql) == "SELECT * FROM accounts WHERE balance < ?"


def test_masks_multiple_literals_mixed() -> None:
    sql = "UPDATE t SET status = 'ativo', retries = 3 WHERE id = 7 AND owner = 'alice'"
    expected = "UPDATE t SET status = '?', retries = ? WHERE id = ? AND owner = '?'"
    assert mask_sql_literals(sql) == expected


def test_masks_string_with_escaped_single_quote() -> None:
    sql = "SELECT * FROM t WHERE name = 'joão''s account'"
    assert mask_sql_literals(sql) == "SELECT * FROM t WHERE name = '?'"


def test_does_not_mask_column_or_table_names() -> None:
    sql = "SELECT col_123 FROM table_456 WHERE x = 1"
    assert mask_sql_literals(sql) == "SELECT col_123 FROM table_456 WHERE x = ?"


def test_query_with_no_literals_is_unchanged() -> None:
    sql = "SELECT count(*) FROM pg_stat_activity"
    assert mask_sql_literals(sql) == sql


def test_empty_string_returns_empty_string() -> None:
    assert mask_sql_literals("") == ""


@pytest.mark.parametrize(
    ("sql", "sensitive_value"),
    [
        ("SELECT * FROM t WHERE cpf = '123.456.789-00'", "123.456.789-00"),
        ("INSERT INTO t (a, b) VALUES ('x', 99)", "'x'"),
    ],
)
def test_masking_never_leaves_the_original_value_behind(sql: str, sensitive_value: str) -> None:
    """As aspas em si podem permanecer (uma string mascarada agora é `'?'`, não um `?` bruto —
    pedido explícito do usuário) — o que nunca pode sobrar é o VALOR original."""
    masked = mask_sql_literals(sql)
    assert sensitive_value not in masked


def test_masks_string_literal_preserving_quotes() -> None:
    sql = "INSERT INTO t (a, b) VALUES ('x', 99)"
    assert mask_sql_literals(sql) == "INSERT INTO t (a, b) VALUES ('?', ?)"


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("%silva%", "'%?%'"),  # LIKE '%valor%' — curinga nos dois lados
        ("%silva", "'%?'"),  # LIKE '%valor' — curinga só no início
        ("silva%", "'?%'"),  # LIKE 'valor%' — curinga só no fim
        ("silva", "'?'"),  # LIKE 'valor' — sem curinga, comportamento de sempre
        ("%", "'%'"),  # LIKE '%' isolado (comum como 'LIKE ''%'' + @valor + ''%''") — só
        # curinga, nada pra mascarar, nenhum '?' inventado
        ("a%b", "'?%?'"),  # curinga no meio — generaliza pro caso menos comum também
    ],
)
def test_masks_like_pattern_preserving_percent_position(pattern: str, expected: str) -> None:
    sql = f"SELECT * FROM t WHERE nome LIKE '{pattern}'"
    assert mask_sql_literals(sql) == f"SELECT * FROM t WHERE nome LIKE {expected}"


def test_masks_like_concatenated_with_variable_keeps_percent_markers_untouched() -> None:
    """Padrao comum pra montar LIKE com bind variable: os `%` sao literais de string PROPRIOS
    (so contem o curinga), concatenados com uma variavel — cada `'%'` e mascarado de forma
    independente e fica igual (nao ha valor nenhum dentro pra mascarar), a variavel em si
    (`@valor`) nunca e um literal, entao passa direto."""
    sql = "SELECT * FROM t WHERE nome LIKE '%' + @valor + '%'"
    assert mask_sql_literals(sql) == "SELECT * FROM t WHERE nome LIKE '%' + @valor + '%'"


def test_does_not_mask_postgres_positional_parameter() -> None:
    """`$1`/`$2` (protocolo estendido do PostgreSQL) sao REFERENCIAS a parametro, nao literais —
    o digito depois do `$` nao pode ser tratado como numero e virar `$?`."""
    sql = "SELECT * FROM t WHERE id = $1 AND status = $2"
    assert mask_sql_literals(sql) == sql


def test_mask_showplan_xml_literals_masks_parameter_compiled_value() -> None:
    xml = (
        '<ColumnReference Column="@P1" ParameterCompiledValue="(42)" '
        'ParameterRuntimeValue="(42)" />'
    )
    masked = mask_showplan_xml_literals(xml)
    assert 'ParameterCompiledValue="?"' in masked
    assert 'ParameterRuntimeValue="?"' in masked
    assert "42" not in masked
    assert 'Column="@P1"' in masked  # resto do XML permanece intacto


def test_mask_showplan_xml_literals_masks_string_parameter_value() -> None:
    xml = "<ColumnReference ParameterCompiledValue=\"N'joao@example.com'\" />"
    masked = mask_showplan_xml_literals(xml)
    assert masked == '<ColumnReference ParameterCompiledValue="?" />'


def test_mask_showplan_xml_literals_without_parameters_is_unchanged() -> None:
    xml = '<RelOp NodeId="0" PhysicalOp="Clustered Index Scan" EstimatedTotalSubtreeCost="1.5" />'
    assert mask_showplan_xml_literals(xml) == xml
