"""Testes reais de `app.masking.sql_masking.mask_sql_literals` — `Docs/rules/03-sql-privacy.md`."""

from __future__ import annotations

import hashlib

import pytest

from app.masking.sql_masking import (
    mask_showplan_xml_literals,
    mask_sql_literals,
    mask_sql_literals_for_display,
)


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


def test_strips_trailing_nul_byte_from_driver_artifact() -> None:
    """Bug real (Autklinux20, driver jTDS, 2026-09-22): o SQL Server as vezes devolve texto de
    query com um byte NUL sobrando no final - o PostgreSQL rejeita `\\x00` em qualquer coluna de
    texto (`CharacterNotInRepertoireError`), o que derrubava o insert do ciclo inteiro."""
    sql = "FETCH API_CURSOR0000000000000496\x00"
    assert "\x00" not in mask_sql_literals(sql)
    assert "\x00" not in mask_sql_literals_for_display(sql)


def test_strips_nul_byte_in_the_middle_of_text() -> None:
    sql = "SELECT * FROM t WHERE name = 'jo\x00ao'"
    assert "\x00" not in mask_sql_literals(sql)
    assert "\x00" not in mask_sql_literals_for_display(sql)


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


def test_does_not_mask_sqlserver_auto_param_placeholder() -> None:
    """Regressao (bug real relatado pelo usuario ao vivo): o cabecalho de auto-parametrizacao do
    SQL Server nomeia os parametros como `@0`/`@1` (sem prefixo de letra) quando o motor
    parametriza automaticamente uma query ad-hoc — sao REFERENCIAS a parametro, nao literais,
    igual a `$1`/`$2` do Postgres. Sem a exclusao do `@` no lookbehind, `@0`/`@1` virava `@?`/`@?`,
    apagando a distincao entre os parametros."""
    sql = "(@0 int,@1 tinyint)UPDATE [T] set [QUANTIDADE] = @0  WHERE [ID]=@1"
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


def test_mask_showplan_xml_literals_strips_nul_byte() -> None:
    xml = '<RelOp NodeId="0" PhysicalOp="Clustered Index Scan" />\x00'
    assert "\x00" not in mask_showplan_xml_literals(xml)


class TestMaskSqlLiteralsForDisplay:
    """`mask_sql_literals_for_display` — mascaramento seletivo (só PII), usado exclusivamente
    para o texto EXIBIDO (`sql_text_masked`/`sql_text_full_masked`), nunca para `sql_hash`."""

    def test_date_literal_is_shown_unmasked_even_without_a_recognizable_column(self) -> None:
        sql = "SELECT * FROM t WHERE data_criacao = '2024-01-15'"
        assert mask_sql_literals_for_display(sql) == sql

    def test_date_with_time_and_offset_is_shown_unmasked(self) -> None:
        sql = "SELECT * FROM t WHERE dt = '2024-01-15T10:30:00.123+00:00'"
        assert mask_sql_literals_for_display(sql) == sql

    def test_waitfor_delay_time_literal_is_shown_unmasked(self) -> None:
        """Bug real relatado pelo usuario ao vivo: `WAITFOR DELAY` nao tem nenhuma coluna
        associada (nao e `coluna = literal`), entao caia no padrao seguro (mascarar) — um
        intervalo de tempo isolado (`HH:MM:SS`) nao e dado pessoal/sensivel, mesma logica da
        data, mostrado incondicionalmente."""
        sql = "WAITFOR DELAY '00:00:10'"
        assert mask_sql_literals_for_display(sql) == sql

    def test_date_like_value_in_unrelated_column_is_still_shown(self) -> None:
        """O pedido do usuário foi incondicional ("os parâmetros de data devem ser gravados e
        exibidos") — sem distinguir por nome de coluna, então mesmo uma coluna de nome genérico
        com um valor em formato de data é mostrada."""
        sql = "SELECT * FROM t WHERE nascimento = '1990-05-20'"
        assert mask_sql_literals_for_display(sql) == sql

    def test_non_pii_column_literal_is_shown_unmasked(self) -> None:
        sql = "SELECT * FROM t WHERE status = 'ATIVO'"
        assert mask_sql_literals_for_display(sql) == sql

    def test_non_pii_numeric_column_literal_is_shown_unmasked(self) -> None:
        sql = "SELECT * FROM orders WHERE retries = 3"
        assert mask_sql_literals_for_display(sql) == sql

    @pytest.mark.parametrize(
        "column",
        ["cpf", "c.cpf", "cnpj", "email", "telefone", "nome", "senha", "cartao", "conta"],
    )
    def test_pii_column_literal_is_masked(self, column: str) -> None:
        sql = f"SELECT * FROM t WHERE {column} = 'valor_sensivel'"
        masked = mask_sql_literals_for_display(sql)
        assert "valor_sensivel" not in masked
        assert masked == f"SELECT * FROM t WHERE {column} = '?'"

    def test_pii_column_literal_mixed_with_non_pii_in_same_query(self) -> None:
        sql = "UPDATE clientes SET status = 'ATIVO', cpf = '123.456.789-00' WHERE id = 7"
        masked = mask_sql_literals_for_display(sql)
        assert "'ATIVO'" in masked
        assert "123.456.789-00" not in masked
        assert masked == "UPDATE clientes SET status = 'ATIVO', cpf = '?' WHERE id = 7"

    def test_literal_without_identifiable_column_defaults_to_masked(self) -> None:
        """`IN (...)` — nenhum literal individual tem uma coluna imediatamente antes dele
        (posição fora do escopo desta primeira versão) — cai no padrão seguro: mascarar."""
        sql = "SELECT * FROM t WHERE id IN (1, 2, 3)"
        assert mask_sql_literals_for_display(sql) == "SELECT * FROM t WHERE id IN (?, ?, ?)"

    def test_insert_values_without_column_list_defaults_to_masked(self) -> None:
        """Sem lista de colunas explicita, nao ha como saber a ordem sem o schema real da
        tabela — permanece no padrao seguro (mascarar)."""
        sql = "INSERT INTO t VALUES ('x', 99)"
        assert mask_sql_literals_for_display(sql) == "INSERT INTO t VALUES ('?', ?)"

    def test_insert_values_with_explicit_column_list_classifies_by_position(self) -> None:
        sql = "INSERT INTO t (status, cpf) VALUES ('ATIVO', '123')"
        assert (
            mask_sql_literals_for_display(sql)
            == "INSERT INTO t (status, cpf) VALUES ('ATIVO', '?')"
        )

    def test_insert_values_with_explicit_column_list_handles_multiple_rows(self) -> None:
        sql = "INSERT INTO t (id, cpf) VALUES (1, '111'), (2, '222')"
        assert (
            mask_sql_literals_for_display(sql)
            == "INSERT INTO t (id, cpf) VALUES (1, '?'), (2, '?')"
        )

    def test_insert_values_with_explicit_column_list_skips_commas_inside_string_literal(
        self,
    ) -> None:
        """Uma virgula DENTRO de um literal de string (ex.: um endereco) nao pode ser contada
        como separador de posicao na tupla — senao a coluna seguinte ("quantidade", nao-PII)
        ficaria desalinhada e cairia no padrao seguro (mascarar) por engano."""
        sql = "INSERT INTO t (apelido, quantidade) VALUES ('a, b', 42)"
        assert (
            mask_sql_literals_for_display(sql)
            == "INSERT INTO t (apelido, quantidade) VALUES ('a, b', 42)"
        )

    def test_like_percent_markers_still_preserved_for_pii_column(self) -> None:
        sql = "SELECT * FROM t WHERE email LIKE '%silva%'"
        assert mask_sql_literals_for_display(sql) == "SELECT * FROM t WHERE email LIKE '%?%'"

    def test_sql_hash_is_unaffected_by_display_masking_changes(self) -> None:
        """Garante a separação de responsabilidades documentada em `mask_sql_literals`: o hash
        usado para agrupar execuções (Top SQL) continua vindo só de `mask_sql_literals` — duas
        execuções da MESMA query com valores não-PII diferentes têm o mesmo `sql_hash`, mesmo que
        `mask_sql_literals_for_display` mostre esses valores de forma diferente."""
        sql_a = "SELECT * FROM t WHERE status = 'ATIVO' AND id = 1"
        sql_b = "SELECT * FROM t WHERE status = 'INATIVO' AND id = 2"

        hash_a = hashlib.sha256(mask_sql_literals(sql_a).encode("utf-8")).hexdigest()
        hash_b = hashlib.sha256(mask_sql_literals(sql_b).encode("utf-8")).hexdigest()
        assert hash_a == hash_b

        assert mask_sql_literals_for_display(sql_a) != mask_sql_literals_for_display(sql_b)
