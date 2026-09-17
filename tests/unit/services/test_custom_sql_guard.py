import pytest

from app.services.custom_sql_guard import CustomSqlRejectedError, validate_custom_sql


def test_allows_plain_select() -> None:
    validate_custom_sql("SELECT COUNT(*) FROM pix_eventos WHERE status = 'pendente'")


@pytest.mark.parametrize(
    "keyword",
    [
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
    ],
)
def test_rejects_blocked_keyword(keyword: str) -> None:
    with pytest.raises(CustomSqlRejectedError) as exc_info:
        validate_custom_sql(f"{keyword} something")
    assert exc_info.value.keyword == keyword


def test_rejects_blocked_keyword_case_insensitive() -> None:
    with pytest.raises(CustomSqlRejectedError):
        validate_custom_sql("delete from t")


def test_allows_keyword_inside_string_literal() -> None:
    validate_custom_sql("SELECT COUNT(*) FROM t WHERE status = 'DELETE'")


def test_rejects_keyword_alongside_legit_select() -> None:
    with pytest.raises(CustomSqlRejectedError):
        validate_custom_sql("SELECT 1; DROP TABLE t")


def test_does_not_false_positive_on_substring_of_identifier() -> None:
    """`created_at`/`updated_at` nao devem disparar CREATE/UPDATE (boundary de palavra)."""
    validate_custom_sql("SELECT COUNT(*) FROM t WHERE created_at > now() - interval '1 day'")
