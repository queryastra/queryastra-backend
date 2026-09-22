"""Testes de `compute_flag_reason` (`app/collectors/transaction_flagging.py`) — critério
compartilhado por SQL Server e PostgreSQL de quando uma transação aberta vira uma linha em
`transaction_events`. Cobre especificamente a tag "sleeping" (novas tarefas do usuário, itens
1/2) — as tags pré-existentes (bloqueio/erro/orfa) já são cobertas indiretamente pelos testes de
adaptador, mas nunca tiveram um teste unitário isolado da própria função."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.collectors.transaction_flagging import compute_flag_reason


def test_returns_none_when_no_criterion_applies() -> None:
    assert (
        compute_flag_reason(
            involved_in_blocking=False,
            has_error=False,
            begin_time=datetime.now(UTC),
            now=datetime.now(UTC),
            orphan_threshold_minutes=5,
        )
        is None
    )


def test_sleeping_tag_added_when_idle_and_past_threshold() -> None:
    begin_time = datetime.now(UTC) - timedelta(seconds=10)
    now = datetime.now(UTC)

    reason = compute_flag_reason(
        involved_in_blocking=False,
        has_error=False,
        begin_time=begin_time,
        now=now,
        orphan_threshold_minutes=999,  # bem alto - garante que "orfa" nao interfere no teste
        is_idle=True,
        sleeping_threshold_ms=5000,
    )

    assert reason == "sleeping"


def test_sleeping_tag_not_added_when_not_idle() -> None:
    begin_time = datetime.now(UTC) - timedelta(seconds=10)
    now = datetime.now(UTC)

    reason = compute_flag_reason(
        involved_in_blocking=False,
        has_error=False,
        begin_time=begin_time,
        now=now,
        orphan_threshold_minutes=999,
        is_idle=False,
        sleeping_threshold_ms=5000,
    )

    assert reason is None


def test_sleeping_tag_not_added_when_below_threshold() -> None:
    begin_time = datetime.now(UTC) - timedelta(seconds=1)
    now = datetime.now(UTC)

    reason = compute_flag_reason(
        involved_in_blocking=False,
        has_error=False,
        begin_time=begin_time,
        now=now,
        orphan_threshold_minutes=999,
        is_idle=True,
        sleeping_threshold_ms=5000,
    )

    assert reason is None


def test_sleeping_tag_disabled_when_threshold_is_none() -> None:
    """`sleeping_threshold_ms=None` (default) desativa a checagem inteira — mesmo comportamento
    de antes desta feature, usado quando nenhum repositório de override foi injetado."""
    begin_time = datetime.now(UTC) - timedelta(hours=1)
    now = datetime.now(UTC)

    reason = compute_flag_reason(
        involved_in_blocking=False,
        has_error=False,
        begin_time=begin_time,
        now=now,
        orphan_threshold_minutes=999,
        is_idle=True,
        sleeping_threshold_ms=None,
    )

    assert reason is None


def test_sleeping_tag_combines_with_blocking_tag() -> None:
    begin_time = datetime.now(UTC) - timedelta(seconds=10)
    now = datetime.now(UTC)

    reason = compute_flag_reason(
        involved_in_blocking=True,
        has_error=False,
        begin_time=begin_time,
        now=now,
        orphan_threshold_minutes=999,
        is_idle=True,
        sleeping_threshold_ms=5000,
    )

    assert reason == "bloqueio,sleeping"


def test_zero_threshold_flags_immediately() -> None:
    """Mesma convenção de `blocking_min_duration_ms`: `0` = sem filtro, marca toda transação
    ociosa imediatamente."""
    begin_time = datetime.now(UTC)
    now = datetime.now(UTC)

    reason = compute_flag_reason(
        involved_in_blocking=False,
        has_error=False,
        begin_time=begin_time,
        now=now,
        orphan_threshold_minutes=999,
        is_idle=True,
        sleeping_threshold_ms=0,
    )

    assert reason == "sleeping"
