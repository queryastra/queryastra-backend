"""Testes de `AlertsRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert import Alert, AlertRule, AlertSilence
from app.models.instance import Instance
from app.repositories.alerts_repository import AlertsRepository


async def test_create_alert_rule_and_list_rules_includes_inactive(
    db_session: AsyncSession,
) -> None:
    repo = AlertsRepository(db_session)
    active_rule = await repo.create_alert_rule(
        AlertRule(
            name="regra-ativa",
            target_type="instance",
            metric_name="metric_x",
            comparison="gt",
            threshold_warning=10.0,
        )
    )
    inactive_rule = await repo.create_alert_rule(
        AlertRule(
            name="regra-inativa",
            target_type="instance",
            metric_name="metric_x",
            comparison="gt",
            threshold_warning=10.0,
            active=False,
        )
    )
    try:
        active_rules = await repo.list_active_rules()
        active_ids = {r.id for r in active_rules}
        assert active_rule.id in active_ids
        assert inactive_rule.id not in active_ids

        all_rules = await repo.list_rules()
        all_ids = {r.id for r in all_rules}
        assert active_rule.id in all_ids
        assert inactive_rule.id in all_ids
    finally:
        await db_session.delete(active_rule)
        await db_session.delete(inactive_rule)
        await db_session.commit()


async def test_get_open_alert_counts_acknowledged_as_open(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    created = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    open_alert = await repo.get_open_alert(alert_rule.id, test_instance.id)
    assert open_alert is not None
    assert open_alert.id == created.id

    created.state = "acknowledged"
    created.acknowledged_at = datetime.now(UTC)
    await repo.update_alert(created)

    still_open = await repo.get_open_alert(alert_rule.id, test_instance.id)
    assert still_open is not None
    assert still_open.id == created.id
    assert still_open.state == "acknowledged"


async def test_count_open_alerts_by_target_excludes_closed_and_missing_targets(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    """`GET /instances` (coluna "Saude" do resumo) usa isto pra `open_alert_count` — alvo sem
    nenhum alerta simplesmente nao aparece no dict (contagem implicita 0), e um alerta 'closed'
    nao conta (mesmo criterio de `get_open_alert`: so 'closed' fecha o episodio)."""
    repo = AlertsRepository(db_session)
    other_instance_id = uuid4()
    await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
            state="open",
        )
    )
    await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="critical",
            state="acknowledged",
        )
    )
    closed = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=other_instance_id,
            severity="warning",
        )
    )
    closed.state = "closed"
    await repo.update_alert(closed)

    counts = await repo.count_open_alerts_by_target([test_instance.id, other_instance_id])

    assert counts[test_instance.id] == 2
    assert other_instance_id not in counts


async def test_get_open_alert_returns_none_when_closed(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    created = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="critical",
        )
    )
    created.state = "closed"
    created.resolved_at = datetime.now(UTC)
    await repo.update_alert(created)

    assert await repo.get_open_alert(alert_rule.id, test_instance.id) is None


async def test_update_alert_persists_mutations(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    created = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    created.severity = "critical"
    created.description = "escalou"
    updated = await repo.update_alert(created)

    assert updated.severity == "critical"
    fetched = await repo.get_alert_by_id(created.id)
    assert fetched is not None
    assert fetched.severity == "critical"
    assert fetched.description == "escalou"


async def test_list_alerts_resolves_names_and_filters_by_state(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    open_alert = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )
    closed_alert = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="critical",
            state="closed",
        )
    )

    all_items = await repo.list_alerts(None, 100)
    all_ids = {i.alert.id for i in all_items}
    assert open_alert.id in all_ids
    assert closed_alert.id in all_ids
    by_id = {i.alert.id: i for i in all_items}
    assert by_id[open_alert.id].alert_name == alert_rule.name
    assert by_id[open_alert.id].target_name == test_instance.name

    only_open = await repo.list_alerts("open", 100)
    open_ids = {i.alert.id for i in only_open}
    assert open_alert.id in open_ids
    assert closed_alert.id not in open_ids


async def test_get_alert_with_names_resolves_target_name(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    created = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    item = await repo.get_alert_with_names(created.id)
    assert item is not None
    assert item.alert_name == alert_rule.name
    assert item.target_name == test_instance.name


async def test_get_active_silence_matches_general_and_specific_rule(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    general_silence = await repo.create_alert_silence(
        AlertSilence(target_type="instance", target_id=test_instance.id, alert_rule_id=None)
    )

    try:
        # `alert_rule_id=NULL` no silence e "coringa": casa com qualquer regra desse alvo.
        matched = await repo.get_active_silence("instance", test_instance.id, alert_rule.id)
        assert matched is not None
        assert matched.id == general_silence.id
    finally:
        await db_session.delete(general_silence)
        await db_session.commit()


async def test_get_active_silence_specific_rule_does_not_match_other_rule(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    other_rule = await repo.create_alert_rule(
        AlertRule(
            name="outra-regra",
            target_type="instance",
            metric_name="metric_y",
            comparison="lt",
            threshold_warning=5.0,
        )
    )
    specific_silence = await repo.create_alert_silence(
        AlertSilence(
            target_type="instance", target_id=test_instance.id, alert_rule_id=other_rule.id
        )
    )

    try:
        assert await repo.get_active_silence("instance", test_instance.id, alert_rule.id) is None
        matched = await repo.get_active_silence("instance", test_instance.id, other_rule.id)
        assert matched is not None
        assert matched.id == specific_silence.id
    finally:
        await db_session.delete(specific_silence)
        await db_session.commit()
        await db_session.delete(other_rule)
        await db_session.commit()


async def test_get_active_silence_respects_silenced_until_window(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    now = datetime.now(UTC)
    expired_silence = await repo.create_alert_silence(
        AlertSilence(
            target_type="instance",
            target_id=test_instance.id,
            alert_rule_id=alert_rule.id,
            silenced_until=now - timedelta(minutes=5),
        )
    )
    try:
        assert await repo.get_active_silence("instance", test_instance.id, alert_rule.id) is None
    finally:
        await db_session.delete(expired_silence)
        await db_session.commit()

    future_silence = await repo.create_alert_silence(
        AlertSilence(
            target_type="instance",
            target_id=test_instance.id,
            alert_rule_id=alert_rule.id,
            silenced_until=now + timedelta(minutes=5),
        )
    )
    try:
        matched = await repo.get_active_silence("instance", test_instance.id, alert_rule.id)
        assert matched is not None
        assert matched.id == future_silence.id
    finally:
        await db_session.delete(future_silence)
        await db_session.commit()


async def test_list_alert_silences_filters_by_target(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    silence = await repo.create_alert_silence(
        AlertSilence(
            target_type="instance", target_id=test_instance.id, alert_rule_id=alert_rule.id
        )
    )
    try:
        by_target = await repo.list_alert_silences(test_instance.id)
        assert {s.id for s in by_target} == {silence.id}

        all_silences = await repo.list_alert_silences(None)
        assert silence.id in {s.id for s in all_silences}
    finally:
        await db_session.delete(silence)
        await db_session.commit()


async def test_delete_alert_silence_returns_true_and_removes_row(
    db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    silence = await repo.create_alert_silence(
        AlertSilence(
            target_type="instance", target_id=test_instance.id, alert_rule_id=alert_rule.id
        )
    )

    deleted = await repo.delete_alert_silence(silence.id)
    assert deleted is True
    assert await repo.list_alert_silences(test_instance.id) == []


async def test_delete_alert_silence_missing_id_returns_false(db_session: AsyncSession) -> None:
    repo = AlertsRepository(db_session)
    assert await repo.delete_alert_silence(uuid4()) is False
