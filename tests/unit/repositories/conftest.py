"""Fixtures dos testes de repositorio de coleta continua — banco real `ab_xql_pm_dev`.

Diferente do resto do scaffold (que usa repositorio fake em memoria, ver
`tests/integration/api/test_instances_registration.py`), estes testes tocam o TimescaleDB real
de desenvolvimento (`DATABASE_URL` de `.env`, ja com a migration
`66ae4664300c_hypertables` aplicada — `metric_samples`/`query_samples`/`blocking_events`/
`sql_exclusions` sao tabelas reais). Cada teste cria sua propria `Instance` e
`test_instance` limpa tudo que referencia essa instancia ao final (respeitando FK: linhas de
`query_samples`/`blocking_events` primeiro, depois a propria `Instance` — `sql_exclusions` tem
`ON DELETE CASCADE`, mas e limpa explicitamente tambem por clareza).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import async_session_factory, engine
from app.models.alert import Alert, AlertNotification, AlertRule, AlertSilence
from app.models.instance import Instance
from app.repositories._tables import (
    blocking_events_table,
    metric_samples_table,
    query_samples_table,
    sql_exclusions_table,
)


@pytest.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    # `pytest-asyncio` (modo "auto") cria um event loop novo por teste; o `engine` global de
    # `app.db.session` mantem um pool de conexoes `asyncpg` presas ao loop em que foram abertas.
    # Sem descartar o pool ao final de cada teste, a proxima chamada tenta fechar/reusar uma
    # conexao presa a um loop ja fechado ("Event loop is closed"). `dispose()` aqui, ainda com o
    # loop atual vivo, evita isso — custo aceitavel neste conjunto pequeno de testes.
    async with async_session_factory() as session:
        yield session
    await engine.dispose()


@pytest.fixture
async def test_instance(db_session: AsyncSession) -> AsyncIterator[Instance]:
    instance = Instance(
        name=f"test-instance-{uuid4()}",
        db_type="postgresql",
        host="127.0.0.1",
        port=5432,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
    )
    db_session.add(instance)
    await db_session.commit()
    await db_session.refresh(instance)

    yield instance

    await db_session.execute(
        sa.delete(query_samples_table).where(query_samples_table.c.instance_id == instance.id)
    )
    await db_session.execute(
        sa.delete(blocking_events_table).where(blocking_events_table.c.instance_id == instance.id)
    )
    await db_session.execute(
        sa.delete(sql_exclusions_table).where(sql_exclusions_table.c.instance_id == instance.id)
    )
    await db_session.execute(
        sa.delete(metric_samples_table).where(metric_samples_table.c.target_id == instance.id)
    )
    # Alertas/notificacoes/silenciamentos que tenham esta instancia como alvo — ordem de FK:
    # alert_notifications antes de alerts (nao apaga alert_rules aqui: uma regra nao e escopada
    # por instancia, quem cria uma regra em teste usa a fixture `alert_rule`, que cuida do
    # proprio ciclo de vida).
    await db_session.execute(
        sa.delete(AlertNotification).where(
            AlertNotification.alert_id.in_(
                sa.select(Alert.id).where(Alert.target_id == instance.id)
            )
        )
    )
    await db_session.execute(sa.delete(Alert).where(Alert.target_id == instance.id))
    await db_session.execute(sa.delete(AlertSilence).where(AlertSilence.target_id == instance.id))
    await db_session.commit()
    await db_session.delete(instance)
    await db_session.commit()


@pytest.fixture
async def alert_rule(db_session: AsyncSession) -> AsyncIterator[AlertRule]:
    """Regra de alerta de threshold simples descartavel — cuida do proprio ciclo de vida (regra
    nao e escopada por instancia, entao nao entra na limpeza de `test_instance`)."""
    rule = AlertRule(
        name=f"test-rule-{uuid4()}",
        target_type="instance",
        metric_name="test_metric",
        comparison="gte",
        threshold_warning=1.0,
    )
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)

    yield rule

    await db_session.execute(
        sa.delete(AlertNotification).where(
            AlertNotification.alert_id.in_(
                sa.select(Alert.id).where(Alert.alert_rule_id == rule.id)
            )
        )
    )
    await db_session.execute(sa.delete(Alert).where(Alert.alert_rule_id == rule.id))
    await db_session.execute(sa.delete(AlertSilence).where(AlertSilence.alert_rule_id == rule.id))
    await db_session.commit()
    await db_session.delete(rule)
    await db_session.commit()
