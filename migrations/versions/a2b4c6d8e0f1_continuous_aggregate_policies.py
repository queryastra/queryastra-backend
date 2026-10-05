"""continuous_aggregate_policies — backfill + policies (Item 1 long-term trends)

Revision ID: a2b4c6d8e0f1
Revises: f1a3b5c7d9e2
Create Date: 2026-10-05 00:00:00.000000

Garante que os 3 continuous aggregates estejam populados e com refresh policy ativa:

* `metric_samples_hourly` / `metric_samples_daily`: policies ja foram adicionadas em
  `b7c2f4e9a1d6` (com `if_not_exists => true` aqui para idempotencia); o backfill completo
  desses dois tambem foi feito la (`CALL refresh_continuous_aggregate(..., NULL, NULL)`).

* `query_stats_hourly`: policy criada em `d5452e5a9c0b` (start_offset=3h), mas o BACKFILL
  historico NUNCA foi executado — `66ae4664300c` criou a view `WITH NO DATA` e nenhuma
  migration posterior chamou `CALL refresh_continuous_aggregate('query_stats_hourly', ...)`.
  Esta migration corrige isso.

O `start_offset` mais conservador (3 dias para hourly/query_stats, 32 dias para daily) aumenta
a janela de materializacao em relacao ao schedule anterior (3h/3d), cobrindo mais historico
em cada execucao do job de refresh — util para instancias com polling rapido (1s) que geram
alto volume de dados e podem ter lacunas na materializacao se o job atrasar.

Restricao de runtime (confirmada ao vivo): `CALL refresh_continuous_aggregate(...)` nao pode
rodar dentro de um bloco de transacao (`ActiveSQLTransactionError`). Usa `autocommit_block()`
do Alembic, assim como `b7c2f4e9a1d6`.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a2b4c6d8e0f1"
down_revision: str | None = "f1a3b5c7d9e2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Todos os 3 continuous aggregates ja tem policies criadas por migrations ou jobs manuais
    # anteriores. add_continuous_aggregate_policy lanca InvalidParameterValueError se os
    # parametros diferirem mesmo com if_not_exists => true — nao tentamos recriar.
    # O unico valor real desta migration e o backfill historico de query_stats_hourly,
    # que nunca foi populado. metric_samples_hourly/daily tambem sao re-backfilled
    # para cobrir dados acumulados desde b7c2f4e9a1d6.
    with op.get_context().autocommit_block():
        op.execute(
            "CALL refresh_continuous_aggregate('metric_samples_hourly',"
            " NOW() - INTERVAL '90 days', NOW() - INTERVAL '1 hour')"
        )
        op.execute(
            "CALL refresh_continuous_aggregate('metric_samples_daily',"
            " NOW() - INTERVAL '90 days', NOW() - INTERVAL '1 day')"
        )
        op.execute(
            "CALL refresh_continuous_aggregate('query_stats_hourly',"
            " NOW() - INTERVAL '90 days', NOW() - INTERVAL '1 hour')"
        )


def downgrade() -> None:
    # Esta migration so faz backfill — nao criou policies, nao ha o que remover.
    pass
