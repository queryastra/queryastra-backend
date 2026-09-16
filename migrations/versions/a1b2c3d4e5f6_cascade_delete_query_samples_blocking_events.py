"""cascade delete query_samples/blocking_events on instance delete

Revision ID: a1b2c3d4e5f6
Revises: d3a7b9c1e6f4
Create Date: 2026-09-16 09:30:00.000000

Bug real encontrado ao implementar a exclusao de instancia (`DELETE /api/v1/instances/{id}`,
`InstancesRepository.delete()`): `query_samples.instance_id` e `blocking_events.instance_id`
foram criadas na migration `66ae4664300c_hypertables` via SQL bruto (`REFERENCES instances(id)`,
sem `ON DELETE CASCADE`) — ao contrario de TODAS as demais tabelas que referenciam `instances`
(`transaction_events`, `blocking_incidents`, `deadlock_events`, `missing_index_suggestions`,
`backup_status`, `sql_exclusions`, `instance_servers`, `instance_maintenance_windows`, todas
criadas com `ondelete="CASCADE"` explicito). Confirmado ao vivo contra `ab_xql_pm_dev`
(`pg_constraint.confdeltype = 'a'`/NO ACTION nas duas) — excluir qualquer instancia com pelo
menos uma amostra de query ou um evento de bloqueio ja coletado (ou seja, qualquer instancia
monitorada ha mais de um ciclo de poll) falharia com `IntegrityError` (violacao de FK), nunca
testado porque a feature de exclusao ainda nao tinha sido exercitada contra dado real.

`ALTER TABLE ... DROP/ADD CONSTRAINT` no hypertable "pai" se propaga automaticamente para todos
os chunks (existentes e futuros) — comportamento documentado do TimescaleDB (constraints sao
herdadas pelos chunks a partir da definicao da hypertable), confirmado apos aplicar esta
migration verificando `pg_constraint` em cada chunk (`_timescaledb_internal._hyper_*_chunk`).
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "d3a7b9c1e6f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE query_samples DROP CONSTRAINT query_samples_instance_id_fkey"
    )
    op.execute(
        "ALTER TABLE query_samples "
        "ADD CONSTRAINT query_samples_instance_id_fkey "
        "FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE"
    )
    op.execute(
        "ALTER TABLE blocking_events DROP CONSTRAINT blocking_events_instance_id_fkey"
    )
    op.execute(
        "ALTER TABLE blocking_events "
        "ADD CONSTRAINT blocking_events_instance_id_fkey "
        "FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE blocking_events DROP CONSTRAINT blocking_events_instance_id_fkey"
    )
    op.execute(
        "ALTER TABLE blocking_events "
        "ADD CONSTRAINT blocking_events_instance_id_fkey "
        "FOREIGN KEY (instance_id) REFERENCES instances(id)"
    )
    op.execute(
        "ALTER TABLE query_samples DROP CONSTRAINT query_samples_instance_id_fkey"
    )
    op.execute(
        "ALTER TABLE query_samples "
        "ADD CONSTRAINT query_samples_instance_id_fkey "
        "FOREIGN KEY (instance_id) REFERENCES instances(id)"
    )
