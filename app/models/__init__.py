"""Modelos SQLAlchemy — tabelas relacionais simples do MVP (Fase 1).

Import agregador: garante que `Base.metadata` (usado por `migrations/env.py` no autogenerate)
enxerga todos os modelos abaixo. Modelos de RBAC/usuarios/system_settings/masking_reveal_audit
sao Fase 2 (`Docs/database-schema.md` §5) e ficam fora deste MVP de proposito.
"""

from app.models.alert import Alert, AlertNotification, AlertRule, AlertSilence
from app.models.backup_status import BackupStatus
from app.models.instance import Instance, InstanceMaintenanceWindow
from app.models.missing_index_suggestion import MissingIndexSuggestion
from app.models.server import InstanceServer, Server
from app.models.transaction_event import TransactionEvent

__all__ = [
    "Instance",
    "InstanceMaintenanceWindow",
    "Server",
    "InstanceServer",
    "AlertRule",
    "Alert",
    "AlertNotification",
    "AlertSilence",
    "MissingIndexSuggestion",
    "BackupStatus",
    "TransactionEvent",
]
