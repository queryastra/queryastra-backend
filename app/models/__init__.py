"""Modelos SQLAlchemy — tabelas relacionais simples do MVP (Fase 1).

Import agregador: garante que `Base.metadata` (usado por `migrations/env.py` no autogenerate)
enxerga todos os modelos abaixo. Modelos de RBAC/usuarios/masking_reveal_audit sao Fase 2/4
(`Docs/database-schema.md` §5) e ficam fora deste MVP de proposito. `SystemSettings` é a
EXCEÇÃO — implementado como tabela singleton dedicada (não o `system_settings` genérico
chave/valor documentado ali), primeira preferência global editável pelo usuário.
"""

from app.models.alert import Alert, AlertNotification, AlertRule, AlertSilence
from app.models.audit_log import AuditLog
from app.models.backup_status import BackupStatus
from app.models.blocking_incident import BlockingIncident
from app.models.deadlock_event import DeadlockEvent
from app.models.instance import Instance, InstanceMaintenanceWindow
from app.models.missing_index_suggestion import MissingIndexSuggestion
from app.models.password_reset_token import PasswordResetToken
from app.models.query_plan_snapshot import QueryPlanSnapshot
from app.models.server import InstanceServer, Server
from app.models.system_license import SystemLicense
from app.models.system_settings import SystemSettings
from app.models.transaction_event import TransactionEvent
from app.models.user import User

__all__ = [
    "AuditLog",
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
    "BlockingIncident",
    "DeadlockEvent",
    "QueryPlanSnapshot",
    "SystemLicense",
    "SystemSettings",
    "User",
    "PasswordResetToken",
]
