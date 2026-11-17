"""Formato unico dos eventos de alerta publicados no `app.core.events.event_bus`.

Consumidores: `/ws/live` (filtra por `instance_id` conforme o acesso do usuario conectado) e
`app.services.notification_service` (gera notificacoes in-app). Carregar `instance_id`,
`severity` e `rule_name` no proprio evento evita que cada consumidor precise reler o alerta.
"""

from __future__ import annotations

from app.models.alert import Alert

# Kinds que correspondem a um `Alert` NOVO (nao atualizacao de um ja existente).
ALERT_CREATED_KINDS = frozenset({"triggered", "recurrence_escalated"})


def build_alert_event(alert: Alert, *, kind: str, rule_name: str | None = None) -> dict:
    event_type = "alert_created" if kind in ALERT_CREATED_KINDS else "alert_updated"
    event: dict = {
        "type": event_type,
        "alert_id": str(alert.id),
        "kind": kind,
        "severity": alert.severity,
        "state": alert.state,
        "target_type": alert.target_type,
        "target_id": str(alert.target_id),
        "rule_name": rule_name,
    }
    if alert.target_type == "instance":
        event["instance_id"] = str(alert.target_id)
    return event
