"""Helper de registro de auditoria — adiciona entrada à sessão sem fazer commit."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit_log import AuditLog


async def record_audit(
    session: AsyncSession,
    *,
    actor: str,
    action: str,
    ip_address: str | None = None,
    target_type: str | None = None,
    target_id: str | None = None,
    target_name: str | None = None,
    detail: dict | None = None,
) -> None:
    """Adiciona entrada de auditoria à sessão. O chamador é responsável por fazer commit."""
    session.add(
        AuditLog(
            actor_username=actor,
            action=action,
            ip_address=ip_address,
            target_type=target_type,
            target_id=target_id,
            target_name=target_name,
            detail=detail,
        )
    )
