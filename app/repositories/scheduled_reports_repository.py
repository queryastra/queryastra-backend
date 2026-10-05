"""CRUD de relatorios agendados."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.scheduled_report import ScheduledReport


class ScheduledReportsRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> list[ScheduledReport]:
        result = await self._session.execute(
            sa.select(ScheduledReport).order_by(ScheduledReport.created_at.desc())
        )
        return list(result.scalars().all())

    async def list_due(self, now: datetime) -> list[ScheduledReport]:
        """Relatorios ativos com next_run_at <= now."""
        result = await self._session.execute(
            sa.select(ScheduledReport).where(
                ScheduledReport.is_active.is_(True),
                ScheduledReport.next_run_at.is_not(None),
                ScheduledReport.next_run_at <= now,
            )
        )
        return list(result.scalars().all())

    async def get_by_id(self, report_id: uuid.UUID) -> ScheduledReport | None:
        result = await self._session.execute(
            sa.select(ScheduledReport).where(ScheduledReport.id == report_id)
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        instance_id: uuid.UUID,
        report_type: str,
        title: str,
        params: dict,
        schedule_cron: str,
        email_recipients: list[str],
        next_run_at: datetime | None,
        is_recurring: bool = True,
    ) -> ScheduledReport:
        report = ScheduledReport(
            instance_id=instance_id,
            report_type=report_type,
            title=title,
            params=params,
            schedule_cron=schedule_cron,
            email_recipients=email_recipients,
            next_run_at=next_run_at,
            is_recurring=is_recurring,
        )
        self._session.add(report)
        await self._session.flush()
        await self._session.refresh(report)
        return report

    async def update_after_run(
        self,
        report: ScheduledReport,
        status: str,
        error: str | None,
        next_run_at: datetime | None,
    ) -> None:
        report.last_run_at = datetime.now(UTC)
        report.last_run_status = status
        report.last_run_error = error
        report.next_run_at = next_run_at
        self._session.add(report)

    async def delete(self, report: ScheduledReport) -> None:
        await self._session.delete(report)

    async def set_active(self, report: ScheduledReport, active: bool) -> None:
        report.is_active = active
        self._session.add(report)
