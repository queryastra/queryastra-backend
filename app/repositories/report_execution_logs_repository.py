"""Repositório de `report_execution_logs` — histórico de execuções de relatórios."""

from __future__ import annotations

import uuid
from datetime import datetime

import sqlalchemy as sa

from app.models.report_execution_log import ReportExecutionLog
from app.repositories.base import BaseRepository


class ReportExecutionLogsRepository(BaseRepository):
    async def insert(
        self,
        *,
        report_id: uuid.UUID | None,
        report_title: str,
        report_type: str,
        instance_id: uuid.UUID | None,
        instance_name: str | None,
        executed_at: datetime,
        status: str,
        error: str | None,
        email_recipients: list[str],
    ) -> ReportExecutionLog:
        row = ReportExecutionLog(
            report_id=report_id,
            report_title=report_title,
            report_type=report_type,
            instance_id=instance_id,
            instance_name=instance_name,
            executed_at=executed_at,
            status=status,
            error=error,
            email_recipients=email_recipients,
        )
        self._session.add(row)
        return row

    async def list_filtered(
        self,
        *,
        title: str | None = None,
        report_type: str | None = None,
        instance_id: uuid.UUID | None = None,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[ReportExecutionLog]:
        stmt = sa.select(ReportExecutionLog)
        if title:
            stmt = stmt.where(ReportExecutionLog.report_title.ilike(f"%{title}%"))
        if report_type:
            stmt = stmt.where(ReportExecutionLog.report_type == report_type)
        if instance_id:
            stmt = stmt.where(ReportExecutionLog.instance_id == instance_id)
        if from_dt:
            stmt = stmt.where(ReportExecutionLog.executed_at >= from_dt)
        if to_dt:
            stmt = stmt.where(ReportExecutionLog.executed_at <= to_dt)
        stmt = stmt.order_by(ReportExecutionLog.executed_at.desc()).limit(limit).offset(offset)
        return list((await self._session.execute(stmt)).scalars().all())
