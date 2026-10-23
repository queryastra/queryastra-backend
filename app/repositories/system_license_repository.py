"""Repositorio para `SystemLicense` — acesso ao registro singleton de licenca/plano."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.system_license import SystemLicense


class SystemLicenseRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_or_create(self) -> SystemLicense:
        result = await self._session.execute(select(SystemLicense).limit(1))
        row = result.scalar_one_or_none()
        if row is None:
            row = SystemLicense(plan_id="trial", trial_started_at=datetime.now(UTC))
            self._session.add(row)
            await self._session.flush()
        return row

    async def get(self) -> SystemLicense | None:
        result = await self._session.execute(select(SystemLicense).limit(1))
        return result.scalar_one_or_none()

    async def update_plan(
        self,
        plan_id: str,
        subscription_expires_at: datetime | None = None,
        license_key: str | None = None,
    ) -> SystemLicense:
        row = await self.get_or_create()
        row.plan_id = plan_id
        row.subscription_expires_at = subscription_expires_at
        if license_key is not None:
            row.license_key = license_key
        await self._session.flush()
        return row
