"""Modelo `system_license` — registro singleton com plano e datas da licenca do sistema."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SystemLicense(Base):
    __tablename__ = "system_license"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    plan_id: Mapped[str] = mapped_column(String(20), nullable=False, default="trial")
    trial_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    subscription_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    license_key: Mapped[str | None] = mapped_column(String(30), nullable=True)
