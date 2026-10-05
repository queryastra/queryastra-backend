"""Modelo de usuario para autenticacao e RBAC.

Tres papeis suportados: 'admin' | 'operator' | 'viewer'. Ver `app/api/deps.py` para as
dependencias de autorizacao e `app/services/auth_service.py` para hashing/JWT.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="viewer")
    # role values: "admin" | "operator" | "viewer"
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    is_active: Mapped[bool] = mapped_column(nullable=False, default=True)
