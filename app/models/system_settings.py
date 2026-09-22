"""Modelo `SystemSettings` — primeira preferência global editável pelo usuário, persistida no
banco (não `.env`/`app/core/config.py`) — tela "Configurações → Geral" no frontend.

Tabela SINGLETON (1 linha só, `id` fixo em `1` via CHECK) — mais simples que o `system_settings`
genérico chave/valor documentado em `Docs/database-schema.md` §5 (Fase 2, nunca implementado):
hoje só existe UM parâmetro global editável, então uma coluna própria (`blocking_min_duration_ms`)
é direta e tipada, sem precisar de serialização de `value`/`value_type`. Se mais preferências
globais surgirem, cada uma ganha sua própria coluna aqui — revisar para o modelo chave/valor só
se isso crescer o suficiente para justificar.

Lida a cada ciclo de coleta por `PollingWorker`/`PollingSupervisor` (não cacheada como
`app.core.config.Settings`) — uma mudança feita na tela vale a partir do próximo ciclo de poll,
sem precisar reiniciar o backend.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, SmallInteger
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base

SYSTEM_SETTINGS_SINGLETON_ID = 1


class SystemSettings(Base):
    """Linha única (`id=1`) de preferências globais editáveis pelo usuário."""

    __tablename__ = "system_settings"
    __table_args__ = (CheckConstraint("id = 1", name="ck_system_settings_singleton"),)

    id: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True, default=SYSTEM_SETTINGS_SINGLETON_ID
    )
    # Tempo mínimo de espera (ms) para um par bloqueador/bloqueado ser persistido em
    # `blocking_events` — filtra ruído de bloqueios muito curtos/irrelevantes. 0 = sem filtro
    # (comportamento anterior a esta feature, captura tudo).
    blocking_min_duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Tempo mínimo (ms) que uma transação precisa estar aberta E com a sessão ociosa (sem request
    # ativa/"sleeping" no SQL Server, "idle in transaction" no PostgreSQL) para ganhar a tag
    # "sleeping" em `transaction_events.flag_reason` — mesmo padrão/convenção de
    # `blocking_min_duration_ms` (0 = sem filtro, marca toda transação ociosa imediatamente).
    # Diferente de `transaction_orphan_threshold_minutes` (`app/core/config.py`, `.env`, global
    # fixo, sem override) — este é editável na UI e tem override por instância/banco.
    sleeping_min_duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
