"""Base comum de repositorio — `Docs/specs/02-backend-contract.md` ("repositories: TimescaleDB").

Repositorios encapsulam acesso a dados; servicos (`app/services`) nao devem montar SQL/ORM query
diretamente. Cada repositorio concreto recebe uma `AsyncSession` (`app/db/session.py`) por
metodo ou construtor, conforme o padrao de unit-of-work escolhido na implementacao real.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession


class BaseRepository:
    """Repositorio base: guarda a sessao assincrona injetada pela camada de service/API."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
