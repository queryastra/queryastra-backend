"""Repositorio de `Server` (host monitorado) — `Docs/database-schema.md` §1.

Ver `app/models/server.py` para os campos. `get_or_create_by_hostname` e usado pelo endpoint de
ingestao de metricas de host (`app/api/v1/hosts.py`) — diferente de `Instance`, um `Server` nao
exige cadastro manual previo: a primeira metrica recebida de um hostname novo o auto-registra.
"""

from __future__ import annotations

from sqlalchemy import select

from app.models.server import Server
from app.repositories.base import BaseRepository


class ServersRepository(BaseRepository):
    """Acesso a dados de servidores/hosts monitorados."""

    async def get_by_hostname(self, hostname: str) -> Server | None:
        result = await self._session.execute(select(Server).where(Server.hostname == hostname))
        return result.scalar_one_or_none()

    async def create_server(self, server: Server) -> Server:
        self._session.add(server)
        await self._session.commit()
        await self._session.refresh(server)
        return server

    async def list_active(self) -> list[Server]:
        result = await self._session.execute(select(Server).where(Server.active.is_(True)))
        return list(result.scalars().all())

    async def get_or_create_by_hostname(self, hostname: str, os_type: str) -> Server:
        """Busca por hostname; se nao existir, cria com `active=True` e o `os_type` informado.

        Usado pelo endpoint de ingestao — a primeira metrica de um host novo o auto-registra.
        `exporter_endpoint` (`Docs/database-schema.md` §1) documenta host:port de um
        node_exporter/windows_exporter scrapeado; nesse fluxo de push nao existe endpoint
        scrapeado nenhum, entao grava um placeholder (`push:<hostname>`) so para satisfazer a
        coluna `NOT NULL` — nunca usado para conectar em lugar nenhum.
        """
        existing = await self.get_by_hostname(hostname)
        if existing is not None:
            return existing
        return await self.create_server(
            Server(
                hostname=hostname,
                os_type=os_type,
                exporter_endpoint=f"push:{hostname}",
                active=True,
            )
        )
