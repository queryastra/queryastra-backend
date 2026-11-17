"""Repositorio de usuarios — CRUD para autenticacao/RBAC.

Ver `app/models/user.py` para o modelo e `app/services/auth_service.py` para hashing/JWT.
"""

from __future__ import annotations

from sqlalchemy import func, select

from app.models.user import User
from app.repositories.base import BaseRepository


class UsersRepository(BaseRepository):
    """Repositorio de acesso a `users`."""

    async def get_by_username(self, username: str) -> User | None:
        """Busca usuario pelo username (case-sensitive). Retorna `None` se nao encontrado."""
        result = await self._session.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()

    async def get_by_id(self, user_id: int) -> User | None:
        """Busca usuario pelo id. Retorna `None` se nao encontrado."""
        result = await self._session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    async def create(
        self, username: str, hashed_password: str, role: str, email: str | None = None
    ) -> User:
        """Cria um novo usuario. A sessao deve ser commitada pelo chamador."""
        user = User(username=username, hashed_password=hashed_password, role=role, email=email)
        self._session.add(user)
        await self._session.flush()
        await self._session.refresh(user)
        return user

    async def get_by_email(self, email: str) -> User | None:
        """Busca usuario pelo e-mail (case-insensitive). Retorna `None` se nao encontrado."""
        result = await self._session.execute(
            select(User).where(func.lower(User.email) == email.strip().lower())
        )
        return result.scalar_one_or_none()

    async def update_email(self, user_id: int, email: str | None) -> User | None:
        """Define (ou limpa, com `None`) o e-mail do usuario. A sessao deve ser commitada pelo
        chamador. Unicidade e validada pelo chamador (`get_by_email`) e pela constraint."""
        user = await self.get_by_id(user_id)
        if user is None:
            return None
        user.email = email
        await self._session.flush()
        await self._session.refresh(user)
        return user

    async def list_active_admin_emails(self) -> list[str]:
        """E-mails dos administradores ativos que tem e-mail cadastrado."""
        result = await self._session.execute(
            select(User.email).where(
                User.role == "admin", User.is_active.is_(True), User.email.is_not(None)
            )
        )
        return [email for email in result.scalars().all() if email]

    async def list_all(self) -> list[User]:
        """Lista todos os usuarios, ordenados por id."""
        result = await self._session.execute(select(User).order_by(User.id))
        return list(result.scalars().all())

    async def update_role(self, user_id: int, role: str) -> User | None:
        """Altera o papel de um usuario. Retorna o usuario atualizado ou `None` se nao existe."""
        user = await self.get_by_id(user_id)
        if user is None:
            return None
        user.role = role
        await self._session.flush()
        await self._session.refresh(user)
        return user

    async def deactivate(self, user_id: int) -> User | None:
        """Desativa um usuario (`is_active=False`). Retorna o usuario ou `None` se nao existe."""
        user = await self.get_by_id(user_id)
        if user is None:
            return None
        user.is_active = False
        await self._session.flush()
        await self._session.refresh(user)
        return user

    async def reactivate(self, user_id: int) -> User | None:
        """Reativa um usuario (`is_active=True`). Retorna o usuario ou `None` se nao existe."""
        user = await self.get_by_id(user_id)
        if user is None:
            return None
        user.is_active = True
        await self._session.flush()
        await self._session.refresh(user)
        return user

    async def delete(self, user_id: int) -> bool:
        """Remove permanentemente um usuario. Retorna `True` se removido, `False` se nao existe."""
        user = await self.get_by_id(user_id)
        if user is None:
            return False
        await self._session.delete(user)
        await self._session.flush()
        return True
