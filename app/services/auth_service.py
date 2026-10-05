"""Servico de autenticacao — hashing de senha e JWT.

Usado em `app/api/v1/auth.py` (endpoints de login) e `app/api/deps.py` (dependencias de
autorizacao). Nao depende do banco — opera apenas sobre strings e configuracoes.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from jose import jwt
from passlib.context import CryptContext

from app.core.config import get_settings

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    """Retorna o hash bcrypt de `plain`."""
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Verifica se `plain` corresponde ao hash `hashed`."""
    return _pwd_context.verify(plain, hashed)


def create_access_token(user_id: int, username: str, role: str) -> str:
    """Cria um JWT assinado com o segredo configurado.

    Payload: sub=str(user_id), username=username, role=role, exp=agora+expire_hours.
    """
    settings = get_settings()
    expire = datetime.now(UTC) + timedelta(hours=settings.jwt_expire_hours)
    payload = {
        "sub": str(user_id),
        "username": username,
        "role": role,
        "exp": expire,
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    """Decodifica e valida o JWT. Lanca `jose.JWTError` se invalido ou expirado."""
    settings = get_settings()
    return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
