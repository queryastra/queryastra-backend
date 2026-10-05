"""Testes unitarios do servico de autenticacao — hashing e JWT.

Sem banco: apenas logica pura de criptografia/token.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from jose import JWTError, jwt

from app.core.config import get_settings
from app.services.auth_service import (
    create_access_token,
    decode_token,
    hash_password,
    verify_password,
)


def test_hash_and_verify_password() -> None:
    """Hash de 'secret' deve verificar 'secret' (True) e 'wrong' (False)."""
    hashed = hash_password("secret")
    assert hashed != "secret"  # nunca armazena em claro
    assert verify_password("secret", hashed) is True
    assert verify_password("wrong", hashed) is False


def test_create_and_decode_token() -> None:
    """Token criado para user_id=1/admin deve decodificar com os campos corretos."""
    token = create_access_token(user_id=1, username="admin", role="admin")
    payload = decode_token(token)
    assert payload["sub"] == "1"
    assert payload["username"] == "admin"
    assert payload["role"] == "admin"
    # exp deve ser futuro
    exp = datetime.fromtimestamp(payload["exp"], tz=UTC)
    assert exp > datetime.now(UTC)


def test_decode_expired_token() -> None:
    """Token com exp no passado deve lancar JWTError."""
    settings = get_settings()
    expired_payload = {
        "sub": "1",
        "username": "admin",
        "role": "admin",
        "exp": datetime.now(UTC) - timedelta(seconds=1),
    }
    expired_token = jwt.encode(
        expired_payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm
    )
    with pytest.raises(JWTError):
        decode_token(expired_token)
