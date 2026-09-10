"""Cifra o par (username, senha) do fallback de credencial em uma `CredentialReference` opaca.

Fase 1 do MVP (Q5 do ARCHITECTURE_SPEC.md ainda pendente para cofre corporativo): criptografia
simetrica Fernet, chave fora do banco (`CREDENTIAL_ENCRYPTION_KEY`, `app/core/config.py`).
Complementa `app/security/credential_reference.py` (que so *resolve* a referencia) — a senha em
texto puro so passa por esta funcao no momento do cadastro, nunca e persistida diretamente
(Docs/rules/01-credential-security.md).
"""

from __future__ import annotations

import json

from cryptography.fernet import Fernet

from app.core.config import get_settings
from app.core.logging import get_logger
from app.security.credential_reference import CredentialReference

logger = get_logger(__name__)


def encrypt_credential(username: str, password: str) -> CredentialReference:
    """Criptografa username+password com Fernet e devolve referencia opaca para persistir em
    `instances.encrypted_credential_ref`. Nunca loga a senha.
    """
    payload = json.dumps({"username": username, "secret_value": password}).encode()
    token = Fernet(get_settings().credential_encryption_key.encode()).encrypt(payload)
    logger.info("Credencial de fallback cifrada (username_len=%d)", len(username))
    return CredentialReference(token.decode())
