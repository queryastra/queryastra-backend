"""Round-trip de `encrypt_credential` -> `CredentialResolver.resolve`.

Regra critica (Docs/rules/01-credential-security.md): a senha em texto puro nunca deve aparecer
na referencia opaca persistida em `instances.encrypted_credential_ref`.
"""

from __future__ import annotations

import pytest

from app.security.credential_encryption import encrypt_credential
from app.security.credential_reference import CredentialReference, CredentialResolver


async def test_round_trip_recovers_original_username_and_password() -> None:
    reference = encrypt_credential("monitoring_user", "correct horse battery staple")

    resolved = await CredentialResolver().resolve(reference)

    assert resolved.username == "monitoring_user"
    assert resolved.secret_value == "correct horse battery staple"


def test_encrypted_reference_never_contains_plaintext_password() -> None:
    password = "correct horse battery staple"
    reference = encrypt_credential("monitoring_user", password)

    assert password not in reference
    assert "monitoring_user" not in reference


async def test_tampered_reference_raises_credential_resolution_error() -> None:
    from app.security.credential_reference import CredentialResolutionError

    reference = encrypt_credential("monitoring_user", "a-secret")
    tampered = CredentialReference(reference[:-1] + ("A" if reference[-1] != "A" else "B"))

    with pytest.raises(CredentialResolutionError):
        await CredentialResolver().resolve(tampered)
