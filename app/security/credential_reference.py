"""Referencia segura a credencial de coleta — `Docs/rules/01-credential-security.md`.

Regra critica: esta camada so aceita e resolve uma REFERENCIA opaca a segredo
(`instances.encrypted_credential_ref`), nunca uma senha em texto puro. Nao existe, e nao deve
existir, nenhuma funcao aqui que aceite um parametro `password: str`. Se um dia uma senha em
texto puro chegar a este modulo, isso e um bug de arquitetura, nao um caso de uso valido.

"Nunca gravar senha em `instances`, logs, payloads, mensagens de erro ou arquivos de exemplo.
Preferir cofre corporativo por referencia; fallback criptografado deve ter chave fora do banco."
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import NewType

from cryptography.fernet import Fernet, InvalidToken

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Tipo distinto de `str` só para deixar explícito, no type-checker, que este valor é uma
# referência opaca a um segredo — nunca a senha em si. Não desembrulhar isso em `str` puro fora
# desta camada sem necessidade.
CredentialReference = NewType("CredentialReference", str)


@dataclass(frozen=True, slots=True)
class ResolvedCredential:
    """Credencial resolvida em memoria, pronta para abrir uma conexao.

    Deliberadamente sem `__repr__`/`__str__` customizado que exponha `secret_value` — o dataclass
    padrão já usaria repr() nos campos, então nunca fazer `logger.info(str(resolved_credential))`
    ou logar este objeto diretamente. Usar apenas para abrir a conexão e descartar.
    """

    username: str
    secret_value: str


class CredentialResolutionError(Exception):
    """Falha ao resolver uma `CredentialReference` — nunca deve carregar o segredo na mensagem."""


class CredentialResolver:
    """Resolve uma `CredentialReference` em uma `ResolvedCredential`, sem nunca logar o segredo.

    Stub do MVP: a Fase 1 usa `encrypted_fallback` (criptografia simetrica simples, chave fora do
    banco) como caminho principal; um cofre corporativo (Vault/Key Vault/Secrets Manager) fica
    pendente (Q5 do ARCHITECTURE_SPEC.md) até o padrão interno do Autbank ser confirmado.
    """

    async def resolve(self, reference: CredentialReference) -> ResolvedCredential:
        """Resolve a referencia em uma credencial utilizavel.

        Args:
            reference: valor opaco de `instances.encrypted_credential_ref`. Nunca uma senha.

        Raises:
            CredentialResolutionError: referencia invalida, corrompida, ou cifrada com uma chave
                diferente da `CREDENTIAL_ENCRYPTION_KEY` atual.
        """
        logger.info("Resolvendo referencia de credencial (reference_len=%d)", len(reference))
        try:
            plaintext = Fernet(get_settings().credential_encryption_key.encode()).decrypt(
                reference.encode()
            )
        except InvalidToken as exc:
            raise CredentialResolutionError(
                "Referencia de credencial invalida ou corrompida"
            ) from exc
        payload = json.loads(plaintext)
        return ResolvedCredential(
            username=payload["username"], secret_value=payload["secret_value"]
        )
