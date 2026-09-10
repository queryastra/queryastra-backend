"""Parametros de conexao usados por um `Collector` — nunca uma senha em texto puro persistida.

`ConnectionParams` e o unico jeito de passar credencial resolvida (`ResolvedCredential`,
`app/security/credential_reference.py`) ou credencial de teste pontual (fluxo de
cadastro/teste de conexao, `app/services/instance_service.py`) para um adaptador. `secret_value`
nunca aparece em `repr()`/log (Docs/rules/01-credential-security.md).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ConnectionParams:
    """Parametros minimos para abrir uma conexao de coleta com uma instancia monitorada."""

    host: str
    port: int
    auth_method: str  # "integrated" | "encrypted_fallback"
    username: str | None = None
    # `repr=False`: nunca aparece em repr()/log, mesmo acidentalmente
    # (Docs/rules/01-credential-security.md).
    secret_value: str | None = field(default=None, repr=False)
    connect_timeout_seconds: float = 5.0
