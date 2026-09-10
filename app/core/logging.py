"""Logging estruturado da aplicacao.

Regra critica (Docs/rules/03-sql-privacy.md, Docs/rules/01-credential-security.md):

    NUNCA logar:
      - texto de SQL bruto (nao mascarado) capturado de uma instancia monitorada;
      - senha, token, ou connection string completa;
      - `instances.encrypted_credential_ref` resolvido (o segredo em si).

    Logs devem conter identificadores tecnicos (instance_id, sql_hash, nome da capacidade
    afetada) e o motivo do erro — nunca o dado sensivel associado. Ver
    `app/masking/sql_masking.py` para o texto de SQL que e seguro logar/persistir.
"""

from __future__ import annotations

import logging
import sys

from app.core.config import get_settings

_CONFIGURED = False


def configure_logging() -> None:
    """Configura o logging estruturado da aplicacao (idempotente)."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    settings = get_settings()
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )

    root = logging.getLogger()
    root.setLevel(settings.log_level.upper())
    root.handlers = [handler]

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Retorna um logger nomeado, garantindo que a configuracao ja foi aplicada."""
    configure_logging()
    return logging.getLogger(name)
