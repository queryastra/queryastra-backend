"""Configuracao da aplicacao.

Toda configuracao vem de variavel de ambiente (ou `.env` local, nunca commitado) — nunca
hardcoded no codigo. Ver `Docs/rules/01-credential-security.md`: `DATABASE_URL` e a conexao da
aplicacao com o TimescaleDB de persistencia, nao deve ser logada por completo (ver
`app/core/logging.py`).
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuracao central, lida de variaveis de ambiente / arquivo `.env`."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = Field(default="development")

    # Conexao com o TimescaleDB de persistencia da aplicacao (nao confundir com as instancias
    # PostgreSQL/SQL Server monitoradas, que sao cadastradas em `instances`).
    database_url: str = Field(
        default="postgresql+asyncpg://ab_xql_pm:changeme@localhost:5432/ab_xql_pm",
        alias="DATABASE_URL",
    )

    api_v1_prefix: str = Field(default="/api/v1", alias="API_V1_PREFIX")
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Base do cofre de segredos usado para resolver `instances.encrypted_credential_ref`
    # (Docs/rules/01-credential-security.md). Vazio no MVP ate o cofre corporativo ser definido
    # (Q5, ARCHITECTURE_SPEC.md — decisao pendente).
    credential_vault_base_url: str = Field(default="", alias="CREDENTIAL_VAULT_BASE_URL")

    # Chave Fernet (fora do banco) usada para cifrar/decifrar o fallback de credencial
    # (`auth_method='encrypted_fallback'`) — `app/security/credential_encryption.py`,
    # `app/security/credential_reference.py`. Sem default: falha no boot se ausente
    # (Docs/rules/01-credential-security.md — chave nunca pode viver no banco/codigo).
    credential_encryption_key: str = Field(alias="CREDENTIAL_ENCRYPTION_KEY")

    # Timeout por query de coleta e limite de linhas por resultado (Docs/rules/02-safe-collection.md
    # — coleta somente leitura, nunca deve travar ou sobrecarregar a instancia monitorada).
    collection_query_timeout_seconds: float = Field(
        default=5.0, alias="COLLECTION_QUERY_TIMEOUT_SECONDS"
    )
    collection_max_rows: int = Field(default=5000, alias="COLLECTION_MAX_ROWS")

    # Limites da API de leitura contra a hypertable propria da aplicacao (nao confundir com
    # `collection_max_rows`, que e do lado da coleta contra a instancia monitorada —
    # responsabilidade diferente).
    api_read_max_rows: int = Field(default=2000, alias="API_READ_MAX_ROWS")
    api_read_default_lookback_seconds: int = Field(
        default=3600, alias="API_READ_DEFAULT_LOOKBACK_SECONDS"
    )


@lru_cache
def get_settings() -> Settings:
    """Retorna a configuracao (cacheada por processo)."""
    return Settings()
