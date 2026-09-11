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

    # Liga o supervisor de coleta continua (PollingSupervisor) no lifespan do backend. Default
    # false - nunca ligar coleta real fora de um ambiente explicitamente configurado para isso.
    polling_supervisor_enabled: bool = Field(default=False, alias="POLLING_SUPERVISOR_ENABLED")
    # Intervalo (segundos) de reconciliacao de instancias ativas pelo supervisor.
    supervisor_discovery_interval_seconds: int = Field(
        default=15, alias="SUPERVISOR_DISCOVERY_INTERVAL_SECONDS"
    )

    # Liga o worker de avaliacao continua de alertas (AlertingWorker) no lifespan do backend.
    # Default false - nunca ligar avaliacao real fora de um ambiente explicitamente configurado
    # para isso.
    alerting_worker_enabled: bool = Field(default=False, alias="ALERTING_WORKER_ENABLED")
    # Intervalo (segundos) entre ciclos de avaliacao de alert_rules.
    alerting_evaluation_interval_seconds: int = Field(
        default=60, alias="ALERTING_EVALUATION_INTERVAL_SECONDS"
    )
    # Intervalo (segundos) minimo entre lembretes de um alerta critico nao reconhecido.
    alert_critical_reminder_interval_seconds: int = Field(
        default=900, alias="ALERT_CRITICAL_REMINDER_INTERVAL_SECONDS"
    )
    # URL do webhook de notificacao de alerta. Vazio/None = canal webhook desabilitado (notificacao
    # so logada nesse canal).
    alert_webhook_url: str | None = Field(default=None, alias="ALERT_WEBHOOK_URL")

    # Liga o supervisor de checagem de indices ausentes (MissingIndexSupervisor,
    # ARCHITECTURE_SPEC.md Q18/Q21) no lifespan do backend. Default false - mesma politica de
    # seguranca dos demais workers (nunca ligar coleta real sem configuracao explicita).
    missing_index_supervisor_enabled: bool = Field(
        default=False, alias="MISSING_INDEX_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre checagens de indice ausente por instancia ativa. Default 1 dia -
    # o proprio Monitoramento Autbank (Docs/monitoramento-autbank-analysis.md §1) ja roda essa
    # familia de checagem 1x/dia ou 1x/semana, nao a cada poucos segundos.
    missing_index_check_interval_seconds: int = Field(
        default=86400, alias="MISSING_INDEX_CHECK_INTERVAL_SECONDS"
    )
    # Thresholds da heuristica PostgreSQL (Docs/collector-queries.md §10) - sem DMV nativa
    # equivalente ao SQL Server, entao filtramos por tamanho de tabela e volume de seq_scan para
    # nao sinalizar tabela pequena/pouco acessada.
    missing_index_pg_min_table_size_bytes: int = Field(
        default=10_000_000, alias="MISSING_INDEX_PG_MIN_TABLE_SIZE_BYTES"
    )
    missing_index_pg_min_seq_scans: int = Field(default=100, alias="MISSING_INDEX_PG_MIN_SEQ_SCANS")

    # Liga o supervisor de checagem de backup/archiving (BackupStatusSupervisor,
    # ARCHITECTURE_SPEC.md Q16) no lifespan do backend. Default false - mesma politica de
    # seguranca dos demais workers.
    backup_status_supervisor_enabled: bool = Field(
        default=False, alias="BACKUP_STATUS_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre checagens de backup por instancia ativa. Default 1 dia - mesma
    # cadencia baixa do MissingIndexSupervisor, backup nao muda a cada poucos segundos.
    backup_status_check_interval_seconds: int = Field(
        default=86400, alias="BACKUP_STATUS_CHECK_INTERVAL_SECONDS"
    )

    # Canal de e-mail de notificacao de alerta (`app.workers.alerting_worker`). Todos vazios por
    # padrao = canal desabilitado (nenhum envio real, mesmo default seguro do webhook). So
    # `alert_smtp_host` habilita o envio; `alert_email_to` (lista separada por virgula) precisa
    # estar preenchido para ter destinatario de verdade.
    alert_smtp_host: str | None = Field(default=None, alias="ALERT_SMTP_HOST")
    alert_smtp_port: int = Field(default=587, alias="ALERT_SMTP_PORT")
    alert_smtp_username: str | None = Field(default=None, alias="ALERT_SMTP_USERNAME")
    alert_smtp_password: str | None = Field(default=None, alias="ALERT_SMTP_PASSWORD")
    alert_smtp_from: str | None = Field(default=None, alias="ALERT_SMTP_FROM")
    # lista separada por virgula
    alert_email_to: str | None = Field(default=None, alias="ALERT_EMAIL_TO")


@lru_cache
def get_settings() -> Settings:
    """Retorna a configuracao (cacheada por processo)."""
    return Settings()
