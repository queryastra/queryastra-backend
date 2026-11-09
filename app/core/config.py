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
        default="postgresql+asyncpg://queryastra:changeme@localhost:5432/queryastra",
        alias="DATABASE_URL",
    )

    api_v1_prefix: str = Field(default="/api/v1", alias="API_V1_PREFIX")
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3001"])

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

    # Tamanho do thread pool padrao do event loop (usado por TODA chamada `asyncio.to_thread` do
    # processo — pymssql/pyodbc sao sincronos, cada coleta bloqueante ocupa 1 thread ate
    # terminar). O default do proprio Python (`min(32, os.cpu_count() + 4)`) e baixo demais para
    # varias instancias reais coletando em paralelo (confirmado ao vivo: com 7 instancias a
    # polling_interval_seconds=1 + supervisores de baixa frequencia, o pool padrao de 12 threads
    # nesta maquina de 8 nucleos saturava a ponto de ate leituras simples do proprio Postgres da
    # aplicacao (sem nenhuma chamada bloqueante) sofrerem timeout). Aumentar isso NAO elimina o
    # problema de fundo (poll agressivo demais continua caro), so da mais folga.
    backend_executor_max_workers: int = Field(default=64, alias="BACKEND_EXECUTOR_MAX_WORKERS")

    # Tamanho do thread pool DEDICADO por instancia SQL Server (`app/core/instance_executors.py`)
    # — mitigacao para uma chamada `pymssql` genuinamente travada (sem timeout confiavel no nivel
    # do socket/SO, ver docstring do modulo) nunca esgotar a capacidade de OUTRAS instancias.
    # Pequeno de proposito: cada instancia normalmente so tem 1 chamada em voo por vez
    # (PollingWorker sequencial) + os supervisores de baixa frequencia que tambem tocam essa
    # mesma instancia esporadicamente — poucas vagas bastam pro caso saudavel, e uma instancia
    # travada consumir todas as SUAS proprias vagas e o comportamento aceito (ela para de
    # coletar sozinha, sem afetar mais ninguem).
    instance_executor_max_workers: int = Field(default=6, alias="INSTANCE_EXECUTOR_MAX_WORKERS")

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

    # Base URL da instancia do AB SQL Insight (Etapa 10/11 - painel "Analise SQL Insight" no
    # detalhe de query). Vazio/None = capacidade desligada (painel simplesmente nao aparece, sem
    # erro). Ex.: http://autklinux4:9100
    sql_insight_base_url: str | None = Field(default=None, alias="SQL_INSIGHT_BASE_URL")
    # Timeout curto (segundos) para a chamada HTTP ao SQL Insight - qualquer erro (timeout, 5xx,
    # servico fora) degrada para "indisponivel", nunca derruba a tela de detalhe de query.
    sql_insight_timeout_seconds: float = Field(default=3.0, alias="SQL_INSIGHT_TIMEOUT_SECONDS")

    # ── IA para diagnóstico contextual de queries (painel "Recomendações") ────────────
    # Provedor de IA: "openai" (padrão; também serve Azure OpenAI e Ollama via AI_BASE_URL),
    # "anthropic" (Anthropic Claude). Sem configuração = painel aparece com mensagem
    # instruindo como ativar, nunca um erro.
    ai_provider: str | None = Field(default=None, alias="AI_PROVIDER")
    # Chave de API do provedor (obrigatória para openai/anthropic; Ollama self-hosted dispensa).
    ai_api_key: str | None = Field(default=None, alias="AI_API_KEY")
    # Base URL customizada — para Azure OpenAI (ex.: https://meu-recurso.openai.azure.com)
    # ou Ollama (ex.: http://localhost:11434). Vazio = endpoint padrão do provedor.
    ai_base_url: str | None = Field(default=None, alias="AI_BASE_URL")
    # Modelo a usar. Padrão por provedor: openai→gpt-4o-mini, anthropic→claude-haiku-5-5.
    ai_model: str | None = Field(default=None, alias="AI_MODEL")
    # Timeout (segundos) para a chamada ao provedor de IA.
    ai_timeout_seconds: float = Field(default=20.0, alias="AI_TIMEOUT_SECONDS")

    # Liga o supervisor de checagem de indices ausentes (MissingIndexSupervisor,
    # ARCHITECTURE_SPEC.md Q18/Q21) no lifespan do backend. Default false - mesma politica de
    # seguranca dos demais workers (nunca ligar coleta real sem configuracao explicita).
    missing_index_supervisor_enabled: bool = Field(
        default=False, alias="MISSING_INDEX_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre checagens de indice ausente por instancia ativa. Default 1 dia -
    # o monitoramento legado (Docs/monitoramento-bk-analysis.md §1) ja roda essa
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

    # Etapa 10 (rastreamento de transacoes) - a partir de quantos minutos aberta uma transacao
    # vira candidata a "orfa" (`is_orphan_candidate` em GET /transactions/{instance_id}). So um
    # destaque visual/ordenacao - nunca fecha nem interfere na transacao de verdade.
    transaction_orphan_threshold_minutes: int = Field(
        default=5, alias="TRANSACTION_ORPHAN_THRESHOLD_MINUTES"
    )

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

    # Etapa 10, redesenho: por quanto tempo reter transacoes/bloqueios ja ENCERRADOS e deadlocks
    # antes de expurgar de vez (app/workers/problem_events_retention_worker.py). Confirmado com o
    # usuario: 90 dias — mesma ordem de grandeza ja usada no projeto para dado de detalhe
    # (retention_policy de metric_samples e 30 dias; escolhido um valor maior aqui porque
    # locks/transacoes/deadlocks problematicos sao eventos raros, nao volumosos como metricas).
    problem_events_retention_days: int = Field(default=90, alias="PROBLEM_EVENTS_RETENTION_DAYS")
    # Liga o worker de expurgo (ProblemEventsRetentionWorker) no lifespan do backend. Default
    # false - mesma politica de seguranca dos demais workers.
    problem_events_retention_worker_enabled: bool = Field(
        default=False, alias="PROBLEM_EVENTS_RETENTION_WORKER_ENABLED"
    )
    # Intervalo (segundos) entre rodadas de expurgo. Default 1 dia - expurgo por idade nao precisa
    # rodar com mais frequencia que isso.
    problem_events_retention_check_interval_seconds: int = Field(
        default=86400, alias="PROBLEM_EVENTS_RETENTION_CHECK_INTERVAL_SECONDS"
    )

    # Etapa 10, item 7 (Deadlocks) - liga o supervisor de extracao de deadlocks
    # (DeadlockSupervisor, so SQL Server) no lifespan do backend. Default false - mesma politica
    # de seguranca dos demais workers.
    deadlock_supervisor_enabled: bool = Field(default=False, alias="DEADLOCK_SUPERVISOR_ENABLED")
    # Intervalo (segundos) entre leituras do ring buffer system_health por instancia ativa.
    # Default 5 minutos - o ring buffer ja retem historico recente sozinho, nao precisa ser
    # raspado a cada poucos segundos como a coleta de alta frequencia.
    deadlock_check_interval_seconds: int = Field(
        default=300, alias="DEADLOCK_CHECK_INTERVAL_SECONDS"
    )
    # Timeout dedicado (nao reaproveita `collection_query_timeout_seconds`, pensado pra polling de
    # alta frequencia) - ler o target `event_file` via `sys.fn_xe_file_target_read_file` exige
    # abrir e escanear arquivos `.xel` em disco (varios MB cada, tipicamente uns 4-5 arquivos
    # retidos), bem mais lento que o antigo `ring_buffer` em memoria. Confirmado ao vivo: com o
    # timeout generico (5s) varias instancias reais estouravam esse limite. 30s e generoso o
    # suficiente pra essa checagem de baixa frequencia (1x a cada `deadlock_check_interval_seconds`,
    # nunca por instancia mais de uma vez por ciclo) sem arriscar travar o supervisor.
    deadlock_query_timeout_seconds: float = Field(
        default=30.0, alias="DEADLOCK_QUERY_TIMEOUT_SECONDS"
    )

    # Etapa 10, item 3 (captura confiavel de erro por transacao) - liga o supervisor de sessao
    # Extended Events dedicada (TransactionErrorSupervisor, so SQL Server) no lifespan do backend.
    # Default false - mesma politica de seguranca dos demais workers.
    transaction_error_supervisor_enabled: bool = Field(
        default=False, alias="TRANSACTION_ERROR_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre leituras da sessao dedicada por instancia ativa. Default 2
    # minutos - mais frequente que deadlock (5min) porque um erro de transacao e mais comum e
    # mais util de ver rapido, mas ainda baixa frequencia (nao a cada poucos segundos).
    transaction_error_check_interval_seconds: int = Field(
        default=120, alias="TRANSACTION_ERROR_CHECK_INTERVAL_SECONDS"
    )
    # Timeout dedicado - mesmo racional/valor de `deadlock_query_timeout_seconds` (ler `.xel` em
    # disco tem o mesmo perfil de custo do target de deadlock).
    transaction_error_query_timeout_seconds: float = Field(
        default=30.0, alias="TRANSACTION_ERROR_QUERY_TIMEOUT_SECONDS"
    )

    # Autenticacao JWT (RBAC — app/services/auth_service.py).
    jwt_secret_key: str = Field(
        default="change-me-in-production-use-a-long-random-string", alias="JWT_SECRET_KEY"
    )
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    jwt_expire_hours: int = Field(default=8, alias="JWT_EXPIRE_HOURS")
    # Credenciais do usuario administrador criado automaticamente no startup.
    admin_username: str = Field(default="admin", alias="ADMIN_USERNAME")
    admin_password: str = Field(default="admin", alias="ADMIN_PASSWORD")

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

    # Fase 2, item 2 (rule_type="custom_sql") - timeout de QUERY para a execucao periodica de SQL
    # customizado escrito pelo usuario (`Collector.run_scalar_query`). Independente de
    # `collection_query_timeout_seconds` porque e um subsistema diferente (avaliacao de alerta,
    # nao coleta de rotina) — pode ter um timeout mais generoso sem afetar o ciclo de poll.
    custom_sql_alert_timeout_seconds: float = Field(
        default=5.0, alias="CUSTOM_SQL_ALERT_TIMEOUT_SECONDS"
    )

    # Fase 2, item 2 (catalogo de alertas expandido: freespace + replicacao) - liga o worker de
    # baixa frequencia que coleta essas metricas (ExtendedHealthMetricsSupervisor). Default false -
    # mesma politica de seguranca dos demais workers.
    extended_health_metrics_supervisor_enabled: bool = Field(
        default=False, alias="EXTENDED_HEALTH_METRICS_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre coletas de freespace/replicacao por instancia ativa. Default 1
    # hora - freespace/replicacao nao mudam o suficiente para justificar o ciclo de poll rapido,
    # mas tambem nao sao tao estaveis quanto backup/missing-index (1x/dia).
    extended_health_metrics_check_interval_seconds: int = Field(
        default=3600, alias="EXTENDED_HEALTH_METRICS_CHECK_INTERVAL_SECONDS"
    )

    # Fase 2, item 5 (rule_type="plan_compile_timeout") - liga o worker de baixa frequencia que
    # escaneia o cache de planos do SQL Server procurando timeout/limite de memoria de compilacao
    # (PlanCompileTimeoutSupervisor). Default false - mesma politica de seguranca dos demais
    # workers. So SQL Server (Postgres nao tem cache de plano exposto do mesmo jeito).
    plan_compile_timeout_supervisor_enabled: bool = Field(
        default=False, alias="PLAN_COMPILE_TIMEOUT_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre varreduras do cache de planos por instancia ativa. Default 1 dia -
    # mesma cadencia baixa do MissingIndexSupervisor/BackupStatusSupervisor.
    plan_compile_timeout_check_interval_seconds: int = Field(
        default=86400, alias="PLAN_COMPILE_TIMEOUT_CHECK_INTERVAL_SECONDS"
    )

    # Historico de plano de execucao (item priorizado pelo usuario apos a Fase 2) - liga o
    # worker de baixa frequencia que varre o cache de planos do SQL Server procurando MUDANCA de
    # plano por query (PlanHistorySupervisor). Default false - mesma politica de seguranca dos
    # demais workers. So SQL Server (Postgres nao tem cache de plano exposto do mesmo jeito).
    plan_history_supervisor_enabled: bool = Field(
        default=False, alias="PLAN_HISTORY_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre varreduras do cache de planos por instancia ativa. Default 1 dia
    # - mesma cadencia baixa do MissingIndexSupervisor/BackupStatusSupervisor/
    # PlanCompileTimeoutSupervisor (plano nao muda com frequencia suficiente para justificar um
    # ciclo mais curto).
    plan_history_check_interval_seconds: int = Field(
        default=86400, alias="PLAN_HISTORY_CHECK_INTERVAL_SECONDS"
    )

    # Item 4 — Relatorios agendados (ReportWorker). Default false — mesmo padrao de seguranca
    # dos demais workers: nunca liga coleta/geracao automatica sem configuracao explicita.
    report_worker_enabled: bool = Field(default=False, alias="REPORT_WORKER_ENABLED")
    # Intervalo (segundos) com que o worker checa relatorios vencidos. Default 60s (1 min) para
    # nao atrasar demais uma execucao agendada para uma hora exata.
    report_worker_check_interval_seconds: int = Field(
        default=60, alias="REPORT_WORKER_CHECK_INTERVAL_SECONDS"
    )

    # Fase 6 T60 — Trending de queries via pg_stat_statements (QueryStatsSupervisor).
    # Default false — mesma politica de seguranca dos demais workers.
    query_stats_supervisor_enabled: bool = Field(
        default=False, alias="QUERY_STATS_SUPERVISOR_ENABLED"
    )
    # Intervalo (segundos) entre snapshots de pg_stat_statements por instancia ativa.
    # Default 3600s (1h) — resolucao horaria e suficiente para detectar regressoes; mais
    # frequente aumentaria o volume de dados sem ganho proporcional de observabilidade.
    query_stats_check_interval_seconds: int = Field(
        default=3600, alias="QUERY_STATS_CHECK_INTERVAL_SECONDS"
    )

    # Supabase — usado exclusivamente para verificar chaves de licença (tabela `licenses`).
    # Vazio/None = validação de licença desabilitada (modo offline: só aceita plano trial/free).
    supabase_url: str | None = Field(default=None, alias="SUPABASE_URL")
    supabase_service_key: str | None = Field(default=None, alias="SUPABASE_SERVICE_KEY")

    # Stripe — usado para criar sessões do Customer Portal (gerenciar/cancelar assinatura).
    # Vazio/None = funcionalidade desabilitada (botão não aparece na tela de licença).
    stripe_secret_key: str | None = Field(default=None, alias="STRIPE_SECRET_KEY")

    # Auto-update — verifica GitHub Releases periodicamente e exibe banner para admin.
    # Habilitado por default (true); desative com UPDATE_CHECK_ENABLED=false se o servidor
    # nao tiver acesso a internet ou se a politica de seguranca proibir chamadas externas.
    update_check_enabled: bool = Field(default=True, alias="UPDATE_CHECK_ENABLED")
    # Intervalo (segundos) entre verificacoes. Default 86400s (1 dia) — sobrescreva com
    # UPDATE_CHECK_INTERVAL_SECONDS para ambientes onde se prefere verificar com mais frequencia.
    update_check_interval_seconds: int = Field(
        default=86400, alias="UPDATE_CHECK_INTERVAL_SECONDS"
    )

    # Defaults gravados no banco na primeira inicializacao (system_settings seed no lifespan de
    # app/main.py). Altere os valores em producao pelo app (tela Configuracoes -> Geral) — o
    # seed so e aplicado uma vez (INSERT ON CONFLICT DO NOTHING), entao uma linha ja existente
    # (usuario ja configurou) nunca e sobrescrita por um restart do backend.
    blocking_min_duration_ms: int = Field(default=0, alias="BLOCKING_MIN_DURATION_MS")
    sleeping_min_duration_ms: int = Field(default=0, alias="SLEEPING_MIN_DURATION_MS")


@lru_cache
def get_settings() -> Settings:
    """Retorna a configuracao (cacheada por processo)."""
    return Settings()
