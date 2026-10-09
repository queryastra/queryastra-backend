"""Taxonomia canonica de wait time cross-engine — `Docs/wait-taxonomy.md`.

9 categorias canonicas usadas no campo `wait_type` de `query_samples`
(`Docs/database-schema.md` §3). O valor nativo do motor (`wait_event_native`) e sempre
preservado em paralelo — esta enum nunca substitui o dado original, so o classifica.
"""

from __future__ import annotations

from enum import StrEnum


class WaitCategory(StrEnum):
    """Categoria canonica de wait, comum a PostgreSQL e SQL Server."""

    CPU = "CPU"
    IO = "IO"
    LOCK = "LOCK"
    LATCH = "LATCH"
    MEMORY = "MEMORY"
    NETWORK = "NETWORK"
    IPC = "IPC"
    IDLE = "IDLE"
    OTHER = "OTHER"


class WaitBucket(StrEnum):
    """Recorte resumido derivado de `WaitCategory` (achado `CON_WAIT_CATEGORIES`, wait-taxonomy.md).

    Mapeamento: IO -> DISK; CPU, MEMORY -> CPU_MEM; todo o resto -> OTHER.
    """

    DISK = "DISK"
    CPU_MEM = "CPU_MEM"
    OTHER = "OTHER"


# Mapeamento PostgreSQL: `wait_event_type` (pg_stat_activity) -> categoria canonica.
# Ver Docs/wait-taxonomy.md — coluna "Mapeamento PostgreSQL".
_POSTGRESQL_WAIT_EVENT_TYPE_MAP: dict[str, WaitCategory] = {
    "IO": WaitCategory.IO,
    "Lock": WaitCategory.LOCK,
    "LWLock": WaitCategory.LATCH,
    "BufferPin": WaitCategory.LATCH,
    "Client": WaitCategory.NETWORK,
    "IPC": WaitCategory.IPC,
    "Timeout": WaitCategory.IDLE,
    "Activity": WaitCategory.IDLE,
    "Extension": WaitCategory.OTHER,
}

# Mapeamento SQL Server: prefixo/valor de `wait_type` nativo -> categoria canonica.
# Ver Docs/wait-taxonomy.md — coluna "Mapeamento SQL Server (exemplos)". Lista de exemplos, nao
# exaustiva — todo wait_type nao reconhecido cai em OTHER (regra de implementacao #3).
_SQLSERVER_WAIT_TYPE_EXACT_MAP: dict[str, WaitCategory] = {
    "SOS_SCHEDULER_YIELD": WaitCategory.CPU,
    "THREADPOOL": WaitCategory.CPU,
    "WRITELOG": WaitCategory.IO,
    "IO_COMPLETION": WaitCategory.IO,
    "ASYNC_IO_COMPLETION": WaitCategory.IO,
    "RESOURCE_SEMAPHORE": WaitCategory.MEMORY,
    "RESOURCE_SEMAPHORE_QUERY_COMPILE": WaitCategory.MEMORY,
    "ASYNC_NETWORK_IO": WaitCategory.NETWORK,
    "CXPACKET": WaitCategory.IPC,
    "CXCONSUMER": WaitCategory.IPC,
    "EXCHANGE": WaitCategory.IPC,
    "SLEEP_TASK": WaitCategory.IDLE,
    "WAITFOR": WaitCategory.IDLE,
    "BROKER_TASK_STOP": WaitCategory.IDLE,
}

# Prefixos SQL Server (curinga) — ex.: PAGEIOLATCH_SH, LCK_M_X, PAGELATCH_EX.
_SQLSERVER_WAIT_TYPE_PREFIX_MAP: dict[str, WaitCategory] = {
    "PAGEIOLATCH_": WaitCategory.IO,
    "LCK_M_": WaitCategory.LOCK,
    "PAGELATCH_": WaitCategory.LATCH,
    "LATCH_": WaitCategory.LATCH,
}

# MySQL: EVENT_NAME de performance_schema.events_waits_current
# Formato: "wait/<category>/<subcategory>/..." ou "idle"
_MYSQL_WAIT_EVENT_PREFIX_MAP: dict[str, WaitCategory] = {
    "wait/io/file/": WaitCategory.IO,
    "wait/io/table/": WaitCategory.IO,
    "wait/io/global_index/": WaitCategory.IO,
    "wait/io/net/": WaitCategory.NETWORK,
    "wait/io/socket/": WaitCategory.NETWORK,
    "wait/lock/table/": WaitCategory.LOCK,
    "wait/lock/metadata/": WaitCategory.LOCK,
    "wait/synch/mutex/": WaitCategory.LATCH,
    "wait/synch/rwlock/": WaitCategory.LATCH,
    "wait/synch/cond/": WaitCategory.LATCH,
    "wait/synch/sxlock/": WaitCategory.LATCH,
}


# Oracle: WAIT_CLASS nativo de V$SESSION_WAIT / V$SESSION
_ORACLE_WAIT_CLASS_MAP: dict[str, WaitCategory] = {
    "User I/O": WaitCategory.IO,
    "System I/O": WaitCategory.IO,
    "Commit": WaitCategory.IO,
    "Application": WaitCategory.LOCK,
    "Concurrency": WaitCategory.LATCH,
    "Network": WaitCategory.NETWORK,
    "Cluster": WaitCategory.IPC,
    "Idle": WaitCategory.IDLE,
    "Other": WaitCategory.OTHER,
    "Administrative": WaitCategory.OTHER,
    "Configuration": WaitCategory.OTHER,
    "Scheduler": WaitCategory.CPU,
}

# IBM DB2: STATE_CHANGE_REASON / ACTIVITY_TYPE de MON_GET_ACTIVITY
_DB2_WAIT_CLASS_MAP: dict[str, WaitCategory] = {
    "LOCK_WAIT": WaitCategory.LOCK,
    "LATCH_WAIT": WaitCategory.LATCH,
    "PREFETCH_WAIT": WaitCategory.IO,
    "PAGE_WRITE": WaitCategory.IO,
    "PAGE_READ": WaitCategory.IO,
    "NETWORK_RECEIVE": WaitCategory.NETWORK,
    "NETWORK_SEND": WaitCategory.NETWORK,
    "INTERRUPT_WAIT": WaitCategory.IPC,
    "THREADWAIT": WaitCategory.IDLE,
    "WAIT_FOR_LOCK_RELEASE": WaitCategory.LOCK,
}


def map_wait_event(engine: str, native_value: str | None) -> WaitCategory:
    """Classifica um `wait_event`/`wait_type` nativo na categoria canonica cross-engine.

    Args:
        engine: ``"postgresql"`` ou ``"sqlserver"``.
        native_value: valor bruto do motor (ex.: ``"Lock"``, ``"LCK_M_X"``). ``None``/vazio
            (PostgreSQL sem wait ativo) e classificado como ``CPU`` — ausencia de wait_event no
            Postgres significa que a sessao esta rodando em CPU (ver wait-taxonomy.md).

    Returns:
        A `WaitCategory` canonica. Nunca falha — valor nao reconhecido cai em `OTHER` (regra de
        implementacao #3 de `Docs/wait-taxonomy.md`); quem chamar deve registrar essa ocorrencia
        (log/metrica interna) para revisao periodica da tabela.
    """
    if engine in ("postgresql", "cockroachdb", "aurora_postgresql", "redshift"):
        if not native_value:
            return WaitCategory.CPU
        return _POSTGRESQL_WAIT_EVENT_TYPE_MAP.get(native_value, WaitCategory.OTHER)

    if engine == "sqlserver":
        if not native_value:
            return WaitCategory.OTHER
        if native_value in _SQLSERVER_WAIT_TYPE_EXACT_MAP:
            return _SQLSERVER_WAIT_TYPE_EXACT_MAP[native_value]
        for prefix, category in _SQLSERVER_WAIT_TYPE_PREFIX_MAP.items():
            if native_value.startswith(prefix):
                return category
        return WaitCategory.OTHER

    if engine in ("mysql", "aurora_mysql"):
        if not native_value or native_value == "idle":
            return WaitCategory.IDLE
        for prefix, category in _MYSQL_WAIT_EVENT_PREFIX_MAP.items():
            if native_value.startswith(prefix):
                return category
        return WaitCategory.OTHER

    if engine == "oracle":
        if not native_value:
            return WaitCategory.CPU
        return _ORACLE_WAIT_CLASS_MAP.get(native_value, WaitCategory.OTHER)

    if engine == "db2":
        if not native_value:
            return WaitCategory.CPU
        return _DB2_WAIT_CLASS_MAP.get(native_value, WaitCategory.OTHER)

    raise ValueError(f"engine desconhecido: {engine!r}")


def wait_bucket_for(category: WaitCategory) -> WaitBucket:
    """Deriva o `wait_bucket` resumido (DISK/CPU_MEM/OTHER) a partir da categoria canonica."""
    if category == WaitCategory.IO:
        return WaitBucket.DISK
    if category in (WaitCategory.CPU, WaitCategory.MEMORY):
        return WaitBucket.CPU_MEM
    return WaitBucket.OTHER
