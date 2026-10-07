"""Repositorio da hypertable `query_samples`.

`query_samples` (`Docs/database-schema.md` §3) e hypertable, criada por
`migrations/versions/66ae4664300c_hypertables.py`. Antes de inserir, todo texto de SQL ja deve
ter passado por `app.masking.sql_masking.mask_sql_literals` (feito pelo coletor, ao normalizar
`NormalizedQuerySample`) e pela checagem de `sql_exclusions` (`Docs/rules/03-sql-privacy.md`:
"Aplicar exclusoes por hash, programa ou usuario antes de gravar `query_samples`."), feita aqui.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

import sqlalchemy as sa

from app.normalizers.models import NormalizedQuerySample
from app.repositories._tables import query_samples_table, sql_exclusions_table
from app.repositories.base import BaseRepository

# Granularidade de bucket de tempo aceita por `wait_time_by_bucket_and_*` — "day"/"hour" (ja
# existiam) mais as 4 granularidades sub-hora da "Fatia de Tempo" (`ten_minutes`/`minute`/
# `ten_seconds`/`second`), ver `QuerySamplesRepository._bucket_sql`.
TimeBucket = Literal["day", "hour", "ten_minutes", "minute", "ten_seconds", "second"]


class QuerySamplesRepository(BaseRepository):
    """Persistencia de `NormalizedQuerySample` na hypertable `query_samples`.

    Regras de `sql_exclusions`: `instance_id IS NULL` vale para todas as instancias; com
    `instance_id` preenchido, so para aquela instancia. Comparacao de `match_value` e sempre
    exata (sem regex/glob) contra `sql_hash`/`program`/`db_user` conforme `match_type`.

    ## `effective_wait_ms` — correcao do bug de contagem de wait time (2026-09-11)

    `sys.dm_exec_requests.wait_time` (SQL Server) e o equivalente do coletor PostgreSQL reportam
    "ha quanto tempo a espera ATUAL dura", nao um delta desde o ultimo poll — o valor cresce
    monotonicamente enquanto a MESMA espera continua e so zera quando uma nova espera comeca.
    Antes desta correcao, toda agregacao aqui somava `wait_time_ms` bruto por amostra
    (`SUM(wait_time_ms)`); para uma espera mais longa que o intervalo de poll (~10s), isso soma a
    mesma espera varias vezes (cada amostra e uma "foto" cumulativa da mesma espera), inflando o
    total proporcionalmente a `duracao_da_espera / intervalo_de_poll`. Confirmado ao vivo contra
    o banco de dev real (instancia `Autklinux20`, sessao 797, banco `AB_APLICAUT_AX`): 14 amostras
    consecutivas da mesma sessao/wait mostrando `wait_time_ms` crescendo 40162 -> 50241 -> ... ->
    173425 — a soma ingenua das 14 amostras (~1.496.529 ms) superestima em ~8,6x a duracao real da
    espera (173.425 ms), e chega a exceder o proprio tempo de parede decorrido entre a 1a e a
    ultima amostra (~133s).

    Correcao (opcao B, escolhida explicitamente pelo usuario): toda query de agregacao abaixo usa
    uma CTE `effective` que calcula, por amostra,
    `effective_wait_ms` = incremento real desde a amostra anterior da MESMA sessao, quando essa
    amostra anterior tinha o mesmo `wait_event_native` e um `wait_time_ms` menor ou igual (== a
    mesma espera continuando); caso contrario (espera mudou, zerou, ou e a primeira amostra da
    sessao na janela) conta o valor cheio da amostra (e uma espera nova comecando do zero). Isso
    elimina a re-contagem sem "arredondar pra cima" esperas curtas (que ja eram corretas antes,
    pois cada amostra ja captura uma espera diferente quando ela dura menos que o intervalo de
    poll). `LAG(...) OVER (PARTITION BY session_id ORDER BY time)` so e usada quando `session_id`
    e `wait_event_native` nao sao nulos (varias sessoes podem ter `session_id IS NULL` ao mesmo
    tempo — particionar por NULL as agruparia todas numa unica particao artificial).

    Limitacao conhecida, documentada e aceita (nao corrigida): a CTE so enxerga amostras dentro da
    janela `[since, until]` pedida — a primeira amostra de uma sessao dentro da janela sempre conta
    o valor cheio, mesmo que seja continuacao de uma espera que comecou ANTES de `since`. Afeta so
    a borda inicial da janela, nao o volume agregado como um todo.

    ## `_BENIGN_WAIT_EVENT_NATIVE` — filtro de waits benignos/de background (2026-09-11)

    Mesmo com `effective_wait_ms`, o `WAITFOR` de um job do SQL Agent em loop
    (`AB_DEBITOPARCELAS`/etc.) ainda dominava o total em testes reais — waits benignos/de
    infraestrutura distorcem a visualizacao de Top Waits. A mesma logica de exclusao e
    documentada por Paul Randal (sqlskills.com, referencia amplamente citada na comunidade de
    DBAs de SQL Server, cuja lista completa e bem maior — inclui coisas como LAZYWRITER_SLEEP,
    CHECKPOINT_QUEUE etc.). **NAO usamos a lista completa do Randal**: ela inclui
    `PREEMPTIVE_OS_FLUSHFILEBUFFERS`, que e um wait de I/O real relevante para monitoramento.
    Por isso `_BENIGN_WAIT_EVENT_NATIVE` fica restrito ao subconjunto de waits de background
    que genuinamente nao representam carga de trabalho do usuario, mais `BROKER_RECEIVE_WAITFOR`
    (variante Service Broker do mesmo padrao de job em loop, mesmo mecanismo do `WAITFOR`).

    Zerado (nao removido) dentro de `effective_wait_ms` — uma amostra com wait benigno ainda
    conta em `execution_count` (a query realmente executou), so nao contribui pro total de wait
    time, exatamente como uma sessao "rodando" sem espera ativa. Nos metodos que agrupam POR
    `wait_event_native` (`top_wait_types`, `wait_breakdown_by_database`,
    `wait_breakdown_by_sql_hash`, `wait_time_by_bucket_and_wait_type`), a linha e excluida do
    `GROUP BY` inteiramente (nao aparece como "WAITFOR: 0ms" poluindo a lista).

    **Limitacao que este filtro NAO resolve** (documentada, nao corrigida): mesmo depois deste
    filtro, esperas de I/O curtas e frequentes (`PAGEIOLATCH_EX`/`_SH`, `WRITELOG`,
    `PREEMPTIVE_OS_FLUSHFILEBUFFERS`) ainda aparecem com magnitude menor com intervalos de poll
    maiores — o intervalo de poll padrao aqui e ~10s; esperas breves (poucos ms a poucos
    segundos) tem uma chance proporcionalmente menor de serem capturadas por um poll a cada 10s
    do que por um poll mais frequente. E uma limitacao estrutural de
    resolucao de amostragem, nao um bug de agregacao — reduzir `polling_interval_seconds` da
    instancia (ja configuravel) aproximaria mais os numeros, ao custo de mais overhead na
    instancia monitorada (ARCHITECTURE_SPEC.md Q3).
    """

    _BENIGN_WAIT_EVENT_NATIVE: tuple[str, ...] = (
        "WAITFOR",
        "BROKER_RECEIVE_WAITFOR",
        "SLEEP_TASK",
        "CLR_MANUAL_EVENT",
        "BROKER_EVENTHANDLER",
        "SLEEP_BPOOL_FLUSH",
    )

    _EFFECTIVE_WAIT_SELECT = """
        SELECT
            time, session_id, sql_hash, sql_name, sql_text_masked, database_name, db_user,
            program, machine, wait_type, wait_event_native, wait_time_ms,
            CASE
                WHEN wait_event_native = ANY(:benign_wait_types) THEN 0
                WHEN session_id IS NOT NULL
                 AND wait_event_native IS NOT NULL
                 AND wait_time_ms IS NOT NULL
                 AND wait_event_native = LAG(wait_event_native)
                     OVER (PARTITION BY session_id ORDER BY time)
                 AND LAG(wait_time_ms) OVER (PARTITION BY session_id ORDER BY time) IS NOT NULL
                 AND wait_time_ms >= LAG(wait_time_ms) OVER (PARTITION BY session_id ORDER BY time)
                THEN wait_time_ms - LAG(wait_time_ms) OVER (PARTITION BY session_id ORDER BY time)
                ELSE wait_time_ms
            END AS effective_wait_ms,
            CASE
                WHEN wait_event_native IS NOT NULL OR coalesce(wait_time_ms, 0) <> 0 THEN 0
                WHEN session_id IS NOT NULL
                 AND cpu_time_ms IS NOT NULL
                 AND LAG(cpu_time_ms) OVER (PARTITION BY session_id ORDER BY time) IS NOT NULL
                 AND cpu_time_ms >= LAG(cpu_time_ms) OVER (PARTITION BY session_id ORDER BY time)
                THEN cpu_time_ms - LAG(cpu_time_ms) OVER (PARTITION BY session_id ORDER BY time)
                ELSE coalesce(cpu_time_ms, 0)
            END AS effective_cpu_ms
        FROM query_samples
        WHERE instance_id = :instance_id AND time >= :since AND time <= :until
    """
    """Bloco `SELECT` reutilizado dentro da CTE `effective AS (...)` de cada metodo abaixo — todas
    as colunas usadas por alguma agregacao downstream (incluindo `db_user`, usado pela dimensao
    "Usuarios"), mais `effective_wait_ms`/`effective_cpu_ms` ja calculados. Filtro de
    `database_name` (quando existe) e aplicado FORA desta CTE (na query
    externa), nunca aqui dentro — a continuidade de uma espera/CPU e uma propriedade da sessao
    fisica, independente de banco de dados; filtrar `database_name` antes do `LAG` cortaria a
    amostra anterior sempre que ela (impossivelmente, mas por seguranca) aparecesse com outro
    banco. Todo metodo que usa esta CTE deve incluir
    `"benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE)` no dict de params.

    `effective_cpu_ms` — fecha o gap de CPU nao coberto por esperas nomeadas:
    `sys.dm_exec_requests.cpu_time`
    (SQL Server) e cumulativo por REQUEST (zera quando uma nova consulta comeca, nao quando o
    wait_event_native muda dentro da mesma consulta — ao contrario do wait, que reseta por
    mudanca de tipo de espera) — por isso o delta aqui usa so `cpu_time_ms >= LAG(...)` como sinal
    de continuidade, sem exigir que `wait_event_native` seja o mesmo. So atribuido quando
    `wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0` (a amostra nao tem NENHUMA
    espera ativa — genuinamente rodando na CPU). As duas condicoes juntas, nao so
    `wait_event_native`, por dois motivos confirmados contra dado real: (1) um teste/fixture pode
    legitimamente ter `wait_time_ms` preenchido sem um `wait_event_native` nomeado — "espera sem
    nome conhecido", nao "sem espera", nao deve cair no bucket de CPU; (2) o SQL Server real
    reporta `wait_time_ms = 0` (nao NULL) quando nao ha espera ativa (confirmado contra
    `Autklinux20` ao vivo) — testar so `IS NULL` deixaria de fora a maioria das amostras
    genuinamente sem espera. Quando ha espera ativa, o tempo ja e contado em `effective_wait_ms`,
    entao contar aqui tambem duplicaria. PostgreSQL nao expoe um equivalente direto (`cpu_time_ms`
    fica sempre `NULL` la) — `effective_cpu_ms` sai sempre 0 nesse caso, sem quebrar a soma."""

    _NOT_BENIGN_WAIT_FILTER = (
        "(wait_event_native IS NULL OR wait_event_native <> ALL(:benign_wait_types))"
    )
    """Fragmento de `WHERE` usado pelos metodos que agrupam POR `wait_event_native`
    (`top_wait_types`, `wait_breakdown_by_database`, `wait_breakdown_by_sql_hash`,
    `wait_time_by_bucket_and_wait_type`) — exclui a linha do `GROUP BY` inteiramente em vez de so
    zerar `effective_wait_ms` (que a CTE `effective` ja faz), pra nao poluir a lista com uma
    entrada "WAITFOR: 0ms". `wait_event_native IS NULL` precisa ser tratado explicitamente:
    `NULL <> ALL(array)` avalia pra `NULL` (nao `TRUE`), o que um `WHERE` trataria como exclusao —
    sem essa clausula extra, toda amostra sem wait ativo desapareceria da lista."""

    # "Fatia de Tempo" (drill-down temporal fino dentro de uma unica hora): granularidades sub-hora, em
    # segundos de largura do bucket. O Postgres nao tem unidade nativa de `date_trunc` pra
    # 10min/1min/10s/1s (so segundo/minuto/hora/dia/...), entao usamos aritmetica de epoch
    # (`floor(epoch/N)*N`) pra essas — mesmo padrao ja usado em `query_stats_by_bucket` pro caso
    # "ten_minutes". "day"/"hour" continuam usando `date_trunc` nativo (sem mudanca).
    _SUB_HOUR_BUCKET_SECONDS: dict[str, int] = {
        "ten_minutes": 600,
        "minute": 60,
        "ten_seconds": 10,
        "second": 1,
    }

    @classmethod
    def _bucket_sql(cls, bucket: str) -> tuple[str, str]:
        """Devolve `(bucket_start_sql, group_by_sql)` pro `bucket` pedido. `bucket` e sempre um
        `Literal` fechado validado pelo tipo do chamador (nunca texto arbitrario vindo do
        cliente), entao interpolar a expressao SQL resolvida (nao o valor em si) e seguro aqui."""
        width = cls._SUB_HOUR_BUCKET_SECONDS.get(bucket)
        if width is not None:
            return (
                f"to_timestamp(floor(extract(epoch FROM time) / {width}) * {width})",
                f"floor(extract(epoch FROM time) / {width})",
            )
        return (
            "(date_trunc(:bucket, time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')",
            "date_trunc(:bucket, time AT TIME ZONE 'UTC')",
        )

    _NOT_CPU_ONLY_FILTER = "NOT (wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0)"
    """Fragmento de `WHERE` usado no lado "espera nomeada" do UNION ALL dos metodos que agora
    incluem o bucket sintetico 'Memory/CPU' (`top_wait_types`, `wait_breakdown_by_database`,
    `wait_breakdown_by_sql_hash`) — sem isso, uma amostra sem NENHUMA espera ativa contaria DUAS
    vezes: uma como 'CPU: Xms' (correto) e outra como 'desconhecido: 0ms' (ruido). `wait_time_ms`
    diferente de zero (uma espera "sem nome conhecido") continua aparecendo normalmente como
    'desconhecido: Xms' — so a combinacao exata de nenhum wait_event_native E nenhum wait_time_ms
    de verdade (NULL OU 0 — ver docstring de `_EFFECTIVE_WAIT_SELECT`) e excluida daqui."""

    async def insert_batch(self, samples: list[NormalizedQuerySample]) -> None:
        if not samples:
            return

        instance_ids = {s.instance_id for s in samples}
        result = await self._session.execute(
            sa.select(
                sql_exclusions_table.c.instance_id,
                sql_exclusions_table.c.match_type,
                sql_exclusions_table.c.match_value,
            ).where(
                sa.or_(
                    sql_exclusions_table.c.instance_id.in_(instance_ids),
                    sql_exclusions_table.c.instance_id.is_(None),
                )
            )
        )
        exclusions = list(result.all())

        def is_excluded(sample: NormalizedQuerySample) -> bool:
            for instance_id, match_type, match_value in exclusions:
                if instance_id is not None and instance_id != sample.instance_id:
                    continue
                if match_type == "sql_hash" and sample.sql_hash == match_value:
                    return True
                if match_type == "program_name" and sample.program == match_value:
                    return True
                if match_type == "db_user" and sample.db_user == match_value:
                    return True
            return False

        rows = [
            {
                "time": s.time,
                "instance_id": s.instance_id,
                "sql_hash": s.sql_hash,
                "sql_name": s.sql_name,
                "sql_text_masked": s.sql_text_masked,
                "sql_text_full_masked": s.sql_text_full_masked,
                "sql_text_encrypted": s.sql_text_encrypted,
                "wait_type": s.wait_type.value if s.wait_type is not None else None,
                "wait_event_native": s.wait_event_native,
                "wait_time_ms": s.wait_time_ms,
                "cpu_time_ms": s.cpu_time_ms,
                "duration_ms": s.duration_ms,
                "session_id": s.session_id,
                "db_user": s.db_user,
                "program": s.program,
                "database_name": s.database_name,
                "machine": s.machine,
            }
            for s in samples
            if not is_excluded(s)
        ]
        if not rows:
            return
        await self._session.execute(sa.insert(query_samples_table), rows)
        await self._session.commit()

    async def list_active(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Amostras brutas de `query_samples` no periodo — leitura direta da hypertable, os
        continuous aggregates (`query_stats_hourly`) estao vazios e nunca atualizados."""
        stmt = (
            sa.select(query_samples_table)
            .where(
                query_samples_table.c.instance_id == instance_id,
                query_samples_table.c.time >= since,
                query_samples_table.c.time <= until,
            )
            .order_by(query_samples_table.c.time.desc())
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).fetchall())

    async def list_distinct_database_names(self, instance_id: UUID, since: datetime) -> list[str]:
        """Nomes de banco distintos que essa instância teve nos últimos `since` — bem mais barato
        que `top_sql_by_database` (bug real relatado pelo usuário ao vivo: o select de banco de
        dados da tela de Configurações usava `top_sql_by_database`, que roda a CTE de
        `effective_wait_ms`/`effective_cpu_ms` — window function `LAG()` por sessão sobre TODO o
        histórico — só pra listar nomes; numa instância com muito volume, isso deixava o select
        "travado" por vários segundos, ou nem abrir de fora antes do usuário desistir). Um simples
        `SELECT DISTINCT database_name` não precisa de nenhuma dessas contas."""
        stmt = (
            sa.select(query_samples_table.c.database_name)
            .where(
                query_samples_table.c.instance_id == instance_id,
                query_samples_table.c.time >= since,
                query_samples_table.c.database_name.is_not(None),
            )
            .distinct()
            .order_by(query_samples_table.c.database_name)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.fetchall()]

    async def list_databases_for_hash(
        self, instance_id: UUID, sql_hash: str, since: datetime
    ) -> list[str]:
        """Nomes de banco onde `sql_hash` foi visto desde `since` — usado pelo endpoint de plano
        de execucao para restringir a busca no Query Store aos bancos relevantes, evitando varrer
        todos os bancos da instancia."""
        stmt = (
            sa.select(query_samples_table.c.database_name)
            .where(
                query_samples_table.c.instance_id == instance_id,
                query_samples_table.c.sql_hash == sql_hash,
                query_samples_table.c.time >= since,
                query_samples_table.c.database_name.is_not(None),
            )
            .distinct()
            .order_by(query_samples_table.c.database_name)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.fetchall()]

    async def top_sql(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        database_name: str | None = None,
        db_user: str | None = None,
        program: str | None = None,
        machine: str | None = None,
    ) -> list[sa.Row]:
        """Agrega `query_samples` bruto por `sql_hash`: execution_count/total/avg via GROUP BY;
        sql_name/sql_text_masked/database_name da linha mais recente por hash (DISTINCT ON); wait
        predominante = moda de wait_type por hash (window function). SQL literal via `sa.text`
        (mais legivel que expression builder para DISTINCT ON + moda).

        `database_name` (nivel 2 do drill-down hierarquico: Instancia -> Banco -> Query) filtra
        as amostras a um unico banco quando informado; `db_user` (nivel 2 da dimensao "Usuarios",
        mesmo padrao) filtra a um unico usuario. `None`/`None` (default) mantem o comportamento
        historico de agregar tudo junto sob o mesmo `sql_hash` — os filtros sao aplicados na
        query externa, apos a CTE `effective` calcular `effective_wait_ms` sem restricao (ver
        docstring da classe). `total_wait_time_ms`/`avg_wait_time_ms` somam/promediam
        `effective_wait_ms + effective_cpu_ms` (delta real por amostra, incluindo o tempo de CPU
        sem espera ativa — ver docstring de `_EFFECTIVE_WAIT_SELECT`), nao mais `wait_time_ms`
        bruto — ver docstring da classe para o porque. Somar os dois aqui (em vez de manter
        `total_wait_time_ms` só-espera) é o que faz os segmentos de `wait_breakdown` (que já
        incluem o bucket 'Memory/CPU') somarem exatamente ao total da barra, sem estourar a
        largura."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            filtered AS (
                SELECT * FROM effective
                WHERE (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
                  AND (CAST(:db_user AS text) IS NULL OR db_user = :db_user)
                  AND (CAST(:program AS text) IS NULL OR program = :program)
                  AND (CAST(:machine AS text) IS NULL OR machine = :machine)
            ),
            agg AS (
                SELECT sql_hash, count(*) AS execution_count,
                       coalesce(sum(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                           AS total_wait_time_ms,
                       coalesce(avg(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                           AS avg_wait_time_ms
                FROM filtered
                GROUP BY sql_hash
            ),
            latest AS (
                SELECT DISTINCT ON (sql_hash) sql_hash, sql_name, sql_text_masked, database_name
                FROM filtered
                ORDER BY sql_hash, time DESC
            ),
            wait_counts AS (
                SELECT sql_hash, wait_type, count(*) AS wait_count
                FROM filtered
                WHERE wait_type IS NOT NULL AND {self._NOT_BENIGN_WAIT_FILTER}
                GROUP BY sql_hash, wait_type
            ),
            wait_mode AS (
                SELECT sql_hash, wait_type,
                       row_number() OVER (PARTITION BY sql_hash ORDER BY wait_count DESC) AS rn
                FROM wait_counts
            )
            SELECT agg.sql_hash, latest.sql_name, latest.sql_text_masked, latest.database_name,
                   agg.execution_count, agg.total_wait_time_ms, agg.avg_wait_time_ms,
                   wait_mode.wait_type AS predominant_wait_type
            FROM agg
            JOIN latest ON latest.sql_hash = agg.sql_hash
            LEFT JOIN wait_mode ON wait_mode.sql_hash = agg.sql_hash AND wait_mode.rn = 1
            ORDER BY agg.total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "database_name": database_name,
                "db_user": db_user,
                "program": program,
                "machine": machine,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def top_sql_by_wait_type(
        self,
        instance_id: UUID,
        wait_event_native: str,
        since: datetime,
        until: datetime,
        limit: int,
    ) -> list[sa.Row]:
        """Drill-down a partir da dimensao "Waits" (folha): quais queries mais consumiram um
        `wait_event_native` especifico, clicando num tipo de espera no `WaitTypeBarChart`. Mesmo
        padrao de CTE de `top_sql()`, mas filtrando `filtered` por `wait_event_native` em vez de
        `database_name` — `total_wait_time_ms`/`avg_wait_time_ms` refletem so a contribuicao
        DESSE tipo de espera (nao o total da query em todos os tipos), ja que a CTE `filtered` so
        contem linhas onde esse wait estava ativo. `predominant_wait_type` vem da linha mais
        recente de `latest` (categoria CANONICA, `wait_type` — nao `wait_event_native` — mesmo
        campo/semantica de `top_sql()`, usado pelo frontend para colorir por `WaitCategory`); nao
        precisa de moda/`wait_mode` como em `top_sql()` porque `filtered` ja fixa um unico
        `wait_event_native`, que mapeia sempre pra uma unica categoria canonica.

        `wait_event_native == "Memory/CPU"` e um caso especial: esse label e SINTETICO (nunca
        gravado em `query_samples` — so existe na agregacao, ver `top_wait_types`), entao filtrar
        por igualdade literal nunca acharia nada. Filtra por `wait_event_native IS NULL` (a mesma
        condicao usada pra gerar o bucket 'Memory/CPU') e soma `effective_cpu_ms` em vez de
        `effective_wait_ms`."""
        is_cpu_bucket = wait_event_native == "CPU"
        wait_filter = (
            "wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0"
            if is_cpu_bucket
            else "wait_event_native = :wait_event_native"
        )
        time_column = "effective_cpu_ms" if is_cpu_bucket else "effective_wait_ms"
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            filtered AS (
                SELECT * FROM effective
                WHERE {wait_filter}
            ),
            agg AS (
                SELECT sql_hash, count(*) AS execution_count,
                       coalesce(sum({time_column}), 0) AS total_wait_time_ms,
                       coalesce(avg({time_column}), 0) AS avg_wait_time_ms
                FROM filtered
                GROUP BY sql_hash
            ),
            latest AS (
                SELECT DISTINCT ON (sql_hash)
                       sql_hash, sql_name, sql_text_masked, database_name, wait_type
                FROM filtered
                ORDER BY sql_hash, time DESC
            )
            SELECT agg.sql_hash, latest.sql_name, latest.sql_text_masked, latest.database_name,
                   agg.execution_count, agg.total_wait_time_ms, agg.avg_wait_time_ms,
                   latest.wait_type AS predominant_wait_type
            FROM agg
            JOIN latest ON latest.sql_hash = agg.sql_hash
            ORDER BY agg.total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "wait_event_native": wait_event_native,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def top_sql_by_database(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por `database_name` (nivel 1 do drill-down hierarquico:
        Instancia -> Banco -> Query). Mesmo padrao de CTE de `top_sql()`, mas agrupando por
        `database_name` em vez de `sql_hash`. Linhas com `database_name` NULL sao agrupadas sob a
        label 'desconhecido' (COALESCE), nunca descartadas. `total_wait_time_ms`/`avg_wait_time_ms`
        somam/promediam `effective_wait_ms + effective_cpu_ms` (delta real por amostra, incluindo
        CPU sem espera ativa) — ver docstring da classe e de `_EFFECTIVE_WAIT_SELECT`."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                COALESCE(database_name, 'desconhecido') AS database_name,
                count(*) AS execution_count,
                coalesce(sum(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS total_wait_time_ms,
                coalesce(avg(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS avg_wait_time_ms
            FROM effective
            GROUP BY COALESCE(database_name, 'desconhecido')
            ORDER BY total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def top_wait_types(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por `wait_event_native` — dimensao "Waits" do drill-down
        (irma de "Bancos de dados"/"SQL": `top_sql_by_database`/`top_sql`), folha da propria
        dimensao (um tipo de espera nao se decompoe em mais nada, ao contrario de banco/query, que
        se decompoem por wait nativo). `wait_event_native` NULL vira 'desconhecido' (COALESCE),
        nunca descartado. `total_wait_time_ms`/`avg_wait_time_ms` somam/promediam
        `effective_wait_ms` (delta real por amostra) — ver docstring da classe. Waits benignos/de
        background (`_BENIGN_WAIT_EVENT_NATIVE`) sao excluidos do `GROUP BY` inteiramente (nao
        aparecem como "WAITFOR: 0ms"). Inclui um bucket sintetico 'Memory/CPU' (`effective_cpu_ms`,
        amostras sem nenhuma espera ativa) via UNION ALL — fecha o gap de CPU nao coberto por
        esperas nomeadas (ver docstring de `_EFFECTIVE_WAIT_SELECT`)."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            combined AS (
                SELECT COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                       effective_wait_ms AS ms
                FROM effective
                WHERE {self._NOT_BENIGN_WAIT_FILTER} AND {self._NOT_CPU_ONLY_FILTER}
                UNION ALL
                SELECT 'CPU' AS wait_event_native, effective_cpu_ms AS ms
                FROM effective
                WHERE wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0
            )
            SELECT
                wait_event_native,
                count(*) AS execution_count,
                coalesce(sum(ms), 0) AS total_wait_time_ms,
                coalesce(avg(ms), 0) AS avg_wait_time_ms
            FROM combined
            GROUP BY wait_event_native
            ORDER BY total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_breakdown_by_database(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`database_name`, `wait_event_native`) — decomposicao do
        wait time de cada banco (nivel 1 do drill-down) pelo tipo de espera NATIVO do engine (ex.:
        `PAGEIOLATCH_SH`, `ASYNC_NETWORK_IO` no SQL Server; nomes de `pg_stat_activity.wait_event`
        no PostgreSQL) — granularidade maior que os 9 buckets canonicos cross-engine de
        `wait_type` (usados em `wait_summary()`), a pedido explicito de ver o wait nativo por
        banco, nao so a categoria agregada. `database_name`/`wait_event_native`
        NULL viram 'desconhecido' (COALESCE), nunca descartados — mesmo padrao de
        `top_sql_by_database`. `total_wait_time_ms` soma `effective_wait_ms` (delta real por
        amostra) — ver docstring da classe. Waits benignos/de background excluidos do `GROUP BY`
        inteiramente (ver `_NOT_BENIGN_WAIT_FILTER`). Inclui um bucket sintetico 'Memory/CPU' por
        banco (`effective_cpu_ms`) via UNION ALL — mesmo gap fechado em `top_wait_types`."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            combined AS (
                SELECT COALESCE(database_name, 'desconhecido') AS database_name,
                       COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                       effective_wait_ms AS ms
                FROM effective
                WHERE {self._NOT_BENIGN_WAIT_FILTER} AND {self._NOT_CPU_ONLY_FILTER}
                UNION ALL
                SELECT COALESCE(database_name, 'desconhecido') AS database_name,
                       'CPU' AS wait_event_native,
                       effective_cpu_ms AS ms
                FROM effective
                WHERE wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0
            )
            SELECT
                database_name,
                wait_event_native,
                coalesce(sum(ms), 0) AS total_wait_time_ms
            FROM combined
            GROUP BY database_name, wait_event_native
            ORDER BY database_name, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    # --- Dimensao "Usuarios" (irma de "Bancos de dados"/"SQL"/"Waits") ----------------------

    async def top_users(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por `db_user` — dimensao "Usuarios" do drill-down (irma de
        "Bancos de dados"/"SQL"/"Waits"), folha da propria dimensao (um usuario nao se decompoe
        em mais nada, ao contrario de banco/query, que se decompoem por wait nativo — aqui a
        decomposicao e so visual, ver `wait_breakdown_by_db_user`). Mesmo padrao de CTE de
        `top_sql_by_database`, agrupando por `db_user` em vez de `database_name`. Linhas com
        `db_user` NULL sao agrupadas sob a label 'desconhecido' (COALESCE), nunca descartadas.
        `total_wait_time_ms`/`avg_wait_time_ms` somam/promediam `effective_wait_ms +
        effective_cpu_ms` (delta real por amostra, incluindo CPU sem espera ativa) — ver
        docstring da classe e de `_EFFECTIVE_WAIT_SELECT`."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                COALESCE(db_user, 'desconhecido') AS db_user,
                count(*) AS execution_count,
                coalesce(sum(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS total_wait_time_ms,
                coalesce(avg(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS avg_wait_time_ms
            FROM effective
            GROUP BY COALESCE(db_user, 'desconhecido')
            ORDER BY total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_breakdown_by_db_user(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`db_user`, `wait_event_native`) — decomposicao do wait
        time de cada usuario (dimensao "Usuarios") pelo tipo de espera NATIVO do engine. Mesmo
        padrao de `wait_breakdown_by_database`, granularidade `db_user` em vez de
        `database_name`. `db_user`/`wait_event_native` NULL viram 'desconhecido' (COALESCE),
        nunca descartados. Waits benignos/de background excluidos do `GROUP BY` inteiramente
        (ver `_NOT_BENIGN_WAIT_FILTER`). Inclui um bucket sintetico 'Memory/CPU' por usuario
        (`effective_cpu_ms`) via UNION ALL — mesmo gap fechado em `top_wait_types`."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            combined AS (
                SELECT COALESCE(db_user, 'desconhecido') AS db_user,
                       COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                       effective_wait_ms AS ms
                FROM effective
                WHERE {self._NOT_BENIGN_WAIT_FILTER} AND {self._NOT_CPU_ONLY_FILTER}
                UNION ALL
                SELECT COALESCE(db_user, 'desconhecido') AS db_user,
                       'CPU' AS wait_event_native,
                       effective_cpu_ms AS ms
                FROM effective
                WHERE wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0
            )
            SELECT
                db_user,
                wait_event_native,
                coalesce(sum(ms), 0) AS total_wait_time_ms
            FROM combined
            GROUP BY db_user, wait_event_native
            ORDER BY db_user, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def top_programs(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                COALESCE(program, 'desconhecido') AS program,
                count(*) AS execution_count,
                coalesce(sum(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS total_wait_time_ms,
                coalesce(avg(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS avg_wait_time_ms
            FROM effective
            GROUP BY COALESCE(program, 'desconhecido')
            ORDER BY total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_breakdown_by_program(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            combined AS (
                SELECT COALESCE(program, 'desconhecido') AS program,
                       COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                       effective_wait_ms AS ms
                FROM effective
                WHERE {self._NOT_BENIGN_WAIT_FILTER} AND {self._NOT_CPU_ONLY_FILTER}
                UNION ALL
                SELECT COALESCE(program, 'desconhecido') AS program,
                       'CPU' AS wait_event_native,
                       effective_cpu_ms AS ms
                FROM effective
                WHERE wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0
            )
            SELECT
                program,
                wait_event_native,
                coalesce(sum(ms), 0) AS total_wait_time_ms
            FROM combined
            GROUP BY program, wait_event_native
            ORDER BY program, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_program(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: TimeBucket = "day",
    ) -> list[sa.Row]:
        bucket_start_sql, group_by_sql = self._bucket_sql(bucket)
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                {bucket_start_sql} AS bucket_start,
                COALESCE(program, 'desconhecido') AS program,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            GROUP BY
                {group_by_sql},
                COALESCE(program, 'desconhecido')
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def top_machines(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                COALESCE(machine, 'desconhecido') AS machine,
                count(*) AS execution_count,
                coalesce(sum(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS total_wait_time_ms,
                coalesce(avg(coalesce(effective_wait_ms, 0) + effective_cpu_ms), 0)
                    AS avg_wait_time_ms
            FROM effective
            GROUP BY COALESCE(machine, 'desconhecido')
            ORDER BY total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_breakdown_by_machine(
        self, instance_id: UUID, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            combined AS (
                SELECT COALESCE(machine, 'desconhecido') AS machine,
                       COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                       effective_wait_ms AS ms
                FROM effective
                WHERE {self._NOT_BENIGN_WAIT_FILTER} AND {self._NOT_CPU_ONLY_FILTER}
                UNION ALL
                SELECT COALESCE(machine, 'desconhecido') AS machine,
                       'CPU' AS wait_event_native,
                       effective_cpu_ms AS ms
                FROM effective
                WHERE wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0
            )
            SELECT
                machine,
                wait_event_native,
                coalesce(sum(ms), 0) AS total_wait_time_ms
            FROM combined
            GROUP BY machine, wait_event_native
            ORDER BY machine, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_machine(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: TimeBucket = "day",
    ) -> list[sa.Row]:
        bucket_start_sql, group_by_sql = self._bucket_sql(bucket)
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                {bucket_start_sql} AS bucket_start,
                COALESCE(machine, 'desconhecido') AS machine,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            GROUP BY
                {group_by_sql},
                COALESCE(machine, 'desconhecido')
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_breakdown_by_sql_hash(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        database_name: str | None = None,
        db_user: str | None = None,
        sql_hash: str | None = None,
        program: str | None = None,
        machine: str | None = None,
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`sql_hash`, `wait_event_native`) — decomposicao do wait
        time de cada query (nivel 2 do drill-down) pelo tipo de espera NATIVO do engine. Mesma
        ideia de `wait_breakdown_by_database`, granularidade `sql_hash` em vez de
        `database_name`; aceita os mesmos filtros opcionais `database_name`/`db_user` de
        `top_sql()` (nivel 2 so quer a decomposicao das queries daquele banco/usuario).
        `sql_hash` (Etapa 11) filtra a UM hash especifico — usado pelo detalhe de query (nivel 3)
        pra alimentar o painel Advisor com a decomposicao de wait so daquela query. Filtrados
        apos a CTE `effective` (nao antes, ver docstring da classe). `total_wait_time_ms` soma
        `effective_wait_ms` (delta real por amostra). Waits benignos/de background excluidos do
        `GROUP BY` inteiramente (ver `_NOT_BENIGN_WAIT_FILTER`). Inclui um bucket sintetico
        'Memory/CPU' por `sql_hash` (`effective_cpu_ms`) via UNION ALL — mesmo gap fechado em
        `top_wait_types`."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT}),
            combined AS (
                SELECT sql_hash,
                       COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                       effective_wait_ms AS ms
                FROM effective
                WHERE (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
                    AND (CAST(:db_user AS text) IS NULL OR db_user = :db_user)
                    AND (CAST(:sql_hash AS text) IS NULL OR sql_hash = :sql_hash)
                    AND (CAST(:program AS text) IS NULL OR program = :program)
                    AND (CAST(:machine AS text) IS NULL OR machine = :machine)
                    AND {self._NOT_BENIGN_WAIT_FILTER} AND {self._NOT_CPU_ONLY_FILTER}
                UNION ALL
                SELECT sql_hash, 'CPU' AS wait_event_native, effective_cpu_ms AS ms
                FROM effective
                WHERE (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
                    AND (CAST(:db_user AS text) IS NULL OR db_user = :db_user)
                    AND (CAST(:sql_hash AS text) IS NULL OR sql_hash = :sql_hash)
                    AND (CAST(:program AS text) IS NULL OR program = :program)
                    AND (CAST(:machine AS text) IS NULL OR machine = :machine)
                    AND wait_event_native IS NULL AND coalesce(wait_time_ms, 0) = 0
            )
            SELECT
                sql_hash,
                wait_event_native,
                coalesce(sum(ms), 0) AS total_wait_time_ms
            FROM combined
            GROUP BY sql_hash, wait_event_native
            ORDER BY sql_hash, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "database_name": database_name,
                "db_user": db_user,
                "sql_hash": sql_hash,
                "program": program,
                "machine": machine,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def query_stats_by_bucket(
        self,
        instance_id: UUID,
        sql_hash: str,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: Literal["day", "hour", "ten_minutes"] = "day",
    ) -> list[sa.Row]:
        """Execucoes + tempo de espera por bucket de tempo, para UM `sql_hash` especifico —
        Etapa 11 (grafico "Execucoes x tempo de espera por execucao" no detalhe de query, nivel
        3). Mesmo molde de `wait_time_by_bucket_and_sql_hash`, mas filtrado a um unico hash (nao
        agrupado por hash) e somando tambem `count(*)` como `execution_count` — o hash ja e
        conhecido pelo chamador (rota do proprio detalhe da query), nao precisa reaparecer no
        SELECT.

        `bucket="ten_minutes"` (a pedido explicito do usuario, pra quando o periodo em analise ja
        e uma unica hora — bucket "hour" nesse caso produziria 1 barra so, inutil): `date_trunc`
        do Postgres NAO tem uma unidade nativa de 10 minutos (so segundo/minuto/hora/dia/...),
        entao usamos aritmetica de epoch (`floor(epoch/600)*600`) em vez de `date_trunc` — `bucket`
        e um `Literal` fechado validado pelo tipo, nunca texto arbitrario do cliente, entao
        interpolar a expressao SQL condicionalmente (nao como bind param) e seguro aqui."""
        bucket_start_sql = (
            "to_timestamp(floor(extract(epoch FROM time) / 600) * 600)"
            if bucket == "ten_minutes"
            else "(date_trunc(:bucket, time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')"
        )
        group_by_sql = (
            "floor(extract(epoch FROM time) / 600)"
            if bucket == "ten_minutes"
            else "date_trunc(:bucket, time AT TIME ZONE 'UTC')"
        )
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                {bucket_start_sql} AS bucket_start,
                count(*) AS execution_count,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            WHERE sql_hash = :sql_hash
            GROUP BY {group_by_sql}
            ORDER BY bucket_start
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "sql_hash": sql_hash,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_database(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: TimeBucket = "day",
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`bucket` de tempo, `database_name`) — visao historica
        "Tudo" do nivel 1 do drill-down quando `bucket="day"` (um dia calendario por barra,
        equivalente ao grafico multi-dia do "Top SQL"), o detalhamento por hora DENTRO de um dia
        especifico quando
        `bucket="hour"`, ou uma granularidade sub-hora da "Fatia de Tempo"
        (`ten_minutes`/`minute`/`ten_seconds`/`second`, ver `_bucket_sql`). `bucket` sempre em UTC
        (`time AT TIME ZONE 'UTC'`), independente do timezone de sessao do Postgres. O `AT TIME
        ZONE 'UTC'` externo (envolvendo o `date_trunc` inteiro) converte o timestamp NAIVE de
        volta pra `timestamptz` — sem isso, asyncpg devolve um datetime Python naive e o FastAPI
        serializa sem sufixo de fuso (`"2026-09-11T14:00:00"`, sem `Z`/offset), o que faz
        `new Date(...)` no navegador interpretar o instante como fuso LOCAL em vez de UTC (bug
        real encontrado ao implementar o `HourBarChart` do frontend — corrigido aqui, na fonte).
        Sem filtro de `since`/`until` restritivo o suficiente (bucket="day" tipicamente varre
        tudo), isto pode varrer o historico inteiro de `query_samples` (sem retention policy
        hoje) — aceitavel no volume atual, mas pode exigir paginacao/particionamento se o
        historico crescer muito. `total_wait_time_ms` soma `effective_wait_ms` (delta real por
        amostra, ver docstring da classe) — uma espera continua que atravessa a fronteira de um
        bucket tem seu delta inteiro atribuido ao bucket da amostra mais recente (aproximacao
        aceita, mesma limitacao de borda ja documentada na classe)."""
        bucket_start_sql, group_by_sql = self._bucket_sql(bucket)
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                {bucket_start_sql} AS bucket_start,
                COALESCE(database_name, 'desconhecido') AS database_name,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            GROUP BY
                {group_by_sql},
                COALESCE(database_name, 'desconhecido')
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_db_user(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: TimeBucket = "day",
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`bucket` de tempo, `db_user`) — visao historica "Tudo"
        da dimensao "Usuarios" (`bucket="day"`), detalhamento por hora dentro de um dia
        (`bucket="hour"`), ou uma granularidade sub-hora da "Fatia de Tempo" (ver `_bucket_sql`).
        Mesmas ressalvas de `wait_time_by_bucket_and_database` (`bucket_start` convertido de
        volta pra `timestamptz` via `AT TIME ZONE 'UTC'` externo)."""
        bucket_start_sql, group_by_sql = self._bucket_sql(bucket)
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                {bucket_start_sql} AS bucket_start,
                COALESCE(db_user, 'desconhecido') AS db_user,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            GROUP BY
                {group_by_sql},
                COALESCE(db_user, 'desconhecido')
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_sql_hash(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        database_name: str | None = None,
        db_user: str | None = None,
        wait_event_native: str | None = None,
        program: str | None = None,
        machine: str | None = None,
        bucket: TimeBucket = "day",
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`bucket` de tempo, `sql_hash`) — visao historica "Tudo" do
        nivel 2 do drill-down (`bucket="day"`), detalhamento por hora dentro de um dia
        (`bucket="hour"`), ou uma granularidade sub-hora da "Fatia de Tempo" (ver `_bucket_sql`).
        Mesmas ressalvas de `wait_time_by_bucket_and_database` (`bucket_start` convertido de
        volta pra `timestamptz` via `AT TIME ZONE 'UTC'` externo, pra sair com `Z` no JSON);
        aceita os mesmos filtros opcionais `database_name`/`db_user` de
        `top_sql()`/`wait_breakdown_by_sql_hash()`, mais `wait_event_native` (usado pela "Fatia
        de Tempo" a partir da folha da dimensao "Waits" — `top_sql_by_wait_type()`).
        `total_wait_time_ms` soma `effective_wait_ms` (delta real por amostra) — filtrados apos a
        CTE `effective`, ver docstring da classe."""
        bucket_start_sql, group_by_sql = self._bucket_sql(bucket)
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                {bucket_start_sql} AS bucket_start,
                sql_hash,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            WHERE (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
              AND (CAST(:db_user AS text) IS NULL OR db_user = :db_user)
              AND (
                CAST(:wait_event_native AS text) IS NULL OR wait_event_native = :wait_event_native
              )
              AND (CAST(:program AS text) IS NULL OR program = :program)
              AND (CAST(:machine AS text) IS NULL OR machine = :machine)
            GROUP BY {group_by_sql}, sql_hash
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "database_name": database_name,
                "db_user": db_user,
                "wait_event_native": wait_event_native,
                "program": program,
                "machine": machine,
                "bucket": bucket,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_time_by_bucket_and_wait_type(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int,
        bucket: TimeBucket = "day",
    ) -> list[sa.Row]:
        """Agrega `query_samples` por (`bucket` de tempo, `wait_event_native`) — visao historica
        "Tudo" da dimensao "Waits" (`bucket="day"`), detalhamento por hora dentro de um dia
        (`bucket="hour"`), ou uma granularidade sub-hora da "Fatia de Tempo" (ver `_bucket_sql`).
        Mesmas ressalvas de `wait_time_by_bucket_and_database` (`bucket_start` convertido de
        volta pra `timestamptz` via `AT TIME ZONE 'UTC'` externo). `total_wait_time_ms` soma
        `effective_wait_ms` (delta real por amostra) — ver docstring da classe. Waits benignos/de
        background excluidos do `GROUP BY` inteiramente (ver `_NOT_BENIGN_WAIT_FILTER`)."""
        bucket_start_sql, group_by_sql = self._bucket_sql(bucket)
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                {bucket_start_sql} AS bucket_start,
                COALESCE(wait_event_native, 'desconhecido') AS wait_event_native,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            WHERE {self._NOT_BENIGN_WAIT_FILTER}
            GROUP BY
                {group_by_sql},
                COALESCE(wait_event_native, 'desconhecido')
            ORDER BY bucket_start, total_wait_time_ms DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "bucket": bucket,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def query_detail(
        self, instance_id: UUID, sql_hash: str, since: datetime, until: datetime, limit: int
    ) -> list[sa.Row]:
        """Amostras (nao agregadas) de um `sql_hash` especifico — nivel 3 (folha) do drill-down
        hierarquico. Usado para mostrar o historico de execucoes de uma query.

        BUG REAL corrigido (relatado pelo usuario ao vivo: soma das durações desta lista nao
        batia com o total da barra do grafico de espera, e o wait nativo/tipo tambem parecia
        "errado"): esta query devolvia `wait_time_ms` BRUTO (cumulativo desde o inicio da MESMA
        espera continua, ver docstring da classe) direto da tabela, sem passar pela CTE
        `_EFFECTIVE_WAIT_SELECT` que TODO metodo agregado desta classe ja usa — a soma das
        amostras brutas nao tinha nenhuma relacao confiavel com o total agregado (podia sobrar
        OU faltar, dependendo de quantas amostras consecutivas cairam na mesma espera continua).
        Corrigido computando `effective_wait_ms`/`effective_cpu_ms` (mesmo delta por sessao) e
        devolvendo os dois - o valor exibido na UI passa a ser a soma dos dois por amostra,
        exatamente a mesma unidade que compoe o total do grafico."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                qs.*,
                e.effective_wait_ms,
                e.effective_cpu_ms
            FROM query_samples AS qs
            INNER JOIN effective AS e
                ON e.session_id IS NOT DISTINCT FROM qs.session_id AND e.time = qs.time
            WHERE qs.instance_id = :instance_id AND qs.sql_hash = :sql_hash
              AND qs.time >= :since AND qs.time <= :until
            ORDER BY qs.time DESC
            LIMIT :limit
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "sql_hash": sql_hash,
                "since": since,
                "until": until,
                "limit": limit,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def wait_summary(
        self, instance_id: UUID, since: datetime, until: datetime
    ) -> list[sa.Row]:
        """Agrega `query_samples` por `wait_type` (categoria canonica cross-engine, ver
        `app/normalizers/wait_taxonomy.py`) — usada na visao geral de waits/bloqueios e na barra
        de severidade de wait da lista de instancias (`InstanceRow`). `total_wait_time_ms` soma
        `effective_wait_ms` (delta real por amostra, nao mais `wait_time_ms` bruto) — ver
        docstring da classe. Reescrita de expression-builder para SQL literal (`sa.text`) porque
        precisa da `LAG()` window function da CTE `effective`, compartilhada com os demais
        metodos desta classe."""
        stmt = sa.text(
            f"""
            WITH effective AS ({self._EFFECTIVE_WAIT_SELECT})
            SELECT
                wait_type,
                coalesce(sum(effective_wait_ms), 0) AS total_wait_time_ms
            FROM effective
            WHERE wait_type IS NOT NULL
            GROUP BY wait_type
            ORDER BY total_wait_time_ms DESC
        """
        )
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "benign_wait_types": list(self._BENIGN_WAIT_EVENT_NATIVE),
            },
        )
        return list(result.fetchall())

    async def latest_by_session(
        self, instance_id: UUID, session_ids: Sequence[str], since: datetime
    ) -> dict[str, sa.Row]:
        """Amostra mais recente por `session_id` — usada pra enriquecer a arvore/detalhe de
        bloqueio com o TEXTO do comando (`blocking_events` nao guarda isso, so o hash — o resto
        do enriquecimento, identidade/wait/recurso, vem denormalizado no proprio evento, ver
        `app.normalizers.models.NormalizedBlockingEvent`). `sql_text_full_masked` e usado pela
        tela de detalhe de sessao (Etapa 9), reaproveitando `HighlightedSqlText` do frontend —
        sessao sem NENHUMA amostra recente (bloqueador ocioso que nunca rodou nada) simplesmente
        nao entra no dict, nao inventa dado."""
        if not session_ids:
            return {}
        stmt = sa.text("""
            SELECT DISTINCT ON (session_id)
                session_id, sql_hash, sql_text_masked, sql_text_full_masked, db_user
            FROM query_samples
            WHERE instance_id = :instance_id AND session_id = ANY(:session_ids) AND time >= :since
            ORDER BY session_id, time DESC
        """)
        result = await self._session.execute(
            stmt,
            {"instance_id": instance_id, "session_ids": list(session_ids), "since": since},
        )
        return {row.session_id: row for row in result.fetchall()}

    async def latest_text_by_sql_hash(
        self, instance_id: UUID, sql_hashes: Sequence[str]
    ) -> dict[str, str]:
        """Resolve `sql_hash` -> `sql_text_masked` (uma amostra QUALQUER com aquele hash, a mais
        recente) — usado pelo histórico de incidentes de bloqueio (`GET /blocking/{id}/incidents`,
        item 22) para exibir o "Comando" de cada lado sem o problema real que `latest_by_session`
        teria aqui: um incidente antigo (já encerrado há dias) referencia um `session_id` que o
        motor pode ter RECICLADO pra uma sessão completamente diferente depois — resolver pelo
        hash (endereçamento por conteúdo, estável) em vez da sessão evita mostrar o comando errado.
        Sem filtro de tempo — `query_samples` não tem retenção configurada (ao contrário de
        `metric_samples`), então um hash de um incidente antigo ainda deve ser resolvível; sem
        amostra nenhuma com aquele hash, o hash simplesmente não entra no dict (não inventa
        dado)."""
        if not sql_hashes:
            return {}
        stmt = sa.text("""
            SELECT DISTINCT ON (sql_hash) sql_hash, sql_text_masked
            FROM query_samples
            WHERE instance_id = :instance_id AND sql_hash = ANY(:sql_hashes)
            ORDER BY sql_hash, time DESC
        """)
        result = await self._session.execute(
            stmt, {"instance_id": instance_id, "sql_hashes": list(sql_hashes)}
        )
        return {row.sql_hash: row.sql_text_masked for row in result.fetchall()}

    async def list_distinct_machines_by_instance(self, since: datetime) -> list[sa.Row]:
        """Máquinas clientes distintas observadas se conectando a cada instância desde `since` —
        base do "Mapa de Dependências" (`TopologyService`): `machine` já é gravado por ambos os
        coletores (PostgreSQL: `client_addr`; SQL Server: `host_name`) em toda amostra de sessão
        ativa, então uma dependência real entre 2 instâncias monitoradas (ex.: linked server,
        `dblink`, um job rodando no mesmo host de outra instância) já aparece aqui sem nenhuma
        coleta nova — o `TopologyService` é quem resolve se uma `machine` corresponde a outra
        instância cadastrada, a um `Server` monitorado, ou fica como "externo" (não inventa
        identidade). `sample_count` é só um peso pra espessura da aresta no mapa, não uma métrica
        de wait time."""
        stmt = sa.text("""
            SELECT instance_id, machine, count(*) AS sample_count
            FROM query_samples
            WHERE time >= :since AND machine IS NOT NULL AND machine <> ''
            GROUP BY instance_id, machine
        """)
        result = await self._session.execute(stmt, {"since": since})
        return list(result.fetchall())
