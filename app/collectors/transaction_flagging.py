"""Criterio compartilhado de "esta transacao merece virar um `NormalizedTransactionEvent`?"

Etapa 10, redesenho pos-uso real: `transaction_events` deixou de ser um espelho de TODA transacao
aberta (custo de escrita desnecessario, e inutil pra investigacao — a transacao fechava antes de
dar tempo de clicar) e virou um log de incidente retido — so transacoes envolvidas em bloqueio,
com erro capturado, abertas por tempo suspeito ("orfa") ou (depois, via correlacao assincrona do
`DeadlockSupervisor` — ver `TransactionEventsRepository.tag_open_transactions_by_session`)
envolvidas em deadlock chegam a ser gravadas.

Usado por AMBOS os coletores (`app.collectors.sqlserver.adapter`/`app.collectors.postgresql.
adapter`) — mesma logica, evita duplicar a montagem da string de tags em cada um.
"""

from __future__ import annotations

from datetime import datetime

_TAG_BLOCKING = "bloqueio"
_TAG_ERROR = "erro"
_TAG_ORPHAN = "orfa"
_TAG_SLEEPING = "sleeping"


def compute_flag_reason(
    *,
    involved_in_blocking: bool,
    has_error: bool,
    begin_time: datetime | None,
    now: datetime,
    orphan_threshold_minutes: int,
    is_idle: bool = False,
    sleeping_threshold_ms: int | None = None,
) -> str | None:
    """Tags combinadas (separadas por virgula) explicando por que gravar esta transacao — `None`
    quando nenhum criterio se aplica (transacao aberta comum, nao deve virar uma linha).

    `is_idle`/`sleeping_threshold_ms` (novas tarefas do usuario, itens 1/2): a sessao dona da
    transacao esta ociosa agora (sem request ativa/"sleeping" no SQL Server, "idle in
    transaction" no PostgreSQL) HA PELO MENOS `sleeping_threshold_ms` desde que a transacao abriu.
    Diferente de `_TAG_ORPHAN` (qualquer transacao aberta ha muito tempo, sessao ativa ou nao,
    limiar fixo em `.env`), esta tag e mais especifica (exige sessao ociosa) e o limiar e
    editavel na UI com override por instancia/banco (`system_settings.sleeping_min_duration_ms`,
    resolvido pelo chamador — `sleeping_threshold_ms=None` desativa a checagem, mesmo
    comportamento de antes desta feature)."""
    tags: list[str] = []
    if involved_in_blocking:
        tags.append(_TAG_BLOCKING)
    if has_error:
        tags.append(_TAG_ERROR)
    if begin_time is not None:
        age_minutes = (now - begin_time).total_seconds() / 60
        if age_minutes >= orphan_threshold_minutes:
            tags.append(_TAG_ORPHAN)
        if is_idle and sleeping_threshold_ms is not None:
            age_ms = (now - begin_time).total_seconds() * 1000
            if age_ms >= sleeping_threshold_ms:
                tags.append(_TAG_SLEEPING)
    return ",".join(tags) if tags else None
