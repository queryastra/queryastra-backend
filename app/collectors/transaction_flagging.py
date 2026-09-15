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


def compute_flag_reason(
    *,
    involved_in_blocking: bool,
    has_error: bool,
    begin_time: datetime | None,
    now: datetime,
    orphan_threshold_minutes: int,
) -> str | None:
    """Tags combinadas (separadas por virgula) explicando por que gravar esta transacao — `None`
    quando nenhum criterio se aplica (transacao aberta comum, nao deve virar uma linha)."""
    tags: list[str] = []
    if involved_in_blocking:
        tags.append(_TAG_BLOCKING)
    if has_error:
        tags.append(_TAG_ERROR)
    if begin_time is not None:
        age_minutes = (now - begin_time).total_seconds() / 60
        if age_minutes >= orphan_threshold_minutes:
            tags.append(_TAG_ORPHAN)
    return ",".join(tags) if tags else None
