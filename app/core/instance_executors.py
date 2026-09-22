"""Isolamento de thread-pool por instancia — mitigacao para `pymssql` sincrono e nao-cancelavel.

Bug real relatado ao vivo pelo usuario ("todos os servidores SQL aparecendo como indisponivel")
e investigado nesta sessao: `pymssql` nao tem nenhum mecanismo de timeout garantido no nivel do
socket/SO (confirmado: a API publica do modulo nao expoe socket nem `settimeout()` — o parametro
`timeout=`/`login_timeout=` ja usado em toda chamada e o timeout de protocolo do proprio FreeTDS,
documentado como nao confiavel para uma conexao genuinamente travada, ex. rede caida sem RST/FIN).
Quando isso acontece, `asyncio.wait_for` so abandona a ESPERA do lado asyncio — a thread real, do
executor padrao do event loop, fica presa NAQUELA chamada para sempre (Python nao tem API segura
para matar uma thread de fora). Como o executor padrao e COMPARTILHADO entre todas as instancias,
cada travamento consome 1 vaga permanentemente ate esgotar o pool inteiro — nesse ponto, TODAS as
instancias (nao so a travada) ficam sem thread disponivel, aparecendo como indisponiveis.

Correcao: cada instancia monitorada (SQL Server) ganha seu proprio `ThreadPoolExecutor` DEDICADO,
pequeno (`instance_executor_max_workers`), criado sob demanda e reaproveitado entre ciclos — uma
instancia com todas as suas threads travadas para de coletar sozinha (aparece corretamente como
indisponivel, o comportamento esperado), mas NUNCA mais consome a capacidade de outra instancia.
Nao elimina o problema de uma thread travada em si (impossivel de forma segura em Python puro,
sem isolamento por processo) — contem o raio de explosao a UMA instancia.
"""

from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_executors: dict[UUID, ThreadPoolExecutor] = {}
_adhoc_executor: ThreadPoolExecutor | None = None
_lock = threading.Lock()


def _get_or_create_executor(instance_id: UUID) -> ThreadPoolExecutor:
    executor = _executors.get(instance_id)
    if executor is not None:
        return executor
    with _lock:
        executor = _executors.get(instance_id)
        if executor is None:
            max_workers = get_settings().instance_executor_max_workers
            executor = ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix=f"instance-{instance_id}",
            )
            _executors[instance_id] = executor
            logger.info(
                "Thread pool dedicado criado para instance_id=%s (max_workers=%s)",
                instance_id,
                max_workers,
            )
        return executor


def _get_adhoc_executor() -> ThreadPoolExecutor:
    """Usado quando ainda nao ha `instance_id` (ex.: teste de conexao no wizard de cadastro,
    antes da instancia existir) — risco aceitavel, e uma acao pontual disparada pelo usuario, nao
    um worker recorrente de fundo que acumularia travamentos com o tempo."""
    global _adhoc_executor
    if _adhoc_executor is not None:
        return _adhoc_executor
    with _lock:
        if _adhoc_executor is None:
            _adhoc_executor = ThreadPoolExecutor(
                max_workers=get_settings().instance_executor_max_workers,
                thread_name_prefix="instance-adhoc",
            )
        return _adhoc_executor


async def run_in_instance_executor[T](
    instance_id: UUID | None, func: Callable[..., T], *args: object
) -> T:
    """Equivalente a `asyncio.to_thread(func, *args)`, mas isolado por `instance_id` — roda no
    pool dedicado dessa instancia (ver docstring do modulo) em vez do executor padrao
    compartilhado do event loop."""
    loop = asyncio.get_running_loop()
    executor = (
        _get_adhoc_executor() if instance_id is None else _get_or_create_executor(instance_id)
    )
    return await loop.run_in_executor(executor, functools.partial(func, *args))


def shutdown_all() -> None:
    """Libera todos os executores dedicados — chamado no shutdown do lifespan do backend.
    `wait=False`: uma thread travada nunca termina sozinha (ver docstring do modulo), esperar por
    ela travaria o proprio shutdown do processo; `cancel_futures=True` so cancela tarefas ainda
    NAO iniciadas (enfileiradas), nunca uma ja em execucao."""
    with _lock:
        for executor in _executors.values():
            executor.shutdown(wait=False, cancel_futures=True)
        _executors.clear()
        global _adhoc_executor
        if _adhoc_executor is not None:
            _adhoc_executor.shutdown(wait=False, cancel_futures=True)
            _adhoc_executor = None
