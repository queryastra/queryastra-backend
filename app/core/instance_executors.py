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

Watchdog de saturacao: sem mais nada, o pool dedicado de uma instancia cuja rede caiu em silencio
(sem RST/FIN) satura depois de `max_workers` ciclos presos e a instancia fica ERR para sempre, ate
reiniciar o backend — mesmo depois de a rede voltar. Antes de cada submissao, os
`concurrent.futures.Future`s em voo da instancia sao conferidos: se TODAS as `max_workers` threads
estao executando ha mais de `instance_executor_stuck_threshold_seconds`, o executor e descartado e
recriado. As threads antigas ficam orfas ate o SO derrubar o socket, mas novos ciclos rodam
imediatamente em threads novas. Uma chamada lenta porem legitima que estivesse entre as "presas"
nao perde o resultado: ela continua na thread antiga e entrega o retorno normalmente. As threads
orfas entram em `_released_threads`, para `exit_if_threads_stuck` nao deixar o processo pendurado
no shutdown por causa delas.
"""

from __future__ import annotations

import asyncio
import functools
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
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


class _InFlight:
    """Uma chamada submetida ao pool de uma instancia. `started_at` e preenchido pela PROPRIA
    thread quando a chamada comeca a executar — o tempo parado na fila nao conta como "presa"."""

    __slots__ = ("future", "started_at")

    def __init__(self) -> None:
        self.started_at: float | None = None
        self.future: Future | None = None


# Chamadas em voo por instancia, para o watchdog de saturacao. Acesso exclusivo do event loop
# (asyncio e single-thread) — sem lock; a thread de trabalho so escreve `started_at`.
_live_futures: dict[UUID, list[_InFlight]] = {}

_released_threads: list[threading.Thread] = []


def _get_executor_with_watchdog(instance_id: UUID) -> ThreadPoolExecutor:
    """Executor da instancia, recriado se todas as suas threads estiverem presas (ver docstring
    do modulo)."""
    settings = get_settings()
    max_workers = settings.instance_executor_max_workers
    threshold = settings.instance_executor_stuck_threshold_seconds
    now = time.monotonic()

    # Descarta as ja concluidas (sucesso, erro ou canceladas na fila pelo `asyncio.wait_for`).
    live = [c for c in _live_futures.get(instance_id, []) if c.future and not c.future.done()]
    _live_futures[instance_id] = live

    stuck = [c for c in live if c.started_at is not None and now - c.started_at > threshold]
    if len(stuck) < max_workers:
        return _get_or_create_executor(instance_id)

    with _lock:
        old = _executors.get(instance_id)
        if old is not None:
            _released_threads.extend(old._threads)
            # `cancel_futures=False`: cancelar uma tarefa enfileirada faria o `await` de quem a
            # submeteu levantar `CancelledError` sem que a task tenha sido cancelada (pode
            # derrubar o loop do worker). Na pratica a fila ja esta vazia — o `wait_for` de cada
            # chamador cancela a propria tarefa enfileirada ao estourar o timeout.
            old.shutdown(wait=False, cancel_futures=False)
        new_executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"instance-{instance_id}",
        )
        _executors[instance_id] = new_executor
    _live_futures[instance_id] = []
    logger.warning(
        "Watchdog: pool saturado para instance_id=%s (%d/%d threads presas ha mais de %ss) — "
        "executor recriado, threads antigas abandonadas",
        instance_id,
        len(stuck),
        max_workers,
        threshold,
    )
    return new_executor


async def run_in_instance_executor[T](
    instance_id: UUID | None, func: Callable[..., T], *args: object
) -> T:
    """Equivalente a `asyncio.to_thread(func, *args)`, mas isolado por `instance_id` — roda no
    pool dedicado dessa instancia (ver docstring do modulo) em vez do executor padrao
    compartilhado do event loop. Com `instance_id`, passa pelo watchdog de saturacao."""
    if instance_id is None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_get_adhoc_executor(), functools.partial(func, *args))

    executor = _get_executor_with_watchdog(instance_id)
    call = _InFlight()

    def _run() -> T:
        call.started_at = time.monotonic()
        return func(*args)

    cf_future: Future[T] = executor.submit(_run)
    call.future = cf_future
    # Registrado ANTES do await: se o `wait_for` do chamador estourar, a chamada continua
    # rastreada e e contada como presa na proxima submissao.
    _live_futures.setdefault(instance_id, []).append(call)
    return await asyncio.wrap_future(cf_future)


def shutdown_all() -> None:
    """Libera todos os executores dedicados — chamado no shutdown do lifespan do backend.
    `wait=False`: uma thread travada nunca termina sozinha (ver docstring do modulo), esperar por
    ela travaria o proprio shutdown do processo; `cancel_futures=True` so cancela tarefas ainda
    NAO iniciadas (enfileiradas), nunca uma ja em execucao."""
    global _adhoc_executor
    with _lock:
        pools = list(_executors.values())
        if _adhoc_executor is not None:
            pools.append(_adhoc_executor)
        for executor in pools:
            _released_threads.extend(executor._threads)
            executor.shutdown(wait=False, cancel_futures=True)
        _executors.clear()
        _adhoc_executor = None
    _live_futures.clear()


def stuck_threads() -> list[threading.Thread]:
    return [t for t in _released_threads if t.is_alive()]


def exit_if_threads_stuck(grace_seconds: float) -> None:
    """`wait=False` nao basta: na saida do interpretador o `concurrent.futures` ainda faz join de
    toda thread de pool, entao uma chamada de driver travada segura o processo para sempre (Ctrl+C
    e o reload do uvicorn ficam pendurados). Um timer daemon encerra o processo se, passado o
    prazo, ainda houver thread presa — desligamentos normais terminam antes e nada acontece."""

    def _check() -> None:
        stuck = stuck_threads()
        if stuck:
            logger.warning(
                "%d thread(s) de instancia presa(s) em chamada de driver apos o shutdown — "
                "encerrando o processo a forca",
                len(stuck),
            )
            os._exit(0)

    timer = threading.Timer(grace_seconds, _check)
    timer.daemon = True
    timer.start()
