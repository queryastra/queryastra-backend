"""Testes de `app.core.instance_executors` — isolamento de thread-pool por instancia.

Regressao central: uma chamada travada numa instancia NUNCA pode bloquear/atrasar uma chamada
de OUTRA instancia — o motivo de existir deste modulo (bug real relatado ao vivo: "todos os
servidores SQL aparecendo como indisponivel", causado por um pool de threads COMPARTILHADO se
esgotando com chamadas travadas de qualquer instancia).
"""

from __future__ import annotations

import asyncio
import threading
import time
from uuid import uuid4

from app.core import instance_executors


def setup_function() -> None:
    # Cada teste comeca com o registro limpo - evita um executor de um teste anterior vazar
    # pro proximo (ex.: contagem de threads/instancias).
    instance_executors.shutdown_all()


def teardown_function() -> None:
    instance_executors.shutdown_all()


def _thread_name() -> str:
    return threading.current_thread().name


async def test_same_instance_reuses_the_same_executor() -> None:
    instance_id = uuid4()
    first_name = await instance_executors.run_in_instance_executor(instance_id, _thread_name)
    second_name = await instance_executors.run_in_instance_executor(instance_id, _thread_name)

    assert first_name.startswith(f"instance-{instance_id}")
    assert second_name.startswith(f"instance-{instance_id}")


async def test_different_instances_get_different_executors() -> None:
    instance_a = uuid4()
    instance_b = uuid4()

    name_a = await instance_executors.run_in_instance_executor(instance_a, _thread_name)
    name_b = await instance_executors.run_in_instance_executor(instance_b, _thread_name)

    assert name_a.startswith(f"instance-{instance_a}")
    assert name_b.startswith(f"instance-{instance_b}")
    assert name_a != name_b


async def test_none_instance_id_uses_shared_adhoc_executor() -> None:
    """Teste de conexao pre-cadastro (sem instance_id ainda) - risco aceitavel, acao pontual do
    usuario, nao um worker recorrente."""
    first_name = await instance_executors.run_in_instance_executor(None, _thread_name)
    second_name = await instance_executors.run_in_instance_executor(None, _thread_name)

    assert first_name.startswith("instance-adhoc")
    assert second_name.startswith("instance-adhoc")


def _block_until_released(release_event: threading.Event) -> None:
    release_event.wait(timeout=5)


def _return_immediately() -> str:
    return "ok"


async def test_stuck_call_in_one_instance_never_blocks_another_instance() -> None:
    """A regressao central deste modulo: uma chamada travada (simulada aqui por um
    `threading.Event` nunca liberado durante o teste) numa instancia A nao atrasa NADA de uma
    chamada concorrente na instancia B - o oposto do bug real com o executor padrao
    compartilhado (uma thread presa esgota o pool inteiro, afetando todas as instancias)."""
    instance_a = uuid4()
    instance_b = uuid4()
    release_event = threading.Event()

    stuck_task = asyncio.create_task(
        instance_executors.run_in_instance_executor(
            instance_a, _block_until_released, release_event
        )
    )
    await asyncio.sleep(0.05)  # garante que a chamada travada ja ocupou a thread da instancia A

    start = time.perf_counter()
    result = await asyncio.wait_for(
        instance_executors.run_in_instance_executor(instance_b, _return_immediately),
        timeout=2,
    )
    elapsed = time.perf_counter() - start

    assert result == "ok"
    assert elapsed < 1.0  # nao esperou a instancia A travada, nem perto do timeout de 5s dela

    release_event.set()  # libera a chamada travada pra nao vazar thread entre testes
    await asyncio.wait_for(stuck_task, timeout=5)


async def test_shutdown_all_clears_the_registry() -> None:
    instance_id = uuid4()
    await instance_executors.run_in_instance_executor(instance_id, _thread_name)
    assert instance_id in instance_executors._executors  # noqa: SLF001 - inspeciona estado interno

    instance_executors.shutdown_all()

    assert instance_executors._executors == {}  # noqa: SLF001
