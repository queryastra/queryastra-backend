"""Barramento de eventos in-process, single-process (sem Redis/fila externa - suficiente para
o deployment atual de instancia unica por site, ver ARCHITECTURE_SPEC.md Q19)."""

import asyncio


class EventBus:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[dict]] = set()

    def subscribe(self) -> asyncio.Queue[dict]:
        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=100)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict]) -> None:
        self._subscribers.discard(queue)

    def publish(self, event: dict) -> None:
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # assinante lento perde evento, nao trava o publisher


event_bus = EventBus()  # singleton do processo
