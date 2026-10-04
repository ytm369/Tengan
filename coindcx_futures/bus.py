from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(eq=False, slots=True)
class Subscription(Generic[T]):
    queue: asyncio.Queue[T]
    dropped: int = 0


class EventBus(Generic[T]):
    """Non-blocking fan-out. A slow subscriber loses its oldest queued event."""

    def __init__(self, queue_size: int = 2048) -> None:
        self.queue_size = queue_size
        self._subscribers: set[Subscription[T]] = set()

    def subscribe(self, queue_size: int | None = None) -> Subscription[T]:
        sub: Subscription[T] = Subscription(asyncio.Queue(maxsize=queue_size or self.queue_size))
        self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: Subscription[T]) -> None:
        self._subscribers.discard(sub)

    def publish(self, event: T) -> None:
        for sub in tuple(self._subscribers):
            if sub.queue.full():
                try:
                    sub.queue.get_nowait()
                    sub.queue.task_done()
                    sub.dropped += 1
                except asyncio.QueueEmpty:
                    pass
            try:
                sub.queue.put_nowait(event)
            except asyncio.QueueFull:
                sub.dropped += 1

