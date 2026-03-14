from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable
from typing import cast

from .core import BaseNode, END, Result, TransformNode
from .result import Err, Ok


def map_node[InT, OutT](
    fn: Callable[[InT], OutT | Awaitable[OutT]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, OutT]:
    async def wrapped(item: Result[InT]) -> list[Result[OutT]]:
        if isinstance(item, Err):
            return [item]
        result = fn(item.value)
        if inspect.isawaitable(result):
            result = await result
        return [Ok(result)]

    return TransformNode(
        wrapped, name=name or getattr(fn, "__name__", None), queue_size=queue_size
    )


def filter_node[InT](
    predicate: Callable[[InT], bool | Awaitable[bool]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, InT]:
    async def wrapped(item: Result[InT]) -> list[Result[InT]]:
        if isinstance(item, Err):
            return [item]
        keep = predicate(item.value)
        if inspect.isawaitable(keep):
            keep = await keep
        return [item] if keep else []

    return TransformNode(
        wrapped,
        name=name or getattr(predicate, "__name__", None),
        queue_size=queue_size,
    )


def flat_map_node[InT, OutT](
    fn: Callable[[InT], Iterable[OutT] | Awaitable[Iterable[OutT]]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, OutT]:
    async def wrapped(item: Result[InT]) -> list[Result[OutT]]:
        if isinstance(item, Err):
            return [item]
        result = fn(item.value)
        if inspect.isawaitable(result):
            result = await result
        return [Ok(value) for value in result]

    return TransformNode(
        wrapped, name=name or getattr(fn, "__name__", None), queue_size=queue_size
    )


class BatchNode[T](BaseNode):
    def __init__(
        self, size: int, *, name: str | None = None, queue_size: int = 100
    ) -> None:
        if size <= 0:
            raise ValueError("size must be > 0")
        super().__init__(name=name or "batch", queue_size=queue_size)
        self.size = size
        self._buffer: list[T] = []

    async def _flush(self) -> None:
        if self._buffer:
            batch = self._buffer
            self._buffer = []
            await self._emit(Ok(batch))

    async def run(self) -> None:
        cancelled = False
        try:
            expected_ends = len(self.upstreams)
            while True:
                item = await self.queue.get()
                if item is END:
                    self._closed_upstreams += 1
                    if self._closed_upstreams >= expected_ends:
                        await self._flush()
                        break
                    continue

                event = cast(Result[T], item)
                if isinstance(event, Err):
                    await self._emit(event)
                    continue

                self._buffer.append(event.value)
                if len(self._buffer) >= self.size:
                    await self._flush()
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled:
                await self._finish()


def batch_node[T](
    size: int, *, name: str | None = None, queue_size: int = 100
) -> BatchNode[T]:
    return BatchNode(size=size, name=name, queue_size=queue_size)


def sink_node[InT](
    fn: Callable[[InT], object | Awaitable[object]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, object]:
    async def wrapped(item: Result[InT]) -> list[Result[object]]:
        if isinstance(item, Err):
            return [item]
        result = fn(item.value)
        if inspect.isawaitable(result):
            await result
        return []

    return TransformNode(
        wrapped, name=name or getattr(fn, "__name__", None), queue_size=queue_size
    )


class RecoverNode[InT, RecoveredT](BaseNode):
    def __init__(
        self,
        fn: Callable[
            [Exception], Iterable[RecoveredT] | Awaitable[Iterable[RecoveredT]]
        ],
        *,
        name: str | None = None,
        queue_size: int = 100,
    ) -> None:
        super().__init__(
            name=name or getattr(fn, "__name__", None), queue_size=queue_size
        )
        self.fn = fn

    async def run(self) -> None:
        cancelled = False
        try:
            expected_ends = len(self.upstreams)
            while True:
                item = await self.queue.get()
                if item is END:
                    self._closed_upstreams += 1
                    if self._closed_upstreams >= expected_ends:
                        break
                    continue

                event = cast(Result[InT], item)
                if isinstance(event, Ok):
                    await self._emit(event)
                    continue

                try:
                    result = self.fn(event.error)
                    if inspect.isawaitable(result):
                        result = await result
                    outputs = [Ok(output) for output in result]
                except Exception as exc:
                    outputs = [Err(exc)]

                for output in outputs:
                    await self._emit(output)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled:
                await self._finish()


def recover_node[InT, RecoveredT](
    fn: Callable[[Exception], Iterable[RecoveredT] | Awaitable[Iterable[RecoveredT]]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> RecoverNode[InT, RecoveredT]:
    return RecoverNode(fn, name=name, queue_size=queue_size)
