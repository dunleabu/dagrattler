from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TypeVar, cast

from .core import (
    BaseNode,
    END,
    StreamResult,
    TransformNode,
    _normalize_result,
    to_async_iter,
)
from .result import Err, Ok

InT = TypeVar("InT")
OutT = TypeVar("OutT")
RecoveredT = TypeVar("RecoveredT")


def map_node(
    fn: Callable[[InT], StreamResult[OutT]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, OutT]:
    return TransformNode(
        fn, name=name or getattr(fn, "__name__", None), queue_size=queue_size
    )


def filter_node(
    predicate: Callable[[InT], bool],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, InT]:
    def _filter(value: InT) -> list[InT]:
        return [value] if predicate(value) else []

    return TransformNode(
        _filter,
        name=name or getattr(predicate, "__name__", None),
        queue_size=queue_size,
    )


def flat_map_node(
    fn: Callable[[InT], StreamResult[OutT]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, OutT]:
    return TransformNode(
        fn, name=name or getattr(fn, "__name__", None), queue_size=queue_size
    )


class BatchNode[InT](BaseNode):
    def __init__(
        self, size: int, *, name: str | None = None, queue_size: int = 100
    ) -> None:
        if size <= 0:
            raise ValueError("size must be > 0")
        super().__init__(name=name or "batch", queue_size=queue_size)
        self.size = size
        self._buffer: list[InT] = []

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
                if isinstance(item, Err):
                    await self._emit(item)
                    continue
                if isinstance(item, Ok):
                    self._buffer.append(cast(InT, item.value))
                else:
                    self._buffer.append(cast(InT, item))
                if len(self._buffer) >= self.size:
                    await self._flush()
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled:
                await self._finish()


def batch_node(
    size: int, *, name: str | None = None, queue_size: int = 100
) -> BatchNode[InT]:
    return BatchNode(size=size, name=name, queue_size=queue_size)


def sink_node(
    fn: Callable[[InT], StreamResult[OutT]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, OutT]:
    return TransformNode(
        fn, name=name or getattr(fn, "__name__", None), queue_size=queue_size
    )


class RecoverNode[InT, RecoveredT](BaseNode):
    def __init__(
        self,
        fn: Callable[[Exception], StreamResult[RecoveredT]],
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
                if isinstance(item, Ok):
                    await self._emit(item)
                    continue
                if isinstance(item, Err):
                    try:
                        result = self.fn(item.error)
                        async for output in to_async_iter(result):
                            await self._emit(_normalize_result(output))
                    except Exception as exc:
                        await self._emit(Err(exc))
                    continue
                await self._emit(_normalize_result(cast(InT, item)))
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled:
                await self._finish()


def recover_node(
    fn: Callable[[Exception], StreamResult[RecoveredT]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> RecoverNode[InT, RecoveredT]:
    return RecoverNode(fn, name=name, queue_size=queue_size)
