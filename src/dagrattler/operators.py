"""Convenience operators built on top of the core result-based transform model."""

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
    """Create a node that maps each successful input value to one output value.

    Args:
        fn: Callable applied to each ``Ok`` value. It may be sync or async.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Returns:
        A ``TransformNode`` that forwards ``Err`` values unchanged and emits one
        ``Ok`` result for each successful input.
    """

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
    """Create a node that keeps successful inputs matching a predicate.

    Args:
        predicate: Callable returning whether an ``Ok`` value should be kept. It may
            be sync or async.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Returns:
        A ``TransformNode`` that forwards matching ``Ok`` values, drops non-matching
        ones, and propagates ``Err`` values unchanged.
    """

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
    """Create a node that expands each successful input into zero or more outputs.

    Args:
        fn: Callable applied to each ``Ok`` value. It returns an iterable of output
            values and may be sync or async.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Returns:
        A ``TransformNode`` that emits one ``Ok`` per returned output value and
        propagates ``Err`` values unchanged.
    """

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
    """Node that groups successful inputs into fixed-size lists.

    Args:
        size: Number of successful input values to buffer before emitting a batch.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Outputs:
        Emits ``Ok[list[T]]`` batches and forwards ``Err`` values unchanged.
    """

    def __init__(
        self, size: int, *, name: str | None = None, queue_size: int = 100
    ) -> None:
        """Initialize a batching node.

        Args:
            size: Number of successful values per emitted batch. Must be positive.
            name: Optional node name shown in task names and debugging output.
            queue_size: Maximum number of queued input results for the node.
        """

        if size <= 0:
            raise ValueError("size must be > 0")
        super().__init__(name=name or "batch", queue_size=queue_size)
        self.size = size
        self._buffer: list[T] = []

    async def _flush(self) -> None:
        """Emit the current batch buffer if it contains any values."""

        if self._buffer:
            batch = self._buffer
            self._buffer = []
            await self._emit(Ok(batch))

    async def run(self) -> None:
        """Consume upstream results and emit buffered batches downstream."""

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
    """Create a batching node for successful input values.

    Args:
        size: Number of successful values per emitted batch. Must be positive.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Returns:
        A ``BatchNode`` that emits ``Ok[list[T]]`` batches and forwards ``Err``
        values unchanged.
    """

    return BatchNode(size=size, name=name, queue_size=queue_size)


def sink_node[InT](
    fn: Callable[[InT], object | Awaitable[object]],
    *,
    name: str | None = None,
    queue_size: int = 100,
) -> TransformNode[InT, object]:
    """Create a node that performs side effects for each successful input value.

    Args:
        fn: Callable invoked for each ``Ok`` value. It may be sync or async.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Returns:
        A ``TransformNode`` that produces no outputs for successful inputs and
        forwards ``Err`` values unchanged.
    """

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
    """Node that converts error results into replacement successful outputs.

    Args:
        fn: Callable invoked for each ``Err`` value. It returns replacement output
            values and may be sync or async.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Outputs:
        Forwards ``Ok`` values unchanged and emits ``Ok`` results created from the
        recovery outputs for each incoming ``Err``.
    """

    def __init__(
        self,
        fn: Callable[
            [Exception], Iterable[RecoveredT] | Awaitable[Iterable[RecoveredT]]
        ],
        *,
        name: str | None = None,
        queue_size: int = 100,
    ) -> None:
        """Initialize a recovery node.

        Args:
            fn: Callable invoked for each incoming ``Err`` value.
            name: Optional node name shown in task names and debugging output.
            queue_size: Maximum number of queued input results for the node.
        """

        super().__init__(
            name=name or getattr(fn, "__name__", None), queue_size=queue_size
        )
        self.fn = fn

    async def run(self) -> None:
        """Consume upstream results and recover from errors when possible."""

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
    """Create a node that replaces errors with fallback output values.

    Args:
        fn: Callable invoked for each ``Err`` value. It returns replacement output
            values and may be sync or async.
        name: Optional node name shown in task names and debugging output.
        queue_size: Maximum number of queued input results for the node.

    Returns:
        A ``RecoverNode`` that forwards ``Ok`` values unchanged and emits one
        ``Ok`` result per recovery output value.
    """

    return RecoverNode(fn, name=name, queue_size=queue_size)
