from __future__ import annotations

import abc
import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Iterable
from typing import Any

from .result import Err, Ok, ensure_exception


class _EndSentinel:
    __slots__ = ()


END = _EndSentinel()


def _is_single_value(value: Any) -> bool:
    return isinstance(value, (str, bytes, Ok, Err))


async def to_async_iter(value: Any) -> AsyncIterator[Any]:
    if value is None:
        return
        yield
    if isinstance(value, Awaitable):
        async for item in to_async_iter(await value):
            yield item
        return
    if hasattr(value, "__aiter__"):
        async for item in value:
            yield item
        return
    if isinstance(value, Iterable) and not _is_single_value(value):
        for item in value:
            yield item
        return
    yield value


def _normalize_result(item: Any) -> Ok[Any] | Err:
    if isinstance(item, (Ok, Err)):
        return item
    return Ok(item)


class BaseNode(abc.ABC):
    def __init__(self, *, name: str | None = None, queue_size: int = 100) -> None:
        self.name = name or self.__class__.__name__
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
        self.upstreams: list[BaseNode] = []
        self.downstreams: list[BaseNode] = []
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._closed_upstreams = 0

    def connect(self, *others: BaseNode) -> BaseNode | tuple[BaseNode, ...]:
        for other in others:
            if other not in self.downstreams:
                self.downstreams.append(other)
            if self not in other.upstreams:
                other.upstreams.append(self)
        if len(others) == 1:
            return others[0]
        return others

    async def _emit(self, item: Any) -> None:
        for downstream in self.downstreams:
            await downstream.queue.put(item)

    async def _finish(self) -> None:
        for downstream in self.downstreams:
            await downstream.queue.put(END)

    @abc.abstractmethod
    async def run(self) -> None:
        raise NotImplementedError


class TransformNode(BaseNode):
    def __init__(
        self,
        func: Any,
        *args: Any,
        name: str | None = None,
        queue_size: int = 100,
        handle_errors: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(name=name or getattr(func, "__name__", None), queue_size=queue_size)
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.handle_errors = handle_errors

    async def _invoke(self, value: Any) -> None:
        try:
            result = self.func(value, *self.args, **self.kwargs)
            async for output in to_async_iter(result):
                await self._emit(_normalize_result(output))
        except Exception as exc:
            await self._emit(Err(exc))

    async def run(self) -> None:
        try:
            expected_ends = len(self.upstreams)
            while True:
                item = await self.queue.get()
                if item is END:
                    self._closed_upstreams += 1
                    if self._closed_upstreams >= expected_ends:
                        break
                    continue
                if isinstance(item, Err) and not self.handle_errors:
                    await self._emit(item)
                    continue
                if isinstance(item, Ok):
                    await self._invoke(item.value)
                    continue
                if isinstance(item, Err):
                    await self._invoke(item.error)
                    continue
                await self._invoke(item)
        finally:
            await self._finish()


class Emitter:
    def __init__(self, node: SourceNode) -> None:
        self._node = node

    async def emit(self, item: Any) -> None:
        await self._node._emit(_normalize_result(item))

    async def emit_ok(self, value: Any) -> None:
        await self._node._emit(Ok(value))

    async def emit_err(self, error: BaseException) -> None:
        await self._node._emit(Err(ensure_exception(error)))


class SourceNode(BaseNode):
    def __init__(
        self,
        producer: Any,
        *args: Any,
        name: str | None = None,
        queue_size: int = 1,
        **kwargs: Any,
    ) -> None:
        super().__init__(name=name or getattr(producer, "__name__", None), queue_size=queue_size)
        self.producer = producer
        self.args = args
        self.kwargs = kwargs

    async def run(self) -> None:
        emitter = Emitter(self)
        try:
            result = self.producer(emitter, *self.args, **self.kwargs)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            await emitter.emit_err(exc)
        finally:
            await self._finish()


class NodeSpec:
    def __init__(
        self,
        func: Any,
        *,
        handle_errors: bool = False,
        queue_size: int = 100,
    ) -> None:
        self.func = func
        self.handle_errors = handle_errors
        self.queue_size = queue_size
        self.__name__ = getattr(func, "__name__", self.__class__.__name__)

    def __call__(self, *args: Any, **kwargs: Any) -> TransformNode:
        name = kwargs.pop("name", None)
        queue_size = kwargs.pop("queue_size", self.queue_size)
        handle_errors = kwargs.pop("handle_errors", self.handle_errors)
        return TransformNode(
            self.func,
            *args,
            name=name or self.__name__,
            queue_size=queue_size,
            handle_errors=handle_errors,
            **kwargs,
        )


def node(
    func: Any | None = None,
    *,
    handle_errors: bool = False,
    queue_size: int = 100,
) -> NodeSpec | Any:
    def decorator(inner: Any) -> NodeSpec:
        return NodeSpec(inner, handle_errors=handle_errors, queue_size=queue_size)

    if func is None:
        return decorator
    return decorator(func)


class Graph:
    def __init__(self) -> None:
        self.nodes: list[BaseNode] = []

    def add(self, *nodes: BaseNode) -> Graph:
        for node in nodes:
            if node not in self.nodes:
                self.nodes.append(node)
        return self

    def source(
        self,
        producer: Any,
        *args: Any,
        name: str | None = None,
        queue_size: int = 1,
        **kwargs: Any,
    ) -> SourceNode:
        node = SourceNode(producer, *args, name=name, queue_size=queue_size, **kwargs)
        self.add(node)
        return node

    async def start(self) -> None:
        for node in self.nodes:
            if node._started:
                continue
            node._task = asyncio.create_task(node.run(), name=node.name)
            node._started = True

    async def wait(self) -> None:
        tasks = [node._task for node in self.nodes if node._task is not None]
        if tasks:
            await asyncio.gather(*tasks)

    async def cancel(self) -> None:
        tasks = [node._task for node in self.nodes if node._task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self) -> None:
        await self.start()
        await self.wait()
