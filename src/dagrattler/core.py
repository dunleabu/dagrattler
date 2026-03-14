from __future__ import annotations

import abc
import asyncio
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import Concatenate, ParamSpec, TypeVar, cast, overload

from .result import Err, Ok, ensure_exception


class _EndSentinel:
    __slots__ = ()


END = _EndSentinel()

InT = TypeVar("InT")
OutT = TypeVar("OutT")
EmittedT = TypeVar("EmittedT")
P = ParamSpec("P")

type EmittedItem[T] = T | Ok[T] | Err
type StreamResult[T] = (
    EmittedItem[T]
    | Iterable[EmittedItem[T]]
    | AsyncIterable[EmittedItem[T]]
    | Awaitable[StreamResult[T] | None]
    | None
)
type ProducerResult = Awaitable[None] | None


def _is_single_value(value: object) -> bool:
    return isinstance(value, (str, bytes, Ok, Err))


async def to_async_iter[T](value: StreamResult[T]) -> AsyncIterator[EmittedItem[T]]:
    if value is None:
        return
        yield
    if isinstance(value, Awaitable):
        async for item in to_async_iter(await value):
            yield item
        return
    if hasattr(value, "__aiter__"):
        async for item in cast(AsyncIterable[EmittedItem[T]], value):
            yield item
        return
    if isinstance(value, Iterable) and not _is_single_value(value):
        for item in cast(Iterable[EmittedItem[T]], value):
            yield item
        return
    yield value


def _normalize_result[T](item: EmittedItem[T]) -> Ok[T] | Err:
    if isinstance(item, (Ok, Err)):
        return item
    return Ok(item)


class BaseNode(abc.ABC):
    def __init__(self, *, name: str | None = None, queue_size: int = 100) -> None:
        self.name = name or self.__class__.__name__
        self.queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_size)
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

    async def _emit(self, item: object) -> None:
        for downstream in self.downstreams:
            await downstream.queue.put(item)

    async def _finish(self) -> None:
        for downstream in self.downstreams:
            await downstream.queue.put(END)

    @abc.abstractmethod
    async def run(self) -> None:
        raise NotImplementedError


class TransformNode[InT, OutT](BaseNode):
    def __init__(
        self,
        func: Callable[..., StreamResult[OutT]],
        *args: object,
        name: str | None = None,
        queue_size: int = 100,
        handle_errors: bool = False,
        **kwargs: object,
    ) -> None:
        super().__init__(
            name=name or getattr(func, "__name__", None), queue_size=queue_size
        )
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.handle_errors = handle_errors

    async def _invoke(self, value: InT) -> None:
        try:
            result = self.func(value, *self.args, **self.kwargs)
            async for output in to_async_iter(result):
                await self._emit(_normalize_result(output))
        except Exception as exc:
            await self._emit(Err(exc))

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
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled:
                await self._finish()


class Emitter[EmittedT]:
    def __init__(self, node: SourceNode[EmittedT]) -> None:
        self._node = node

    async def emit(self, item: EmittedItem[EmittedT]) -> None:
        await self._node._emit(_normalize_result(item))

    async def emit_ok(self, value: EmittedT) -> None:
        await self._node._emit(Ok(value))

    async def emit_err(self, error: BaseException) -> None:
        await self._node._emit(Err(ensure_exception(error)))


class SourceNode[OutT](BaseNode):
    def __init__(
        self,
        producer: Callable[..., ProducerResult],
        *args: object,
        name: str | None = None,
        queue_size: int = 1,
        **kwargs: object,
    ) -> None:
        super().__init__(
            name=name or getattr(producer, "__name__", None), queue_size=queue_size
        )
        self.producer = producer
        self.args = args
        self.kwargs = kwargs

    async def run(self) -> None:
        emitter = Emitter(self)
        cancelled = False
        try:
            result = self.producer(emitter, *self.args, **self.kwargs)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            cancelled = True
            raise
        except Exception as exc:
            await emitter.emit_err(exc)
        finally:
            if not cancelled:
                await self._finish()


class NodeSpec[InT, OutT, **P]:
    def __init__(
        self,
        func: Callable[Concatenate[InT, P], StreamResult[OutT]],
        *,
        handle_errors: bool = False,
        queue_size: int = 100,
    ) -> None:
        self.func = func
        self.handle_errors = handle_errors
        self.queue_size = queue_size
        self.__name__ = getattr(func, "__name__", self.__class__.__name__)

    def __call__(
        self,
        *args: object,
        name: str | None = None,
        queue_size: int | None = None,
        handle_errors: bool | None = None,
        **kwargs: object,
    ) -> TransformNode[InT, OutT]:
        return TransformNode(
            self.func,
            *args,
            name=name or self.__name__,
            queue_size=queue_size if queue_size is not None else self.queue_size,
            handle_errors=(
                handle_errors if handle_errors is not None else self.handle_errors
            ),
            **kwargs,
        )


@overload
def node(
    func: Callable[Concatenate[InT, P], StreamResult[OutT]],
    *,
    handle_errors: bool = False,
    queue_size: int = 100,
) -> NodeSpec[InT, OutT, P]: ...


@overload
def node[InT, OutT, **P](
    func: None = None,
    *,
    handle_errors: bool = False,
    queue_size: int = 100,
) -> Callable[
    [Callable[Concatenate[InT, P], StreamResult[OutT]]], NodeSpec[InT, OutT, P]
]: ...


def node(
    func: Callable[Concatenate[InT, P], StreamResult[OutT]] | None = None,
    *,
    handle_errors: bool = False,
    queue_size: int = 100,
) -> (
    NodeSpec[InT, OutT, P]
    | Callable[
        [Callable[Concatenate[InT, P], StreamResult[OutT]]], NodeSpec[InT, OutT, P]
    ]
):
    def decorator(
        inner: Callable[Concatenate[InT, P], StreamResult[OutT]],
    ) -> NodeSpec[InT, OutT, P]:
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

    def source[OutT](
        self,
        producer: Callable[..., ProducerResult],
        *args: object,
        name: str | None = None,
        queue_size: int = 1,
        **kwargs: object,
    ) -> SourceNode[OutT]:
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

    async def run(self) -> None:
        await self.start()
        await self.wait()

    async def cancel(self) -> None:
        tasks = [node._task for node in self.nodes if node._task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
