"""Core graph and node primitives for asynchronous DAG-style stream processing."""

from __future__ import annotations

import abc
import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Concatenate, cast, overload

from .result import Err, Ok, ensure_exception


class _EndSentinel:
    """Marker object used to signal that an upstream node has finished."""

    __slots__ = ()


END = _EndSentinel()

type Result[T] = Ok[T] | Err
type NodeFn[InT, OutT] = Callable[[Result[InT]], Awaitable[list[Result[OutT]]]]
type ProducerResult = Awaitable[None] | None


class BaseNode(abc.ABC):
    """Abstract base class for nodes connected together inside a graph."""

    def __init__(self, *, name: str | None = None, queue_size: int = 100) -> None:
        """Initialize shared node state such as queues and graph connections."""

        self.name = name or self.__class__.__name__
        self.queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_size)
        self.upstreams: list[BaseNode] = []
        self.downstreams: list[BaseNode] = []
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._closed_upstreams = 0

    def connect(self, *others: BaseNode) -> BaseNode | tuple[BaseNode, ...]:
        """Connect this node to downstream nodes and return the connected node or nodes."""

        for other in others:
            if other not in self.downstreams:
                self.downstreams.append(other)
            if self not in other.upstreams:
                other.upstreams.append(self)
        if len(others) == 1:
            return others[0]
        return others

    async def _emit(self, item: Result[object]) -> None:
        """Forward one result item to every downstream node."""

        for downstream in self.downstreams:
            await downstream.queue.put(item)

    async def _finish(self) -> None:
        """Notify downstream nodes that this node has completed."""

        for downstream in self.downstreams:
            await downstream.queue.put(END)

    @abc.abstractmethod
    async def run(self) -> None:
        """Execute the node until its input is exhausted or it is cancelled."""

        raise NotImplementedError


class TransformNode[InT, OutT](BaseNode):
    """Node that applies a core ``Result -> list[Result]`` transform function."""

    def __init__(
        self,
        func: NodeFn[InT, OutT],
        *,
        name: str | None = None,
        queue_size: int = 100,
    ) -> None:
        """Store the transform function and invocation options for later execution."""

        super().__init__(
            name=name or getattr(func, "__name__", None), queue_size=queue_size
        )
        self.func = func

    async def run(self) -> None:
        """Consume upstream results, transform them, and forward outputs downstream."""

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

                try:
                    outputs = await self.func(cast(Result[InT], item))
                except Exception as exc:
                    outputs = [Err(exc)]

                for output in outputs:
                    await self._emit(cast(Result[object], output))
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled:
                await self._finish()


class Emitter[T]:
    """Helper passed to source producers so they can emit graph results."""

    def __init__(self, node: SourceNode[T]) -> None:
        """Bind the emitter to the source node that will publish its outputs."""

        self._node = node

    async def emit(self, item: Result[T]) -> None:
        """Emit a result through the source node."""

        await self._node._emit(cast(Result[object], item))

    async def emit_ok(self, value: T) -> None:
        """Emit a successful value through the source node."""

        await self._node._emit(Ok(value))

    async def emit_err(self, error: BaseException) -> None:
        """Emit an error result through the source node."""

        await self._node._emit(Err(ensure_exception(error)))


class SourceNode[OutT](BaseNode):
    """Node that starts a graph by running a producer callback."""

    def __init__(
        self,
        producer: Callable[..., ProducerResult],
        *args: object,
        name: str | None = None,
        queue_size: int = 1,
        **kwargs: object,
    ) -> None:
        """Store the producer callable and arguments used to generate source items."""

        super().__init__(
            name=name or getattr(producer, "__name__", None), queue_size=queue_size
        )
        self.producer = producer
        self.args = args
        self.kwargs = kwargs

    async def run(self) -> None:
        """Run the producer and emit any raised exception as an error result."""

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


def _map_result[InT, OutT, **P](
    func: Callable[Concatenate[InT, P], OutT],
    *args: object,
    **kwargs: object,
) -> NodeFn[InT, OutT]:
    """Adapt a plain value transform into the core result-based node function."""

    async def wrapped(item: Result[InT]) -> list[Result[OutT]]:
        if isinstance(item, Err):
            return [item]
        return [Ok(func(item.value, *args, **kwargs))]

    return wrapped


class NodeSpec[InT, OutT, **P]:
    """Callable factory that creates configured map-style transform nodes."""

    def __init__(
        self,
        func: Callable[Concatenate[InT, P], OutT],
        *,
        queue_size: int = 100,
    ) -> None:
        """Capture a transform function together with default node options."""

        self.func = func
        self.queue_size = queue_size
        self.__name__ = getattr(func, "__name__", self.__class__.__name__)

    def __call__(
        self,
        *args: object,
        name: str | None = None,
        queue_size: int | None = None,
        **kwargs: object,
    ) -> TransformNode[InT, OutT]:
        """Instantiate a transform node using the stored function and defaults."""

        return TransformNode(
            _map_result(self.func, *args, **kwargs),
            name=name or self.__name__,
            queue_size=queue_size if queue_size is not None else self.queue_size,
        )


@overload
def node[InT, OutT, **P](
    func: Callable[Concatenate[InT, P], OutT],
    *,
    queue_size: int = 100,
) -> NodeSpec[InT, OutT, P]: ...


@overload
def node[InT, OutT, **P](
    func: None = None,
    *,
    queue_size: int = 100,
) -> Callable[[Callable[Concatenate[InT, P], OutT]], NodeSpec[InT, OutT, P]]: ...


def node[InT, OutT, **P](
    func: Callable[Concatenate[InT, P], OutT] | None = None,
    *,
    queue_size: int = 100,
) -> (
    NodeSpec[InT, OutT, P]
    | Callable[[Callable[Concatenate[InT, P], OutT]], NodeSpec[InT, OutT, P]]
):
    """Wrap a plain transform function in a reusable map-style ``NodeSpec``."""

    def decorator(
        inner: Callable[Concatenate[InT, P], OutT],
    ) -> NodeSpec[InT, OutT, P]:
        """Create a ``NodeSpec`` for the decorated transform function."""

        return NodeSpec(inner, queue_size=queue_size)

    if func is None:
        return decorator
    return decorator(func)


class Graph:
    """Container that owns nodes and manages their lifecycle as a group."""

    def __init__(self) -> None:
        """Initialize an empty graph."""

        self.nodes: list[BaseNode] = []

    def add(self, *nodes: BaseNode) -> Graph:
        """Add nodes to the graph if they are not already present."""

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
        """Create, register, and return a source node for the given producer."""

        node = SourceNode(producer, *args, name=name, queue_size=queue_size, **kwargs)
        self.add(node)
        return node

    async def start(self) -> None:
        """Start all graph nodes that have not already been started."""

        for node in self.nodes:
            if node._started:
                continue
            node._task = asyncio.create_task(node.run(), name=node.name)
            node._started = True

    async def wait(self) -> None:
        """Wait for all started node tasks to finish."""

        tasks = [node._task for node in self.nodes if node._task is not None]
        if tasks:
            await asyncio.gather(*tasks)

    async def run(self) -> None:
        """Start the graph and wait for all nodes to complete."""

        await self.start()
        await self.wait()

    async def cancel(self) -> None:
        """Cancel all running node tasks and wait for their shutdown."""

        tasks = [node._task for node in self.nodes if node._task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
