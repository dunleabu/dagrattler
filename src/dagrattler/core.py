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
    """Abstract base class for nodes connected together inside a graph.

    Args:
        name: Optional node name used for task naming and debugging output.
        queue_size: Maximum number of incoming items buffered for the node.

    Attributes:
        queue: Async queue containing upstream ``Result`` values or ``END``.
        upstreams: Nodes that feed this node.
        downstreams: Nodes that receive this node's outputs.
    """

    def __init__(self, *, name: str | None = None, queue_size: int = 100) -> None:
        """Initialize a node with queue storage and graph linkage state.

        Args:
            name: Optional node name used for task naming and debugging output.
            queue_size: Maximum number of incoming items buffered for the node.
        """

        self.name = name or self.__class__.__name__
        self.queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_size)
        self.upstreams: list[BaseNode] = []
        self.downstreams: list[BaseNode] = []
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._closed_upstreams = 0

    def connect(self, *others: BaseNode) -> BaseNode | tuple[BaseNode, ...]:
        """Connect this node to downstream nodes.

        Args:
            others: One or more downstream nodes to receive this node's outputs.

        Returns:
            The single downstream node when one node is passed, otherwise a tuple of
            all connected nodes.
        """

        for other in others:
            if other not in self.downstreams:
                self.downstreams.append(other)
            if self not in other.upstreams:
                other.upstreams.append(self)
        if len(others) == 1:
            return others[0]
        return others

    async def _emit(self, item: Result[object]) -> None:
        """Forward one result item to every downstream node.

        Args:
            item: The ``Ok`` or ``Err`` value to place on each downstream queue.
        """

        for downstream in self.downstreams:
            await downstream.queue.put(item)

    async def _finish(self) -> None:
        """Notify downstream nodes that this node has completed.

        Outputs:
            Places the ``END`` sentinel on each downstream queue.
        """

        for downstream in self.downstreams:
            await downstream.queue.put(END)

    @abc.abstractmethod
    async def run(self) -> None:
        """Execute the node until its input is exhausted or it is cancelled.

        Outputs:
            Implementation-defined ``Result`` values written to downstream nodes.
        """

        raise NotImplementedError


class TransformNode[InT, OutT](BaseNode):
    """Node that applies the core async ``Result -> list[Result]`` transform.

    Args:
        func: Async callable receiving one input ``Result`` and returning zero or more
            output ``Result`` values.
        name: Optional node name used for task naming and debugging output.
        queue_size: Maximum number of incoming items buffered for the node.
    """

    def __init__(
        self,
        func: NodeFn[InT, OutT],
        *,
        name: str | None = None,
        queue_size: int = 100,
    ) -> None:
        """Store the async transform used to process queued input results.

        Args:
            func: Async callable receiving one input ``Result`` and returning zero or
                more output ``Result`` values.
            name: Optional node name used for task naming and debugging output.
            queue_size: Maximum number of incoming items buffered for the node.
        """

        super().__init__(
            name=name or getattr(func, "__name__", None), queue_size=queue_size
        )
        self.func = func

    async def run(self) -> None:
        """Consume upstream results, transform them, and forward outputs downstream.

        Outputs:
            Emits each ``Result`` returned by ``func`` and converts raised exceptions
            into a single ``Err`` output.
        """

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
    """Helper passed to source producers so they can emit graph results.

    Args:
        node: The source node that will publish emitted results.
    """

    def __init__(self, node: SourceNode[T]) -> None:
        """Bind the emitter to the source node that will publish its outputs.

        Args:
            node: The source node that will publish emitted results.
        """

        self._node = node

    async def emit(self, item: Result[T]) -> None:
        """Emit a result through the source node.

        Args:
            item: The ``Ok`` or ``Err`` value to place on downstream queues.
        """

        await self._node._emit(cast(Result[object], item))

    async def emit_ok(self, value: T) -> None:
        """Emit a successful value through the source node.

        Args:
            value: The successful payload to wrap in ``Ok``.
        """

        await self._node._emit(Ok(value))

    async def emit_err(self, error: BaseException) -> None:
        """Emit an error result through the source node.

        Args:
            error: The failure to normalize into ``Err`` and emit.
        """

        await self._node._emit(Err(ensure_exception(error)))


class SourceNode[OutT](BaseNode):
    """Node that starts a graph by running a producer callback.

    Args:
        producer: Callable invoked with an ``Emitter`` plus any extra arguments. It
            may be sync or async and emits source results via the emitter.
        name: Optional node name used for task naming and debugging output.
        queue_size: Maximum number of incoming items buffered for the node.
    """

    def __init__(
        self,
        producer: Callable[..., ProducerResult],
        *args: object,
        name: str | None = None,
        queue_size: int = 1,
        **kwargs: object,
    ) -> None:
        """Store the producer callable and arguments used to generate source items.

        Args:
            producer: Callable invoked with an ``Emitter`` plus any extra arguments.
            args: Positional arguments forwarded to ``producer`` after the emitter.
            name: Optional node name used for task naming and debugging output.
            queue_size: Maximum number of incoming items buffered for the node.
            kwargs: Keyword arguments forwarded to ``producer`` after the emitter.
        """

        super().__init__(
            name=name or getattr(producer, "__name__", None), queue_size=queue_size
        )
        self.producer = producer
        self.args = args
        self.kwargs = kwargs

    async def run(self) -> None:
        """Run the producer and emit any raised exception as an error result.

        Outputs:
            Emits results pushed through the producer's ``Emitter`` and forwards one
            ``Err`` result if the producer raises.
        """

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
    """Callable factory that creates configured map-style transform nodes.

    Args:
        func: Plain value transform called for each successful input.
        queue_size: Default queue size applied to created nodes.
    """

    def __init__(
        self,
        func: Callable[Concatenate[InT, P], OutT],
        *,
        queue_size: int = 100,
    ) -> None:
        """Capture a transform function together with default node options.

        Args:
            func: Plain value transform called for each successful input.
            queue_size: Default queue size applied to created nodes.
        """

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
        """Instantiate a transform node using the stored function and defaults.

        Args:
            args: Positional arguments bound after each incoming value.
            name: Optional node name used for task naming and debugging output.
            queue_size: Optional queue size overriding the stored default.
            kwargs: Keyword arguments bound after each incoming value.

        Returns:
            A ``TransformNode`` that applies the stored function to successful inputs
            and forwards ``Err`` values unchanged.
        """

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
    """Wrap a plain transform function in a reusable map-style ``NodeSpec``.

    Args:
        func: Plain value transform to expose as a node factory. When omitted, the
            function returns a decorator.
        queue_size: Default queue size applied to created nodes.

    Returns:
        Either a ``NodeSpec`` for ``func`` or a decorator that builds one later.
    """

    def decorator(
        inner: Callable[Concatenate[InT, P], OutT],
    ) -> NodeSpec[InT, OutT, P]:
        """Create a ``NodeSpec`` for the decorated transform function."""

        return NodeSpec(inner, queue_size=queue_size)

    if func is None:
        return decorator
    return decorator(func)


class Graph:
    """Container that owns nodes and manages their lifecycle as a group.

    Attributes:
        nodes: The nodes registered with the graph.
    """

    def __init__(self) -> None:
        """Initialize an empty graph.

        Outputs:
            Creates a graph with no registered nodes.
        """

        self.nodes: list[BaseNode] = []

    def add(self, *nodes: BaseNode) -> Graph:
        """Add nodes to the graph if they are not already present.

        Args:
            nodes: Nodes to register with the graph.

        Returns:
            The graph instance so calls can be chained.
        """

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
        """Create, register, and return a source node for the given producer.

        Args:
            producer: Callable invoked with an ``Emitter`` plus any extra arguments.
            args: Positional arguments forwarded to ``producer`` after the emitter.
            name: Optional node name used for task naming and debugging output.
            queue_size: Maximum number of incoming items buffered for the node.
            kwargs: Keyword arguments forwarded to ``producer`` after the emitter.

        Returns:
            The newly created ``SourceNode``.
        """

        node = SourceNode(producer, *args, name=name, queue_size=queue_size, **kwargs)
        self.add(node)
        return node

    async def start(self) -> None:
        """Start all graph nodes that have not already been started.

        Outputs:
            Creates and stores an ``asyncio`` task for each unstarted node.
        """

        for node in self.nodes:
            if node._started:
                continue
            node._task = asyncio.create_task(node.run(), name=node.name)
            node._started = True

    async def wait(self) -> None:
        """Wait for all started node tasks to finish.

        Outputs:
            Returns when every started node task has completed.
        """

        tasks = [node._task for node in self.nodes if node._task is not None]
        if tasks:
            await asyncio.gather(*tasks)

    async def run(self) -> None:
        """Start the graph and wait for all nodes to complete.

        Outputs:
            Returns after all registered node tasks finish.
        """

        await self.start()
        await self.wait()

    async def cancel(self) -> None:
        """Cancel all running node tasks and wait for their shutdown.

        Outputs:
            Requests cancellation for each running node task and waits for cleanup.
        """

        tasks = [node._task for node in self.nodes if node._task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
