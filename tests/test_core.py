import asyncio

from dagrattler import Err, Graph, Ok, Result, TransformNode, node


async def collect_outputs(node: TransformNode) -> list[object]:
    outputs = []
    while True:
        item = await node.queue.get()
        if item is not None and item.__class__.__name__ == "_EndSentinel":
            break
        outputs.append(item)
    return outputs


def test_connect_builds_links_correctly() -> None:
    async def identity(item):
        return [item]

    left = TransformNode(identity, name="left")
    right = TransformNode(identity, name="right")

    returned = left.connect(right)

    assert returned is right
    assert right in left.downstreams
    assert left in right.upstreams


def test_simple_linear_pipeline() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2, 3]:
            await emitter.emit_ok(value)

    async def inc(item: Result[int]) -> list[Result[int]]:
        if isinstance(item, Err):
            return [item]
        return [Ok(item.value + 1)]

    async def sink(item: Result[int]) -> list[Result[object]]:
        if isinstance(item, Ok):
            seen.append(item.value)
        return []

    graph = Graph()
    source = graph.source(producer, name="source")
    inc_node = TransformNode(inc, name="inc")
    sink_node = TransformNode(sink, name="sink")
    source.connect(inc_node)
    inc_node.connect(sink_node)
    graph.add(inc_node, sink_node)

    asyncio.run(graph.run())

    assert seen == [2, 3, 4]


def test_fan_out() -> None:
    left_seen: list[int] = []
    right_seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2, 3]:
            await emitter.emit_ok(value)

    async def left(item: Result[int]) -> list[Result[object]]:
        if isinstance(item, Ok):
            left_seen.append(item.value)
        return []

    async def right(item: Result[int]) -> list[Result[object]]:
        if isinstance(item, Ok):
            right_seen.append(item.value * 10)
        return []

    graph = Graph()
    source = graph.source(producer)
    left_node = TransformNode(left, name="left")
    right_node = TransformNode(right, name="right")
    source.connect(left_node, right_node)
    graph.add(left_node, right_node)

    asyncio.run(graph.run())

    assert left_seen == [1, 2, 3]
    assert right_seen == [10, 20, 30]


def test_merge_from_multiple_upstreams() -> None:
    seen: list[int] = []

    async def first(emitter) -> None:
        await emitter.emit_ok(1)
        await emitter.emit_ok(2)

    async def second(emitter) -> None:
        await emitter.emit_ok(10)
        await emitter.emit_ok(20)

    async def merge(item: Result[int]) -> list[Result[int]]:
        return [item]

    async def sink(item: Result[int]) -> list[Result[object]]:
        if isinstance(item, Ok):
            seen.append(item.value)
        return []

    graph = Graph()
    source_a = graph.source(first, name="a")
    source_b = graph.source(second, name="b")
    merge_node = TransformNode(merge, name="merge")
    sink_node = TransformNode(sink, name="sink")
    source_a.connect(merge_node)
    source_b.connect(merge_node)
    merge_node.connect(sink_node)
    graph.add(merge_node, sink_node)

    asyncio.run(graph.run())

    assert seen == [1, 2, 10, 20]


def test_merge_waits_for_end_from_all_upstreams() -> None:
    seen: list[str] = []

    async def first(emitter) -> None:
        await emitter.emit_ok("a1")

    async def second(emitter) -> None:
        await asyncio.sleep(0.01)
        await emitter.emit_ok("b1")

    async def merge(item: Result[str]) -> list[Result[str]]:
        return [item]

    async def sink(item: Result[str]) -> list[Result[object]]:
        if isinstance(item, Ok):
            seen.append(item.value)
        return []

    graph = Graph()
    source_a = graph.source(first, name="a")
    source_b = graph.source(second, name="b")
    merge_node = TransformNode(merge, name="merge")
    sink_node = TransformNode(sink, name="sink")
    source_a.connect(merge_node)
    source_b.connect(merge_node)
    merge_node.connect(sink_node)
    graph.add(merge_node, sink_node)

    asyncio.run(graph.run())

    assert seen == ["a1", "b1"]


def test_node_decorator_builds_transform_node() -> None:
    @node(queue_size=50)
    def sample(value: int, amount: int) -> int:
        return value + amount

    instance = sample(3, name="custom", queue_size=7)

    assert isinstance(instance, TransformNode)
    assert instance.name == "custom"
    assert instance.queue.maxsize == 7
    assert sample.__name__ == "sample"


def test_transform_node_can_emit_multiple_results() -> None:
    seen: list[object] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(1)

    async def transform(item: Result[int]) -> list[Result[int]]:
        if isinstance(item, Err):
            return [item]
        return [item, Err(ValueError("bad"))]

    async def sink(item: Result[int]) -> list[Result[object]]:
        if isinstance(item, Ok):
            seen.append(item.value)
        else:
            seen.append(item.error)
        return []

    graph = Graph()
    source = graph.source(producer)
    transform_node = TransformNode(transform, name="transform")
    sink_node = TransformNode(sink, name="sink")
    source.connect(transform_node)
    transform_node.connect(sink_node)
    graph.add(transform_node, sink_node)

    asyncio.run(graph.run())

    assert seen[0] == 1
    assert isinstance(seen[1], ValueError)


def test_source_exception_becomes_err_result() -> None:
    seen: list[object] = []

    async def producer(_emitter) -> None:
        raise RuntimeError("source failed")

    async def sink(item: Result[object]) -> list[Result[object]]:
        if isinstance(item, Err):
            seen.append(item.error)
        return []

    graph = Graph()
    source = graph.source(producer)
    sink_node = TransformNode(sink, name="sink")
    source.connect(sink_node)
    graph.add(sink_node)

    asyncio.run(graph.run())

    assert len(seen) == 1
    assert isinstance(seen[0], RuntimeError)
