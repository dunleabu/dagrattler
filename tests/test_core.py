import asyncio

from dagrattler import Err, Graph, Ok, TransformNode, node


async def collect_outputs(node: TransformNode) -> list[object]:
    outputs = []
    while True:
        item = await node.queue.get()
        if item is not None and item.__class__.__name__ == "_EndSentinel":
            break
        outputs.append(item)
    return outputs


def test_connect_builds_links_correctly() -> None:
    left = TransformNode(lambda x: x, name="left")
    right = TransformNode(lambda x: x, name="right")

    returned = left.connect(right)

    assert returned is right
    assert right in left.downstreams
    assert left in right.upstreams


def test_simple_linear_pipeline() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2, 3]:
            await emitter.emit_ok(value)

    graph = Graph()
    source = graph.source(producer, name="source")
    inc = TransformNode(lambda value: value + 1, name="inc")
    sink = TransformNode(lambda value: seen.append(value), name="sink")
    source.connect(inc)
    inc.connect(sink)
    graph.add(inc, sink)

    asyncio.run(graph.run())

    assert seen == [2, 3, 4]


def test_fan_out() -> None:
    left_seen: list[int] = []
    right_seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2, 3]:
            await emitter.emit_ok(value)

    graph = Graph()
    source = graph.source(producer)
    left = TransformNode(lambda value: left_seen.append(value), name="left")
    right = TransformNode(lambda value: right_seen.append(value * 10), name="right")
    source.connect(left, right)
    graph.add(left, right)

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

    graph = Graph()
    source_a = graph.source(first, name="a")
    source_b = graph.source(second, name="b")
    merge = TransformNode(lambda value: value, name="merge")
    sink = TransformNode(lambda value: seen.append(value), name="sink")
    source_a.connect(merge)
    source_b.connect(merge)
    merge.connect(sink)
    graph.add(merge, sink)

    asyncio.run(graph.run())

    assert seen == [1, 2, 10, 20]


def test_merge_waits_for_end_from_all_upstreams() -> None:
    seen: list[str] = []

    async def first(emitter) -> None:
        await emitter.emit_ok("a1")

    async def second(emitter) -> None:
        await asyncio.sleep(0.01)
        await emitter.emit_ok("b1")

    graph = Graph()
    source_a = graph.source(first, name="a")
    source_b = graph.source(second, name="b")
    merge = TransformNode(lambda value: value, name="merge")
    sink = TransformNode(lambda value: seen.append(value), name="sink")
    source_a.connect(merge)
    source_b.connect(merge)
    merge.connect(sink)
    graph.add(merge, sink)

    asyncio.run(graph.run())

    assert seen == ["a1", "b1"]


def test_node_decorator_builds_transform_node() -> None:
    @node(handle_errors=True, queue_size=50)
    def sample(value: int, amount: int) -> int:
        return value + amount

    instance = sample(3, name="custom", queue_size=7, handle_errors=False)

    assert isinstance(instance, TransformNode)
    assert instance.name == "custom"
    assert instance.queue.maxsize == 7
    assert instance.handle_errors is False
    assert sample.__name__ == "sample"


def test_explicit_results_pass_through() -> None:
    seen: list[object] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(1)

    graph = Graph()
    source = graph.source(producer)
    transform = TransformNode(
        lambda value: [Ok(value), Err(ValueError("bad"))], name="transform"
    )
    sink = TransformNode(
        lambda value: seen.append(value), name="sink", handle_errors=True
    )
    source.connect(transform)
    transform.connect(sink)
    graph.add(transform, sink)

    asyncio.run(graph.run())

    assert seen[0] == 1
    assert isinstance(seen[1], ValueError)


def test_source_exception_becomes_err_data() -> None:
    seen: list[object] = []

    async def producer(_emitter) -> None:
        raise RuntimeError("source failed")

    graph = Graph()
    source = graph.source(producer)
    sink = TransformNode(
        lambda value: seen.append(value), name="sink", handle_errors=True
    )
    source.connect(sink)
    graph.add(sink)

    asyncio.run(graph.run())

    assert len(seen) == 1
    assert isinstance(seen[0], RuntimeError)
