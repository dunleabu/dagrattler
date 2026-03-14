import asyncio

import pytest

from dagrattler import Graph
from dagrattler.operators import (
    batch_node,
    filter_node,
    flat_map_node,
    map_node,
    recover_node,
    sink_node,
)


def test_map_node() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2]:
            await emitter.emit_ok(value)

    graph = Graph()
    source = graph.source(producer)
    mapper = map_node(lambda value: value * 3)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(mapper)
    mapper.connect(sink)
    graph.add(mapper, sink)

    asyncio.run(graph.run())

    assert seen == [3, 6]


def test_map_node_accepts_async_callable() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2]:
            await emitter.emit_ok(value)

    async def triple(value: int) -> int:
        await asyncio.sleep(0)
        return value * 3

    graph = Graph()
    source = graph.source(producer)
    mapper = map_node(triple)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(mapper)
    mapper.connect(sink)
    graph.add(mapper, sink)

    asyncio.run(graph.run())

    assert seen == [3, 6]


def test_filter_node() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2, 3, 4]:
            await emitter.emit_ok(value)

    graph = Graph()
    source = graph.source(producer)
    filtered = filter_node(lambda value: value % 2 == 0)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(filtered)
    filtered.connect(sink)
    graph.add(filtered, sink)

    asyncio.run(graph.run())

    assert seen == [2, 4]


def test_filter_node_accepts_async_callable() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        for value in [1, 2, 3, 4]:
            await emitter.emit_ok(value)

    async def is_even(value: int) -> bool:
        await asyncio.sleep(0)
        return value % 2 == 0

    graph = Graph()
    source = graph.source(producer)
    filtered = filter_node(is_even)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(filtered)
    filtered.connect(sink)
    graph.add(filtered, sink)

    asyncio.run(graph.run())

    assert seen == [2, 4]


def test_flat_map_node() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(3)

    graph = Graph()
    source = graph.source(producer)
    flat_mapper = flat_map_node(lambda value: [value, value + 1])
    sink = sink_node(lambda value: seen.append(value))
    source.connect(flat_mapper)
    flat_mapper.connect(sink)
    graph.add(flat_mapper, sink)

    asyncio.run(graph.run())

    assert seen == [3, 4]


def test_flat_map_node_accepts_async_callable() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(3)

    async def expand(value: int) -> list[int]:
        await asyncio.sleep(0)
        return [value, value + 1]

    graph = Graph()
    source = graph.source(producer)
    flat_mapper = flat_map_node(expand)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(flat_mapper)
    flat_mapper.connect(sink)
    graph.add(flat_mapper, sink)

    asyncio.run(graph.run())

    assert seen == [3, 4]


def test_batch_node() -> None:
    seen: list[list[int]] = []

    async def producer(emitter) -> None:
        for value in [1, 2, 3, 4, 5]:
            await emitter.emit_ok(value)

    graph = Graph()
    source = graph.source(producer)
    batches = batch_node(2)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(batches)
    batches.connect(sink)
    graph.add(batches, sink)

    asyncio.run(graph.run())

    assert seen == [[1, 2], [3, 4], [5]]


def test_batch_node_rejects_non_positive_size() -> None:
    with pytest.raises(ValueError):
        batch_node(0)


def test_sink_node_side_effects() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(5)

    graph = Graph()
    source = graph.source(producer)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(sink)
    graph.add(sink)

    asyncio.run(graph.run())

    assert seen == [5]


def test_sink_node_accepts_async_callable() -> None:
    seen: list[int] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(5)

    async def record(value: int) -> None:
        await asyncio.sleep(0)
        seen.append(value)

    graph = Graph()
    source = graph.source(producer)
    sink = sink_node(record)
    source.connect(sink)
    graph.add(sink)

    asyncio.run(graph.run())

    assert seen == [5]


def test_recover_node() -> None:
    seen: list[object] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(1)
        await emitter.emit_err(ValueError("bad"))
        await emitter.emit_ok(2)

    graph = Graph()
    source = graph.source(producer)
    recovered = recover_node(lambda exc: [str(exc).upper()])
    sink = sink_node(lambda value: seen.append(value))
    source.connect(recovered)
    recovered.connect(sink)
    graph.add(recovered, sink)

    asyncio.run(graph.run())

    assert seen == [1, "BAD", 2]


def test_recover_node_accepts_async_callable() -> None:
    seen: list[object] = []

    async def producer(emitter) -> None:
        await emitter.emit_ok(1)
        await emitter.emit_err(ValueError("bad"))
        await emitter.emit_ok(2)

    async def recover(exc: Exception) -> list[str]:
        await asyncio.sleep(0)
        return [str(exc).upper()]

    graph = Graph()
    source = graph.source(producer)
    recovered = recover_node(recover)
    sink = sink_node(lambda value: seen.append(value))
    source.connect(recovered)
    recovered.connect(sink)
    graph.add(recovered, sink)

    asyncio.run(graph.run())

    assert seen == [1, "BAD", 2]
