from __future__ import annotations

import asyncio

from .core import Emitter, Graph
from .operators import batch_node, filter_node, map_node, recover_node, sink_node


async def ticker(
    emitter: Emitter[int], *, count: int = 10, delay: float = 0.01
) -> None:
    for value in range(1, count + 1):
        await emitter.emit_ok(value)
        await asyncio.sleep(delay)


async def main() -> None:
    graph = Graph()

    source = graph.source(ticker, count=10, delay=0.0, name="ticker")
    doubled = map_node(lambda value: value * 2, name="double")
    evens = filter_node(lambda value: value % 4 == 0, name="only_even")
    batches = batch_node(3, name="batch")
    batch_sums = map_node(sum, name="sum_batches")
    recovered = recover_node(lambda exc: [-1], name="recover")
    merged_sink = sink_node(lambda value: print(f"merged sink <- {value}"), name="sink")

    graph.add(doubled, evens, batches, batch_sums, recovered, merged_sink)

    source.connect(doubled, recovered)
    doubled.connect(evens)
    evens.connect(batches)
    batches.connect(batch_sums)
    batch_sums.connect(merged_sink)
    recovered.connect(merged_sink)

    await graph.run()


if __name__ == "__main__":
    asyncio.run(main())
