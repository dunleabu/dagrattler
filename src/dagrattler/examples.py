from __future__ import annotations

import asyncio
from typing import Any

from .core import Graph, node
from .operators import batch_node, map_node, recover_node, sink_node
from .result import Err


@node
def double(value: int) -> int:
    return value * 2


@node
def only_even(value: int) -> list[int]:
    return [value] if value % 2 == 0 else []


@node(handle_errors=True)
def log_errors(value: Any) -> Any:
    if isinstance(value, Exception):
        print(f"log_errors: {value}")
        return Err(value)
    return value


@node
def explode_on_seven(value: int) -> int:
    if value == 7:
        raise ValueError("boom: seven")
    return value


async def ticker(emitter: Any, *, count: int = 10, delay: float = 0.01) -> None:
    for value in range(1, count + 1):
        await emitter.emit_ok(value)
        await asyncio.sleep(delay)


async def main() -> None:
    graph = Graph()

    source = graph.source(ticker, count=10, delay=0.0, name="ticker")

    doubled = double(name="double")
    evens = only_even(name="only_even")
    batches = batch_node(3, name="batch")
    batch_sums = map_node(sum, name="sum_batches")
    risky = explode_on_seven(name="explode_on_seven")
    logged = log_errors(name="log_errors")
    recovered = recover_node(lambda exc: [-1], name="recover")
    merged_sink = sink_node(lambda value: print(f"merged sink <- {value}"), name="sink")

    graph.add(
        doubled, evens, batches, batch_sums, risky, logged, recovered, merged_sink
    )

    source.connect(doubled, risky)
    doubled.connect(evens)
    evens.connect(batches)
    batches.connect(batch_sums)
    batch_sums.connect(merged_sink)

    risky.connect(logged)
    logged.connect(recovered)
    recovered.connect(merged_sink)

    await graph.run()


if __name__ == "__main__":
    asyncio.run(main())
