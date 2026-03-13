# dagrattler

`dagrattler` is a small async streaming DAG framework.

## Install

```bash
pip install "dagrattler @ git+https://github.com/dunleabu/dagrattler.git"
```

## Usage

```python
import asyncio

from dagrattler import Graph, node, sink_node


@node
def double(value: int) -> int:
    return value * 2


async def producer(emitter) -> None:
    for value in [1, 2, 3]:
        await emitter.emit_ok(value)


async def main() -> None:
    seen: list[int] = []

    graph = Graph()
    source = graph.source(producer)
    transform = double()
    sink = sink_node(lambda value: seen.append(value))

    source.connect(transform)
    transform.connect(sink)
    graph.add(transform, sink)

    await graph.run()
    print(seen)


asyncio.run(main())
```
