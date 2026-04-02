"""Runnable demos for workflow-py.

Usage:
    python examples/basic.py              # Run all examples
    python examples/basic.py simple       # Simple pipeline
    python examples/basic.py resources    # Shared resources
    python examples/basic.py collective   # Collective step
    python examples/basic.py progress     # Progress tracking
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from typing import Any

from workflow import Workflow, WorkflowOptions, WorkItem
from workflow.types import StepContext


@dataclass
class Uppercase:
    name: str = "uppercase"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return input.upper()


@dataclass
class AddPrefix:
    name: str = "add-prefix"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return f"PROCESSED: {input}"


@dataclass
class Tag:
    name: str = "tag"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        cfg = ctx.resources["config"]
        return f"{cfg['prefix']}{cfg['separator']}{input}"


@dataclass
class Score:
    name: str = "score"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return len(input)


@dataclass
class Summarize:
    name: str = "summarize"
    mode: str = "collective"

    async def run(self, items: Any, ctx: StepContext) -> Any:
        total = sum(i["data"] for i in items)
        avg = total / len(items)
        return [
            {"id": i["id"], "summary": f"Score: {i['data']}, Average: {avg:.1f}"}
            for i in items
        ]


@dataclass
class SlowProcess:
    name: str = "process"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        await asyncio.sleep(input * 0.1)
        return f"done in {input * 100}ms"


async def demo_simple():
    """Simple two-step pipeline."""
    print("\n=== Simple Pipeline ===\n")

    results = await (
        Workflow(base_dir=".example-pipeline")
        .step(Uppercase())
        .step(AddPrefix())
        .run([
            WorkItem(id="item-1", data="hello world"),
            WorkItem(id="item-2", data="foo bar"),
            WorkItem(id="item-3", data="pipeline test"),
        ])
    )

    for r in results:
        print(f"  {r.id}: {r.outputs['add-prefix']}")
    print()


async def demo_resources():
    """Pipeline with shared resources."""
    print("\n=== Shared Resources ===\n")

    results = await (
        Workflow(base_dir=".example-resources")
        .resource("config", {"prefix": "v2", "separator": "-"})
        .step(Tag())
        .run([
            WorkItem(id="a", data="alpha"),
            WorkItem(id="b", data="beta"),
        ])
    )

    for r in results:
        print(f"  {r.id}: {r.outputs['tag']}")
    print()


async def demo_collective():
    """Collective step for aggregation."""
    print("\n=== Collective Step ===\n")

    results = await (
        Workflow(base_dir=".example-collective")
        .step(Score())
        .step(Summarize())
        .run([
            WorkItem(id="short", data="hi"),
            WorkItem(id="medium", data="hello world"),
            WorkItem(id="long", data="this is a longer string for testing"),
        ])
    )

    for r in results:
        print(f"  {r.id}: {r.outputs['summarize']}")
    print()


async def demo_progress():
    """Progress tracking."""
    print("\n=== Progress Tracking ===\n")

    results = await (
        Workflow(base_dir=".example-progress")
        .step(SlowProcess())
        .run(
            [
                WorkItem(id="fast", data=1),
                WorkItem(id="medium", data=3),
                WorkItem(id="slow", data=5),
            ],
            WorkflowOptions(
                concurrency=2,
                on_progress=lambda p: print(f"  Progress: {p.completed}/{p.total} ({p.current_step})"),
            ),
        )
    )

    print()
    for r in results:
        print(f"  {r.id}: {r.outputs['process']}")
    print()


DEMOS = {
    "simple": demo_simple,
    "resources": demo_resources,
    "collective": demo_collective,
    "progress": demo_progress,
}


async def main():
    args = sys.argv[1:]

    if args:
        for name in args:
            if name in DEMOS:
                await DEMOS[name]()
            else:
                print(f"Unknown demo: {name}")
                print(f"Available: {', '.join(DEMOS.keys())}")
                sys.exit(1)
    else:
        for demo_fn in DEMOS.values():
            await demo_fn()


if __name__ == "__main__":
    asyncio.run(main())
