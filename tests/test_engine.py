"""Tests for the workflow engine."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from dataclasses import dataclass, field
from typing import Any

import pytest

from workflow import FileStore, Workflow, WorkflowOptions, WorkItem
from workflow.types import ItemState, StepContext


# ---------------------------------------------------------------------------
# Reusable step fixtures
# ---------------------------------------------------------------------------


@dataclass
class UpperStep:
    """Concurrent step that uppercases string data."""

    name: str = "upper"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return str(input).upper()


@dataclass
class DoubleStep:
    """Concurrent step that doubles a number."""

    name: str = "double"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return input * 2


@dataclass
class AddTenStep:
    """Concurrent step that adds ten to a number."""

    name: str = "add_ten"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return input + 10


@dataclass
class SummarizeStep:
    """Collective step that joins all items into a summary."""

    name: str = "summarize"
    mode: str = "collective"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        combined = " | ".join(str(item["data"]) for item in input)
        return [{"id": item["id"], "summary": combined} for item in input]


@dataclass
class FailOnceStep:
    """Step that fails on the first attempt, succeeds on retry."""

    name: str = "flaky"
    mode: str = "concurrent"
    _attempts: dict[str, int] = field(default_factory=dict)

    async def run(self, input: Any, ctx: StepContext) -> Any:
        count = self._attempts.get(ctx.item_id, 0)
        self._attempts[ctx.item_id] = count + 1
        if count == 0:
            raise ValueError(f"Flaky failure for {ctx.item_id}")
        return f"recovered-{input}"


@dataclass
class AlwaysFailStep:
    """Step that always raises an error."""

    name: str = "boom"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        raise RuntimeError("permanent error")


@pytest.fixture
def tmp_dir() -> Any:
    """Create a temporary directory for test stores."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Basic workflow: single concurrent step
# ---------------------------------------------------------------------------


async def test_single_concurrent_step(tmp_dir: str) -> None:
    """Single concurrent step produces correct output."""
    items = [WorkItem(id="a", data="hello")]
    wf = Workflow(id="single-1", base_dir=tmp_dir)
    wf.step(UpperStep())
    results = await wf.run(items)

    assert len(results) == 1
    assert results[0].status == "completed"
    assert results[0].step_outputs["upper"] == "HELLO"


# ---------------------------------------------------------------------------
# Multi-step workflow: 2+ concurrent steps in sequence
# ---------------------------------------------------------------------------


async def test_multi_step_concurrent(tmp_dir: str) -> None:
    """Two concurrent steps execute in sequence."""
    items = [WorkItem(id="n1", data=5), WorkItem(id="n2", data=10)]
    wf = Workflow(id="multi-1", base_dir=tmp_dir)
    wf.step(DoubleStep()).step(AddTenStep())
    results = await wf.run(items)

    outputs = {r.id: r.step_outputs for r in results}
    assert outputs["n1"]["double"] == 10
    assert outputs["n1"]["add_ten"] == 20
    assert outputs["n2"]["double"] == 20
    assert outputs["n2"]["add_ten"] == 30


# ---------------------------------------------------------------------------
# Collective step: receives all items
# ---------------------------------------------------------------------------


async def test_collective_step_receives_all_items(tmp_dir: str) -> None:
    """Collective step receives all items as a list."""
    received: list[Any] = []

    @dataclass
    class SpyCollective:
        name: str = "spy"
        mode: str = "collective"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            received.extend(input)
            return [f"done-{item['id']}" for item in input]

    items = [WorkItem(id="c1", data="x"), WorkItem(id="c2", data="y")]
    wf = Workflow(id="coll-1", base_dir=tmp_dir)
    wf.step(SpyCollective())
    await wf.run(items)

    assert len(received) == 2
    ids = {r["id"] for r in received}
    assert ids == {"c1", "c2"}


# ---------------------------------------------------------------------------
# Mixed concurrent + collective steps
# ---------------------------------------------------------------------------


async def test_mixed_concurrent_and_collective(tmp_dir: str) -> None:
    """Concurrent then collective step works end to end."""
    items = [WorkItem(id="a", data="hello"), WorkItem(id="b", data="world")]
    wf = Workflow(id="mixed-1", base_dir=tmp_dir)
    wf.step(UpperStep()).step(SummarizeStep())
    results = await wf.run(items)

    assert len(results) == 2
    assert all(r.status == "completed" for r in results)
    for r in results:
        assert "summarize" in r.step_outputs


# ---------------------------------------------------------------------------
# Resource injection
# ---------------------------------------------------------------------------


async def test_resource_injection(tmp_dir: str) -> None:
    """Resources are available in step context."""

    @dataclass
    class ResourceStep:
        name: str = "use-resource"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            return input * ctx.resources["multiplier"]

    items = [WorkItem(id="res1", data=7)]
    wf = Workflow(id="resource-1", base_dir=tmp_dir)
    wf.resource("multiplier", 3).step(ResourceStep())
    results = await wf.run(items)

    assert results[0].step_outputs["use-resource"] == 21


async def test_multiple_resources(tmp_dir: str) -> None:
    """Multiple resources can be registered and accessed."""

    @dataclass
    class MultiResourceStep:
        name: str = "multi-res"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            return input + ctx.resources["offset"] + ctx.resources["bonus"]

    items = [WorkItem(id="mr1", data=10)]
    wf = Workflow(id="multi-res-1", base_dir=tmp_dir)
    wf.resource("offset", 5).resource("bonus", 100)
    wf.step(MultiResourceStep())
    results = await wf.run(items)

    assert results[0].step_outputs["multi-res"] == 115


# ---------------------------------------------------------------------------
# Progress callback
# ---------------------------------------------------------------------------


async def test_progress_callback(tmp_dir: str) -> None:
    """Progress callback is called with correct counts."""
    reports: list[Any] = []

    items = [WorkItem(id="p1", data="a"), WorkItem(id="p2", data="b")]
    wf = Workflow(id="progress-1", base_dir=tmp_dir)
    wf.step(UpperStep())
    opts = WorkflowOptions(on_progress=lambda p: reports.append(p))
    await wf.run(items, opts)

    assert len(reports) > 0
    assert all(r.current_step == "upper" for r in reports)
    assert reports[-1].completed + reports[-1].failed == 2


# ---------------------------------------------------------------------------
# Concurrency limit
# ---------------------------------------------------------------------------


async def test_concurrency_limit(tmp_dir: str) -> None:
    """Semaphore enforces max parallel items."""
    peak = {"current": 0, "max": 0}

    @dataclass
    class SlowStep:
        name: str = "slow"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            peak["current"] += 1
            if peak["current"] > peak["max"]:
                peak["max"] = peak["current"]
            await asyncio.sleep(0.05)
            peak["current"] -= 1
            return input

    items = [WorkItem(id=f"i{n}", data=n) for n in range(6)]
    wf = Workflow(id="conc-1", base_dir=tmp_dir)
    wf.step(SlowStep())
    await wf.run(items, WorkflowOptions(concurrency=2))

    assert peak["max"] <= 2


# ---------------------------------------------------------------------------
# Retry on failure
# ---------------------------------------------------------------------------


async def test_retry_on_failure(tmp_dir: str) -> None:
    """Flaky step succeeds after retry."""
    items = [WorkItem(id="f1", data="payload")]
    wf = Workflow(id="retry-1", base_dir=tmp_dir)
    wf.step(FailOnceStep())
    results = await wf.run(items, WorkflowOptions(max_retries=2))

    assert results[0].status == "completed"
    assert results[0].step_outputs["flaky"] == "recovered-payload"


# ---------------------------------------------------------------------------
# Retry exhaustion
# ---------------------------------------------------------------------------


async def test_retry_exhaustion(tmp_dir: str) -> None:
    """Step that always fails marks item as failed with error message."""
    items = [WorkItem(id="pf1", data="x")]
    wf = Workflow(id="fail-1", base_dir=tmp_dir)
    wf.step(AlwaysFailStep())
    results = await wf.run(items, WorkflowOptions(max_retries=1))

    assert results[0].status == "failed"
    assert "permanent error" in (results[0].error or "")


# ---------------------------------------------------------------------------
# Resume after crash
# ---------------------------------------------------------------------------


async def test_resume_skips_completed(tmp_dir: str) -> None:
    """Re-running a workflow skips already-completed steps."""
    items = [WorkItem(id="r1", data="test")]
    wf = Workflow(id="resume-1", base_dir=tmp_dir)
    wf.step(UpperStep())
    results1 = await wf.run(items)
    assert results1[0].step_outputs["upper"] == "TEST"

    tracker = {"ran": False}

    @dataclass
    class TrackingStep:
        name: str = "upper"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            tracker["ran"] = True
            return str(input).upper()

    wf2 = Workflow(id="resume-1", base_dir=tmp_dir)
    wf2.step(TrackingStep())
    results2 = await wf2.run(items)

    assert not tracker["ran"]
    assert results2[0].status == "completed"


# ---------------------------------------------------------------------------
# Empty items list
# ---------------------------------------------------------------------------


async def test_empty_items(tmp_dir: str) -> None:
    """Empty items list produces no errors and no results."""
    wf = Workflow(id="empty-1", base_dir=tmp_dir)
    wf.step(UpperStep())
    results = await wf.run([])

    assert results == []


# ---------------------------------------------------------------------------
# Single item via run_one
# ---------------------------------------------------------------------------


async def test_run_one(tmp_dir: str) -> None:
    """run_one convenience method returns a single ItemState."""
    wf = Workflow(id="one-1", base_dir=tmp_dir)
    wf.step(DoubleStep())
    result = await wf.run_one(WorkItem(id="o1", data=4))

    assert isinstance(result, ItemState)
    assert result.status == "completed"
    assert result.step_outputs["double"] == 8


# ---------------------------------------------------------------------------
# Step ordering
# ---------------------------------------------------------------------------


async def test_step_ordering(tmp_dir: str) -> None:
    """Steps execute in the order they were added."""
    order: list[str] = []

    @dataclass
    class RecorderA:
        name: str = "step_a"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            order.append("a")
            return input

    @dataclass
    class RecorderB:
        name: str = "step_b"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            order.append("b")
            return input

    @dataclass
    class RecorderC:
        name: str = "step_c"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            order.append("c")
            return input

    items = [WorkItem(id="ord1", data=1)]
    wf = Workflow(id="order-1", base_dir=tmp_dir)
    wf.step(RecorderA()).step(RecorderB()).step(RecorderC())
    await wf.run(items)

    assert order == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Item state tracking: status transitions
# ---------------------------------------------------------------------------


async def test_item_state_transitions(tmp_dir: str) -> None:
    """Item transitions from pending through running to completed."""
    observed: list[str] = []

    @dataclass
    class ObserverStep:
        name: str = "observe"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            store = FileStore(tmp_dir)
            state = await store.get_item("trans-1", ctx.item_id)
            if state:
                observed.append(state.status)
            return input

    items = [WorkItem(id="t1", data="x")]
    wf = Workflow(id="trans-1", base_dir=tmp_dir)
    wf.step(ObserverStep())
    results = await wf.run(items)

    assert observed == ["running"]
    assert results[0].status == "completed"


# ---------------------------------------------------------------------------
# Error message captured in ItemState
# ---------------------------------------------------------------------------


async def test_error_message_captured(tmp_dir: str) -> None:
    """Failed item captures the error message string."""

    @dataclass
    class SpecificError:
        name: str = "specific"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            raise ValueError("item-42 had bad data")

    items = [WorkItem(id="e1", data="x")]
    wf = Workflow(id="err-1", base_dir=tmp_dir)
    wf.step(SpecificError())
    results = await wf.run(items, WorkflowOptions(max_retries=0))

    assert results[0].status == "failed"
    assert results[0].error == "item-42 had bad data"


# ---------------------------------------------------------------------------
# Collective step failure marks all items failed
# ---------------------------------------------------------------------------


async def test_collective_failure_marks_all_failed(tmp_dir: str) -> None:
    """If a collective step fails, all items are marked failed."""

    @dataclass
    class FailCollective:
        name: str = "fail-all"
        mode: str = "collective"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            raise RuntimeError("collective boom")

    items = [WorkItem(id="cf1", data="a"), WorkItem(id="cf2", data="b")]
    wf = Workflow(id="coll-fail-1", base_dir=tmp_dir)
    wf.step(FailCollective())
    results = await wf.run(items, WorkflowOptions(max_retries=0))

    assert all(r.status == "failed" for r in results)
    assert all("collective boom" in (r.error or "") for r in results)


# ---------------------------------------------------------------------------
# Method chaining
# ---------------------------------------------------------------------------


async def test_method_chaining(tmp_dir: str) -> None:
    """resource() and step() return the workflow for chaining."""
    wf = Workflow(id="chain-1", base_dir=tmp_dir)
    result = wf.resource("k", "v").step(UpperStep()).step(DoubleStep())
    assert result is wf
