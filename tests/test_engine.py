"""Tests for the workflow engine."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from typing import Any

import pytest

from workflow import FileStore, Workflow, WorkflowOptions, WorkItem
from workflow.types import StepContext


@dataclass
class UpperStep:
    """Concurrent step that uppercases string data."""

    name: str = "upper"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return str(input).upper()


@dataclass
class SummarizeStep:
    """Collective step that joins all items into a summary."""

    name: str = "summarize"
    mode: str = "collective"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        combined = " | ".join(item["data"] for item in input)
        return [{"id": item["id"], "summary": combined} for item in input]


@dataclass
class DoubleStep:
    """Concurrent step that doubles a number."""

    name: str = "double"
    mode: str = "concurrent"

    async def run(self, input: Any, ctx: StepContext) -> Any:
        return input * 2


@dataclass
class FailOnceStep:
    """Step that fails on the first attempt, succeeds on retry."""

    name: str = "flaky"
    mode: str = "concurrent"
    _attempts: dict[str, int] | None = None

    def __post_init__(self) -> None:
        self._attempts = {}

    async def run(self, input: Any, ctx: StepContext) -> Any:
        assert self._attempts is not None
        count = self._attempts.get(ctx.item_id, 0)
        self._attempts[ctx.item_id] = count + 1
        if count == 0:
            raise ValueError(f"Flaky failure for {ctx.item_id}")
        return f"recovered-{input}"


@pytest.fixture
def tmp_dir() -> Any:
    """Create a temporary directory for test stores."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_two_step_workflow(tmp_dir: str) -> None:
    """Test a simple two-step workflow: concurrent then collective."""
    items = [WorkItem(id="a", data="hello"), WorkItem(id="b", data="world")]
    wf = Workflow(id="test-1", base_dir=tmp_dir)
    wf.step(UpperStep()).step(SummarizeStep())
    results = await wf.run(items)

    assert len(results) == 2
    assert all(r.status == "completed" for r in results)
    for r in results:
        assert "summarize" in r.step_outputs


@pytest.mark.asyncio
async def test_concurrent_step_only(tmp_dir: str) -> None:
    """Test a single concurrent step."""
    items = [WorkItem(id="n1", data=5), WorkItem(id="n2", data=10)]
    wf = Workflow(id="test-2", base_dir=tmp_dir)
    wf.step(DoubleStep())
    results = await wf.run(items)

    assert len(results) == 2
    outputs = {r.id: r.step_outputs["double"] for r in results}
    assert outputs["n1"] == 10
    assert outputs["n2"] == 20


@pytest.mark.asyncio
async def test_resume_skips_completed(tmp_dir: str) -> None:
    """Test that re-running a workflow skips already-completed steps."""
    items = [WorkItem(id="r1", data="test")]
    wf = Workflow(id="resume-1", base_dir=tmp_dir)
    wf.step(UpperStep())

    # First run
    results1 = await wf.run(items)
    assert results1[0].step_outputs["upper"] == "TEST"

    # Verify file exists on disk
    store = FileStore(tmp_dir)
    cached = await store.get_step_output("resume-1", "r1", "upper")
    assert cached == "TEST"

    # Second run with a fresh workflow (same id)
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

    # The step should not have re-run since output is cached
    assert not tracker["ran"]
    assert results2[0].status == "completed"


@pytest.mark.asyncio
async def test_retry_on_failure(tmp_dir: str) -> None:
    """Test that a flaky step succeeds after retry."""
    items = [WorkItem(id="f1", data="payload")]
    wf = Workflow(id="retry-1", base_dir=tmp_dir)
    wf.step(FailOnceStep())
    results = await wf.run(items, WorkflowOptions(max_retries=2))

    assert len(results) == 1
    assert results[0].status == "completed"
    assert results[0].step_outputs["flaky"] == "recovered-payload"


@pytest.mark.asyncio
async def test_permanent_failure(tmp_dir: str) -> None:
    """Test that a step that always fails marks the item as failed."""

    @dataclass
    class AlwaysFailStep:
        name: str = "boom"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            raise RuntimeError("permanent error")

    items = [WorkItem(id="pf1", data="x")]
    wf = Workflow(id="fail-1", base_dir=tmp_dir)
    wf.step(AlwaysFailStep())
    results = await wf.run(items, WorkflowOptions(max_retries=1))

    assert results[0].status == "failed"
    assert "permanent error" in (results[0].error or "")


@pytest.mark.asyncio
async def test_resource_injection(tmp_dir: str) -> None:
    """Test that resources are accessible in step context."""

    @dataclass
    class ResourceStep:
        name: str = "use-resource"
        mode: str = "concurrent"

        async def run(self, input: Any, ctx: StepContext) -> Any:
            multiplier = ctx.resources["multiplier"]
            return input * multiplier

    items = [WorkItem(id="res1", data=7)]
    wf = Workflow(id="resource-1", base_dir=tmp_dir)
    wf.resource("multiplier", 3).step(ResourceStep())
    results = await wf.run(items)

    assert results[0].step_outputs["use-resource"] == 21


@pytest.mark.asyncio
async def test_progress_callback(tmp_dir: str) -> None:
    """Test that the progress callback is invoked."""
    progress_reports: list[Any] = []

    items = [WorkItem(id="p1", data="a"), WorkItem(id="p2", data="b")]
    wf = Workflow(id="progress-1", base_dir=tmp_dir)
    wf.step(UpperStep())
    opts = WorkflowOptions(on_progress=lambda p: progress_reports.append(p))
    await wf.run(items, opts)

    assert len(progress_reports) > 0
    assert progress_reports[0].current_step == "upper"
