"""Workflow engine. Chains steps together and runs work items through them."""

from __future__ import annotations

import asyncio
import logging
import random
import string
import time
from datetime import datetime, timezone
from typing import Any

from .store import FileStore
from .types import ItemState, Step, StepContext, WorkflowOptions, WorkflowProgress, WorkflowStore

_log = logging.getLogger("workflow")


def _generate_id(prefix: str = "wf") -> str:
    """Generate a unique workflow ID."""
    ts = hex(int(time.time() * 1000))[2:]
    rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{prefix}-{ts}-{rand}"


def _now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


class _StepLogger:
    """Logger scoped to an item + step."""

    def __init__(self, item_id: str, step_name: str) -> None:
        self._prefix = f"[{item_id}:{step_name}]"

    def info(self, msg: str, data: dict[str, Any] | None = None) -> None:
        _log.info("%s %s %s", self._prefix, msg, data or "")

    def warn(self, msg: str, data: dict[str, Any] | None = None) -> None:
        _log.warning("%s %s %s", self._prefix, msg, data or "")

    def error(self, msg: str, data: dict[str, Any] | None = None) -> None:
        _log.error("%s %s %s", self._prefix, msg, data or "")


class Workflow:
    """Stage-based pipeline engine.

    Chains steps together and runs work items through them with
    per-item concurrency, retry, and filesystem persistence.
    """

    def __init__(
        self,
        *,
        id: str | None = None,
        store: WorkflowStore | None = None,
        base_dir: str = ".workflow",
    ) -> None:
        self._id = id or _generate_id()
        self._store: WorkflowStore = store or FileStore(base_dir)
        self._steps: list[Step] = []
        self._resources: dict[str, Any] = {}

    def resource(self, name: str, value: Any) -> Workflow:
        """Register a shared resource available to all steps via ctx.resources."""
        self._resources[name] = value
        return self

    def step(self, step: Step) -> Workflow:
        """Add a step to the pipeline. Steps run in the order added."""
        self._steps.append(step)
        return self

    async def run_one(self, item: Any, options: WorkflowOptions | None = None) -> ItemState:
        """Run a single item through the full pipeline."""
        opts = options or WorkflowOptions()
        opts.concurrency = 1
        results = await self.run([item], opts)
        return results[0]

    async def run(self, items: list[Any], options: WorkflowOptions | None = None) -> list[ItemState]:
        """Run all items through the pipeline. Returns a list of ItemState objects."""
        opts = options or WorkflowOptions()
        signal = opts.signal or asyncio.Event()
        states = await self._init_states(items)
        await self._process_all_steps(states, opts, signal)
        await self._finalize_states(states)
        return list(states.values())

    async def _init_states(self, items: list[Any]) -> dict[str, ItemState]:
        """Initialize or restore item states from the store."""
        states: dict[str, ItemState] = {}
        for item in items:
            state = await self._init_single_item(item)
            states[item.id] = state
        return states

    async def _init_single_item(self, item: Any) -> ItemState:
        """Initialize or restore a single item's state."""
        existing = await self._store.get_item(self._id, item.id)
        if existing and existing.status == "completed":
            return existing
        state = existing or self._new_item_state(item.id)
        await self._store.save_step_output(self._id, item.id, "_input", item.data)
        return state

    def _new_item_state(self, item_id: str) -> ItemState:
        """Create a fresh ItemState for a new item."""
        first_step = self._steps[0].name if self._steps else ""
        return ItemState(
            id=item_id,
            current_step=first_step,
            status="pending",
            created_at=_now_iso(),
            updated_at=_now_iso(),
        )

    async def _process_all_steps(
        self, states: dict[str, ItemState], opts: WorkflowOptions, signal: asyncio.Event
    ) -> None:
        """Process each step in order."""
        for step in self._steps:
            if signal.is_set():
                break
            needs_processing = await self._filter_pending(step, states)
            if not needs_processing:
                continue
            await self._dispatch_step(step, needs_processing, states, opts, signal)

    async def _filter_pending(self, step: Step, states: dict[str, ItemState]) -> list[ItemState]:
        """Filter items that still need processing for a given step."""
        pending = [s for s in states.values() if self._needs_step(s, step)]
        needs_processing: list[ItemState] = []
        for item in pending:
            cached = await self._store.get_step_output(self._id, item.id, step.name)
            if cached is not None:
                self._apply_cached(item, step, cached)
                await self._store.save_item(self._id, item)
            else:
                needs_processing.append(item)
        return needs_processing

    def _needs_step(self, state: ItemState, step: Step) -> bool:
        """Check if an item needs processing for a step."""
        if state.status == "failed":
            return False
        if state.status == "completed":
            return self._step_index(state.current_step) <= self._step_index(step.name)
        return True

    def _apply_cached(self, item: ItemState, step: Step, cached: Any) -> None:
        """Apply a cached step output to an item."""
        item.step_outputs[step.name] = cached
        item.current_step = self._next_step_name(step.name)
        item.updated_at = _now_iso()

    async def _dispatch_step(
        self,
        step: Step,
        items: list[ItemState],
        states: dict[str, ItemState],
        opts: WorkflowOptions,
        signal: asyncio.Event,
    ) -> None:
        """Dispatch a step based on its mode."""
        if step.mode == "collective":
            await self._run_collective_step(step, items, opts.max_retries, signal)
        else:
            await self._run_concurrent_step(step, items, opts, signal)

    async def _run_concurrent_step(
        self, step: Step, items: list[ItemState], opts: WorkflowOptions, signal: asyncio.Event
    ) -> None:
        """Run a concurrent step across all items with a semaphore for parallelism."""
        sem = asyncio.Semaphore(opts.concurrency)
        progress = {"completed": 0, "failed": 0}

        async def bounded(item: ItemState) -> None:
            async with sem:
                if signal.is_set():
                    return
                await self._process_concurrent_item(step, item, opts.max_retries, signal, progress)
                self._emit_progress(opts, items, step, progress)

        await asyncio.gather(*(bounded(item) for item in items))

    async def _process_concurrent_item(
        self, step: Step, item: ItemState, max_retries: int, signal: asyncio.Event, progress: dict[str, int]
    ) -> None:
        """Process a single item through a concurrent step with retry."""
        input_data = await self._get_step_input(step, item)
        ctx = self._build_context(step, item.id, signal)
        await self._mark_running(item, step)
        result = await self._execute_with_retry(step, input_data, ctx, max_retries)
        await self._apply_concurrent_result(step, item, result, progress)

    async def _get_step_input(self, step: Step, item: ItemState) -> Any:
        """Retrieve the input for a step from the previous step's output."""
        prev = self._prev_step_name(step.name)
        source = prev if prev else "_input"
        return await self._store.get_step_output(self._id, item.id, source)

    def _build_context(self, step: Step, item_id: str, signal: asyncio.Event) -> StepContext:
        """Build a StepContext for step execution."""
        return StepContext(
            item_id=item_id,
            resources=self._resources,
            log=_StepLogger(item_id, step.name),
            get_cache=lambda name: self._store.get_step_output(self._id, item_id, name),
            signal=signal,
        )

    async def _mark_running(self, item: ItemState, step: Step) -> None:
        """Mark an item as running for a step."""
        item.current_step = step.name
        item.status = "running"
        item.updated_at = _now_iso()
        await self._store.save_item(self._id, item)

    async def _execute_with_retry(self, step: Step, input_data: Any, ctx: StepContext, max_retries: int) -> Any:
        """Execute a step with exponential backoff retry. Returns output or an error."""
        last_error: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                return await step.run(input_data, ctx)
            except Exception as err:
                last_error = err
                if attempt < max_retries:
                    ctx.log.warn(f"Attempt {attempt + 1} failed, retrying...", {"error": str(err)})
                    await asyncio.sleep(min(1.0 * (2**attempt), 10.0))
        return last_error

    async def _apply_concurrent_result(
        self, step: Step, item: ItemState, result: Any, progress: dict[str, int]
    ) -> None:
        """Apply the result of a concurrent step execution to the item."""
        if isinstance(result, Exception):
            await self._mark_failed(item, result)
            progress["failed"] += 1
        else:
            await self._mark_step_complete(step, item, result)
            progress["completed"] += 1

    async def _mark_step_complete(self, step: Step, item: ItemState, output: Any) -> None:
        """Mark a step as complete and save its output."""
        await self._store.save_step_output(self._id, item.id, step.name, output)
        item.step_outputs[step.name] = output
        item.current_step = self._next_step_name(step.name)
        item.status = "pending"
        item.updated_at = _now_iso()
        await self._store.save_item(self._id, item)

    async def _mark_failed(self, item: ItemState, error: Exception) -> None:
        """Mark an item as failed."""
        item.status = "failed"
        item.error = str(error)
        item.updated_at = _now_iso()
        await self._store.save_item(self._id, item)

    def _emit_progress(
        self, opts: WorkflowOptions, items: list[ItemState], step: Step, progress: dict[str, int]
    ) -> None:
        """Emit a progress callback if one is configured."""
        if not opts.on_progress:
            return
        opts.on_progress(WorkflowProgress(
            total=len(items),
            completed=progress["completed"],
            failed=progress["failed"],
            running=sum(1 for i in items if i.status == "running"),
            current_step=step.name,
        ))

    async def _run_collective_step(
        self, step: Step, items: list[ItemState], max_retries: int, signal: asyncio.Event
    ) -> None:
        """Run a collective step that receives all items at once."""
        inputs = await self._gather_collective_inputs(step, items)
        ctx = self._build_context(step, "_collective", signal)
        result = await self._execute_with_retry(step, inputs, ctx, max_retries)
        await self._apply_collective_result(step, items, result)

    async def _gather_collective_inputs(self, step: Step, items: list[ItemState]) -> list[dict[str, Any]]:
        """Gather inputs for a collective step from all items."""
        inputs: list[dict[str, Any]] = []
        for item in items:
            data = await self._get_step_input(step, item)
            inputs.append({"id": item.id, "data": data})
        return inputs

    async def _apply_collective_result(self, step: Step, items: list[ItemState], result: Any) -> None:
        """Apply collective step results to items."""
        if isinstance(result, Exception):
            for item in items:
                await self._mark_failed(item, result)
            return
        if isinstance(result, list) and len(result) == len(items):
            await self._apply_per_item_collective(step, items, result)
        else:
            await self._store.save_step_output(self._id, "_collective", step.name, result)

    async def _apply_per_item_collective(self, step: Step, items: list[ItemState], outputs: list[Any]) -> None:
        """Apply per-item outputs from a collective step."""
        for i, item in enumerate(items):
            await self._mark_step_complete(step, item, outputs[i])

    async def _finalize_states(self, states: dict[str, ItemState]) -> None:
        """Mark non-failed items as completed."""
        for state in states.values():
            if state.status != "failed":
                state.status = "completed"
                state.updated_at = _now_iso()
                await self._store.save_item(self._id, state)

    def _step_index(self, name: str) -> int:
        """Get the index of a step by name."""
        for i, s in enumerate(self._steps):
            if s.name == name:
                return i
        return -1

    def _next_step_name(self, name: str) -> str:
        """Get the next step's name, or '_done' if this is the last step."""
        idx = self._step_index(name)
        if idx < len(self._steps) - 1:
            return self._steps[idx + 1].name
        return "_done"

    def _prev_step_name(self, name: str) -> str | None:
        """Get the previous step's name, or None if this is the first step."""
        idx = self._step_index(name)
        return self._steps[idx - 1].name if idx > 0 else None
