"""Types for the workflow pipeline engine."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable


class Logger(Protocol):
    """Logging interface for step execution."""

    def info(self, msg: str, data: dict[str, Any] | None = None) -> None: ...
    def warn(self, msg: str, data: dict[str, Any] | None = None) -> None: ...
    def error(self, msg: str, data: dict[str, Any] | None = None) -> None: ...


@dataclass
class StepContext:
    """Context passed to each step during execution."""

    item_id: str
    resources: dict[str, Any]
    log: Logger
    get_cache: Any  # Callable[[str], Awaitable[Any | None]]
    signal: asyncio.Event


@runtime_checkable
class Step(Protocol):
    """A single step in a workflow pipeline."""

    name: str
    mode: str  # "concurrent" or "collective"

    async def run(self, input: Any, ctx: StepContext) -> Any: ...


@dataclass
class WorkItem:
    """A work item flowing through the pipeline."""

    id: str
    data: Any


@dataclass
class ItemState:
    """Tracks the state of a single item in the workflow."""

    id: str
    current_step: str = ""
    status: str = "pending"  # pending | running | completed | failed
    step_outputs: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    attempts: int = 0
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class WorkflowStore(Protocol):
    """Storage interface for persisting workflow state."""

    async def save_item(self, workflow_id: str, item: ItemState) -> None: ...
    async def get_item(self, workflow_id: str, item_id: str) -> ItemState | None: ...
    async def list_items(self, workflow_id: str) -> list[ItemState]: ...
    async def save_step_output(self, workflow_id: str, item_id: str, step_name: str, output: Any) -> None: ...
    async def get_step_output(self, workflow_id: str, item_id: str, step_name: str) -> Any | None: ...


@dataclass
class WorkflowProgress:
    """Progress report emitted during workflow execution."""

    total: int
    completed: int
    failed: int
    running: int
    current_step: str


@dataclass
class WorkflowOptions:
    """Options for running a workflow."""

    concurrency: int = 5
    max_retries: int = 2
    on_progress: Any | None = None  # Callable[[WorkflowProgress], None] | None
    signal: asyncio.Event | None = None
