"""Filesystem-backed workflow store with immutable step outputs."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .types import ItemState


class FileStore:
    """Filesystem-based workflow store.

    Structure:
        {base_dir}/{workflow_id}/_state/{item_id}.json
        {base_dir}/{workflow_id}/{step_name}/{item_id}.json

    Step outputs are immutable. Once written, never overwritten.
    """

    def __init__(self, base_dir: str = ".workflow") -> None:
        self._base_dir = Path(base_dir)

    def _state_dir(self, workflow_id: str) -> Path:
        return self._base_dir / workflow_id / "_state"

    def _step_dir(self, workflow_id: str, step_name: str) -> Path:
        return self._base_dir / workflow_id / step_name

    def _state_path(self, workflow_id: str, item_id: str) -> Path:
        return self._state_dir(workflow_id) / f"{item_id}.json"

    def _step_path(self, workflow_id: str, step_name: str, item_id: str) -> Path:
        return self._step_dir(workflow_id, step_name) / f"{item_id}.json"

    async def save_item(self, workflow_id: str, item: ItemState) -> None:
        """Save item state to disk."""
        path = self._state_path(workflow_id, item.id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(item), indent=2))

    async def get_item(self, workflow_id: str, item_id: str) -> ItemState | None:
        """Load item state from disk."""
        path = self._state_path(workflow_id, item_id)
        return self._read_item_state(path)

    async def list_items(self, workflow_id: str) -> list[ItemState]:
        """List all items in a workflow."""
        state_dir = self._state_dir(workflow_id)
        if not state_dir.exists():
            return []
        return self._collect_items(state_dir)

    async def save_step_output(self, workflow_id: str, item_id: str, step_name: str, output: Any) -> None:
        """Save a step's output. Immutable -- written once, never overwritten."""
        path = self._step_path(workflow_id, step_name, item_id)
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(output, indent=2))

    async def get_step_output(self, workflow_id: str, item_id: str, step_name: str) -> Any | None:
        """Load a step's output from disk."""
        path = self._step_path(workflow_id, step_name, item_id)
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def _read_item_state(self, path: Path) -> ItemState | None:
        """Read and parse an ItemState from a JSON file."""
        if not path.exists():
            return None
        data = json.loads(path.read_text())
        return ItemState(**data)

    def _collect_items(self, state_dir: Path) -> list[ItemState]:
        """Collect all ItemState objects from a state directory."""
        items: list[ItemState] = []
        for entry in state_dir.iterdir():
            if not entry.is_file() or not entry.name.endswith(".json"):
                continue
            state = self._read_item_state(entry)
            if state:
                items.append(state)
        return items
