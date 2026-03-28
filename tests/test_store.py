"""Tests for the FileStore."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from workflow import FileStore
from workflow.types import ItemState


@pytest.fixture
def store(tmp_path: Path) -> FileStore:
    """Create a FileStore backed by a tmp directory."""
    return FileStore(str(tmp_path))


# ---------------------------------------------------------------------------
# Save and load item state
# ---------------------------------------------------------------------------


async def test_save_and_load_item(store: FileStore) -> None:
    """Round-trip an ItemState through save and get."""
    item = ItemState(id="item-1", current_step="step-a", status="running")
    await store.save_item("wf-1", item)
    loaded = await store.get_item("wf-1", "item-1")

    assert loaded is not None
    assert loaded.id == "item-1"
    assert loaded.current_step == "step-a"
    assert loaded.status == "running"


# ---------------------------------------------------------------------------
# Save and load step output
# ---------------------------------------------------------------------------


async def test_save_and_load_step_output(store: FileStore) -> None:
    """Round-trip a step output through save and get."""
    await store.save_step_output("wf-1", "item-1", "upper", "HELLO")
    result = await store.get_step_output("wf-1", "item-1", "upper")
    assert result == "HELLO"


async def test_step_output_complex_data(store: FileStore) -> None:
    """Step output can be a dict or list."""
    data = {"urls": ["https://a.com", "https://b.com"], "count": 2}
    await store.save_step_output("wf-1", "item-1", "crawl", data)
    result = await store.get_step_output("wf-1", "item-1", "crawl")
    assert result == data


# ---------------------------------------------------------------------------
# Immutability: saving same step output twice doesn't overwrite
# ---------------------------------------------------------------------------


async def test_step_output_immutable(store: FileStore) -> None:
    """Second save_step_output is a no-op; original value preserved."""
    await store.save_step_output("wf-1", "item-1", "step-a", "first")
    await store.save_step_output("wf-1", "item-1", "step-a", "second")
    result = await store.get_step_output("wf-1", "item-1", "step-a")
    assert result == "first"


# ---------------------------------------------------------------------------
# List items in a workflow
# ---------------------------------------------------------------------------


async def test_list_items(store: FileStore) -> None:
    """list_items returns all saved items for a workflow."""
    await store.save_item("wf-1", ItemState(id="a"))
    await store.save_item("wf-1", ItemState(id="b"))
    await store.save_item("wf-1", ItemState(id="c"))

    items = await store.list_items("wf-1")
    ids = {i.id for i in items}
    assert ids == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# Non-existent item returns None
# ---------------------------------------------------------------------------


async def test_get_nonexistent_item(store: FileStore) -> None:
    """Getting an item that doesn't exist returns None."""
    result = await store.get_item("wf-1", "ghost")
    assert result is None


# ---------------------------------------------------------------------------
# Non-existent step output returns None
# ---------------------------------------------------------------------------


async def test_get_nonexistent_step_output(store: FileStore) -> None:
    """Getting a step output that doesn't exist returns None."""
    result = await store.get_step_output("wf-1", "item-1", "missing")
    assert result is None


# ---------------------------------------------------------------------------
# Directory creation: dirs are created automatically
# ---------------------------------------------------------------------------


async def test_auto_directory_creation(tmp_path: Path) -> None:
    """Saving creates the directory structure automatically."""
    deep = tmp_path / "nested" / "deep"
    store = FileStore(str(deep))
    await store.save_item("wf-1", ItemState(id="auto"))

    assert (deep / "wf-1" / "_state" / "auto.json").exists()


# ---------------------------------------------------------------------------
# Multiple workflows: verify isolation
# ---------------------------------------------------------------------------


async def test_workflow_isolation(store: FileStore) -> None:
    """Items in different workflow IDs are isolated."""
    await store.save_item("wf-a", ItemState(id="shared-id", status="running"))
    await store.save_item("wf-b", ItemState(id="shared-id", status="failed"))

    a = await store.get_item("wf-a", "shared-id")
    b = await store.get_item("wf-b", "shared-id")
    assert a is not None and a.status == "running"
    assert b is not None and b.status == "failed"


async def test_step_output_isolation(store: FileStore) -> None:
    """Step outputs in different workflows are isolated."""
    await store.save_step_output("wf-a", "item-1", "step-1", "alpha")
    await store.save_step_output("wf-b", "item-1", "step-1", "beta")

    a = await store.get_step_output("wf-a", "item-1", "step-1")
    b = await store.get_step_output("wf-b", "item-1", "step-1")
    assert a == "alpha"
    assert b == "beta"


# ---------------------------------------------------------------------------
# List items on empty / nonexistent workflow
# ---------------------------------------------------------------------------


async def test_list_items_empty(store: FileStore) -> None:
    """list_items returns empty list for nonexistent workflow."""
    items = await store.list_items("no-such-workflow")
    assert items == []


# ---------------------------------------------------------------------------
# Item state update overwrites previous state
# ---------------------------------------------------------------------------


async def test_item_state_overwrite(store: FileStore) -> None:
    """save_item overwrites existing item state (unlike step output)."""
    await store.save_item("wf-1", ItemState(id="up1", status="pending"))
    await store.save_item("wf-1", ItemState(id="up1", status="completed"))

    loaded = await store.get_item("wf-1", "up1")
    assert loaded is not None
    assert loaded.status == "completed"
