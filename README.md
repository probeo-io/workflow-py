# workflow-py

Stage-based pipeline engine for AI workloads. Zero dependencies. Code decides what happens. AI does the work.

Most AI pipelines are linear: fetch data, analyze it, enrich it, summarize it. Some stages call LLMs. Some don't. The control flow is deterministic even when AI is involved. You don't need a graph framework for that.

workflow-py gives you per-item concurrency, retry with backoff, filesystem persistence, and resume-after-crash. No graph theory. No dependency tree. No framework lock-in.

## Install

```bash
pip install workflow-py
```

## Quick Start

```python
import asyncio
from dataclasses import dataclass
from typing import Any

from anymodel import AnyModel
from workflow import Workflow, WorkflowOptions, WorkItem
from workflow.types import StepContext


@dataclass
class Analyze:
    name: str = "analyze"
    mode: str = "concurrent"

    async def run(self, url: Any, ctx: StepContext) -> Any:
        ai = ctx.resources["ai"]
        # fetch page content (use httpx, aiohttp, etc.)
        html = await fetch_page(url)
        result = await ai.chat(
            model="anthropic/claude-sonnet-4-20250514",
            messages=[{"role": "user", "content": f"Summarize this page:\n\n{html[:5000]}"}],
        )
        return {"title": url, "summary": result.message.content}


async def main():
    urls = ["https://example.com/page1", "https://example.com/page2"]
    results = await (
        Workflow(base_dir=".pipeline")
        .resource("ai", AnyModel(api_key="your-key"))
        .step(Analyze())
        .run(
            [WorkItem(id=f"page-{i}", data=url) for i, url in enumerate(urls)],
            WorkflowOptions(concurrency=10, max_retries=2),
        )
    )
    print(results)


asyncio.run(main())
```

## Use Cases

### Content pipeline with anymodel-py

Crawl pages, extract content with an LLM, generate SEO metadata. Each stage is a step. LLM calls happen inside steps, not as routing decisions.

```python
import asyncio
from dataclasses import dataclass
from typing import Any

from anymodel import AnyModel
from workflow import Workflow, WorkflowOptions, WorkItem
from workflow.types import StepContext


@dataclass
class Crawl:
    name: str = "crawl"
    mode: str = "concurrent"

    async def run(self, url: Any, ctx: StepContext) -> Any:
        html = await fetch_page(url)
        return {"url": url, "html": html}


@dataclass
class Extract:
    name: str = "extract"
    mode: str = "concurrent"

    async def run(self, page: Any, ctx: StepContext) -> Any:
        ai = ctx.resources["ai"]
        result = await ai.chat(
            model="anthropic/claude-sonnet-4-20250514",
            messages=[{
                "role": "user",
                "content": f"Extract the main content from this HTML. "
                f"Return JSON with title, description, and bodyText.\n\n{page['html'][:8000]}",
            }],
        )
        import json
        return {**page, "content": json.loads(result.message.content)}


@dataclass
class GenerateMeta:
    name: str = "generate-meta"
    mode: str = "concurrent"

    async def run(self, page: Any, ctx: StepContext) -> Any:
        ai = ctx.resources["ai"]
        result = await ai.chat(
            model="openai/gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": f"Write an SEO meta description (under 160 chars) for this content:\n\n"
                f"Title: {page['content']['title']}\n\n{page['content']['bodyText'][:2000]}",
            }],
        )
        return {**page, "meta": result.message.content}


async def main():
    urls = ["https://example.com/page1", "https://example.com/page2"]
    pipeline = (
        Workflow(base_dir=".content-pipeline")
        .resource("ai", AnyModel(api_key="your-key"))
        .step(Crawl())
        .step(Extract())
        .step(GenerateMeta())
    )
    results = await pipeline.run(
        [WorkItem(id=f"page-{i}", data=url) for i, url in enumerate(urls)],
        WorkflowOptions(
            concurrency=5,
            max_retries=2,
            on_progress=lambda p: print(f"{p.completed}/{p.total} ({p.current_step})"),
        ),
    )
    print(results)


asyncio.run(main())
```

### Research pipeline with anyserp-py + anymodel-py

Search the web for a topic, fetch the top results, analyze them with an LLM, then produce a collective summary.

```python
import asyncio
from dataclasses import dataclass
from typing import Any

from anymodel import AnyModel
from anyserp import AnySerp
from workflow import Workflow, WorkflowOptions, WorkItem
from workflow.types import StepContext


@dataclass
class Search:
    name: str = "search"
    mode: str = "concurrent"

    async def run(self, query: Any, ctx: StepContext) -> Any:
        serp = ctx.resources["serp"]
        results = await serp.search(query=query, num=5)
        return {"query": query, "results": results.organic}


@dataclass
class AnalyzeResults:
    name: str = "analyze"
    mode: str = "concurrent"

    async def run(self, data: Any, ctx: StepContext) -> Any:
        ai = ctx.resources["ai"]
        snippets = "\n".join(f"{r['title']}: {r['snippet']}" for r in data["results"])
        result = await ai.chat(
            model="anthropic/claude-sonnet-4-20250514",
            messages=[{
                "role": "user",
                "content": f'Analyze these search results for "{data["query"]}". '
                f"What are the key themes and findings?\n\n{snippets}",
            }],
        )
        return {**data, "analysis": result.message.content}


@dataclass
class Summarize:
    name: str = "summarize"
    mode: str = "collective"

    async def run(self, items: Any, ctx: StepContext) -> Any:
        ai = ctx.resources["ai"]
        analyses = "\n\n".join(f"## {i['data']['query']}\n{i['data']['analysis']}" for i in items)
        result = await ai.chat(
            model="anthropic/claude-sonnet-4-20250514",
            messages=[{
                "role": "user",
                "content": f"Synthesize these research analyses into a single brief:\n\n{analyses}",
            }],
        )
        return [{"id": i["id"], "summary": result.message.content} for i in items]


async def main():
    queries = ["Python async patterns", "LLM orchestration frameworks"]
    pipeline = (
        Workflow(base_dir=".research")
        .resource("ai", AnyModel(api_key="your-openrouter-key"))
        .resource("serp", AnySerp(provider="serper", api_key="your-serper-key"))
        .step(Search())
        .step(AnalyzeResults())
        .step(Summarize())
    )
    results = await pipeline.run(
        [WorkItem(id=f"query-{i}", data=q) for i, q in enumerate(queries)],
        WorkflowOptions(concurrency=3),
    )
    print(results)


asyncio.run(main())
```

### Batch processing with resume

Process 10,000 items. If it crashes at item 6,000, restart and it picks up where it left off. FileStore writes are immutable. Completed steps are never re-run.

```python
import asyncio
from workflow import Workflow, WorkflowOptions, WorkItem

pipeline = (
    Workflow(
        id="batch-2026-03-28",  # Fixed ID enables resume
        base_dir=".batch-data",
    )
    .step(fetch_step)
    .step(transform_step)
    .step(enrich_step)
)

# First run: processes all 10,000
await pipeline.run(items, WorkflowOptions(concurrency=20))

# Crashes at item 6,000. Restart:
# Items 1-6,000 are skipped (outputs exist on disk).
# Items 6,001-10,000 are processed.
await pipeline.run(items, WorkflowOptions(concurrency=20))
```

## Concepts

- **Step** -- a named unit of work with a `run()` method. Each step declares a `mode`:
  - `concurrent` -- runs per-item, many in parallel (up to the concurrency limit)
  - `collective` -- waits for all items, runs once with the full set (useful for summarization, aggregation)
- **Workflow** -- chains steps together. Supports resource injection, progress callbacks, and abort signals.
- **FileStore** -- filesystem persistence with immutable write-once outputs. Enables resume after crash.

## API

### `Workflow(*, id=None, store=None, base_dir=".workflow")`

| Option | Type | Default | Description |
|---|---|---|---|
| `id` | `str` | auto-generated | Workflow run identifier. Use a fixed ID to enable resume. |
| `store` | `WorkflowStore` | `FileStore` | Persistence backend |
| `base_dir` | `str` | `".workflow"` | Base directory for `FileStore` |

### `.resource(name, value)`

Register a shared resource available to all steps via `ctx.resources`.

### `.step(step)`

Add a step to the pipeline. Steps run in the order they are added.

### `await .run(items, options?)`

Run all items through the pipeline. Returns a list of `ItemState` objects.

| Option | Type | Default | Description |
|---|---|---|---|
| `concurrency` | `int` | `5` | Max parallel items for concurrent steps |
| `max_retries` | `int` | `2` | Retry attempts per item per step (exponential backoff) |
| `on_progress` | `Callable` | `None` | Progress callback |
| `signal` | `asyncio.Event` | `None` | Cancellation signal (set the event to abort) |

### `await .run_one(item, options?)`

Run a single item through the full pipeline. Returns one `ItemState`.

### `FileStore`

Filesystem-backed store. Step outputs are immutable. Once written, never overwritten. Safe to resume after interruption.

### `StepContext`

Every step receives a context object:

| Property | Type | Description |
|---|---|---|
| `item_id` | `str` | Current item's ID |
| `resources` | `dict[str, Any]` | Shared resources registered via `.resource()` |
| `log` | `Logger` | Logger scoped to this item + step |
| `get_cache` | `async (step_name) -> Any` | Read a previous step's cached output |
| `signal` | `asyncio.Event` | Cancellation signal |

## Why not LangGraph?

LangGraph is for agent orchestration. Cyclic graphs where an LLM decides what to do next. That's the right tool when you need non-deterministic routing.

But most AI workloads are pipelines. Items flow through stages. Some stages call LLMs. The control flow is deterministic. For that, LangGraph adds complexity without value:

| | workflow-py | LangGraph |
|---|---|---|
| Dependencies | 0 | 23 packages, 53 MB |
| Mental model | Steps in order | Nodes, edges, state reducers, graphs |
| Concurrency | Per-item with limits | Fan-out via Send pattern |
| Persistence | FileStore (filesystem) | Checkpointer (Postgres, memory) |
| Resume | Immutable step outputs | Checkpoint after every node |
| Lock-in | None | Requires langchain-core (12 MB) |

## License

MIT
