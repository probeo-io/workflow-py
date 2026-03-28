# Changelog

## 0.1.0 (2026-03-27)

- Initial release
- Workflow engine with concurrent and collective step modes
- FileStore for filesystem persistence with immutable step outputs
- Per-item concurrency with asyncio semaphore
- Retry with exponential backoff
- Resume-after-crash via write-once step outputs
- Resource injection and progress callbacks
- Abort signal support
