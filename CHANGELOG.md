# Changelog

## [2.0.0] - 2026-06-10

### 🚀 Performance
- Incremental sync: ~30s → **0.4s** (~75x faster) via remote hash skip
- Concurrent note content fetching (default 5 workers, configurable)
- KB helper instance caching — no redundant lookups per upload/delete
- Async state file writes — no longer blocks the event loop

### 🛡️ Reliability
- HTTP requests now retry with exponential backoff (default 3 attempts)
- `restore_deleted` uses concurrent batch doc existence checks instead of O(n) sequential calls
- Precise exception handling: ConnectError / Timeout / HTTP error codes distinguished

### 📏 Resource Protection
- New `max_file_size` config: skip oversized notes to save embedding resources
- Smoother yield control: `await asyncio.sleep(0.1)` every 5 notes instead of `sleep(1)` every 10

### ⚙️ New Config Options
- `max_file_size` (int, default 100): Max note size in KB, 0 = unlimited
- `concurrent_fetches` (int, default 5): Parallel note fetches
- `retry_count` (int, default 3): API retry count

## [1.0.0] - 2026-06-10

### Added
- Initial release
- Incremental sync with content hash comparison
- Obsidian syntax cleanup (wikilinks, highlights, comments, callouts, embeds)
- Exclude patterns (glob)
- Auto/manual sync with configurable interval
- `obsidian_sync` / `obsidian_status` / `obsidian_reset` commands
- Auto-recreate knowledge base if missing
- Restore deleted documents from knowledge base
