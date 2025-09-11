# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

### Known Issues
- Embeddings generation consumes all of daemon IPC bandwidth. This will become immediately apparent after the onnx plugin is loaded with `yams plugin load onnx`. The system will attempt to generate all missing embeddings.
- We have noticed high CPU usage of the daemon when idling. We will continue to investigate and optimize this issue.

## [v0.6.13] - 2025-09-11

### Hotfixes
- Build stabilizations and improvements from MacOS testing

## [v0.6.12] - 2025-09-11

### Added
- CLI: `yams graph` — read‑only graph viewer that mirrors `get --graph`.
  - Usage: `yams graph <hash> [--depth 1-5]` or `yams graph --name "<path>" [--depth N]`.
  - Shows related documents via the knowledge graph without fetching content.
- Config: added `[daemon]` section to `examples/config.v2.toml` with `enable = true`, `auto_load_plugins = true`, optional `plugin_dir`, and notes about `YAMS_PLUGIN_DIR` discovery.
- CLI/build: restored Hot/Cold mode helpers and include wiring for list/grep/retrieval.
- `yams doctor`
  - Yams plugin and daemon health are now under one command
  - Doctor checks for vector DB vs model embedding dimension mismatch with clear fix steps.
  - Doctor warnings for missing companion files (config.json, tokenizer.json) next to model.onnx.
- Plugin System
  - Generic C‑ABI plugin loader with trust policy (scan/load/unload; trust list/add/remove)
  - IPC handlers for plugin scan/load/unload/trust; CLI `yams plugin` supports list/info/scan/load/unload/trust
  - DR CLI gating: `yams dr` subcommands (status/agent/help) now require plugins and print a guidance message; see docs/PLUGINS.md
  - Spec docs: docs/spec/plugin_spec.md, WIT drafts (object_storage_v1.wit, dr_provider_v1.wit), JSON Schemas (manifest/object_storage_v1/dr_provider_v1)
  - External examples scaffold under docs/examples/plugins and guide at docs/guide/external_dr_plugins.md for out‑of‑tree GPL plugins (S3/R2)
- CLI model tooling
  - `yams model provider`: shows active model provider status (loaded models) and the preferred model from config for transparency.
- Daemon plugin UX
  - Autoload from trusted + standard directories on startup (disable with `YAMS_AUTOLOAD_PLUGINS=0`).
  - Trust‐add autoload: newly trusted directories/files are scanned and loaded immediately (best‑effort).
  - Load by name: resolves logical names by scanning plugin descriptors across default dirs (e.g., `yams plugin load onnx`).
  - External plugin host (metadata only): supports `.yams-plugin.json` layouts for non‑ABI external integrations with trust and listing.
- Embedding provider safeguard
  - When a plugin model_provider_v1 is adopted, the daemon auto‑selects `embeddings.preferred_model = "all-MiniLM-L6-v2"` if the user hasn’t set one (or if it’s set to a non‑ONNX default). This nudges embeddings to the ONNX path by default.
- Status proto v3: extended `StatusResponse` over protobuf to carry readiness (per-subsystem), init progress, `overall_status`, and request counts. Serializer/deserializer updated with back‑compat casting and safe parsing.
- Streaming progress events for long ops: `EmbedEvent` and `ModelLoadEvent` over IPC.
- Startup hints: daemon logs a hint to run `yams repair --embeddings` on new/migrated data dirs and a hint to use `yams stats --verbose` for monitoring.

### Changed
- Documentation: general improvements and clarifications.
- CI: improved build reliability and semantic version enforcement.
- `yams doctor` summary now includes a Knowledge Graph section; when the graph is empty
  it recommends: `yams doctor repair --graph`.
- **PDF Extraction**: The built-in PDF text extraction logic has been refactored into a standalone plugin (`pdf_extractor`) that implements the new `content_extractor_v1` and `search_provider_v1` interfaces.
- Model download command now uses the unified downloader for all artifacts (model + companions).
- Improved repo/URL inference; clearer errors list exact URLs attempted.
- ONNX model provider: batched ONNX inference for efficiency; expanded config parsing (hidden_size, max_position_embeddings).
- Init: default v2 config now includes a `[daemon]` section with `enable = true` and `auto_load_plugins = true` so plugin adoption (e.g., ONNX) works out‑of‑the‑box after `yams init` when plugins are present in standard directories.
- CLI: added DR command (gated) and extended plugin command with install/enable/disable/verify stubs (no‑ops for now)
- Stats default view becomes adaptive: when service‑specific metrics or recommendations are relevant, the non‑verbose path includes the Recommendations and Service Status sections automatically; the compact STATS footer remains.
- Recommendation text updated to be actionable: suggests `yams repair --embeddings` instead of a vague “run indexing”.
- Doctor now performs smarter PID checks: prints the configured PID file and also checks a stable fallback in `/tmp`, reporting active/stale states explicitly.
- CLI daemon alignment: `get`, `grep`, and `list` now initialize `DaemonClient` with the CLI data directory and standardized timeouts (header 30s, body 120s). The CLI also seeds `YAMS_STORAGE` and `YAMS_DATA_DIR` so daemon auto-start binds to the same storage as the CLI (parity with `search`).
- MCP server storage selection: server now resolves and sets `DaemonClient` `dataDir` from environment/config (priority: `YAMS_STORAGE` → `YAMS_DATA_DIR` → `~/.config/yams/config.toml` → `XDG_DATA_HOME`/`HOME`) and seeds the same env vars for consistent daemon storage across processes.
- MCP tools: `get` tool schema now defaults `include_content` to `true` so content is returned by default unless explicitly disabled.
- Embeddings: streaming and stability improvements
  - Client streams batch embeddings by default when batch > 1 and retries transient failures with exponential backoff and dynamic sub‑batching (min 4).
  - Added detailed ONNX per‑item inference progress logs and total timing in ModelManager::runInference for better diagnosis of hangs.
  - Increased default daemon client timeouts to 120s; configurable via environment: `YAMS_REQUEST_TIMEOUT_MS`, `YAMS_HEADER_TIMEOUT_MS`, `YAMS_BODY_TIMEOUT_MS`.
  - Doctor/repair now surfaces EmbeddingEvent progress to stdout during `yams doctor repair --embeddings` so users see live progress.
  - Daemon respects client streaming hints for embedding requests; emits progress chunks and a final response.
  - Daemon logs batch embedding processing time (ms).
 - Daemon mux tuning centralized via TuneAdvisor (header-only):
   - New server-side knobs: `YAMS_SERVER_MAX_INFLIGHT`, `YAMS_SERVER_QUEUE_FRAMES_CAP`, `YAMS_SERVER_QUEUE_BYTES_CAP`, `YAMS_SERVER_WRITER_BUDGET_BYTES`.
   - Existing knobs: `YAMS_WRITER_BUDGET_BYTES`, `YAMS_MAX_MUX_BYTES`, `YAMS_WORKER_THREADS`, `YAMS_CHUNK_SIZE`.
   - SocketServer now honors `TuneAdvisor::chunkSize()` for streaming chunk size (no 32 KiB cap).
   - RequestHandler reads server settings via TuneAdvisor for consistent behavior.

### Fixed
- Knowledge Graph repair: `yams doctor repair --graph` now populates nodes/entities.
  - Implemented SQLite KG node upsert/lookups; previously stubbed methods caused
    `updated=0` (no-op) during repair even with existing tags/metadata.
- Plugin list over protobuf
  - `GetStatsResponse` JSON now embeds `plugins_json`; serializer/deserializer preserve it so `yams plugin list` shows loaded plugins reliably.
- Framing/transport for plugin ops
  - Resolved “Frame build failed” during `plugin scan/load` by aligning CLI and daemon payloads on the protobuf Envelope path.
- Build/link fixes
  - Link `nlohmann_json::nlohmann_json` into `yams_ipc_proto` (proto serializer uses JSON).
  - Link `-ldl` for CLI doctor on Linux (dlopen in `doctor plugin`).
  - Fixed unterminated string literals in plugin CLI output.
  - macOS: CMake configuration and linking fixes for Darwin toolchain (guard LINK_GROUP to Linux; use -force_load where appropriate).
- External plugin host
  - Corrected trust file newline writes, regex escapes, and Result usage in load(); ensured proper namespace scoping to fix build.

### Notes
- When `[embeddings].enable = false`, the daemon disables the model provider and plugin auto‑loading to reduce startup overhead; enable embeddings to allow autoload to adopt a provider (e.g., ONNX).

### Known Issues
- Embeddings with `nomic-embed-text-v1.5` (ONNX) may time out or fail under the daemon's streaming path on some systems.
  - Symptoms: `yams repair --embeddings` reports "Embedding stream failed: Read timeout" and logs show `EmbedDocuments dispatch: model='nomic-embed-text-v1.5'`.
  - Status: under investigation in the experimental branch (plugin/SDK path).
  - Workarounds:
    - Prefer other ONNX models: `all-MiniLM-L6-v2` (fast) or `all-mpnet-base-v2` (higher quality):
      - `yams config embeddings model all-MiniLM-L6-v2` (or pass `--model all-MiniLM-L6-v2` to commands)
      - `yams daemon restart`
    - Raise client timeouts for long embedding runs:
      - `export YAMS_CLIENT_HEADER_TIMEOUT_MS=60000; export YAMS_CLIENT_BODY_TIMEOUT_MS=300000`
    - Reduce batch pressure: `yams config embeddings tune balanced` or `yams config embeddings batch_size 8` then restart daemon.
    - Preload the model and smoke test a single embed:
      - `yams model load all-MiniLM-L6-v2`
      - `yams embed --model all-MiniLM-L6-v2 "hello world"`
    - Ensure the ONNX plugin is loaded and onnxruntime resolves:
      - `yams plugin load onnx`; `ldd /usr/local/lib/yams/plugins/libyams_onnx_plugin.so`
- MIME detection on add: single‑file and directory adds now perform MIME detection (magic → extension fallback) when not provided; MIME is stored in both content metadata and DB.
- Embeddings generation robustness:
  - Batch UTF‑8 sanitization prevents protobuf “invalid UTF‑8” errors.
  - Default embedding targets restricted to text‑like MIME; binary types (e.g., PDFs/images) are skipped unless explicitly included via `--include-mime`.
  - Daemon model preload is attempted only when the model provider is ready to avoid spurious read timeouts; Hybrid backend continues via local fallback.
- Auto-repair during degraded state: RepairCoordinator now allows small background batches while the daemon is not fully ready, limited by a configurable active‑connections threshold; also gates work using live `activeConnections` instead of assuming idle.
- MCP server: store by path could return "File not found" due to fragile path resolution. `handleStoreDocument` now normalizes paths robustly:
  - Strips `file://` scheme, expands `~`, resolves relative paths against `$PWD` and process `current_path()`, and applies `weakly_canonical` best‑effort.
  - Prevents spurious failures on repeated adds and differing working directories.
- Retrieval via daemon appearing to return no content or “not found” while `search` worked. CLI `get` and MCP `get/cat` now reliably hit the same storage as `search`, resolving mismatches caused by unset `dataDir` or differing environments.
