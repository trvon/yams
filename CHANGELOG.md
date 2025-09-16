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
- Daemon may show broken connection with rapid CLI/MCP usage
- Embeddings generation consumes all of daemon IPC bandwidth. This will become immediately apparent after the onnx plugin is loaded with `yams plugin load onnx`. The system will attempt to generate all missing embeddings.
- We have noticed high CPU usage of the daemon when idling. We will continue to investigate and optimize this issue.


## [v0.6.32] - 2025-09-16

### Fixed
- Fixing linking errors seen on ubuntu 24.04
- CI/CD 
  - Sourcehut: Fixing how packaging is exposed
  - Github Actions: Fixing test dependency setup and release package steps

## [v0.6.31] - 2025-09-16

### Fixed
- CI/CD
  - GitHub Actions: tests workflow YAML fixed (indentation of `timeout-minutes` under the job).
  - Docker workflow: ARM64 builds now use QEMU + Buildx on `ubuntu-latest`; native `ubuntu-24.04-arm`
    runners no longer required for ARM publishing. Multi-arch manifest unchanged.

## [v0.6.30] - 2025-08-16

### Fixed
- Fixed regressions in CLI / MCP UI performance from the introduction of the daemon
- ARM Build: Boost `find_package` failure resolved via corrected Conan toolchain path and added generator directory visibility during CMake configure.

### Added
- Packaging: Introduced Homebrew formula templating (`packaging/homebrew/yams.rb.template`) with placeholder substitution for version + sha256 during stable releases.
- Release workflow: Generates and commits `latest.json` manifest plus Homebrew formula in a single pass; adds commit step guarded to stable channel.
- Homebrew: Added `livecheck` block for future tap/live version detection.
- Services
  - RetrievalService parity for CLI and MCP (get/list/grep) with shared facade.
  - DocumentIngestionService used by CLI add and MCP add for daemon‑first ingestion.
- Daemon: Native chunked get protocol handlers (GetInit/GetChunk/GetEnd) backed by in‑memory
  RetrievalSessionManager, enabling efficient large content retrieval and capped buffers.

### Changed
- CI/Build
  - Unpinned Conan across tests and release workflows (allow latest compatible Conan 2.x) while retaining reproducibility via profile + lock hashing.
  - Updated `CMakePresets.json` toolchain paths (removed incorrect nested `build/Debug` / `build/Release` segments) enabling correct Boost discovery on ARM.
  - Extended test job timeout to 50 min (ARM warm builds) and added Conan/Boost diagnostics aiding cache hit analysis.
  - Replaced heredoc inline formula generation with template-driven substitution reducing workflow maintenance surface and diff noise.
- CLI + MCP: Switched get/list/grep to RetrievalService; add paths to DocumentIngestionService to
  eliminate bespoke logic and timeouts/hangs divergences.
- CLI: Guarded `cfg.dataDir` assignment behind `hasExplicitDataDir()` throughout commands per
  refactor plan. Centralized DaemonClient data‑dir resolution in default constructor (env → config
  `core.data_dir` → XDG/HOME → `./yams_data`).
- RetrievalService: `getChunkedBuffer` prefers the native chunked protocol and falls back to unary
  `Get` only for `NotImplemented/Timeout/NetworkError`. Improved name‑smart get to resolve via
  filename when hybrid search returns a path (macOS path aliasing resilience).

## [v0.6.29] - 2025-09-14

### CI Bump
- Fixing github actions by removing conan version pin and fixing test.yml

## [v0.6.28] - 2025-09-14

### CI Bump
- Fixing github actions macos key signing regression

## [v0.6.27] - 2025-09-14

### Changed
- CI/build: Nightly packaging auto-enables on `refs/heads/main` (no time-based scheduler needed). Improved repo-root detection (`yams/` vs `.`) across all tasks, and broadened artifact globs to support both layouts.
- CI/build: Hardened packaging tasks — CPack RPM now guarded by `rpmbuild` availability and won’t fail the job if RPM tooling is missing; DEB packaging remains enabled. Added `set -e` to packaging steps for clearer early exits while keeping RPM optional.
- Knowledge Graph: Added unit tests for alias exact/fuzzy resolution, neighbor retrieval, and doc-entity roundtrips (`kg_store_alias_and_entities_test.cpp`).
- Search + KG: Added SimpleKGScorer end-to-end test validating entity and structural scores and explanations (`kg_scorer_simple_test.cpp`).

## [v0.6.26]

### CI Bump
- Sourcehut debian image updates for packaging on fedora
- Github CI improvements for testing and release builds

## [v0.6.25] - 2025-09-14

### CI Bump
- Fixing github actions cache and release naming

## [v0.6.24] - 2025-09-14

### CI Bump
- Fixing build matrix used in github actions to fix artifact uploading.

## [v0.6.23] - 2025-09-14

### CI Bump
- Increasing test timeouts and fixing packing errors with sourcehut builds

## [v0.6.22] - 2025-09-14

### CI Bump
- Fixing macos env error

## [v0.6.21] - 2025-09-14

### Fixes
- Fixing prefix for sourcehut builds
- Adding cache for Github CI

## [v0.6.20] - 2025-09-13

### Added
- Asynchronous post‑ingest pipeline (daemon): decouples heavy work from `add`/`add_directory`.
  - New `PostIngestQueue` performs extraction → full‑text index → knowledge graph upserts in the background.
  - Queue metrics (threads, queued, processed, failed) with EMAs for latency and throughput are exposed via `status`.
- Knowledge Graph enrichment beyond tags:
  - Lightweight analyzers extract URLs, emails, and optional file paths and add `MENTIONS` edges.
  - Caps and toggles are controlled via `TuneAdvisor`.
- Batch KG operations:
  - `KnowledgeGraphStore::upsertNodes(std::vector<KGNode>)` used in the post‑ingest worker to avoid read‑after‑write races.
  - New `KnowledgeGraphStore::addEdgesUnique(std::vector<KGEdge>)` batches edge inserts with on‑conflict de‑duplication on `(src_node_id, dst_node_id, relation)`.
- TuneAdvisor controls (code‑level, no env):
  - `kgBatchNodesEnabled`, `kgBatchEdgesEnabled` (default true)
  - Analyzer toggles: `analyzerUrls`, `analyzerEmails`, `analyzerFilePaths` (file paths off by default)
  - `maxEntitiesPerDoc` (default 32)
- MCP observability and parity:
  - New `status` tool that returns daemon readiness and counters (includes post‑ingest and MCP worker pools).
  - New `yams://status` resource for symmetry with `yams://stats`.
  - Minimal `doctor` tool summarizes degraded subsystems and suggests actions.

### Changed
- `add` and `add_directory` delegate extraction/index/graph work to the post‑ingest queue; `DocumentService` honors `deferExtraction` to keep adds fast.
- Post‑ingest worker now batches KG node upserts and uses unique batch edge inserts to reduce DB round‑trips.
- Vector/Readiness metrics mirrored into `status.requestCounts` for MCP/CLI parity.

### Fixed
- Daemon readiness consistency across CLI/UI:
  - `DaemonMetrics` now derives boolean `ready` from the lifecycle state (authoritative),
    not deprecated `fullyReady()`. Prevents `doctor` showing "NOT READY, state=Ready".
  - Normalized status strings to lowercase ("ready", "degraded", etc.) in the status path to
    match serializer/client expectations and avoid case-related derivation bugs.
  - Stats `not_ready` flag now reflects lifecycle readiness (Ready/Degraded → `false`) instead of
    always `true`, so `yams stats` won’t unnecessarily fall back to local mode.
  - Bootstrap status JSON (`yams-daemon.status.json`) now writes a lowercase `overall` field for
    consistency with IPC lifecycle strings.
 - Stats daemon reachability:
   - Increased `yams stats` short-mode timeout from 1.5s → 5s and set explicit connect/header/body
     timeouts to avoid premature local fallback, aligning with `status`/`doctor` behavior.
   - `not_ready` heuristic is now lifecycle-driven (Ready/Degraded → false), so stats no longer
     assumes unready by default.
   - Temporary mitigation: `yams stats` currently forces local-only output while we investigate a
     daemon GetStats streaming/header path issue that can stall and cause slow fallbacks. The daemon
     path will be re-enabled once the bug is fixed.

## [v0.6.19] - 2025-09-13

### Fixed
- MCP stdio framing correctness and async send:
  - Fixed double-encoding on outbound async path: `outboundDrainAsync` now uses a framed string sender instead of passing a serialized JSON string to `send(const json&)`.
  - Removed extra CRLF after framed payload; framed responses now send exactly `Content-Length` bytes after the blank line (LSP/MCP compliant).
  - Added `StdioTransport::sendFramedSerialized(const std::string&)` for pre-serialized payloads used by the outbound drain.
- MCP prompts/get schema correctness for MCP Inspector:
  - Roles now use only `assistant` and `user` (removed `system`).
  - `content` is a single object `{ "type": "text", "text": ... }`, not an array.
- macOS/Linux build/link improvements:
  - Resolved yams-daemon undefined symbols on Linux by including IPC OBJECT files in the daemon static lib and linking `yams_ipc_proto`.
  - Fixed yams-mcp-server linking on macOS by avoiding IPC OBJECTs that pull daemon-only symbols; explicitly link IPC/client/proto libs instead.

### Changed
- StdioTransport testability: in test builds (`YAMS_TESTING`), receive() uses `cin.rdbuf()->in_avail()` to support reliable stringstream-driven tests.
- Default server send now includes `Content-Type: application/vscode-jsonrpc; charset=utf-8` and a trailing CRLF when using header framing to maximize client compatibility.
- ONNX plugin dependency hygiene: Removed unintended PDFium bundling logic from the ONNX plugin CMake (it never linked or required PDFium). Added a configure-time guard warning if `pdfium::pdfium` becomes linked accidentally to keep plugin responsibilities isolated. PDF extraction remains solely in the `pdf_extractor` plugin.

### Added
- ONNX/GenAI path (vector library):
  - Scaffolded a minimal GenAI adapter in the vector layer and made GenAI the default when ONNX is enabled; falls back to raw ONNX Runtime when GenAI is unavailable.

### Daemon & Plugins
- Plugin autoload diagnostics: adoption logs now include the plugin file path; environment `YAMS_PLUGIN_DIR` is prioritized over system directories to avoid stale installs.

## [v0.6.18] - 2025-09-13

### Hot fix
- CI Bump : CMake and conan update for 

## [v0.6.17] - 2025-09-13

### Hot fix
- CI Bump : Resolving compile compatibiliy 

### Changed
- Build: Removed legacy `-fcoroutines-ts` flag for modern Clang toolchains (C++20 coroutines are enabled by default). Prevents build failures on distributions where the flag is rejected.
- Build: Fixed macOS Conan host profile parsing error by removing unsupported `[env]` section; libc++ enforcement now handled via workflow sanitization.
- Build: Added defensive scrub in `CMakeLists.txt` to strip any externally injected `-fcoroutines-ts` occurrences from cached flags.

## [v0.6.16] - 2025-09-13

### Hot fix
- CI Bump : Fixing syntax

## [v0.6.15] - 2025-09-13

### Hot fix
- CI Bump : Updating clang version in host.ninja for builds

## [v0.6.14] - 2025-09-12

### Added
 - CLI Doctor:
   - `yams doctor dedupe` — detects (and optionally deletes) duplicate documents grouped by `--mode` (`path|name|hash`) with keep strategies (`--strategy keep-newest|keep-oldest|keep-largest`), dry-run by default, and safety flag
            `--force` for hash mismatches.
   - `yams doctor embeddings clear-degraded` — attempts to clear the embedding degraded state by loading a preferred/available model (interactive confirmation, best-effort).
- Status + CLI plugin diagnostics:
  - StatusResponse now carries typed `providers` and a `skippedPlugins` array (path + reason).
  - `yams plugins list` prefers typed providers and supports `--verbose` to print a “Skipped plugins” section with clear reasons (preflight/dlopen/symbols/name-policy).
- Config and init:
  - New `[daemon].plugin_name_policy` with values `relaxed` (default) or `spec` (require `libyams_*_plugin.*`).
  - `examples/config.v2.toml` documents the field; `yams init --enable-plugins` now writes `plugin_dir` and sets `plugin_name_policy = "spec"` for canonical naming by default.
  - New `[daemon].auto_repair_batch_size` defaulted to 16; daemon applies it when starting the RepairCoordinator.
- macOS plugin robustness:
  - ONNX plugin CMake now bundles ONNX Runtime dylibs (existing) and PDFium dylib into local subdirs and injects `@loader_path/onnxruntime` and `@loader_path/pdfium` into rpaths. SIP‑safe, no DYLD tweaks.

### Changed
- Safer ABI plugin scanning:
  - `scanTarget()` no longer `dlopen()`s candidates (prevents constructor crashes); macOS `dlopen_preflight()` is performed in `load()` and failures are handled gracefully.
  - Filename filter accepts `libyams_*_plugin.*` and `yams_*_plugin.*`; strict policy can be enforced via config/env.
- CLI `yams plugins list` auto‑starts the daemon, triggers a scan, waits briefly for readiness, and prints typed providers. Helpful guidance is printed if no info found.

### Fixed
- macOS crash on plugin scan/load:
  - Eliminated segfaults by avoiding `dlopen()` during scan; added preflight + robust error logging in load path; fixed `dlerror()` usage (read once, avoid null concatenation).
- Repair throttling and backpressure:
  - RepairCoordinator honors config batch size and TuneAdvisor env knobs (`YAMS_REPAIR_MAX_BATCH`, `YAMS_REPAIR_TOKENS_IDLE/BUSY`, `YAMS_REPAIR_MAX_BATCHES_PER_SEC`, etc.) and pauses token issuance when hitting per‑second caps to keep the daemon responsive.
- Linux build/linking:
  - `yams-daemon` now links explicitly against `yams::daemon_ipc` and `yams::daemon_client`, resolving undefined symbol errors (ConnectionFsm/RequestHandler/ResourceTuner) on Ubuntu/ELF toolchains.
- Test/CI stability:
  - `run_sync` rewritten to use a promise + `co_spawn(detached)` and `co_await std::move(aw)` to avoid move/copy pitfalls with Boost.Asio awaitables.
- Examples/config:
  - `examples/config.v2.toml` updated with `plugin_name_policy` and daemon plugin notes; defaults align with init.

### Notes
- If your macOS ONNX plugin previously failed with `libpdfium.dylib` not found, rebuild/reinstall the plugin; the new CMake will bundle PDFium into a local `pdfium/` folder and adjust rpaths to `@loader_path/pdfium`.
- Background repair can be tuned at runtime via environment without restarting; see the `TuneAdvisor` variables above for recommended settings during heavy foreground load.

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
