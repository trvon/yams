---
description: YAMS-first agent with blackboard coordination and persistent memory
argument-hint: [TASK=<description>] [PBI=<pbi-id>] [PHASE=<start|checkpoint|complete>]
---

# Agent Workflow (YAMS + Blackboard)

YAMS is the single source of truth for agent memory. When multiple agents are involved, use the blackboard tools to coordinate work.

## Core Identity

- Default mode: use YAMS for memory (code + notes + decisions + research).
- Coordination mode: always try to register on the blackboard and use it to share findings/tasks.
- If blackboard tools are unavailable, fall back to the YAMS-only workflow (file claiming + metadata).

## Agent ID Convention

- Use a stable, readable ID: `opencode-<task-slug>`.
- Keep `<task-slug>` lowercase ASCII with dashes (example: `opencode-index-speedup`).

## Do / Don't

### Do

- Always register on the blackboard at the start of a task.
- Use the blackboard to coordinate: post findings, create/claim tasks, group work in contexts.
- Search before acting: `yams grep` for code patterns; `yams search` for semantic/concept queries.
- Index as you learn: add code, notes, decisions, and research to YAMS as you go.
- Use metadata consistently so knowledge is queryable across sessions.

### Don't

- Never `git push` without first indexing the work in YAMS.
- Never delete files without first indexing the work in YAMS.
- Don't duplicate the skill docs in this file; keep this file focused on behavior and workflow.

## Safety & Permissions

### Allowed Without Asking

- Read/list files, run targeted tests/lints, run builds when explicitly requested.
- YAMS operations: add/search/grep/graph/session/watch/status/doctor.
- Blackboard operations: post/query/search/claim/update/complete findings and tasks.

### Ask First (Always)

- `git push` (must index in YAMS first)
- Deleting files (must index in YAMS first)
- Installing new dependencies

## Required Metadata (Memory + PBI Tracking)

Attach metadata to every `yams add`.

- `task` - short task slug (example: `index-speedup`)
- `phase` - `start` | `checkpoint` | `complete`
- `owner` - `opencode` (shared owner for multi-agent retrieval)
- `source` - `code` | `note` | `decision` | `research`

Optional (when applicable):

- `pbi` - PBI identifier (example: `PBI-043`)
- `agent_id` - your canonical agent ID (example: `opencode-index-speedup`)

## Project Structure (Where To Look First)

- CLI entry points and command wiring: `src/cli/`
- Service layer and command handlers: `src/app/services/`
- Search implementations (grep/semantic): `src/search/`
- Storage engines and backends: `src/storage/`
- Vector DB + embeddings: `src/vector/`
- Daemon client/server: `src/daemon/`
- MCP server implementation: `src/mcp/`

## Workflow

### 0) Register (Always)

Try blackboard registration first. If it fails, continue with YAMS-only flow.

```text
bb_register_agent({ id: "opencode-<task-slug>", name: "OpenCode Agent", capabilities: ["yams", "code", "coordination"] })
```

### Blackboard Coordination (Minimal)

Keep coordination lightweight: findings capture what was discovered; tasks capture what needs doing.

```text
bb_search_findings({ query: "<keywords>" })
bb_post_finding({ agent_id: "opencode-<task-slug>", topic: "other", title: "<title>", content: "<markdown>" })
bb_create_task({ title: "<work item>", type: "fix", priority: 2, created_by: "opencode-<task-slug>" })
bb_claim_task({ task_id: "<task-id>", agent_id: "opencode-<task-slug>" })
bb_update_task({ task_id: "<task-id>", status: "working" })
```

### 1) Search Existing Knowledge

```bash
# Code patterns first
yams grep "<pattern>" --cwd .

# Semantic/concept search
yams search "$TASK" --limit 20

# Structured metadata lookups
yams search "task=$TASK" --type keyword --limit 20
```

### 1.5) YAMS-First Retrieval Behavior (Control-Loop Friendly)

Use this order whenever you need content for reasoning:

1. `yams search`/`yams grep` to discover candidates
2. `yams get` (or MCP `get`) to read selected artifacts
3. Only then use local file reads if needed for implementation detail

Behavior rules:

- Prefer YAMS retrieval over ad-hoc local search/cat for knowledge lookup.
- Do not mark chunks as "rejected" unless explicitly provided by user/model feedback.
- Treat `served - used` as `not_used` (low-confidence negative), not hard rejection.

Episode signal policy (implicit feedback):

- High-confidence positive episode: `search -> get/read -> no immediate reformulation search`.
- Negative signal: rapid reformulation loops, repeated failed searches, or abandon without `get/read`.
- These are weak-supervision signals for tuning, not ground truth.

Structured reporting (when retrieval is used):

- Include `UsedContext: <chunk_ids or hashes>` when known.
- Include `Citations: <artifact names/hashes/paths>`; if none, write `Citations: none`.
- Preserve `trace_id` in logs/output paths when available for later attribution.

### 2) Start Work (Index Baseline + Claim)

```bash
# Index baseline
yams add . --recursive \
  --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Working on: $TASK" \
  --metadata "pbi=$PBI,task=$TASK,phase=start,owner=opencode,source=code,agent_id=opencode-$TASK"
```

If blackboard is available, claim or create a task there. If not, "claim" files via YAMS metadata:

```bash
yams add - --name "claim-$TASK.md" \
  --metadata "pbi=$PBI,task=$TASK,phase=start,owner=opencode,source=note,agent_id=opencode-$TASK" \
  <<'EOF'
## Claim
Agent: opencode-$TASK
Scope: <paths or subsystems>
Goal: <one sentence>
EOF
```

### 3) Checkpoint (Index What Changed)

```bash
yams add <changed-files> \
  --label "$TASK: checkpoint" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=opencode,source=code,agent_id=opencode-$TASK"
```

### 4) Complete (Index + Close Loop)

```bash
yams add . --recursive \
  --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Completed: $TASK" \
  --metadata "pbi=$PBI,task=$TASK,phase=complete,owner=opencode,source=code,agent_id=opencode-$TASK"
```

## Response Template

```text
TASK: $TASK
PBI: $PBI
PHASE: $PHASE
AGENT: opencode-$TASK

CONTEXT FOUND:
- Blackboard: <notes/findings/tasks>
- YAMS: <docs/paths>

ACTIONS:
- <what changed and why>

INDEXED:
- <files/notes indexed>
- Metadata: task=$TASK,phase=$PHASE,owner=opencode,agent_id=opencode-$TASK

NEXT:
- <next step>

USED_CONTEXT:
- <chunk_ids/hashes if known; else "unknown">

CITATIONS:
- <artifact names/hashes/paths or "none">
```

## PR Checklist

- All modified/new files indexed in YAMS with metadata
- Any coordinated work reflected in blackboard tasks/findings (if available)
- No secret material added to the repo or indexed notes
- Commit/PR message explains "why" (not just "what")

## When Stuck

- Ask one targeted clarifying question, with a recommended default.
- Post a blackboard finding if the answer should help other agents.
- Prefer small, reversible changes; avoid speculative rewrites.

## Context Recovery

If the chat context is compacted or lost, rebuild it from YAMS using `yams list` filtered by `owner=opencode`, `task`, and recent time.

## References (Skills)

- `yams` (YAMS memory + search + graph)
- `yams-blackboard` (blackboard coordination on top of YAMS)

---

## C++ Style Guide (YAMS Patterns)

`.clang-format` handles mechanical formatting (LLVM base, 4-space indent, 100-col limit, Attach braces, `c++20`). This section covers **project-specific patterns** that clang-format cannot enforce.

### Formatting Notes

- Include order (enforced by `.clang-format` priorities):
  1. `"yams/..."` project headers
  2. `<tracy/...>` profiler
  3. `<sqlite3...>` / `<sqlite_vec...>`
  4. Third-party: `<spdlog/...>`, `<nlohmann/...>`, `<CLI11/...>`, `<magic_enum/...>`, `<toml/...>`, `<onnxruntime/...>`
  5. Standard library `<algorithm>`, `<string>`, etc.
- Pointer/reference alignment: left (`int* p`, `const std::string& s`).
- Requires clauses go on their own line.

### Naming Conventions

| Element | Style | Example |
|---------|-------|---------|
| Functions, variables | `camelCase` | `loadConfig()`, `chunkSize` |
| Types (class/struct/enum/concept) | `PascalCase` | `ResourceSnapshot`, `ErrorCode` |
| Compile-time constants | `kPascalCase` | `kBase`, `kNumPriorities`, `kStartupGracePeriod` |
| Member variables | trailing `_` | `running_`, `strand_`, `autoEmbedPolicy_` |
| Static inline atomics | trailing `_` | `postExtractionConcurrentDynamicCap_` |
| Namespaces | `lowercase` | `yams::features`, `yams::config` |
| Files | `snake_case` | `dispatch_utils.hpp`, `indexing_service.cpp` |
| Test files | `*_catch2_test.cpp` | `tuning_reconciliation_catch2_test.cpp` |

### Error Handling: `Result<T>`

Defined in `include/yams/core/types.h`. Variant-based (`std::variant<T, Error>`).

```cpp
// Returning errors
Result<Config> loadConfig(const std::string& path) {
    if (!exists(path))
        return Error{ErrorCode::FileNotFound, "Config not found: " + path};
    // ...
    return Config{...};
}

// Consuming results
auto result = loadConfig(path);
if (!result)                         // explicit operator bool
    return result.error();           // propagate
auto& cfg = result.value();          // access (throws if error)

// Result<void> for side-effect operations
Result<void> saveConfig(const Config& cfg);
```

**When to use what:**

| Mechanism | Use for |
|-----------|---------|
| `Result<T>` | Fallible operations where the caller must handle errors |
| `std::optional<T>` | "Absent" values with no error detail (lookups, parsing) |
| Exceptions | Truly unrecoverable / programming errors (`std::runtime_error`) |

Future: when `YAMS_HAS_EXPECTED` is available on all targets, `Result<T>` may migrate to `std::expected<T, Error>` (APIs are similar).

### Feature Gates (`YAMS_HAS_*`)

Defined in `include/yams/core/cpp23_features.hpp`. Always use these macros, never raw `__cplusplus` checks.

**Available macros:**

| Macro | Feature | Minimum |
|-------|---------|---------|
| `YAMS_HAS_CONSTEXPR_VECTOR` | `constexpr std::vector` | GCC 13, Clang 16 |
| `YAMS_HAS_CONSTEXPR_STRING` | `constexpr std::string` | GCC 13, Clang 16 |
| `YAMS_HAS_CONSTEXPR_CONTAINERS` | Both of the above | GCC 13, Clang 16 |
| `YAMS_HAS_EXPECTED` | `std::expected` | GCC 12, Clang 16 |
| `YAMS_HAS_RANGES` | `std::ranges` / `std::views` | GCC 12, Clang 15 |
| `YAMS_HAS_FLAT_MAP` | `std::flat_map` | GCC 12, Clang 17 |
| `YAMS_HAS_MOVE_ONLY_FUNCTION` | `std::move_only_function` | GCC 12, Clang 17 |
| `YAMS_HAS_STRING_CONTAINS` | `std::string::contains()` | GCC 12, Clang 12 |
| `YAMS_HAS_LIKELY_UNLIKELY` | `[[likely]]` / `[[unlikely]]` | GCC 7, Clang 9 |
| `YAMS_HAS_PROFILES` | C++26 Profiles (experimental) | -- |
| `YAMS_HAS_REFLECTION` | C++26 Reflection (experimental) | -- |

**Pattern:**

```cpp
#include <yams/core/cpp23_features.hpp>

#if YAMS_HAS_RANGES
#include <ranges>
auto filtered = items | std::views::filter(pred);
#else
std::vector<Item> filtered;
std::copy_if(items.begin(), items.end(), std::back_inserter(filtered), pred);
#endif
```

**Convenience macro:** `YAMS_CONSTEXPR_IF_SUPPORTED` expands to `constexpr` (C++23) or `inline` (C++20).

**Compatibility helpers** in `yams::features::`: `string_contains()`, `string_starts_with()`, `string_ends_with()` -- work on both C++20 and C++23.

**Adding a new gate:**

1. Add a `#ifndef` / `#if defined(__cpp_lib_xxx)` / `#define` block in `cpp23_features.hpp`.
2. Add it to the `FeatureInfo` struct.
3. Provide a C++20 fallback everywhere the feature is used.
4. Test with both C++20 and C++23 build configs.

### TuneAdvisor Knob Pattern

Defined in `include/yams/daemon/components/TuneAdvisor.h` (~2800 lines). Central registry for all runtime-tunable parameters.

**Static inline atomic triplet (override -> env -> default):**

```cpp
// 1. Storage (private)
static inline std::atomic<uint32_t> chunkSizeOverride_{0};

// 2. Getter (priority: override -> env -> computed default)
static uint32_t chunkSize() {
    uint32_t ovr = chunkSizeOverride_.load(std::memory_order_relaxed);
    if (ovr) return ovr;
    if (const char* s = std::getenv("YAMS_CHUNK_SIZE")) {
        try { return static_cast<uint32_t>(std::stoull(s)); }
        catch (...) {}
    }
    return 512u * 1024u;  // default: 512 KiB
}

// 3. Setter
static void setChunkSize(uint32_t v) {
    chunkSizeOverride_.store(v, std::memory_order_relaxed);
}
```

**`memory_order_relaxed`** is correct here because these knobs are best-effort advisory values read on hot paths; sequential consistency is not required.

**DynamicCap vs Override (ratchet-down fix):**

The tuning system has two write layers for post-ingest concurrency:

| Layer | Written by | Purpose |
|-------|-----------|---------|
| `*Override_` | User / env | Hard cap, base budget input |
| `*DynamicCap_` | `TuningManager::tick_once()` | Temporary scaling cap, can recover to base |

`postIngestBudgetedConcurrency(bool includeDynamicCaps)`:
- `false` -- returns base budget (for `ResourceGovernor::updateScalingCaps()`)
- `true` -- returns effective value clamped by DynamicCaps (for runtime consumers: `PostIngestQueue`, `EmbeddingService`)

Setting a DynamicCap to 0 clears it (effective value returns to base budget).

### InternalEventBus

Defined in `include/yams/daemon/components/InternalEventBus.h`. Singleton with typed SPSC channels.

```cpp
// Create or get a typed channel
auto channel = InternalEventBus::instance()
    .get_or_create_channel<InternalEventBus::PostIngestTask>(
        "post_ingest_tasks", 4096);

// Push (non-blocking)
channel->try_push(InternalEventBus::PostIngestTask{hash, mimeType});

// Pop (non-blocking, returns std::optional or bool)
InternalEventBus::PostIngestTask task;
if (channel->try_pop(task)) { /* process */ }
```

**Job types** are nested structs: `EmbedJob`, `PostIngestTask`, `KgJob`, `StoreDocumentTask`, etc.

Channels are keyed by string name and cached in a `std::unordered_map<std::string, std::shared_ptr<void>>`. Template type is recovered via `static_pointer_cast`.

### ResourceGovernor Interaction

Singleton in `include/yams/daemon/components/ResourceGovernor.h`. Components should check admission before resource-intensive work.

```cpp
auto& gov = ResourceGovernor::instance();

// Admission checks (call before starting expensive work)
if (!gov.canAdmitWork())     return;  // backpressure
if (!gov.canScaleUp("embed", 2))  return;  // would exceed budget
if (!gov.canLoadModel(modelBytes)) return;  // OOM risk

// Query current scaling limits
uint32_t maxWorkers = gov.maxIngestWorkers();
uint32_t maxEmbed   = gov.maxEmbedConcurrency();
```

**Pressure levels** (`ResourcePressureLevel`): `Normal` -> `Warning` -> `Critical` -> `Emergency`. The governor tracks consecutive ticks at each level and progressively reduces scaling headroom.

**`ResourceSnapshot`** captures a point-in-time view of RSS, CPU, model memory, worker counts, and the computed pressure level.

### Profiling (`profiling.h`)

All macros are no-ops when `TRACY_ENABLE` is not defined. Zero runtime cost in release builds.

```cpp
#include <yams/profiling.h>

void processChunk(ByteSpan data) {
    YAMS_ZONE_SCOPED_N("processChunk");     // named zone
    YAMS_PLOT("ChunkSize", data.size());     // numeric telemetry
    // ...
}
```

**Domain-specific macros:**

| Macro | Use in |
|-------|--------|
| `YAMS_STORAGE_ZONE(op)` | Storage engine operations (blue) |
| `YAMS_ONNX_ZONE(op)` | ONNX inference calls (green) |
| `YAMS_EMBEDDING_ZONE_BATCH(size)` | Embedding batch processing |
| `YAMS_VECTOR_SEARCH_ZONE(k, threshold)` | Vector search operations |
| `YAMS_SET_THREAD_NAME(name)` | Thread pool workers |

### Async Patterns

The daemon uses `boost::asio` coroutines (C++20 coroutines with Boost wrappers).

**Coroutine signature:**

```cpp
boost::asio::awaitable<void> tuningLoop();
boost::asio::awaitable<Response> process(const Request& request);
boost::asio::awaitable<bool> co_openDatabase(const fs::path& dbPath, int timeout_ms,
                                              yams::compat::stop_token token);
```

**Launching:**

```cpp
// co_spawn with future (joinable)
tuningFuture_ = boost::asio::co_spawn(strand_, tuningLoop(), boost::asio::use_future);

// co_spawn detached (fire-and-forget)
boost::asio::co_spawn(ioContext_, [&]() -> boost::asio::awaitable<void> {
    co_await someAsyncOp();
}, boost::asio::detached);
```

**Timed loop pattern:**

```cpp
boost::asio::awaitable<void> TuningManager::tuningLoop() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    while (running_) {
        timer.expires_after(std::chrono::seconds(1));
        co_await timer.async_wait(boost::asio::use_awaitable);
        tick_once();
    }
}
```

### Testing Patterns

Framework: **Catch2**. Test files live in `tests/unit/` and are named `*_catch2_test.cpp`.

**RAII guards for global state:**

TuneAdvisor uses `static inline` atomics, so tests must save/restore state to avoid cross-test contamination.

```cpp
// Saves/restores TuneAdvisor::tuningProfile() across scope
class ProfileGuard {
public:
    explicit ProfileGuard(TuneAdvisor::Profile p) : prev_(TuneAdvisor::tuningProfile()) {
        TuneAdvisor::setTuningProfile(p);
    }
    ~ProfileGuard() { TuneAdvisor::setTuningProfile(prev_); }
    ProfileGuard(const ProfileGuard&) = delete;
    ProfileGuard& operator=(const ProfileGuard&) = delete;
private:
    TuneAdvisor::Profile prev_;
};

// Saves/restores an environment variable
class EnvGuard {
public:
    EnvGuard(const char* name, const char* value);
    ~EnvGuard();  // restores or unsets
    // non-copyable
};

// Saves/restores TuneAdvisor::hardwareConcurrency()
class HwGuard { /* similar pattern */ };
```

**Usage:**

```cpp
TEST_CASE("Budget scales with profile", "[tuning]") {
    EnvGuard envClean("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
    HwGuard hw(16);
    ProfileGuard profile(TuneAdvisor::Profile::Balanced);

    auto budget = TuneAdvisor::postIngestBudgetedConcurrency(false);
    REQUIRE(budget.extraction >= 1);
    REQUIRE(budget.total() <= 16);
}
```

**Clearing atomics between tests:** Set overrides to 0 (or use guards) before each `TEST_CASE` to ensure a clean slate. DynamicCap atomics reset to 0 = "no cap".
