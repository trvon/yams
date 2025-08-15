# App Services Layer (Shared between CLI and MCP)

This module introduces a shared service layer that both the CLI and the MCP server will call to guarantee strict feature parity and reduce code drift. The interfaces are defined as stable contracts and the CLI/MCP will depend on them, not on lower-level repositories directly.

The goal is a single source of truth for application logic:
- One implementation of search/grep/list/restore/update behavior
- Both CLI and MCP call the same code paths
- Easier testing and maintenance

## Directory Layout

- `include/yams/app/services/services.hpp`
  - Public, header-only interfaces and data models (the “contracts”)
  - Shared across all components (CLI, MCP, tests)
- `src/app/services/`
  - Concrete implementations (to be added):
    - `search_service.cpp`
    - `grep_service.cpp`
    - `document_service.cpp`
    - `restore_service.cpp`
  - A small factory to assemble services from an `AppContext` (optional but recommended)

## Interfaces (headers only, to wire later)

The following skeleton interfaces are already defined under `include/yams/app/services/services.hpp`. These will be wired into concrete implementations in `src/app/services/*.cpp`.

### AppContext

Holds shared dependencies used by all services:
- `store` (`IContentStore`)
- `metadataRepo` (`MetadataRepository`)
- `searchExecutor` (`SearchExecutor`)
- `hybridEngine` (`HybridSearchEngine`)

### Search

- `ISearchService::search(const SearchRequest&) -> Result<SearchResponse>`
- Key features to implement:
  - Hash search (full or partial)
  - `type`: `keyword | semantic | hybrid`
  - `fuzzy` + `similarity`
  - `pathsOnly` ergonomics
  - `verbose` score breakdown parity with CLI
  - Line/context/color options for future-friendly formatting parity (noop in service; used by callers to shape output)

### Grep

- `IGrepService::grep(const GrepRequest&) -> Result<GrepResponse>`
- Key features to implement (match CLI):
  - `ignoreCase`, `word`, `invert`
  - `lineNumbers`, `withFilename`
  - `count`, `filesWithMatches`, `filesWithoutMatch`
  - `maxCount`
  - `paths` (optional filters: files/dirs/globs)
  - Context options: `beforeContext`, `afterContext`, `context`

### Document

- `IDocumentService`
  - `store(StoreDocumentRequest) -> Result<StoreDocumentResponse>`
    - Accept either `path` or (`content` + `name`)
    - Apply `tags` and `metadata`
  - `retrieve(RetrieveDocumentRequest) -> Result<RetrieveDocumentResponse>`
    - Optional `outputPath` write
    - `graph`/`depth`/`includeContent`
  - `cat(CatDocumentRequest) -> Result<CatDocumentResponse>`
  - `list(ListDocumentsRequest) -> Result<ListDocumentsResponse>`
    - Filters: `pattern`, `tags`, `type`, `mime`, `extension`, `binary`, `text`
    - Time filters: `created/modified/indexed` (after/before)
    - `recent`, `sortBy`, `sortOrder`
  - `updateMetadata(UpdateMetadataRequest) -> Result<UpdateMetadataResponse>`
    - Accept `pairs` (`key=value`) and/or `keyValues` map
  - `resolveNameToHash(const std::string&) -> Result<std::string>`
  - `deleteByName(DeleteByNameRequest) -> Result<DeleteByNameResponse>`

### Restore

- `IRestoreService`
  - `restoreCollection(RestoreCollectionRequest) -> Result<RestoreResponse>`
  - `restoreSnapshot(RestoreSnapshotRequest) -> Result<RestoreResponse>`
  - Options:
    - `outputDirectory`, `layoutTemplate`
    - `includePatterns`, `excludePatterns`
    - `overwrite`, `createDirs`, `dryRun`

## Planned Concrete Implementations

The following classes will be created to implement the above contracts:

- `SearchServiceImpl : ISearchService`
  - Delegates to `HybridSearchEngine` when available; otherwise falls back to `SearchExecutor`/FTS/fuzzy
  - Returns `pathsOnly` or full results (with verbose score breakdown)
- `GrepServiceImpl : IGrepService`
  - Scans text content, applies regex and context windows
  - Supports all CLI flags and outputs file-level aggregates
- `DocumentServiceImpl : IDocumentService`
  - Wraps `IContentStore` and `MetadataRepository` to provide list/retrieve/store/cat/update
  - Resolves names/patterns and applies filtering/sorting logic consistent with CLI
- `RestoreServiceImpl : IRestoreService`
  - Builds restore targets from collection/snapshot queries
  - Applies include/exclude filters and writes files (or dry-run reports)

Optionally expose a small factory to assemble services using an `AppContext`:

```cpp
// include/yams/app/services/factory.hpp (proposed)
#pragma once
#include <yams/app/services/services.hpp>

namespace yams::app::services {

struct ServiceBundle {
    std::shared_ptr<ISearchService> search;
    std::shared_ptr<IGrepService> grep;
    std::shared_ptr<IDocumentService> document;
    std::shared_ptr<IRestoreService> restore;
};

ServiceBundle makeServices(const AppContext& ctx);

} // namespace yams::app::services
```

## Wiring Plan

1) MCP Server
- Replace direct logic in `MCPServer::*` tools with calls into `ISearchService`, `IGrepService`, `IDocumentService`, and `IRestoreService`.
- Transform service DTOs to the existing MCP response JSON shape to preserve API compatibility.

2) CLI Commands
- Replace duplicated logic in:
  - `search_command.cpp` → `ISearchService`
  - `grep_command.cpp` → `IGrepService`
  - `list_command.cpp`, `cat_command.cpp`, `get_command.cpp`, `delete_command.cpp`, `update_command.cpp` → `IDocumentService`
  - `restore_command.cpp` → `IRestoreService`
- Keep command-line parsing and presentation in CLI, but delegate all core operations to services.

3) Tests
- Write unit tests for each service (no CLI/MCP dependency).
- Update MCP tests to expect identical behavior as CLI since both call the same services.
- Keep small smoke tests in MCP that validate JSON wiring and schema.

## Minimal TODOs (next PRs)

- [ ] Add `search_service.cpp` with keyword/semantic/hybrid behavior and `pathsOnly` mode
- [ ] Add `grep_service.cpp` with full CLI flag parity and file aggregation
- [ ] Add `document_service.cpp` with list filters/sorts, store/retrieve/cat, metadata update
- [ ] Add `restore_service.cpp` with include/exclude pattern support and dry-run/overwrite handling
- [ ] Add `factory.hpp/cpp` to construct the `ServiceBundle` from `AppContext`
- [ ] Wire MCP server handlers to services
- [ ] Migrate CLI commands to services
- [ ] Add service-level tests; stabilize MCP/CLI integration tests

## Design Notes

- The service layer uses simple DTO structs for inputs/outputs to keep JSON and CLI formatting out of core logic.
- Services do not print or emit color/highlighting; that belongs to CLI/MCP presentation layers.
- All time-related filters accept strings (ISO 8601 or relative); parsing happens within the service to match CLI semantics.
- Error handling uses the project’s `Result<T>` type for consistency with existing subsystems.

---

Once these implementations are added, CLI and MCP will be guaranteed to behave identically, reducing duplicate logic and drift while making future enhancements easier and safer.