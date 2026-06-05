# Daemon Startup Refactor: Metadata-First Availability and Async Maintenance

## Goal

Make the YAMS daemon searchable as soon as the content store and metadata repository are usable,
while moving expensive recovery, cleanup, indexing, and maintenance work out of the critical startup
path.

This plan addresses a concrete failure mode observed during startup: the daemon reported it was
"still opening database" for many minutes, but live sampling showed the blocking worker inside
`ServiceManager::maybeAutoVacuumDatabase()` executing SQLite `VACUUM` / WAL checkpoint work. Because
that work ran before `finalizeDatabaseStartup()`, recovery sentinels and stale corrupt DB artifacts
were not cleaned up and clients could not use metadata search.

## Current Problem

`ServiceManager` currently mixes several different responsibilities into one sequential startup path:

1. open/create the metadata DB
2. recover stale WAL files
3. run integrity checks and quarantine corrupt DBs
4. run migrations
5. initialize DB pools and the metadata repository
6. quick-check corrupt DBs for salvage
7. optionally run blocking salvage
8. clean corrupt DBs and `.recovered-*` sentinels, but only when salvage is needed
9. run auto-`VACUUM`
10. initialize vector/search subsystems and build the initial search engine

This makes the daemon availability boundary too late. Work that is not required for basic metadata
search can delay the entire daemon.

### Observed failure mode

- Logs repeatedly emitted:
  - `[ServiceManager] still opening database '/Users/trevon/.local/share/yams/yams.db' (...)`
- A live process sample showed the worker in:
  - `ServiceManager::openDatabaseBlocking`
  - `ServiceManager::maybeAutoVacuumDatabase`
  - `metadata::Database::execute("VACUUM")`
  - SQLite WAL checkpoint / write calls
- Recovery sentinel cleanup is only reached later in `finalizeDatabaseStartup()` and only under
  `quickCheckSalvageNeeded(...).needsSalvage == true`.

## Design Principle

Split startup into two lanes:

```text
critical startup lane
  -> content store
  -> metadata DB open/create
  -> migrations
  -> DB pools + MetadataRepository
  -> metadata search available

async maintenance lane
  -> stale artifact cleanup
  -> corrupt DB salvage
  -> orphan repair / metadata rebuild
  -> FTS/search engine rebuild
  -> vector/topology rebuild
  -> VACUUM / compaction
```

The daemon should consider **metadata search availability** a useful degraded-ready state. Full hybrid
search, vector search, topology rebuilds, and maintenance can converge in the background.

## Target Startup Boundary

The synchronous path should only include work needed to safely answer metadata-backed requests:

- initialize content store
- open or create `yams.db`
- perform bounded integrity checks necessary to avoid opening an unsafe DB
- migrate schema
- initialize connection pools
- construct `MetadataRepository`
- set:
  - `databaseReady = true`
  - `metadataRepoReady = true`
  - database phase `ready`
- write bootstrap/status state

After this point, `SearchRequest` can route to metadata search while `searchEngineReady == false`.
This behavior already exists in the dispatcher: if the hybrid search engine is not ready, the search
request is forced to metadata mode.

## Proposed Component Split

### `StartupCoordinator`

Owns the minimal synchronous boot sequence and readiness transitions.

Responsibilities:

- order critical startup phases
- call DB open/migrate helpers
- initialize `DatabaseManager`
- mark metadata availability
- schedule post-startup jobs
- avoid doing maintenance inline

Candidate methods:

```cpp
Result<void> StartupCoordinator::runCriticalStartup();
void StartupCoordinator::publishMetadataReady();
void StartupCoordinator::schedulePostStartupMaintenance();
```

### `DatabaseStartup`

Encapsulates the metadata DB open/migrate path.

Responsibilities:

- stale WAL recovery, if kept on the critical path
- open/create DB
- bounded integrity check
- quarantine/recreate on hard corruption
- migration
- no `VACUUM`
- no salvage
- no sentinel cleanup unrelated to opening the active DB

Candidate methods:

```cpp
Result<void> DatabaseStartup::openActiveDatabase(const fs::path& dbPath);
Result<void> DatabaseStartup::ensureIntegrityOrRecover(const fs::path& dbPath);
Result<void> DatabaseStartup::migrate(metadata::Database& db);
```

### `RecoveryArtifactManager`

Owns cleanup of quarantine artifacts and recovery sentinels.

Responsibilities:

- list `yams.db.corrupt-*`
- list `yams.db.recovered-*`
- remove stale sentinels independently of whether salvage is needed
- remove corrupt DB files only after policy says they are safe to discard
- report errors without blocking daemon startup

Candidate methods:

```cpp
RecoveryArtifactSnapshot RecoveryArtifactManager::scan(const fs::path& dataDir);
CleanupResult RecoveryArtifactManager::cleanupSentinels(const fs::path& dbPath);
CleanupResult RecoveryArtifactManager::cleanupSafeCorruptDbs(const fs::path& dbPath);
```

Important behavior change:

- `.recovered-*` cleanup must not be gated by `quickCheckSalvageNeeded().needsSalvage`.

### `DatabaseMaintenanceScheduler`

Schedules non-critical DB work after metadata availability.

Responsibilities:

- run salvage if corrupt DBs contain more documents than the active DB
- run orphan repair / CAS metadata rebuild where applicable
- run `VACUUM` only as background maintenance
- apply resource-governor/admission checks before heavy work
- expose status as maintenance, not startup

Candidate methods:

```cpp
void DatabaseMaintenanceScheduler::scheduleSalvageIfNeeded(const fs::path& dbPath);
void DatabaseMaintenanceScheduler::scheduleVacuumIfUseful(const fs::path& dbPath);
void DatabaseMaintenanceScheduler::scheduleArtifactCleanup(const fs::path& dbPath);
```

### `SearchBootstrapScheduler`

Makes search progressively better after metadata search is already available.

Responsibilities:

- publish metadata-only search availability immediately after `MetadataRepository` is ready
- schedule initial FTS/hybrid engine build asynchronously
- schedule vector/topology rebuilds after provider/vector readiness
- keep dispatcher fallback behavior explicit and tested

Candidate methods:

```cpp
void SearchBootstrapScheduler::markMetadataSearchAvailable();
void SearchBootstrapScheduler::scheduleInitialSearchBuild();
void SearchBootstrapScheduler::scheduleVectorTopologyBuildIfReady();
```

## Startup State Model

Avoid a single boolean notion of ready for all clients. Report readiness by capability:

- `content_store_ready`
- `database_ready`
- `metadata_repo_ready`
- `metadata_search_ready`
- `search_engine_ready`
- `vector_index_ready`
- `maintenance_running`
- `maintenance_phase`
- `recovery_artifacts_pending`

Recommended user-facing states:

- `starting`: cannot serve requests yet
- `metadata_ready`: content store + metadata repository usable; metadata search works
- `degraded`: serving metadata search while search/vector/maintenance work continues
- `ready`: full configured capabilities available
- `maintenance`: ready or degraded, with background cleanup/repair running

## Execution Model

### Critical path

```text
ServiceManager::startAll
  -> StartupCoordinator::runCriticalStartup
     -> initializeDataDirAndContentStore
     -> DatabaseStartup::openActiveDatabase
     -> DatabaseStartup::migrate
     -> DatabaseManager::initializePools
     -> publish metadata-ready state
  -> schedulePostStartupMaintenance
```

### Background path

```text
DatabaseMaintenanceScheduler
  -> RecoveryArtifactManager::cleanupSentinels
  -> quickCheckSalvageNeeded
  -> salvageFromAllCorruptDbs, if useful
  -> removeCorruptDbFiles, if safe
  -> scheduleVacuumIfUseful, if idle and enough disk space

SearchBootstrapScheduler
  -> build text/hybrid engine
  -> publish searchEngineReady
  -> schedule vector/topology rebuilds
```

## Concrete Code Changes

### 1. Remove auto-`VACUUM` from `openDatabaseBlocking`

Current critical-path behavior should change from:

```cpp
state_.readiness.databaseReady = true;
maybeAutoVacuumDatabase(dbPath);
setDatabasePhase(dbphase::kReady);
```

to:

```cpp
state_.readiness.databaseReady = true;
setDatabasePhase(dbphase::kReady);
```

Then schedule `maybeAutoVacuumDatabase` through a background maintenance component.

### 2. Decouple sentinel cleanup from salvage-needed

Current cleanup lives inside:

```cpp
if (qc.needsSalvage) {
    ...
    removeCorruptDbFiles(...);
    remove yams.db.recovered-*;
}
```

Move it into an independent helper and call it during async maintenance regardless of salvage result.

### 3. Convert blocking salvage into async repair

If the active DB is open and the repository is usable, startup should not block on corrupt DB salvage.
Instead:

- mark metadata search available
- schedule salvage
- surface maintenance status
- rebuild derived indexes after salvage completes

### 4. Defer initial search engine build

Keep metadata search available even when `searchEngineReady == false`.
Schedule full search engine build after metadata-ready publication.

### 5. Improve status/log names

The watchdog currently says "still opening database" even when the worker is in `VACUUM` or other
post-open work. Split phases so logs say what is actually happening:

- `opening database`
- `checking database integrity`
- `migrating database`
- `initializing metadata repository`
- `running database maintenance: vacuum`
- `running database maintenance: salvage`
- `building search engine`

## Tests

Add focused tests before large refactors.

### Unit tests

- `RecoveryArtifactManager` removes `.recovered-*` sentinels even when salvage is not needed.
- `RecoveryArtifactManager` does not delete corrupt DBs unless cleanup policy allows it.
- `DatabaseStartup::openActiveDatabase` does not call `VACUUM`.
- `DatabaseMaintenanceScheduler` schedules `VACUUM` only after metadata readiness.

### Integration tests

- A daemon with a large DB crosses `metadataRepoReady` before any `VACUUM` starts.
- `SearchRequest` succeeds in metadata mode while `searchEngineReady == false`.
- Corrupt DB salvage runs after the daemon can answer status/search requests.
- Recovery sentinels are eventually cleaned after startup, even when corrupt DB document counts are
  less than or equal to active DB document counts.

### Manual checks

- Start daemon with stale `yams.db.recovered-*` files and no useful corrupt DBs; verify cleanup logs.
- Start daemon with a large DB eligible for `VACUUM`; verify `yams search --type keyword ...` works
  before maintenance completes.
- Sample the daemon during startup and confirm no critical worker is blocked in SQLite `VACUUM`.

## Risks and Mitigations

### Risk: serving from an incomplete DB after quarantine

Mitigation:

- clearly report degraded/maintenance status
- schedule orphan repair and salvage immediately
- rebuild derived indexes after salvage/repair

### Risk: background maintenance competes with foreground search

Mitigation:

- route through `ResourceGovernor`
- use low-priority executor or maintenance lane
- avoid `VACUUM` unless idle and disk space is sufficient

### Risk: corrupt DB cleanup deletes recoverable data

Mitigation:

- keep removal policy conservative
- separate sentinel cleanup from corrupt DB deletion
- only delete corrupt DBs after successful salvage or when quick-check proves active DB has equal or
  greater document coverage

### Risk: status semantics become confusing

Mitigation:

- expose capability-specific readiness
- reserve `ready` for full configured capabilities
- use `metadata_ready` / `degraded` for useful partial availability

## Migration Plan

1. Add tests for current failure modes.
2. Move `VACUUM` out of `openDatabaseBlocking` into a background scheduler.
3. Extract sentinel cleanup helper and run it asynchronously regardless of salvage-needed.
4. Extract DB open/migrate logic into `DatabaseStartup` without behavior changes.
5. Extract salvage/cleanup into `DatabaseMaintenanceScheduler`.
6. Defer initial search engine build behind metadata-ready publication.
7. Split status/log phases and update CLI/status rendering if needed.
8. Remove obsolete inline startup branches from `ServiceManager`.

## Non-Goals

- Do not delete corrupt DBs aggressively without salvage/coverage checks.
- Do not make vector search a startup requirement.
- Do not add new env knobs for this behavior; use typed config if tuning is needed.
- Do not block metadata search on FTS/vector/topology rebuilds.

## Expected Outcome

- Faster daemon availability for metadata and keyword search.
- Recovery sentinels are cleaned eventually and independently of salvage-needed logic.
- Large DB `VACUUM` cannot make the daemon look stuck in startup.
- `ServiceManager` becomes a coordinator rather than a monolithic startup/repair/search maintenance
  implementation.
