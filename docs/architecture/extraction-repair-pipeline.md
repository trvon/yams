# YAMS Content Extraction and Repair Pipeline

**Date:** 2026-06-27
**Purpose:** Describe the current extraction, indexing, and repair flow
**Status:** Current implementation map

---

## Executive Summary

YAMS no longer has one monolithic “extraction pipeline.” Current behavior is split across four owners:

1. **`DocumentService`** — persists bytes, core metadata, path/version state, and lightweight sync work
2. **`IngestService` / `IndexingService`** — decide how ingest enters the daemon and how directory batches are expanded
3. **`PostIngestQueue`** — main extraction + content-index + async enrichment path for normal daemon ingest
4. **`RepairService`** — bounded rebuild/repair path for missing MIME, FTS5 content, graph state, and embeddings

Historical note: older docs and notes may refer to **`RepairCoordinator`**. Current code uses **`RepairService`** instead.

---

## 1. Shared Extractor Registry

`ServiceManager` owns the live extractor registry and passes it into both service-layer and daemon post-ingest code.

Current sources of truth:

- `src/daemon/components/ServiceManager.cpp`
  - `seedBuiltinContentExtractors()`
  - plugin sync into `contentExtractors_`
  - `getAppContext()` → `ctx.contentExtractors = contentExtractors_`
- `src/daemon/components/PostIngestQueue.cpp`
  - receives the same extractor list for normal ingest

Current shape:

- YAMS always seeds the built-in text extractor as a baseline
- plugin content extractors are appended when loaded
- both `DocumentService`/`IndexingService` and `PostIngestQueue` see the same registry through `AppContext`

---

## 2. Normal Single-Document Ingest

Primary daemon path:

```text
StoreDocumentRequest
  -> IngestService::processTask()
  -> DocumentService::store()
  -> PostIngestQueue
  -> extraction + content index commit
  -> async title / entity / symbol / KG / embedding follow-up
```

Key current behavior:

- `src/daemon/components/IngestService.cpp`
  - single-document ingest routes to `DocumentService`
  - sets:
    - `skipInlineContentIndexing = true`
    - `combineMetadataPathTree = true`
- `src/app/services/document_service.cpp`
  - persists the document first
  - does not do the heavy extraction path for daemon-queued ingest
- `src/daemon/components/IngestService.cpp`
  - enqueues successful hashes into `PostIngestQueue`

This split is intentional: normal daemon ingest favors **durable store first, heavier extraction second**.

### Direct synchronous text path

`DocumentService::store()` still has a narrow inline path for direct text adds:

- `src/app/services/document_service.cpp`
  - `applyStoreInlineContentIndex()`

That path only runs when:

- `skipInlineContentIndexing == false`, and
- MIME is already text-like

Use case: direct callers that need immediate searchability without waiting for post-ingest.

---

## 3. Directory / Session / Watch Ingest

Primary batch path:

```text
StoreDocumentTask(recursive or directory)
  -> IngestService::processTask()
  -> IndexingService::addDirectory()
  -> per-file DocumentService::store()
  -> per-file PostIngestQueue enqueue
  -> snapshot metadata + best-effort path/blob KG linking
```

Key current behavior:

- `src/app/services/indexing_service.cpp`
  - expands the directory
  - applies include/exclude filters
  - honors `.gitignore` unless `noGitignore` is set
  - precomputes hash / size / mtime before calling `DocumentService`
  - enqueues post-ingest work for each successfully stored file
  - writes `tree_snapshots` metadata after the batch completes
- `src/daemon/components/IngestService.cpp`
  - intentionally does **not** re-enqueue the whole directory result after `addDirectory()` returns
  - this avoids duplicate post-ingest work

Important limitation:

- `IndexingService` still leaves `treeRootHash` empty until fuller TreeBuilder integration lands
- snapshot metadata exists now, but full Merkle-tree population is still not the active path here

---

## 4. Main Extraction + Content Index Path

For normal daemon ingest, `PostIngestQueue` is the real extraction owner.

Primary functions:

- `src/daemon/components/PostIngestQueue.cpp`
  - `prepareMetadataEntry()`
  - `commitBatchResults()`
  - `dispatchSuccesses()`

### 4.1 Extraction

`prepareMetadataEntry()` currently prefers metadata-aware extraction first:

```text
extractDocumentContent()
  -> text + optional bytes + extractor metadata
  -> fallback to extractDocumentText() when needed
```

Current rules:

- plugin-provided metadata can supply fields like title/author
- plugin-provided title suppresses later GLiNER title inference
- extracted text is capped before persistence to avoid oversized SQLite writes
- non-text enrichments can request retained content bytes for later symbol/entity work

### 4.2 Content persistence and search indexing

`commitBatchResults()` batches successful extractions into metadata/index storage:

- writes `BatchContentEntry` rows with `extractionMethod = "post_ingest"`
- uses `contentIndexWriter_` when available
- otherwise falls back to `metadataRepo->batchInsertContentAndIndex(...)`
- marks extraction status / repair status on failures through the write lane

This is the main path that makes normally ingested documents searchable by extracted content.

### 4.3 Async follow-up stages

After successful content commit, `dispatchSuccesses()` can enqueue:

- knowledge-graph work
- symbol extraction
- entity extraction
- title extraction
- embedding preparation / embedding jobs

Important boundary:

- title + NL extraction is intentionally moved to a separate async stage
- this avoids blocking the main extraction path and reduces contention with embedding/model resources

---

## 5. Repair and Rebuild Paths

### 5.1 CLI surface

`src/cli/commands/repair_command.cpp` is now a **thin RPC client**.

It no longer owns a separate local extraction pipeline. It delegates to daemon-side `RepairService`.

### 5.2 Repair owner

Current repair owner:

- `src/daemon/components/RepairService.cpp`

This service now owns the repair workflows that older docs attributed to `RepairCoordinator`.

### 5.3 MIME repair

Current MIME repair is conservative:

- uses `FileTypeDetector`
- prefers prefix reads from the content store instead of materializing full objects
- updates MIME / extraction state without pretending every repair path is a full reindex

### 5.4 FTS5 repair

Current FTS5 rebuild path:

- uses `extractDocumentText(...)`
- writes content rows through `insertContent(...)`
- rebuilds FTS5 entries through `indexDocumentContent(...)`
- updates extraction status explicitly

This is a **repair path**, not a replay of the full post-ingest enrichment pipeline.

### 5.5 Embedding repair

Current embedding repair path spans:

- `src/daemon/components/RepairService.cpp`
- `src/repair/embedding_repair_util.cpp`

Behavior:

- scans metadata for eligible docs missing embeddings
- extracts text when needed
- best-effort persists extracted text/content status if it was previously missing
- generates document/chunk embeddings through the configured model path

---

## 6. Practical Ownership Map

| Concern | Current owner | Notes |
|---|---|---|
| Persist bytes + core metadata | `DocumentService` | fast durable write path |
| Expand directory ingest | `IndexingService` | per-file store + snapshot metadata |
| Normal extraction + content indexing | `PostIngestQueue` | main runtime extraction path |
| Immediate inline text indexing | `DocumentService` | only when caller leaves inline indexing enabled |
| MIME repair | `RepairService` | prefix-read + detector driven |
| FTS5 rebuild | `RepairService` | bounded repair path |
| Embedding backfill | `RepairService` + `embedding_repair_util` | may extract text if missing |
| Shared extractor registry | `ServiceManager` | built-in + plugin extractors |

---

## 7. Intentional Boundaries and Caveats

1. **Normal ingest is two-stage on purpose.**
   - Store first in `DocumentService`
   - Extract/index later in `PostIngestQueue`

2. **Repair does not fully replay post-ingest enrichments.**
   - FTS5 repair restores searchable text/index state
   - title/entity/symbol/KG work remains owned by normal post-ingest or specialized repair code

3. **Directory snapshots are real, but tree-root population is still partial.**
   - snapshot metadata is written today
   - full tree-root/Merkle integration is not yet the live path here

4. **The extractor registry is unified; the entry paths are not.**
   - same extractor list
   - different orchestration depending on ingest vs repair vs direct sync text add

---

## 8. File Map

- `src/app/services/document_service.cpp`
- `src/app/services/indexing_service.cpp`
- `src/daemon/components/IngestService.cpp`
- `src/daemon/components/PostIngestQueue.cpp`
- `src/daemon/components/RepairService.cpp`
- `src/repair/embedding_repair_util.cpp`
- `src/daemon/components/ServiceManager.cpp`
- `src/cli/commands/repair_command.cpp`
