# Monolith Refactor Program (Codex + YAMS Trackable)

## Scope

- `src/metadata/metadata_repository.cpp`
- `src/mcp/mcp_server.cpp`

Goals:

- Reduce compile blast radius and merge conflict pressure.
- Improve reviewability by extracting high-cohesion subsystems.
- Strengthen test infrastructure so extractions are behavior-preserving and easier to validate.
- Make progress trackable across sessions in YAMS with small, indexed checkpoints.

## Operating Model

- Work in small slices with behavior-preserving extractions first.
- Each slice must have:
  - explicit acceptance criteria
  - targeted tests
  - YAMS checkpoint indexing
- Separate extraction commits from behavior changes.

## Workstreams

### WS1: `mcp_server.cpp` Prompt Subsystem Extraction

Target:

- Extract built-in prompt registry and `prompts/get` prompt assembly from `MCPServer::handleRequest(...)`.

Proposed files:

- `src/mcp/mcp_prompts.cpp`
- `src/mcp/mcp_prompts.hpp`

Move:

- Built-in prompt definitions (`search_codebase`, `summarize_document`, `rag/*`, `session/codex_repo`)
- Built-in prompt message builders for `prompts/get`
- File-backed prompt helpers (`PROMPT-*.md` lookup, filename sanitize, file load)
- `listPrompts()` builtin metadata generation (or delegate from `MCPServer::listPrompts()`)

Keep in `MCPServer` initially:

- JSON-RPC envelope handling
- `createResponse(...)`
- `prompts/get` route wiring (delegates to helper)

Acceptance criteria:

- `prompts/list` output is unchanged except intentional prompt description/name additions.
- `prompts/get` returns the same message schema (`messages`, `role`, `content.type=text`).
- File-backed prompt fallback continues to work.

### WS2: `mcp_server.cpp` Core JSON-RPC Routing Extraction

Target:

- Shrink `MCPServer::handleRequest(...)` by moving method dispatch logic into helpers.

Proposed files:

- `src/mcp/mcp_request_router.cpp`
- `src/mcp/mcp_request_router.hpp`

Move:

- Method routing for trivial/core methods (`ping`, `shutdown`, `resources/*`, `prompts/*`, `logging/setLevel`)
- Reusable helpers for notifications and protocol error shaping

Keep in `MCPServer` initially:

- Transport lifecycle
- async endpoint handlers (`handleSearchDocuments`, `handleListDocuments`, etc.)
- tool invocation path if coupling remains high

Acceptance criteria:

- JSON-RPC responses remain wire-compatible for covered methods.
- Notification semantics (`notifications/*`, `exit`) unchanged.
- Existing MCP tests pass without prompt regressions.

### WS3: `metadata_repository.cpp` Content Batch Ops Extraction

Target:

- Isolate content batch fetch/preview helpers from repository monolith.

Proposed files:

- `src/metadata/repository/content_batch_ops.cpp`
- `src/metadata/repository/content_batch_ops.hpp` (if needed)

Move:

- `batchGetContent(...)`
- `batchGetContentPreview(...)`
- local/shared helpers only used by these paths (`buildInList`, `bindIdList`) if not needed elsewhere

Acceptance criteria:

- `batchGetContentPreview(...)` returns previews for all requested displayed IDs (current behavior target).
- No SQL/schema behavior changes.
- Existing metadata repository tests still pass.

### WS4: `metadata_repository.cpp` Document Query Filter Builder Extraction

Target:

- Centralize document query condition assembly used by list/query paths.

Proposed files:

- `src/metadata/repository/document_query_filters.cpp`
- `src/metadata/repository/document_query_filters.hpp`

Move:

- `BindParam`
- `addTextParam(...)`, `addIntParam(...)`
- `appendDocumentQueryFilters(...)`

Keep in `metadata_repository.cpp` initially:

- `queryDocuments(...)`
- `queryDocumentsForListProjection(...)`

Acceptance criteria:

- Filter parity remains between:
  - `queryDocuments(...)`
  - `queryDocumentsForListProjection(...)`
- No regressions in path filters, metadata filters, repair filters, or FTS contains fallback.

### WS5: `metadata_repository.cpp` Search / FTS / Fuzzy Search Extraction

Target:

- Isolate search-specific SQL and FTS/fuzzy logic from repository core.

Proposed files:

- `src/metadata/repository/search_ops.cpp`
- `src/metadata/repository/search_ops.hpp`

Move:

- `search(...)`
- `fuzzySearch(...)`
- FTS token/query helpers where feasible:
  - `sanitizeFts5UserQuery(...)`
  - token rendering helpers
  - FTS operator helpers

Acceptance criteria:

- Search result schemas unchanged.
- FTS/fuzzy fallback behavior preserved.
- Representative search tests pass (hybrid, fuzzy, failure scenarios).

## Testing Infrastructure Improvements (Cross-Cutting)

These changes should land alongside the refactor program, not after it.

### TI1: Slice-Specific Test Targets / Selection Docs

Goal:

- Reduce test runtime and make each refactor slice verifiable with a small command set.

Deliverables:

- `docs/testing/refactor_test_matrix.md`
- Named test matrix by workstream (WS1-WS5)

Matrix format:

- workstream
- primary targets
- smoke targets
- known expensive targets (optional/nightly)

Example entries:

- WS1/WS2:
  - `mcp_server`
  - `mcp_server_exec`
  - `mcp_protocol_version_features`
  - `mcp_tools_schema_and_smoke`
- WS3/WS4/WS5:
  - `metadata_repository`
  - `metadata_repository_cache`
  - targeted metadata FTS/query tests

### TI2: Behavior-Lock Tests For Prompt Subsystem

Goal:

- Prevent prompt extraction from changing MCP prompt schemas or message shapes.

Add tests for:

- `prompts/list` contains built-ins + file-backed prompts
- `prompts/get` returns message schema with expected `role/content`
- `session/codex_repo` prompt presence and deterministic output shape

Preferred location:

- `tests/unit/mcp/mcp_prompts_catch2_test.cpp` (new)

### TI3: Query Filter Parity Tests (Metadata)

Goal:

- Ensure shared filter builder behaves identically for full docs vs list projections.

Add tests for representative filters:

- exact path / path prefix
- metadata filters (AND/OR)
- tags
- extension/mime/type
- repair filters
- `containsFragment` with FTS on/off fallback

Preferred location:

- `tests/unit/metadata/document_query_filters_parity_catch2_test.cpp` (new)

### TI4: Refactor-Safe Compilation Checks

Goal:

- Catch missing declarations/default args/link issues early in extraction steps.

Recommended commands per slice:

- `meson compile -C build/release yams_mcp`
- `meson compile -C build/release yams_metadata`

If Meson target names differ in local setup, document the equivalent commands in the matrix.

### TI5: Optional Test Infrastructure Follow-Up (If Needed)

Only if pain persists during WS1-WS5:

- Introduce Meson options or wrapper scripts for `mcp-fast` and `metadata-fast` test bundles.
- Add a small CI job for refactor smoke tests (fast gating) separate from full suite.

## Sequencing / Dependencies

Recommended order:

1. WS1 (`mcp` prompts extraction)
2. WS3 (`metadata` content batch ops extraction)
3. WS2 (`mcp` request router extraction)
4. WS4 (`metadata` query filter builder extraction)
5. WS5 (`metadata` search/FTS extraction)

Rationale:

- WS1 and WS3 are the lowest-risk, highest-readability extractions.
- WS4 should follow after the low-risk `metadata` slice to reduce concurrent query logic churn.
- WS5 is highest risk due to FTS/fuzzy behavior and should happen last with tests in place.

## YAMS Tracking Model

Task slug root:

- `monolith-refactor`

Per-workstream task slugs:

- `monolith-refactor-ws1-mcp-prompts`
- `monolith-refactor-ws2-mcp-router`
- `monolith-refactor-ws3-metadata-content-batch`
- `monolith-refactor-ws4-metadata-query-filters`
- `monolith-refactor-ws5-metadata-search-ops`
- `monolith-refactor-test-infra`

Recommended metadata fields on every `yams add`:

- `mode=engineering`
- `task=<workstream-slug>`
- `phase=start|checkpoint|complete`
- `owner=opencode`
- `source=code|note|decision|research`
- `agent_id=opencode-<workstream-slug>`
- `status=open|blocked|done` (recommended)

## YAMS Execution Commands (Templates)

### Program Start (index plan + baseline)

```bash
yams add docs/design/monolith_refactor_program_codex_yams.md \
  --label "Monolith refactor program: baseline" \
  --metadata "mode=engineering,task=monolith-refactor,phase=start,owner=opencode,source=note,agent_id=opencode-monolith-refactor,status=open"
```

### Workstream Start

```bash
yams add - --name "claim-monolith-refactor-ws1-mcp-prompts.md" \
  --metadata "mode=engineering,task=monolith-refactor-ws1-mcp-prompts,phase=start,owner=opencode,source=note,agent_id=opencode-monolith-refactor-ws1-mcp-prompts,status=open" <<'EOF'
## Claim
Scope: src/mcp/mcp_server.cpp prompt subsystem extraction
Targets: src/mcp/mcp_prompts.cpp, src/mcp/mcp_prompts.hpp
Acceptance: prompts/list and prompts/get behavior preserved
EOF
```

### Workstream Checkpoint

```bash
yams add <changed-files> \
  --label "WS1 mcp prompts extraction: checkpoint" \
  --metadata "mode=engineering,task=monolith-refactor-ws1-mcp-prompts,phase=checkpoint,owner=opencode,source=code,agent_id=opencode-monolith-refactor-ws1-mcp-prompts,status=open"
```

### Workstream Complete

```bash
yams add <changed-files> \
  --label "WS1 mcp prompts extraction: complete" \
  --metadata "mode=engineering,task=monolith-refactor-ws1-mcp-prompts,phase=complete,owner=opencode,source=code,agent_id=opencode-monolith-refactor-ws1-mcp-prompts,status=done"
```

### Test Infra Checkpoint (example)

```bash
yams add docs/testing/refactor_test_matrix.md tests/unit/mcp/mcp_prompts_catch2_test.cpp \
  --label "Monolith refactor test infra: checkpoint" \
  --metadata "mode=engineering,task=monolith-refactor-test-infra,phase=checkpoint,owner=opencode,source=code,agent_id=opencode-monolith-refactor-test-infra,status=open"
```

## Milestone Gates

### M1 (after WS1 + WS3)

- Prompt subsystem extracted
- Metadata content batch ops extracted
- Prompt behavior-lock tests added
- Refactor test matrix documented

### M2 (after WS2 + WS4)

- MCP request router extraction merged
- Document query filter builder extracted
- Query filter parity tests added

### M3 (after WS5)

- Search/FTS/fuzzy logic extracted
- Search behavior preserved on targeted suite
- Final YAMS completion notes indexed for all workstreams

## Risks / Controls

- Risk: prompt schema drift during WS1.
  - Control: add prompt message-shape tests before/with extraction.
- Risk: query/filter behavior drift during WS4.
  - Control: parity tests and representative fixtures.
- Risk: FTS/fuzzy regressions during WS5.
  - Control: defer until test infra is in place; extraction-only commit first.

