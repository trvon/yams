# YAMS‑First Agent Protocol for AI Coding Agents & Human Contributors

**Version:** 2025‑09‑10

### 0) Scope & Tooling

* **Authoritative knowledge system:** **YAMS** (Yet Another Memory System).
* **Allowed tools:** YAMS CLI only for search/index/retrieval/grep/list/rm/session.
  External web search allowed **only after** a YAMS search shows no relevant results, and results **must** be cached back into YAMS.
* **Default output:** plain text. Use Markdown only when explicitly requested by the User.

---

## 1) Roles

* **User** — sets priorities, approves changes, bears risk.
* **AI\_Agent** — executes User‑approved tasks/PBIs, using **only YAMS** for persistent knowledge operations.

---

## 2) Agent Operating Loop (Mandatory)

Before any multi‑step task:

1. **PLAN**: produce a concise 3–7 bullet checklist of intended steps.
2. **SEARCH (YAMS‑first)**: run one compound `yams search` (keywords + quoted phrases). Only if zero relevant hits → do web search and immediately **yams add** the findings.
3. **EXECUTE**: after **each** YAMS command or code change:

   * `VALIDATE:` 1–2 lines (PASS/FAIL + reason).
   * If FAIL → self‑correct or roll back; re‑validate.
4. **INDEX SYNC**: after editing files, re‑index with `yams add …` (no system grep/find/rg).
5. **TRACE**: record decisions, references, and hashes in the PBI docs and/or YAMS metadata.
6. **SUMMARY**: end with a brief status, next steps, and YAMS artifacts (names/hashes).

**Response format template (use literally):**

```
PLAN:
- ...

CMD:
<the exact YAMS or other allowed command>

VALIDATE:
PASS|FAIL — <1–2 lines why> → Next: <action>

SUMMARY:
- Status: <pending|in_progress|completed|blocked>
- Artifacts: <paths|names|hashes>
- Next: <1–2 bullets>
```

---

## 3) YAMS Usage Contract

### 3.1 Search & Retrieval (Always first)

* Prefer **one** compound query (multiple terms and quoted phrases):

```bash
yams search "term1 term2 \"important phrase\"" --limit 50
# Optional broader match in same call:
# yams search "term1 term2 \"important phrase\"" --fuzzy --similarity 0.7 --limit 50
```

* Queries that **start with '-'** (look like options) — quote or use `--`:

```bash
yams search -q "--start-group --whole-archive --end-group -force_load Darwin" --paths-only
yams search -- "--start-group --whole-archive --end-group -force_load Darwin" --paths-only
printf '%s\n' "--start-group ..." | yams search --stdin --paths-only
yams search --query-file /tmp/query.txt --paths-only
```

* Regex across **indexed** content:
  `yams grep "class\\s+IndexingPipeline" --include="**/*.hpp,**/*.cpp"`

Note on session scoping:
- Grep and list respect active session include patterns by default. If expected files (e.g., docs) are not returned, add `--no-session` to widen scope or update session pins to include those paths.
  - Example: `yams grep -e "Changelog" --include="**/*.md" --no-session`

### 3.2 Indexing & Updates

* **Do not run** `yams init`; the daemon/storage auto‑initializes.
* Re‑index after **any** edit:

```bash
# Whole repo (common source & docs)
yams add . --recursive --include="*.cpp,*.hpp,*.h,*.py,*.rs,*.go,*.js,*.ts,*.md" --tags "code,working"

# Targeted adds (faster when iterating)
yams add src/ include/ --recursive --include="*.cpp,*.hpp,*.h" --tags "code,source,headers"

# Single file
yams add path/to/file.cpp --tags "code,source"

# Update metadata (e.g., touched timestamp)
yams update --name path/to/file.cpp --metadata "updated=$(date -Iseconds)"
```

Direct file reads (context):
- It is acceptable to read files explicitly provided in the task context without going through YAMS (e.g., `sed -n '1,200p' CHANGELOG.md`). YAMS remains the system of record for persistence, search, and indexing.

### 3.3 Session & Hot Data

```bash
# Pin & warm frequently used paths
yams session pin --path "src/**/*.{cpp,hpp,h}" --tag pinned --tag hot --meta sprint=Q1
yams session warm
yams session list
yams session unpin --path "src/**/experimental/*"
```

### 3.4 Modes & Keepalive

* `YAMS_KEEPALIVE_MS=500`
* Hot/Cold:

  * List: `YAMS_LIST_MODE=hot_only|cold_only|auto` (paths‑only ⇒ hot)
  * Grep: `YAMS_GREP_MODE=hot_only|cold_only|auto`
  * Retrieval: `YAMS_RETRIEVAL_MODE=hot_only|cold_only|auto`
  * Force cold per doc: tag `force_cold` or meta `force_cold=true`

### 3.5 Replace Generic File Ops

```bash
yams list --recent 20
yams add myfile.txt --tags "code,working"
yams get <hash>
yams rm -rf ./build
yams rm '*.log'
yams delete fileA fileB
yams delete -r ./dist
```

Patterns beginning with `-`:
- When a pattern starts with `-`, either terminate options with `--` or use the explicit pattern flag:
  - `yams grep -- "--tags|knowledge graph|kg" --include="docs/**/*.md"`
  - `yams grep -e "--tags|knowledge graph|kg" --include="docs/**/*.md"`

Knowledge Graph tagging (recommended conventions):
- Use `--tags` to scope documents for graph extraction and traversal; keep tags short and composable.
  - Core: `kg` (participates in the knowledge graph), plus a node type tag: `kg:node:doc`, `kg:node:code`, `kg:node:test`, `kg:node:api`.
  - Domain/topic: `topic:<slug>` (e.g., `topic:prompt-eng`, `topic:release-notes`).
  - System/component: `system:<name>`, `component:<name>`.
  - Lifecycle: `status:draft|ready|archived`, `release:v0.6.x`.
- Examples:
  - `yams add docs/PROMPT-eng.md --tags "docs,kg,kg:node:doc,topic:prompt-eng,system:yams"`
  - `yams add src/cli/commands/graph_command.cpp --tags "code,kg,kg:node:code,component:cli,topic:graph"`
  - `yams add docs/changelogs/v0.6.md --tags "docs,kg,kg:node:doc,topic:release-notes,release:v0.6.x"`

Notes:
- Prefer `--metadata` for structured fields and stable identifiers (e.g., `--metadata "kg.id=doc:prompt-protocol"`). Use tags for filtering and graph scoping.

### 3.6 External/Web Research (last resort)

If and only if YAMS search yields nothing relevant:

```bash
# After retrieving external content:
echo "$WEB_SEARCH_RESULT" | yams add - --name "topic-$(date +%Y%m%d)" --tags "web,cache,topic" --metadata "url=$SOURCE_URL"
```

---

## 4) Product Backlog Item (PBI) Management

### 4.1 Canonical Structure (resolved)

* **Per‑PBI directory:** `docs/delivery/<PBIID>/`

  * Must contain: `prd.md` (goals/spec/acceptance), `tasks.md` (task table with IDs & statuses)
* **Master backlog:** `docs/delivery/backlog.md`

### 4.2 Conventions

* Task IDs: `<PBIID>-<nn>` (e.g., `001-1`). Use `-E2E` for end‑to‑end validations.
* Keep PRDs concise; mirror scope changes immediately.

### 4.3 Lifecycle Flow

1. **Plan** — update `prd.md` (goals & acceptance), list granular tasks in `tasks.md`.
2. **Implement** — keep changes scoped to the PBI; add tests near code (unit/widget/integration).
3. **Validate** — run targeted then broader tests; update `tasks.md` and `backlog.md` status/history.
4. **Document** — update READMEs/migrations; record acceptance status.

### 4.4 Consolidation

* When consolidating into a new PBI:

  * Add **“Consolidated into: \<new‑PBI>”** at top of each original `prd.md`.
  * Append history in `backlog.md` (e.g., `consolidate_to: 002`).
  * Move execution/tasks to the new PBI’s `tasks.md`.

### 4.5 YAMS‑First PBI Ops

```bash
# Index all PBIs
yams add docs/delivery/ --recursive --include="*.md" --tags "pbi,backlog"

# Find a PBI by canonical path
yams get --name "docs/delivery/001/prd.md"

# Search PBIs by content (regex)
yams grep "universal content handler" --include="docs/delivery/*/*.md"

# List recent PBI docs
yams list --name "docs/delivery/*/*.md" --recent 50

# Metadata-driven (recommended)
yams update --name "docs/delivery/002/prd.md" --metadata "pbi=002" --metadata "status=in_progress" --metadata "priority=high" --metadata "sprint=2025-Q1"
yams grep "status=in_progress" --include="docs/delivery/*/*.md"
yams grep "sprint=2025-Q1" --include="docs/delivery/*/*.md"
```

### 4.6 Progress Tracking (examples)

```bash
# Count checklist status within a PBI docs set
COMPLETED=$(yams grep "\[x\]" --include="docs/delivery/001/*.md" | wc -l)
PENDING=$(yams grep "\[ \]" --include="docs/delivery/001/*.md" | wc -l)
TOTAL=$((COMPLETED + PENDING))
PERCENT=$((TOTAL>0 ? COMPLETED*100/TOTAL : 0))
echo "Progress: ${COMPLETED}/${TOTAL} (${PERCENT}%)"

# Extract Acceptance Criteria block
yams grep -A 50 -B 2 "## Acceptance Criteria" --include="docs/delivery/001/*.md"
```

---

## 5) Task Management

### 5.1 Standards

* One markdown per task: `task-XXX-short-description.md` (`XXX` zero‑padded).
* Required metadata: `status`, `pbi`, `priority`, `assignee`, `created`, `updated`.
* Optional: `due_date`, `blocked_by`, `estimated_hours`, `actual_hours`, `tags`.

### 5.2 States & Transitions

| State        | Meaning                          |
| ------------ | -------------------------------- |
| pending      | Created, not started             |
| in\_progress | **Only one active task per PBI** |
| completed    | Finished                         |
| blocked      | Waiting on dependency            |
| cancelled    | Abandoned                        |

Transitions must be explicit and tracked in YAMS (`yams update … --metadata "status=…"`) and reflected in `tasks.md`.

### 5.3 PBI–Task Linking

```bash
# Create task linked to a PBI
yams add docs/delivery/001/task-001-implement-handler.md \
  --tags "task,pbi-001" \
  --metadata "pbi=001" \
  --metadata "status=in_progress"

# Find tasks by PBI
yams search "pbi=001" --paths-only
```

---

## 6) Testing Strategy

* Research test strategies via **YAMS first**; archive new insights back into YAMS.
* Prefer deterministic, fast tests. Isolate slow/benchmarks with clear marks.

---

## 7) Governance Rules (Fundamentals)

1. **Knowledge‑First:** YAMS before any external research.
2. **Task‑Only Changes:** All changes map to explicit, approved tasks.
3. **1:1 Task↔PBI:** Each task links to a single PBI.
4. **PRD Conformance:** PBIs align with PRD requirements.
5. **User Authority:** Only the User approves changes.
6. **No Unauthorized Actions:** Work only within documented tasks.
7. **File & Index Sync:** Keep files and YAMS index consistent.
8. **Controlled Creation:** Don’t create files outside agreed dirs without approval.
9. **External Packages:** Search YAMS first; cache/store all external docs in YAMS (tagged).
10. **Granular Tasks:** Atomic, testable units.
11. **DRY:** Single sources with references/hashes.
12. **Constants:** Use named constants for repeated values.
13. **API Docs:** Persist external specs in‑repo and in YAMS.

---

## 8) Safety & Destructive Ops

* Destructive commands (`yams rm`, `yams delete`) require a preceding `PLAN` bullet justifying necessity and a `VALIDATE` line confirming scope/path.

---

## 9) Daily Quick Reference (Agent)

* Check high‑priority PBIs/tasks → `yams list`/`yams grep`.
* **PLAN** → **SEARCH (YAMS)** → **EXECUTE** → **VALIDATE** → **INDEX SYNC** → **SUMMARY**.
* Persist everything in YAMS (hashes, tags, metadata).

---

### Appendix: Handy Snippets

**LLM‑friendly path listing**

```bash
yams search "#include" --paths-only | head -10
yams list --type text --paths-only
```

**Restore from YAMS**

```bash
yams get --name src/indexing/indexing_pipeline.cpp -o ./restored/indexing_pipeline.cpp
# Collections/snapshots when available:
# yams restore --help
```

---
