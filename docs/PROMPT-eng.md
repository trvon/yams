Developer: # Integrated Knowledge Management Policy for AI Coding Agents & Human Contributors: YAMS-Driven Workflows

This document establishes a unified, machine-readable, and authoritative policy for both AI agents and human collaborators in project development. Governance is purely workflow- and rule-based, leveraging YAMS (Yet Another Memory System) as the persistent system of record for all development knowledge, artifacts, and traceability.

---

## Quick Usage Guidance (For AI Agents)
- Before starting any multi-step process, begin with a concise checklist (3-7 bullets) of intended steps.
- After each YAMS command, validate the outcome in 1-2 lines and proceed or self-correct if validation fails.
- Use only explicitly permitted tools; default output to plain text unless markdown is specifically requested.
- Pre-watch default: update and search code via YAMS. After editing files, re-index with YAMS (no external grep/find/rg).
- All codebase search must use YAMS (search/grep subcommands). Do not use system utilities like grep/find/rg for project queries.
 - Use `yams session` to manage an interactive session: pin frequently used paths/tags/metadata and warm them for faster retrieval.
 - Do not run `yams init`; just use `yams add` to index files. The daemon/storage auto-initializes as needed.
 - Prefer a single compound `yams search` query (multiple terms and/or quoted phrases) over multiple sequential searches; avoid `yams grep` unless explicitly available.

---

## 1. Introduction
### 1.1 Key Roles
- **User:** Specifies requirements, sets priorities, approves changes, and maintains responsibility for all code modifications.
- **AI_Agent:** Completes User-assigned tasks and PBIs, making exclusive use of YAMS for all persistent knowledge operations.

### 1.2 Knowledge Management with YAMS
#### Overview:
- YAMS is a Bash CLI utility—a knowledge versioning and discovery tool with built-in full-text, fuzzy, and semantic search, plus content-addressed storage and fast retrieval.
- Features: content-addressed storage, fuzzy full-text search, tagging, metadata, and fast retrieval.
- YAMS operates natively in the shell; all interaction should be via direct CLI commands.

#### Required YAMS Workflows
- **Prior to any web/external search (use one compound query):**
  ```bash
  # Prefer a single compound query (multiple terms/phrases)
  yams search "term1 term2 \"important phrase\"" --limit 50
  # Optionally broaden with fuzzy matching in the same request
  # yams search "term1 term2 \"important phrase\"" --fuzzy --similarity 0.7 --limit 50
  # Only search the web if no relevant results in YAMS
  ```
- **After every web/external result:**
  ```bash
  echo "$WEB_SEARCH_RESULT" | yams add - --name "topic-$(date +%Y%m%d)" --tags "web,cache,topic" --metadata "url=$SOURCE_URL"
  ```
- **Replacing generic file commands (always prefer YAMS):**
  ```bash
  yams list --recent 20
  yams add myfile.txt --tags "code,working"
  yams get <hash>
  yams rm -rf ./build
  yams rm '*.log'
  yams delete fileA fileB fileC
  yams delete -r ./dist
  ```
- **Pre-Watch Code Update Workflow (until folder track/watch is available):**
  ```bash
  # After editing code, re-index changed files or directories
  yams add src/ --recursive --include="*.cpp,*.hpp,*.h" --tags "code,source"
  yams add include/ --recursive --include="*.hpp,*.h" --tags "code,headers"

  # Add a single updated file
  yams add path/to/file.cpp --tags "code,source"

  # Update metadata for an existing document (when needed)
  yams update --name path/to/file.cpp --metadata "updated=$(date -Iseconds)"
  ```

#### Command Reference - Best Practices
- Always search YAMS first before using external resources.
- All new information should be immediately added/tagged into YAMS.
- Retrievals in workflows/documents reference content by YAMS hash.
- All codebase search must be performed with YAMS:
  - Use `yams search` for keyword/fuzzy/semantic queries.
  - Use `yams grep` for regex across indexed content.
  - Do not use system grep/find/rg for repository search.

- **Codebase Indexing (initial import and after edits):**
  ```bash
  # Index entire codebase with proper file type detection
  yams add src/ --recursive --include="*.cpp,*.h" --tags="code,source"
  yams add include/ --recursive --include="*.h,*.hpp" --tags="code,headers"

  # Re-index after local edits (pre-watch)
  yams add . --recursive --include="*.cpp,*.hpp,*.h,*.md" --tags "code,working"
  ```
- **LLM-Friendly Search (YAMS only):**
  ```bash
  # Get only file paths for context efficiency
  yams search "SearchCommand" --paths-only
  yams search "#include" --paths-only | head -10
  yams list --recent 10 --paths-only
  yams list --type text --paths-only

  # Use fuzzy search for broader discovery
  yams search "vector database" --fuzzy --similarity 0.7 --paths-only

  # Regex matches across indexed code (prefer YAMS grep over system grep)
  yams grep "class\\s+IndexingPipeline" --include="**/*.hpp,**/*.cpp"
  ```
- **Session + Hot Data (Phase 1):**
   ```bash
   # Pin items by path pattern; adds a `pinned` tag to matching docs and stores a local pin list
   # Notes:
   # - Pins are tracked locally in: $XDG_STATE_HOME/yams/pinned.json (or ~/.local/state/yams/pinned.json)
   # - The repository is also updated: matching docs receive tag "pinned" and optional tags/metadata you provide
   # - When updating metadata, prefer hash-based updates when available to avoid name collisions; otherwise ensure names are unique or filter your pattern to the intended scope.
   # - Planned flags: `yams session warm --limit N --parallel P` to control scope and concurrency for warming large pin sets.
   # - Planned output: `yams session list --json` to print the local pins registry for scripting pipelines.
   # - Quote glob patterns to avoid your shell expanding them before YAMS sees them

   # Basic pin of markdown docs with an extra tag and metadata
   yams session pin --path "docs/**/*.md" --tag notes --meta owner=team

   # Pin code files with multiple --tag and multiple --meta entries
   yams session pin --path "src/**/*.{cpp,hpp,h}" --tag pinned --tag hot --meta purpose=index --meta sprint=Q1

   # List locally pinned patterns/entries (client-side pin registry)
   yams session list

   # Unpin items (removes 'pinned' tag and sets pinned=false metadata on matching docs)
   yams session unpin --path "docs/**/*.md"

   # Warm pinned items:
   # - Hydrates snippets/metadata and exercises extraction paths for faster follow-up queries
   # - Uses the local pin list to find and read matching documents
   yams session warm

   # Examples to prevent accidental shell expansion — always quote patterns:
   yams session pin --path "*.md" --tag quick
   yams session unpin --path "src/**/experimental/*"
   ```
- **Daemon Keepalive (PBI‑008):**
   - Configure keepalive interval via env: `YAMS_KEEPALIVE_MS=500`.
   - Streaming search now finalizes properly on empty results (no hangs).
 - **Hot/Cold Modes (PBI‑008):**
   - List: `YAMS_LIST_MODE=hot_only|cold_only|auto` (paths-only implies hot).
   - Grep: `YAMS_GREP_MODE=hot_only|cold_only|auto` (hot uses extracted text; cold scans CAS bytes).
   - Retrieval (cat/get): `YAMS_RETRIEVAL_MODE=hot_only|cold_only|auto` (hot uses extracted text cache when present).
   - Force-cold per document: set tag `force_cold` or metadata `force_cold=true` to always prefer cold path regardless of global mode.
- **Codebase Restore (from YAMS)**
  ```bash
  # Restore a file/doc by name or hash
  yams get --name src/indexing/indexing_pipeline.cpp -o ./restored/indexing_pipeline.cpp
  # Or restore collections/snapshots (when available)
  yams restore --help
  ```
- **Enhanced File Detection:** YAMS automatically detects code files (C++, Python, JavaScript, Rust, Go, etc.) using magic number patterns.
- **Comma-Separated Patterns:** Use `--include="*.cpp,*.h,*.md"` for multiple file types.

## 2. Fundamental Policies
1. **Knowledge-First Development:** Always check YAMS before any external research; project knowledge is built up incrementally.
2. **Task-Only Changes:** Changes must tie directly to explicit, approved tasks.
3. **1:1 Task–PBI Mapping:** Each task links to an approved PBI.
4. **PRD Conformance:** PBI requirements must align with the PRD if applicable.
5. **User Authority:** Only Users approve changes and bear associated risk.
6. **No Unauthorized Actions:** Only perform work detailed in documented tasks.
7. **File & Index Sync:** Update both task file and index to reflect current status.
8. **Controlled File Creation:** Do not create files outside of agreed directories without User approval.
9. **External Packages:**
   - Search YAMS first.
   - Cache/store all external results in YAMS immediately.
   - Store relevant docs in designated markdown, tagged accordingly.
10. **Granular Tasks:** Define atomic, testable tasks.
11. **DRY Principle:** Maintain single sources of knowledge, with references.
12. **Consistent Constants:** Use named constants for repeated values.
13. **API Documentation:** Persist API/external specs both in-project and in YAMS.

---

## 3. Product Backlog Item (PBI) Management
### 3.1 PBI Documents
- PBIs maintained as documented markdown files, each with structured metadata.
- **Standard Directory:** `docs/pbi/` (create if not exists)
- **Naming:** `pbi-XXX-feature-name.md` where `XXX` is zero-padded (001, 002, etc.)
- **Initial Indexing:**
  ```bash
  # Index all PBIs when starting work
  yams add docs/pbi/ --recursive --include="*.md" --tags "pbi,backlog"
  ```
- **Metadata Example:**
  ```bash
  yams add docs/pbi/pbi-001-enhanced-search.md \
    --tags "pbi,backlog,feature" \
    --metadata "status=draft|active|completed|cancelled" \
    --metadata "priority=high|medium|low" \
    --metadata "sprint=2024-Q1" \
    --metadata "epic=search-improvements" \
    --metadata "created=$(date -Iseconds)"
  ```

### 3.2 PBI Retrieval Patterns
**Critical for finding and working with PBIs:**
```bash
# Find specific PBI by ID (most reliable method)
yams get --name "docs/pbi/pbi-001-universal-content-handlers.md"

# Search PBIs by content keywords (prefer YAMS grep)
yams grep "universal content handler" --include="docs/pbi/*.md"

# List all PBIs in the system
yams list --name "docs/pbi/*.md" --recent 50

# Get PBI by partial name match
yams get --name "*pbi-001*" | head -200

# Search for PBIs by status
for pbi in docs/pbi/pbi-*.md; do
  echo "=== $(basename $pbi) ==="
  cat "$pbi" | grep -A 1 "Status:"
done

# Find active PBIs (prefer YAMS grep)
yams grep "Status: active" --include="docs/pbi/*.md"
```

#### Quick PBI Lookup
```bash
# Get PBI by canonical path (fastest, when you know the file)
yams get --name "docs/pbi/pbi-002-daemon-architecture.md"

# Fuzzy by ID in content
yams grep "PBI ID:\s*002" --include="docs/pbi/*.md"

# Fuzzy by filename
yams search "pbi-002" --paths-only
```

#### Metadata-driven Search (recommended)
Add machine-readable metadata when adding/updating PBIs so you can query by fields:
```bash
# Initial add (example)
yams add docs/pbi/pbi-002-daemon-architecture.md \
  --tags "pbi,backlog,feature" \
  --metadata "pbi=002" \
  --metadata "status=in_progress" \
  --metadata "priority=high" \
  --metadata "sprint=2025-Q1"

# Update status
yams update --name "docs/pbi/pbi-002-daemon-architecture.md" \
  --metadata "status=in_progress" \
  --metadata "updated=$(date -Iseconds)"
```

Querying by metadata:
```bash
# All active PBIs (prefer YAMS grep)
yams grep "status=in_progress" --include="docs/pbi/*.md"

# By sprint (prefer YAMS grep)
yams grep "sprint=2025-Q1" --include="docs/pbi/*.md"
```

### 3.3 PBI Lifecycle & Updates
- **Before new PBI work:** Use YAMS to find similar/related PBIs or decisions.
  ```bash
  # Search for related PBIs before creating new ones
  yams search "content handler OR file type" --fuzzy --similarity 0.7
  ```
- **Traceability:** All findings & research are stored/tagged for provenance.
- **Status Updates:** Any PBI state change must be reflected in both file and YAMS:
  ```bash
  # After updating PBI status in file
  yams update --name "docs/pbi/pbi-001-*.md" \
    --metadata "status=active" \
    --metadata "updated=$(date -Iseconds)"
  ```

### 3.4 PBI Progress Tracking
**Track implementation phases and checklist items:**
```bash
# Check Phase 1 completion status
yams get --name "docs/pbi/pbi-001-*.md" | grep -A 30 "Phase 1"

# Count completed vs pending items
yams get --name "docs/pbi/pbi-001-*.md" | grep -c "\[x\]"  # Completed
yams get --name "docs/pbi/pbi-001-*.md" | grep -c "\[ \]"  # Pending

# Calculate progress percentage (example for 7 total commands)
COMPLETED=$(yams get --name "docs/pbi/pbi-001-*.md" | grep -c "✅")
TOTAL=7
PERCENT=$((COMPLETED * 100 / TOTAL))
echo "Progress: ${COMPLETED}/${TOTAL} (${PERCENT}%)"

# Update PBI after phase completion
yams update --name "docs/pbi/pbi-001-*.md" \
  --metadata "phase1_complete=true" \
  --metadata "phase1_date=$(date -Iseconds)" \
  --metadata "progress=${PERCENT}"

# Track acceptance criteria progress
yams get --name "docs/pbi/pbi-001-*.md" | sed -n '/## Acceptance Criteria/,/## /p'

# Find specific phase status
yams get --name "docs/pbi/pbi-001-*.md" | grep -E "Phase [0-9]:|✅|⏳|❌"
```

#### Single-PBI Progress Snapshot
```bash
PBI="docs/pbi/pbi-002-daemon-architecture.md"
echo "Completed: $(yams get --name "$PBI" | grep -c "\[x\]")"
echo "Pending:   $(yams get --name "$PBI" | grep -c "\[ \]")"

# Extract Implementation Phases
yams get --name "$PBI" | sed -n '/## Implementation Phases/,/## /p'

# Extract Acceptance Criteria
yams get --name "$PBI" | sed -n '/## Acceptance Criteria/,/## /p'
```

### 3.5 PBI Status Reporting
**Generate status summaries and reports:**
```bash
# Quick PBI status summary
for pbi in docs/pbi/pbi-*.md; do
  if [ -f "$pbi" ]; then
    echo "=== $(basename $pbi) ==="
    grep -E "^- \*\*Status\*\*:|^- \*\*Priority\*\*:|^- \*\*Sprint\*\*:" "$pbi"
    echo "Checklist Progress:"
    echo "  Completed: $(grep -c "\[x\]" "$pbi")"
    echo "  Pending: $(grep -c "\[ \]" "$pbi")"
    echo ""
  fi
done

# Export PBI for review
yams get --name "docs/pbi/pbi-001-*.md" > /tmp/pbi-review.md

# Find PBIs by sprint
yams search "sprint=2024-Q1" --paths-only
```

### 3.6 PBI-Task Integration
- Track PBI progress through associated tasks.
- Link tasks to PBIs using metadata:
  ```bash
  # Create task linked to PBI
  yams add task-001-implement-handler.md \
    --tags "task,pbi-001" \
    --metadata "pbi=001" \
    --metadata "status=in_progress"

  # Find all tasks for a PBI
  yams search "pbi=001" --paths-only | grep "task-"
  ```
- Example: count tasks by status for PBI readiness:
  ```bash
  # Check if all tasks for PBI-001 are complete
  yams search "pbi=001" --paths-only | while read task; do
    yams get --name "$task" | grep "status="
  done
  ```

---

## 4. Task Management
### 4.1 Documentation Standards
- Each task gets a markdown file with standardized metadata for tracking.
- All supporting material (research, context, implementation) is stored in YAMS, appropriately tagged.
- All task transitions/events are synchronized between task files, project index, and YAMS.
- **One In-Progress Rule:** Only one active task per PBI; confirm using YAMS prior to starting.

### 4.2 YAMS-Based Patterns
- **Task naming:** `task-XXX-short-description.md` (`XXX` is zero-padded)
- **Required metadata:** `status`, `pbi`, `priority`, `assignee`, `created`, `updated`
- **Optional:** `due_date`, `blocked_by`, `estimated_hours`, `actual_hours`, `tags`
- **Create/Update/Search Tasks:** Use the corresponding YAMS commands as per best practices examples above.

#### Task Lifecycle States & Transitions
| State         | Description                        |
|---------------|------------------------------------|
| pending       | Created, not started               |
| in_progress   | Active; only one per PBI allowed   |
| completed     | Finished                           |
| blocked       | Cannot proceed (dependency)        |
| cancelled     | Abandoned                          |

- Explicit allowed transitions are enforced and tracked in YAMS.

---

## 5. Test Strategy
- All test strategies and results are researched using YAMS first, and new insights archived in YAMS for repeatability.

---

## 6. YAMS Command Quick Reference
- Search with YAMS before any external actions.
- Capture and store new information in YAMS immediately.
- Reference all YAMS hashes and artifacts in documentation.

---

## 7. Workflow Overview
### 7.1 Workflow Principles
- START: Search YAMS for knowledge & precedents.
- RESEARCH: Use external sources only as a last resort.
- CACHE: Store all discoveries in YAMS.
- DOCUMENT: Reference by YAMS hashes/artifacts.
- PERSIST: Record every decision and change in YAMS for full traceability.

### 7.2 Task Daily Workflow Quick Reference
- Check current and high-priority tasks.
- Start, work, document, and complete via YAMS commands.
- Update and validate all changes and references in YAMS.
