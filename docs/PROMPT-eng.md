Developer: # Integrated Knowledge Management Policy for AI Coding Agents & Human Contributors: YAMS-Driven Workflows

This document establishes a unified, machine-readable, and authoritative policy for both AI agents and human collaborators in project development. Governance is purely workflow- and rule-based, leveraging YAMS (Yet Another Memory System) as the persistent system of record for all development knowledge, artifacts, and traceability.

---

## Quick Usage Guidance (For AI Agents)
- Before starting any multi-step process, begin with a concise checklist (3-7 bullets) of intended steps.
- After each YAMS command, validate the outcome in 1-2 lines and proceed or self-correct if validation fails.
- Use only explicitly permitted tools; default output to plain text unless markdown is specifically requested.
- Pre-watch default: update and search code via YAMS. After editing files, re-index with YAMS (no external grep/find/rg).
- All codebase search must use YAMS (search/grep subcommands). Do not use system utilities like grep/find/rg for project queries.

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
- **Prior to any web/external search:**
  ```bash
  yams search "<query>" --limit 20
  yams search "<query>" --fuzzy --similarity 0.7
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

  # Use fuzzy search for broader discovery
  yams search "vector database" --fuzzy --similarity 0.7 --paths-only

  # Regex matches across indexed code (prefer YAMS grep over system grep)
  yams grep "class\\s+IndexingPipeline" --include="**/*.hpp,**/*.cpp"
  ```
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
- **Naming:** `pbi-XXX-feature-name.md` where `XXX` is zero-padded.
- **Metadata Example:**
  ```bash
  yams add pbi-001-enhanced-search.md \
    --tags "pbi,backlog,feature" \
    --metadata "status=draft|active|completed|cancelled" \
    --metadata "priority=high|medium|low" \
    --metadata "sprint=2024-Q1" \
    --metadata "epic=search-improvements" \
    --metadata "created=$(date -Iseconds)"
  ```

### 3.2 PBI Lifecycle
- **Before new PBI work:** Use YAMS to find similar/related PBIs or decisions.
- **Traceability:** All findings & research are stored/tagged for provenance.
- **Updates:** Any PBI state change must be mirrored into YAMS.

### 3.3 PBI-Task Integration
- Track PBI progress through associated tasks.
- Example: count tasks by status, and check readiness for completion through scripted YAMS queries.

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

### 7.3 Key Advantages
1. Pure YAMS-based task & knowledge management: no extra tooling needed.
2. Full, auditable traceability of all project actions.
3. Flexible and performant task/research queries.
4. Integrated project knowledge, tasks, and code.
5. Easy scripting and automation potential.
6. Portable, shareable artifacts via content hash.
7. High performance via local storage.
8. Unix philosophy compatibility—one powerful tool for knowledge.

This policy ensures every collaboration, decision, and artifact is structured, queryable, persistent, and reusable for all team members. YAMS metadata conventions make task, research, and code management efficient and transparent.
