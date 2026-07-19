---
description: YAMS-first agent with blackboard coordination and persistent memory
argument-hint: "[TASK=<description>] [PHASE=<start|checkpoint|complete>]"
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

## Required Metadata (Memory Tracking)

Attach metadata to every `yams add`.

- `task` - short task slug (example: `index-speedup`)
- `phase` - `start` | `checkpoint` | `complete`
- `owner` - `opencode` (shared owner for multi-agent retrieval)
- `source` - `code` | `note` | `decision` | `research`

Optional (when applicable):

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
yams search "$TASK" --limit 10

# Exact task-memory lookup
yams list --format json --show-metadata \
  --metadata "owner=opencode" --metadata "task=$TASK" \
  --metadata "source=note" --limit 10

# Search/list results are candidates, not recovered memory. Hydrate the
# relevant note, decision, research, or evidence artifact by its exact hash.
yams cat --hash <hash-from-result>
```

Do not rely on a search snippet as task context. Hydrate the most relevant
saved-memory artifacts before acting. For code hits, use the graph and then a
targeted local read; use `yams get --hash <hash> -o <path>` only when a
filesystem copy is needed.

### 1a) Use the Graph for Codebase Navigation

Recent YAMS graph work makes the graph useful as an agent navigation layer, not only as a low-level database. Use it before broad local filesystem exploration when the task is "how does this subsystem connect?", "what else might break?", or "where should I read next?"

```bash
# Agent-readable context for a symbol, file, or natural-language query
yams graph --explore "<symbol-or-file-or-query>" --max-files 8

# Follow hints emitted by search/grep JSON or table output
yams search "<concept>" --limit 10 --json   # read graph_explore_hint
yams grep "<symbol>" --cwd .                # hints appear beside files

# Raw traversal when you need exact edge data
yams graph --name <path> --depth 1 --limit 50
yams graph --node-key <key> -r <relation> --depth 2
yams graph --relations
yams graph --list-types
yams graph --list-type function --scope-cwd --limit 50

# Topology views for unfamiliar areas
yams graph --topology-snapshots
yams graph --topology-clusters
yams graph --cluster <cluster-id>
```

Graph guidance:

- Prefer `--explore` after `yams search` / `yams grep` identifies a likely file; search/grep often provide the exact `graph_explore_hint` to run.
- Use relation summaries (`calls`, `includes`, `contains`, `defined_in`, `located_in`, `has_version`, `semantic_neighbor`) to pick the next file before local `Read`.
- For blast-radius review, start with `--explore` on changed files, then raw `--name`/`--node-key` traversal with relation filters for precise caller/include context.
- If graph exploration fails or returns stale/noisy context, say so and fall back to local reads or LSP navigation.

### 1b) Engineering Quality Gate

For C++ / systems implementation, do not jump straight from discovery to edits:

- identify the smallest observable behavior and seam to test
- write a failing behavior or characterization test before production changes
- use assertions for invariants only, not recoverable runtime failures
- refactor only while tests stay green
- profile before optimizing: define workload/KPI, baseline, change one thing, re-measure

### 2) Start Work (Check Index + Claim)

```bash
# Check first. Only rebuild a missing/stale repo index; do not attach the
# current task to every source file in an already healthy index.
yams status
yams add . --recursive \
  --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Repository baseline" \
  --metadata "task=repo-index,phase=checkpoint,owner=opencode,source=code"
```

Store the claim in YAMS even when blackboard coordination is available, so
session recovery has one concise task artifact:

```bash
yams add - --name "claim-$TASK.md" \
  --metadata "task=$TASK,phase=start,owner=opencode,source=note,agent_id=opencode-$TASK"
```

Send `TASK`, `SCOPE`, `GOAL`, and `NEXT` directly on stdin. Also claim or
create the task on the blackboard when available.

### 3) Checkpoint (Index What Changed)

```bash
yams add <changed-files> \
  --label "$TASK: checkpoint" \
  --metadata "task=$TASK,phase=checkpoint,owner=opencode,source=code,agent_id=opencode-$TASK"
```

### 4) Complete (Handoff + Close Loop)

Store the completed Response Template as a `source=note`, `phase=complete`
artifact, then index only changed files with `phase=complete` metadata.

## Response Template

```text
TASK: $TASK
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

If the chat context is compacted or lost, rebuild it from YAMS with exact
metadata filters, then hydrate the relevant artifacts. A list result alone is
not recovered context.

```bash
yams list --format json --show-metadata \
  --metadata "owner=opencode" --metadata "task=<task>" \
  --metadata "source=note" --limit 10
yams cat --hash <selected-note-or-decision-hash>
```

Change the `source` filter to `decision`, `research`, or `evidence` when that
artifact type carries the needed context.

## References (Skills)

- `yams` (YAMS memory + search + graph)
- `yams-blackboard` (blackboard coordination on top of YAMS)
