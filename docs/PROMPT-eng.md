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
