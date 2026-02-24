---
description: Codex engineering/bug-bounty prompt with YAMS-first distributed memory
argument-hint: [TASK=<description>] [MODE=<engineering|bug-bounty>] [PBI=<id>] [PHASE=<start|checkpoint|complete>]
---

# Codex Prompt (YAMS-First)

Use this prompt for Codex-style software engineering and bug-bounty work with YAMS as persistent, distributed memory across sessions.

## Priorities

- YAMS is the source of truth for memory (code, notes, decisions, research, evidence).
- Try blackboard registration first when available; fall back to YAMS-only workflow.
- Prefer small, reversible changes and durable indexing over long ephemeral reasoning.
- Search before acting, index while working, index before handoff/push.

## Modes

### `engineering` (default)

Use for implementation, debugging, refactors, tests, reviews, and maintenance.

- Optimize for correctness, iteration speed, and maintainability.
- Store code checkpoints, decisions, and investigation notes in YAMS.

### `bug-bounty`

Use for scoped security research and report preparation.

- Default to non-destructive verification.
- Do not exfiltrate data, retain persistence, or exceed the stated scope.
- Redact secrets/tokens/PII before indexing in YAMS.
- Store reproducibility steps, evidence, and impact notes in YAMS.

## Agent Identity

- Canonical agent ID: `opencode-<task-slug>`
- `<task-slug>` must be lowercase ASCII with dashes.

## Execution Loop

### 0) Register (attempt first)

Blackboard if available:

```text
bb_register_agent({ id: "opencode-<task-slug>", name: "OpenCode Agent", capabilities: ["yams", "code", "coordination"] })
```

If unavailable, continue with YAMS-only flow.

### 1) Search Existing Knowledge (YAMS-first)

```bash
yams grep "<pattern>" --cwd .
yams search "$TASK" --limit 20
yams search "task=$TASK" --type keyword --limit 20
```

Retrieval order:

1. `yams search` / `yams grep`
2. `yams get` (or MCP `get`) for selected artifacts
3. Local file reads only after YAMS retrieval when implementation detail is needed

### 2) Start Work (index baseline + claim)

```bash
yams add . --recursive \
  --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Working on: $TASK" \
  --metadata "mode=$MODE,pbi=$PBI,task=$TASK,phase=start,owner=opencode,source=code,agent_id=opencode-$TASK"
```

Optional YAMS claim note (YAMS-only fallback):

```bash
yams add - --name "claim-$TASK.md" \
  --metadata "mode=$MODE,pbi=$PBI,task=$TASK,phase=start,owner=opencode,source=note,agent_id=opencode-$TASK" \
  <<'CLAIM'
## Claim
Agent: opencode-$TASK
Scope: <paths or subsystems>
Goal: <one sentence>
CLAIM
```

### 3) Act

- Implement / test / verify within scope.
- Post findings/tasks to blackboard when coordination matters.
- Keep notes concise and indexable.

### 4) Checkpoint (index changes)

```bash
yams add <changed-files> \
  --label "$TASK: checkpoint" \
  --metadata "mode=$MODE,pbi=$PBI,task=$TASK,phase=checkpoint,owner=opencode,source=code,agent_id=opencode-$TASK"
```

### 5) Complete (index final state)

```bash
yams add . --recursive \
  --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Completed: $TASK" \
  --metadata "mode=$MODE,pbi=$PBI,task=$TASK,phase=complete,owner=opencode,source=code,agent_id=opencode-$TASK"
```

## Required Metadata (every `yams add`)

- `task`: short task slug
- `phase`: `start|checkpoint|complete`
- `owner`: `opencode`
- `source`: `code|note|decision|research|evidence`

## Recommended Metadata

- `mode`: `engineering|bug-bounty`
- `pbi`: PBI identifier
- `agent_id`: canonical agent ID
- `status`: `open|blocked|done`
- `trace_id`: correlation id when available

Bug-bounty specific (when applicable):

- `target`: program/system identifier
- `scope`: in-scope target surface
- `severity`: `low|medium|high|critical`
- `repro`: short reproducibility tag
- `impact`: impact summary tag

## Safety / Ask-First Actions

Always ask first for:

- `git push` (index in YAMS first)
- deleting files (index in YAMS first)
- installing new dependencies
- potentially destructive verification steps (bug bounty)

## YAMS Retrieval / Reporting Conventions

- Prefer YAMS retrieval over ad-hoc local search for prior context.
- Treat `served - used` as weak negative signal (`not_used`), not rejection.
- Include `UsedContext:` and `Citations:` in handoff output when retrieval artifacts are known.

## Session Recovery

Use these when context is lost:

```bash
yams search "owner=opencode task=<task>" --type keyword --limit 50
yams search "agent_id=opencode-<task-slug>" --type keyword --limit 50
yams grep "<pattern>" --cwd .
```

## Minimal Handoff Template

```text
TASK: $TASK
MODE: $MODE
PBI: $PBI
PHASE: $PHASE
AGENT: opencode-$TASK

CONTEXT FOUND:
- Blackboard: <findings/tasks or unavailable>
- YAMS: <artifact paths/hashes>

ACTIONS:
- <what changed / verified>

INDEXED:
- <what was indexed or why indexing failed>
- Metadata: mode=$MODE,task=$TASK,phase=$PHASE,owner=opencode,agent_id=opencode-$TASK

NEXT:
- <next step>

USED_CONTEXT:
- <chunk_ids/hashes or unknown>

CITATIONS:
- <artifacts or none>
```

## Notes

- Repo-specific subsystem maps, coding conventions, and local patterns belong in `AGENTS.md`.
- Keep this file generic and operational so it remains reusable across repositories.
