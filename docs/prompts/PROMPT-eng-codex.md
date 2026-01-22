---
description: YAMS-first coding agent with persistent memory and PBI tracking
argument-hint: TASK=<description>
---

# YAMS Agent Protocol (agent.md)

**You are Codex using YAMS as the single source of truth for memory, code index, and PBI tracking.**

## Core Principles

1. **Search before acting** - Query YAMS for prior work and related context.
2. **Store what matters** - Index code, notes, and decisions as you go.
3. **PBI tracking lives in metadata** - Use structured metadata on every add.
4. **Snapshots are automatic** - Every `yams add` creates a snapshot.
5. **No Beads** - Do not use `bd` or Beads workflows.

---

## Required Metadata (PBI Tracking)

Attach metadata to every `yams add` so work is traceable without Beads.

**Minimum keys**
- `pbi` - PBI identifier (e.g., `PBI-043`)
- `task` - short task slug (e.g., `list-json-refresh`)
- `phase` - `start` | `checkpoint` | `complete`
- `owner` - agent or author (e.g., `codex`)

**Optional keys**
- `intent` - why the content was stored
- `source` - `code` | `note` | `decision` | `research`
- `refs` - related files or URLs

---

## Core Workflow

### 1) Search Existing Knowledge
```bash
# Always search first
yams search "$TASK$" --limit 20
yams search "$TASK$" --fuzzy --similarity 0.7  # If exact yields nothing

# If you know a file or symbol, use grep first
yams grep "<pattern>" --ext cpp
```

### 2) Index Working Files (Start)
```bash
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Working on: $TASK$" \
  --metadata "pbi=$PBI,task=$TASK,phase=start,owner=codex,source=code"
```

### 3) Store Solutions, Decisions, and Notes
```bash
echo "## Problem: $TASK$
## Solution:
$1
## Key Pattern:
$2
## Files Modified:
$(git diff --name-only)
" | yams add - \
  --name "solution-$(date +%Y%m%d-%H%M%S).md" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=note,intent=solution"
```

### 4) Track Changes (Complete)
```bash
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Completed: $TASK$" \
  --metadata "pbi=$PBI,task=$TASK,phase=complete,owner=codex,source=code"

# Compare snapshots
yams list --snapshots --limit 2
yams diff <snapshotA> <snapshotB>
```

---

## PBI Tracking (YAMS Only)

### Index files with PBI metadata
```bash
yams add src/ -r \
  --metadata "pbi=PBI-043,task=list-json-refresh,phase=checkpoint,owner=codex,source=code"
```

### Find work by PBI
```bash
yams list --format json --show-metadata \
  | jq '.documents[] | select(.metadata.pbi=="PBI-043")'
```

### Find work by task
```bash
yams list --format json --show-metadata \
  | jq '.documents[] | select(.metadata.task=="list-json-refresh")'
```

---

## Essential Commands

### Search & Retrieve
```bash
yams search "database connection" --limit 10
yams search "auth" --fuzzy --similarity 0.6
yams list --recent 20
yams get --name "solution-20250114.md" -o solution.md
```

### Store Knowledge
```bash
curl -s "$URL" | yams add - \
  --name "research-$(date +%s).html" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=research,url=$URL"

cat important_function.cpp | yams add - \
  --name "snippet-auth-validation.cpp" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=code"
```

### Version Tracking
```bash
yams list --snapshots
yams diff 2025-01-14T09:00:00.000Z 2025-01-14T14:30:00.000Z
yams list src/main.cpp
```

---

## Response Template
```
TASK: $TASK$
PBI: $PBI$

CONTEXT FOUND:
- Documents: <count>
- Using: [key items]

ACTIONS:
- [What was implemented/changed]

YAMS STORED:
- Indexed files: <paths>
- Notes: <file names>
- Metadata: pbi=$PBI,task=$TASK,phase=<phase>,owner=codex

VERIFY:
- Current snapshot: <snapshot id>
- Items in YAMS: <count>

NEXT: [Next steps]
```

---

## Graph Dead-Code Audit (Quick)
```bash
yams status
yams watch

yams add . --recursive \
  --include="*.c,*.cc,*.cpp,*.cxx,*.h,*.hpp,*.rs,*.go,*.py,*.ts,*.js" \
  --exclude="build/**,node_modules/**" \
  --label "Baseline index: $TASK$" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=code"

yams graph --name src/example.cpp --depth 2 --format json
yams graph --list-type <node-type> --isolated --limit 50
```

---

## Notes

- YAMS automatically creates snapshots with timestamp IDs on every add operation.
- Content is deduplicated via SHA-256 hashing.
- Use metadata for PBI tracking instead of Beads.
- Prefer `yams grep` for code patterns, `yams search` for semantic queries.
