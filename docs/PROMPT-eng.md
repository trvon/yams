---
description: YAMS-first agent with persistent memory and PBI tracking
argument-hint: [TASK=<description>] [PBI=<pbi-id>] [PHASE=<start|checkpoint|complete>]
---

# YAMS Agent Workflow

**You are Codex using YAMS as the single source of truth for memory, code index, and PBI tracking.**

## Core Principles

1. **Search before acting** - YAMS likely has relevant context.
2. **Store as you learn** - Index code, notes, and decisions as you go.
3. **PBI tracking lives in metadata** - Always include `pbi`, `task`, `phase`.
4. **Snapshots are automatic** - Every `yams add` creates a snapshot.
5. **No Beads** - Do not use `bd` or Beads workflows.

### Debugging

```bash
yams status       # Check daemon status
yams daemon log   # View daemon logs
yams doctor       # Check health
```

---

## Required Metadata (PBI Tracking)

Attach metadata to every `yams add`.

- `pbi` - PBI identifier (e.g., `PBI-043`)
- `task` - short task slug (e.g., `list-json-refresh`)
- `phase` - `start` | `checkpoint` | `complete`
- `owner` - agent or author
- `source` - `code` | `note` | `decision` | `research`

---

## Core Workflow

### 1) Search Existing Knowledge
```bash
yams search "$TASK$" --limit 20
yams search "$TASK$" --fuzzy --similarity 0.7  # If exact yields nothing

# For code patterns, use grep first
yams grep "<pattern>" --ext cpp
```

### 2) Start Work (Index a Baseline)
```bash
yams add . --recursive \
  --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Working on: $TASK$" \
  --metadata "pbi=$PBI,task=$TASK,phase=start,owner=codex,source=code"
```

### 3) Checkpoint Progress
```bash
yams add <changed-files> \
  --label "$TASK$: checkpoint" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=code"

echo "## Progress: $TASK$
### Summary
$SUMMARY
" | yams add - \
  --name "checkpoint-$(date +%Y%m%d-%H%M%S).md" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=note"
```

### 4) Complete Work
```bash
yams add . --recursive \
  --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Completed: $TASK$" \
  --metadata "pbi=$PBI,task=$TASK,phase=complete,owner=codex,source=code"

yams list --snapshots --limit 2
yams diff <snapshotA> <snapshotB>
```

---

## Knowledge Storage

### Store Research
```bash
curl -s "$DOCS_URL" | yams add - \
  --name "$PACKAGE-guide.md" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=research,url=$DOCS_URL"
```

### Store Solutions
```bash
echo "## Solution: $PROBLEM_DESCRIPTION

### Context
$WHAT_WE_WERE_TRYING_TO_DO

### Problem
$WHAT_WENT_WRONG

### Solution
$HOW_WE_FIXED_IT

### Code
\`\`\`
$SOLUTION_CODE
\`\`\`
" | yams add - \
  --name "solution-$(date +%Y%m%d)-$SLUG.md" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=note,intent=solution"
```

### Store Decisions
```bash
echo "## Decision: $TITLE

### Date
$(date -Iseconds)

### Context
$WHY_DECISION_NEEDED

### Options Considered
1. $OPTION_1 - Pros: ... Cons: ...
2. $OPTION_2 - Pros: ... Cons: ...

### Decision
$WHAT_WE_CHOSE

### Rationale
$WHY
" | yams add - \
  --name "decision-$SLUG.md" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=decision"
```

---

## Code Search (YAMS First)

**ALWAYS use `yams grep` first for code pattern search.**

```bash
yams grep "authentication middleware" --cwd .
yams grep "error.*handling.*retry" --cwd . --fuzzy
yams search "authentication middleware" --cwd .  # Only if grep is empty
```

---

## Graph Dead-Code Audit

```bash
yams status
yams watch

yams add . --recursive \
  --include "*.c,*.cc,*.cpp,*.cxx,*.h,*.hpp,*.rs,*.go,*.py,*.ts,*.js" \
  --exclude "build/**,node_modules/**" \
  --label "Baseline index: $TASK$" \
  --metadata "pbi=$PBI,task=$TASK,phase=checkpoint,owner=codex,source=code"

yams graph --name src/example.cpp --depth 2 --format json
yams graph --list-type <node-type> --isolated --limit 50
```

---

## Response Template

```
TASK: $TASK$
PBI: $PBI$
PHASE: $PHASE$

CONTEXT FOUND:
- Documents: <count>
- Using: [key items]

ACTIONS:
- [What was implemented/changed]

YAMS STORED:
- Indexed files: <paths>
- Notes: <file names>
- Metadata: pbi=$PBI,task=$TASK,phase=$PHASE,owner=codex

VERIFY:
- Current snapshot: <snapshot id>
- Items in YAMS: <count>

NEXT: [Next steps]
```

---

## Quick Reference (code-accurate)

### YAMS CLI
For detailed CLI reference, use the `/yams` skill or run `yams --help`.

Key commands for this workflow:
- `yams grep` - Code pattern search (use first)
- `yams search` - Semantic/hybrid search
- `yams add` - Index files (`-r` for recursive, `--include`/`--exclude` for filters)
- `yams graph` - Explore relationships (`--name`, `--depth`, `--list-type --isolated`)
- `yams watch` - Auto-index on file changes
- `yams status` / `yams doctor` - Health checks

---

## Critical Rules

1. **PBI tracking uses YAMS metadata** - No external task trackers.
2. **Index ALL code changes in YAMS** - Superior search and graph connections.
3. **Search YAMS before implementing** - ALWAYS use `yams grep` first for code patterns.
4. **Avoid tags** - Use labels and metadata instead.
5. **Store learnings** - Future you will thank present you.
