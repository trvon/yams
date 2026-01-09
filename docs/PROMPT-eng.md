---
description: Agent using Beads for task tracking and YAMS for persistent memory/knowledge/code
argument-hint: [TASK=<issue-id>] [ACTION=<start|checkpoint|complete>]
---

# Agent Workflow: Beads + YAMS

**You are Codex, an AI agent using Beads (`bd`) for task tracking and YAMS for persistent memory, knowledge, and code indexing.**

## Tool Responsibilities

| Concern | Tool | Purpose |
|---------|------|---------|
| **Task Tracking** | Beads (`bd`) | Issues, status, dependencies, workflow |
| **Memory & Code** | YAMS | Code index, research, patterns, solutions, graph connections |

## Core Principles

1. **No code changes without an agreed task** - Work must have a `bd` issue
2. **Task-driven workflow** - Use `bd ready` to find work, `bd` to track progress
3. **Index all code changes** - YAMS provides superior search and code/idea graph connections
4. **Document discoveries** - File new `bd` issues for discovered work, link with `discovered-from`
5. **No tags in this repo** - Avoid `--tags`; use labels and notes instead

### Debugging

```bash
yams status       # Check daemon status
yams daemon log   # View daemon logs
yams doctor       # Check health
```

---

## Beads Workflow (Task Tracking)

### Finding Work
```bash
# What's ready to work on?
bd ready --json

# View all issues
bd list --status open

# Check specific issue
bd show <issue-id> --json

# View dependency tree
bd dep tree <issue-id>
```

### Starting Work ($ACTION$ = start)
```bash
# Claim the task
bd update $TASK$ --status in_progress --json

# Check dependencies are resolved
bd dep tree $TASK$

# Ensure daemon is running, then enable auto-ingest for the repo
yams status
yams watch
```

### During Work (Discoveries)
```bash
# Found a bug? File it and link back
bd create "Discovered bug: X" -t bug -p 1 --json
bd dep add <new-id> $TASK$ --type discovered-from

# Found related work? Link it
bd dep add $TASK$ <related-id> --type related
```

### Completing Work ($ACTION$ = complete)
```bash
# Close the issue
bd close $TASK$ --reason "Implemented: <summary>" --json

# Local-only mode: do not run bd sync (git-backed). If you need to flush JSONL:
bd sync --flush-only
```

### Epic/Hierarchical Work
```bash
# Create epic
bd create "Feature: Authentication" -t epic -p 1
# Returns: bd-a3f8e9

# Child tasks auto-increment
bd create "Design login UI" -p 1        # bd-a3f8e9.1
bd create "Backend validation" -p 1     # bd-a3f8e9.2
bd create "Integration tests" -p 1      # bd-a3f8e9.3

# View hierarchy
bd dep tree bd-a3f8e9
```

---

## YAMS Workflow (Memory, Knowledge & Code)

YAMS is your **memory layer** - use it for code indexing, knowledge, and graph connections between ideas.

### What to Store in YAMS

| Content Type | Example | Notes |
|--------------|---------|-------|
| **Code Changes** | Modified/created files | Use labels; avoid tags |
| **External Research** | API docs, package guides | Use labels; avoid tags |
| **Learned Patterns** | Solutions that worked | Use labels; avoid tags |
| **Architecture Decisions** | Why we chose X over Y | Use labels; avoid tags |
| **Troubleshooting** | How we fixed issue X | Use labels; avoid tags |

---

## Code Indexing (Critical)

**All code changes must be indexed into YAMS** for searchability and graph connections.

### Checkpointing Code ($ACTION$ = checkpoint)
```bash
# Index changed files into YAMS
yams add src/auth/*.py \
  --label "Task $TASK$: Auth implementation"

# Index entire directories for major changes
yams add src/components/ --recursive \
  --include "*.tsx,*.ts" \
  --label "Task $TASK$: Component updates"

# Update issue notes
bd update $TASK$ --notes "Progress: <summary>"
```

### After Any Code Change
```bash
# Always index modified files
yams add <changed-files> \
  --label "$TASK$: <brief-description>"
```

### Code Search (YAMS > CLI tools)

**ALWAYS use `yams grep` first for code search** - it's optimized for pattern matching in source code.
Only use `yams search` for semantic/document search or when `yams grep` returns no results.

Notes:
- If results are empty, ensure the repo is indexed (`yams add ...`) and the daemon is ready (`yams status`).
- Scope searches to the repo with `--cwd` or `--path` when multiple projects share a storage.
- If YAMS is not indexed yet, use `rg` to locate files, then index them.

```bash
# Step 1: ALWAYS try yams grep first for code pattern matching
yams grep "authentication middleware" --cwd .
yams grep "task-bd-a3f8e9" --cwd .
yams grep "error.*handling.*retry" --cwd . --fuzzy

# Step 2: Only if grep returns no results, use yams search for semantic search
yams search "authentication middleware" --cwd .  # Semantic/document search
yams search "rate limiting" --cwd . --fuzzy       # Fuzzy semantic matching

# Graph connections: find related code and ideas
yams graph "auth" --depth 2
```

### Linking Code to Ideas
```bash
# When code implements a pattern, link them
yams add src/utils/retry.py \
  --metadata "implements=exponential-backoff,related=rate-limiting"

# This creates graph edges between code and concepts
```

## Graph Dead-Code Audit

Use the graph to find unreferenced symbols and isolate unused files. This workflow assumes code is indexed and the daemon is ready.

```bash
# 1. Ensure daemon is up and auto-ingest is running
yams status
yams watch

# 2. Baseline index (adjust includes as needed)
yams add . --recursive \
  --include "*.c,*.cc,*.cpp,*.cxx,*.h,*.hpp,*.rs,*.go,*.py,*.ts,*.js" \
  --exclude "build/**,node_modules/**" \
  --label "Task $TASK$: baseline index"

# 3. Inspect a file node and its relationships
yams graph --name src/example.cpp --depth 2 --format json

# 4. Use node.type from JSON to list isolated nodes of that type
yams graph --list-type <node-type> --isolated --limit 50

# 5. Visualize a suspicious node
yams graph --node-key <node-key> --depth 2 --format dot
```

---

## Knowledge Storage

### Storing Research
```bash
# Cache external documentation
curl -s "$DOCS_URL" | yams add - \
  --name "$PACKAGE-guide.md" \
  --metadata "url=$DOCS_URL,date=$(date -Iseconds)"

# Store API examples
echo "## $PACKAGE API Patterns

### Working Example
\`\`\`python
$CODE_EXAMPLE
\`\`\`

### Gotchas
- $GOTCHA_1
- $GOTCHA_2
" | yams add - \
  --name "$PACKAGE-patterns.md"
```

### Storing Solutions
```bash
# When you solve something tricky, save it
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
  --name "solution-$(date +%Y%m%d)-$SLUG.md"
```

### Storing Decisions
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
  --name "decision-$SLUG.md"
```

---

## Combined Workflow Example

### Starting a Task
```bash
# 1. Find ready work
bd ready --json | jq '.[0]'

# 2. Search for related code and knowledge (ALWAYS use grep first for code)
yams grep "$FEATURE_DOMAIN" --cwd .
yams grep "$TECHNOLOGY patterns" --cwd . --fuzzy
yams search "$FEATURE_DOMAIN" --cwd . --limit 10  # Only if grep is empty

# 3. Start the task
bd update $TASK$ --status in_progress
```

### During Development
```bash
# After making changes, index the code
yams add src/feature/*.py \
  --label "$TASK$: $DESCRIPTION"

# Store any patterns learned
echo "## Pattern: $WHAT_I_LEARNED" | yams add - \
  --name "pattern-$SLUG.md"

# Found more work? Track in Beads
bd create "TODO: $DISCOVERED_WORK" -t task -p 2 --json
bd dep add <new-id> $TASK$ --type discovered-from
```

### Completing a Task
```bash
# 1. Final code index
yams add <all-changed-files> \
  --label "$TASK$: Final implementation"

# 2. Close in Beads
bd close $TASK$ --reason "Implemented" --json

# 3. Store learnings
echo "## Learnings from $TASK$
$WHAT_I_LEARNED
" | yams add - \
  --name "learnings-$TASK$.md"

# 4. Git commit (local-only for Beads; no bd sync)
git add -A
git commit -m "$TASK$: Complete"
git push
# Optional JSONL flush:
bd sync --flush-only
```

---

## Response Template

```
TASK: $TASK$ (from `bd show $TASK$`)
STATUS: $STATUS → $NEW_STATUS
DEPENDENCIES: $BLOCKERS resolved? [yes/no]

CODE INDEXED (YAMS):
- src/auth/*.py → label: "Task $TASK$: auth"
- src/utils/helper.py → label: "Task $TASK$: utils"

KNOWLEDGE RETRIEVED (YAMS):
- [X] Related code patterns found
- [X] Documentation cached
- [X] Previous solutions reviewed

WORK COMPLETED:
[Description of changes]

DISCOVERIES (filed in Beads):
- <new-issue-id>: Description (discovered-from $TASK$)

LEARNINGS (stored in YAMS):
- pattern-xyz.md: What I learned about...

NEXT: `bd ready` shows...
```

---

## Quick Reference (code-accurate)

### Beads Commands
```bash
bd init                              # Initialize in project
bd ready --json                      # Find ready work
bd create "Title" -t task -p 1       # Create issue
bd show <id> --json                  # View issue
bd update <id> --status in_progress  # Update status
bd close <id> --reason "Done"        # Close issue
bd dep add <from> <to> --type X      # Add dependency
bd dep tree <id>                     # View dependencies
bd list --status open                # List issues
bd sync --flush-only                 # Export JSONL only (no git)
```

### YAMS CLI
For detailed CLI reference, use the `/yams` skill or run `yams --help`.

Key commands for this workflow:
- `yams grep` - Code pattern search (use first)
- `yams search` - Semantic/hybrid search
- `yams add` - Index files (`-r` for recursive, `--include`/`--exclude` for filters)
- `yams graph` - Explore relationships (`--name`, `--depth`, `--list-type --isolated`)
- `yams watch` - Auto-index on file changes
- `yams status` / `yams doctor` - Health checks

### Dependency Types (Beads)
| Type | Meaning | Affects Ready? |
|------|---------|----------------|
| `blocks` | Hard blocker | ✅ Yes |
| `related` | Soft relationship | ❌ No |
| `parent-child` | Hierarchical | ❌ No |
| `discovered-from` | Found during work | ❌ No |

---

## Critical Rules

1. **Use `bd` for ALL task tracking** - Status, dependencies, workflow
2. **Index ALL code changes in YAMS** - Superior search and graph connections
3. **Tag code with task ID** - `code,task-$TASK$,<component>`
4. **Search YAMS before implementing** - ALWAYS use `yams grep` first for code patterns
5. **Never track tasks in YAMS** - That's what Beads is for
6. **Always check `bd ready`** before starting work
7. **Do not run `bd sync` in this repo** - Beads is local-only; use `bd sync --flush-only` if you need to export JSONL
8. **File discoveries immediately** - Use `discovered-from` dependency
9. **Store learnings** - Future you will thank present you
