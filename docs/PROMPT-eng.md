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

# Sync beads database
bd sync
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

| Content Type | Example | Tags |
|--------------|---------|------|
| **Code Changes** | Modified/created files | `code,task-$TASK$,<component>` |
| **External Research** | API docs, package guides | `documentation,external,<package>` |
| **Learned Patterns** | Solutions that worked | `pattern,solution,<domain>` |
| **Architecture Decisions** | Why we chose X over Y | `decision,architecture` |
| **Troubleshooting** | How we fixed issue X | `troubleshooting,<area>` |

---

## Code Indexing (Critical)

**All code changes must be indexed into YAMS** for searchability and graph connections.

### Checkpointing Code ($ACTION$ = checkpoint)
```bash
# Index changed files into YAMS
yams add src/auth/*.py \
  --tags "code,task-$TASK$,auth,python" \
  --label "Task $TASK$: Auth implementation"

# Index entire directories for major changes
yams add src/components/ --recursive \
  --include "*.tsx,*.ts" \
  --tags "code,task-$TASK$,frontend" \
  --label "Task $TASK$: Component updates"

# Update issue notes
bd update $TASK$ --notes "Progress: <summary>"
```

### After Any Code Change
```bash
# Always index modified files
yams add <changed-files> \
  --tags "code,task-$TASK$,<component>" \
  --label "$TASK$: <brief-description>"
```

### Code Search (Why YAMS > CLI tools)
```bash
# Find how we implemented something before
yams search "authentication middleware" --tags "code"

# Find all code related to a feature
yams search "task-bd-a3f8e9" --tags "code"

# Find code patterns across the project
yams search "error handling retry" --tags "code,pattern"

# Graph connections: find related code and ideas
yams graph "auth" --depth 2

# Find code that relates to a concept
yams search "rate limiting" --fuzzy
```

### Linking Code to Ideas
```bash
# When code implements a pattern, link them
yams add src/utils/retry.py \
  --tags "code,pattern,retry,error-handling" \
  --metadata "implements=exponential-backoff,related=rate-limiting"

# This creates graph edges between code and concepts
```

---

## Knowledge Storage

### Storing Research
```bash
# Cache external documentation
curl -s "$DOCS_URL" | yams add - \
  --name "$PACKAGE-guide.md" \
  --tags "documentation,external,$PACKAGE" \
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
  --name "$PACKAGE-patterns.md" \
  --tags "pattern,api,$PACKAGE"
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
  --name "solution-$(date +%Y%m%d)-$SLUG.md" \
  --tags "solution,pattern,$DOMAIN"
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
  --name "decision-$SLUG.md" \
  --tags "decision,architecture,$AREA"
```

---

## Combined Workflow Example

### Starting a Task
```bash
# 1. Find ready work
bd ready --json | jq '.[0]'

# 2. Search for related code and knowledge
yams search "$FEATURE_DOMAIN" --tags "code" --limit 10
yams search "$TECHNOLOGY patterns" --tags "pattern,solution"

# 3. Start the task
bd update $TASK$ --status in_progress
```

### During Development
```bash
# After making changes, index the code
yams add src/feature/*.py \
  --tags "code,task-$TASK$,$COMPONENT" \
  --label "$TASK$: $DESCRIPTION"

# Store any patterns learned
echo "## Pattern: $WHAT_I_LEARNED" | yams add - \
  --name "pattern-$SLUG.md" \
  --tags "pattern,$DOMAIN"

# Found more work? Track in Beads
bd create "TODO: $DISCOVERED_WORK" -t task -p 2 --json
bd dep add <new-id> $TASK$ --type discovered-from
```

### Completing a Task
```bash
# 1. Final code index
yams add <all-changed-files> \
  --tags "code,task-$TASK$,complete" \
  --label "$TASK$: Final implementation"

# 2. Close in Beads
bd close $TASK$ --reason "Implemented" --json

# 3. Store learnings
echo "## Learnings from $TASK$
$WHAT_I_LEARNED
" | yams add - \
  --name "learnings-$TASK$.md" \
  --tags "learnings,$DOMAIN,task-$TASK$"

# 4. Git commit & sync
git add -A
git commit -m "$TASK$: Complete"
git push
bd sync
```

---

## Response Template

```
TASK: $TASK$ (from `bd show $TASK$`)
STATUS: $STATUS → $NEW_STATUS
DEPENDENCIES: $BLOCKERS resolved? [yes/no]

CODE INDEXED (YAMS):
- src/auth/*.py → tags: code,task-$TASK$,auth
- src/utils/helper.py → tags: code,task-$TASK$,utils

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
bd sync                              # Sync with git
```

### YAMS CLI (selected commands)
- **add**: multi-path add with daemon-first flow; supports `--name`, `--tags`, `--metadata`, `--mime-type`, `--no-auto-mime`, `--no-embeddings`, collections/snapshots (`--collection`, `--snapshot-id`, `--snapshot-label`), directory controls (`-r/--recursive`, `--include`, `--exclude`, `--verify`), session scope (`--global/--no-session`), and daemon robustness (`--daemon-timeout-ms`, `--daemon-retries`, `--daemon-backoff-ms`, `--daemon-ready-timeout-ms`). Stdin supported via `-`.
- **search**: queries via positional args or `-q/--query`; accepts `--stdin`/`--query-file`; defaults to `--type hybrid`; fuzzy toggle `-f/--fuzzy` with `--similarity`; `--paths-only`; grouping controls (`--no-group-versions`, `--versions {latest|all}`, `--versions-topk`, `--versions-sort`, `--no-tools`, `--json-grouped`); session scope (`--session`, `--global/--no-session`); `--cwd`; streaming (`--streaming`, `--chunk-size`, header/body timeouts); literal text `-F/--literal-text`; display (`--show-hash`, `-v/--verbose`, `--json`); line/context (`-n/--line-numbers`, `-A/-B/-C`); hash search `--hash`; tag filters (`--tags`, `--match-all-tags`); include globs; file filters (`--ext`, `--mime`, `--file-type`, `--text-only`, `--binary-only`); time filters (`--created-*`, `--modified-*`, `--indexed-*`).
- **graph**: target by positional `hash`, `--name`, `--node-key`, or `--node-id`; traversal depth `--depth 1-5`; filter relations with `--relation/-r`; list-only mode via `--list-type` (use `--isolated` to find nodes with no incoming edges); pagination `--limit/--offset`; output `--format table|json|dot` or `--json`; `--verbose` shows properties/hashes; `--prop-filter` for property text.
- **session**: lifecycle (`start`, `use`, `ls`, `show --json`, `rm`); selectors (`add --path/--tag/--meta`, `rm-path`, `list --json`); warming (`warm --limit --snippet-len --cores --memory-gb --time-ms --aggressive`); tagging/annotation (`tags --add/--remove`, `annotate --meta`); cache (`clear`); import/export (`save`, `load --name`); emit (`emit --kind names|paths|hashes --materialized --json`); watch (`--start/--stop --interval --session`); session isolation (`create`, `open`, `close`, `status --json`); maintenance (`merge --exclude --dry-run`, `discard --confirm`, `diff --base --target --dir --type --json`). Env: `YAMS_SESSION_CURRENT` selects default.
- **doctor**: flags `--json`, `--fix`, `--fix-config-dims`, `--recreate-vectors [--dim] [--stop-daemon]`; subcommands: `daemon`, `plugin <target> [--iface --iface-version --no-daemon]`, `plugins`, `embeddings clear-degraded`, `repair` (`--embeddings|--fts5|--graph|--all`), `validate --graph`, `dedupe` (`--apply`, `--mode {path|name|hash}`, `--strategy {keep-newest|keep-oldest|keep-largest}`, `--force`, `--verbose`), `prune` (`--apply`, categories via `--category/-c`, `--extension/-e`, `--older-than`, `--larger-than`, `--smaller-than`, `-v`), `tuning [--apply]`.
- **daemon**: manage daemon with `start` (`--socket`, `--data-dir/--storage`, `--pid-file`, `--log-level`, `--config`, `--daemon-binary`, `--foreground`, `--restart`), `stop [--force]`, `status [-d/--detailed]`, `restart`, `doctor`, `log` (`-n`, `-f`, `--level`).
- **status / stats**: `yams status` (alias `yams stats`) with `--json`, `-v/--verbose`, `--no-physical`.
- **model**: manage ONNX models. Flags `--list`, `--download <name>`, `--info <name>`, `--output`, `--url`, `--force`, `--check`; subcommands mirror these plus `download` options (`--hf`, `--revision`, `--token`, `--offline`, `--apply-config`) and `provider` for daemon model status.
- **serve**: start MCP server; default quiet; `--daemon-socket` override; `--verbose` shows banner (legacy `--quiet` is no-op).
- **Other core commands**: `cat`, `get`, `list/ls`, `delete/rm`, `config`, `init`, `migrate`, `uninstall`, `update`, `diff`, `tree`, `grep`, `download`, `restore`, `repair`, `dr`, `completion`, `plugin` (see individual help for flags).

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
4. **Search YAMS before implementing** - Find related code and patterns first
5. **Never track tasks in YAMS** - That's what Beads is for
6. **Always check `bd ready`** before starting work
7. **Always run `bd sync`** before ending a session
8. **File discoveries immediately** - Use `discovered-from` dependency
9. **Store learnings** - Future you will thank present you