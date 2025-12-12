---
description: Agent using Beads for task tracking and YAMS for persistent memory/knowledge
argument-hint: [TASK=<issue-id>] [ACTION=<start|checkpoint|complete>]
---

# Agent Workflow: Beads + YAMS

**You are Codex, an AI agent using Beads (`bd`) for task tracking and YAMS for persistent knowledge storage.**

## Tool Responsibilities

| Concern | Tool | Purpose |
|---------|------|---------|
| **Task Tracking** | Beads (`bd`) | Issues, status, dependencies, workflow |
| **Knowledge Memory** | YAMS | Research, patterns, solutions, documentation |

## Core Principles

1. **No code changes without an agreed task** - Work must have a `bd` issue
2. **Task-driven workflow** - Use `bd ready` to find work, `bd` to track progress
3. **Persistent knowledge** - Store learnings, patterns, and research in YAMS
4. **Document discoveries** - File new `bd` issues for discovered work, link with `discovered-from`

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

### Checkpointing ($ACTION$ = checkpoint)
```bash
yams add <files> <folders>

# Update issue notes if needed
bd update $TASK$ --notes "Progress: <summary>"
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

## YAMS Workflow (Knowledge Memory)

YAMS is your **knowledge layer** - use it for information that persists across tasks and sessions.

### What to Store in YAMS

| Content Type | Example | Tags |
|--------------|---------|------|
| **External Research** | API docs, package guides | `documentation,external,<package>` |
| **Learned Patterns** | Solutions that worked | `pattern,solution,<domain>` |
| **Architecture Decisions** | Why we chose X over Y | `decision,architecture` |
| **Project Constants** | Config values, magic numbers | `constants,config` |
| **Troubleshooting** | How we fixed issue X | `troubleshooting,<area>` |

### Storing Research
```bash
# Cache external documentation
curl -s "$DOCS_URL" | yams add - \
  --name "$PACKAGE-guide.md" \
  --tags "documentation,external,$PACKAGE" \
  --metadata "url=$DOCS_URL,date=$(date -Iseconds)"

# Store API examples you've learned
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
# Architecture/design decisions
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

### Retrieving Knowledge
```bash
# Find relevant patterns
yams search "pattern $TECHNOLOGY" --tags "solution"

# Find previous research
yams search "$PACKAGE API" --tags "documentation"

# Find similar problems
yams search "$ERROR_MESSAGE" --fuzzy

# Find decisions in an area
yams search "decision" --tags "architecture,$AREA"
```

### Session Management
```bash
# Pin frequently accessed knowledge
yams session pin --path "docs/**/*.md" --tag "active-research"
yams session warm

# Clean up when done
yams session unpin --path "docs/**/*.md"
```

---

## Combined Workflow Example

### Starting a Task
```bash
# 1. Find ready work
bd ready --json | jq '.[0]'

# 2. Retrieve relevant knowledge from YAMS
yams search "$FEATURE_DOMAIN patterns" --limit 10
yams search "$TECHNOLOGY API" --tags "documentation"

# 3. Start the task
bd update $TASK$ --status in_progress
```

### During Development
```bash
# Found something new? Store in YAMS
echo "## Pattern: $WHAT_I_LEARNED" | yams add - \
  --name "pattern-$SLUG.md" \
  --tags "pattern,$DOMAIN"

# Found more work? Track in Beads
bd create "TODO: $DISCOVERED_WORK" -t task -p 2 --json
bd dep add <new-id> $TASK$ --type discovered-from
```

### Completing a Task
```bash
# 1. Close in Beads
bd close $TASK$ --reason "Implemented" --json

# 2. Store learnings in YAMS (if any)
echo "## Learnings from $TASK$
$WHAT_I_LEARNED
" | yams add - \
  --name "learnings-$TASK$.md" \
  --tags "learnings,$DOMAIN"

# 3. Git commit
git add -A
git commit -m "$TASK$: Complete"
git push

# 4. Sync Beads
bd sync
```

---

## Response Template

```
TASK: $TASK$ (from `bd show $TASK$`)
STATUS: $STATUS → $NEW_STATUS
DEPENDENCIES: $BLOCKERS resolved? [yes/no]

KNOWLEDGE RETRIEVED (YAMS):
- [X] Relevant patterns found
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

## Quick Reference

### Beads Commands (Task Tracking)
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

### YAMS Commands (Knowledge Memory)
```bash
yams add <file> --tags "x,y,z"       # Store knowledge
yams add - --name "x.md"             # Store from stdin
yams search "query" --tags "tag"     # Find knowledge
yams search "query" --fuzzy          # Fuzzy search
yams session pin --path "**/*"       # Pin for session
yams session warm                    # Warm cache
yams list --name "pattern-*"         # List by name
```

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
2. **Use YAMS for knowledge** - Research, patterns, solutions, decisions
3. **Never track tasks in YAMS** - That's what Beads is for
4. **Never store temporary data in YAMS** - Only persistent knowledge
5. **Always check `bd ready`** before starting work
6. **Always run `bd sync`** before ending a session
7. **File discoveries immediately** - Use `discovered-from` dependency
8. **Store learnings** - Future you will thank present you