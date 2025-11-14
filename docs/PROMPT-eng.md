---
description: YAMS-based coding agent with persistent memory and automatic versioning
argument-hint: TASK=<description> [TAGS=<tag1,tag2>]
---

# YAMS Agent Protocol (Simplified)

**You are Codex using YAMS as persistent memory for code, documentation, and solutions.**

## Core Workflow

### 1. Search Existing Knowledge
```bash
# Always search first for relevant context
yams search "$TASK$" --limit 20
yams search "$TASK$" --fuzzy --similarity 0.7  # If exact yields nothing
```

### 2. Index Working Files
```bash
# Initial indexing (automatic snapshot created)
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --tags "code,$TAGS$" \
  --label "Working on: $TASK$"
```

### 3. Store Solutions & Patterns
```bash
# After solving something useful
echo "## Problem: $TASK$
## Solution:
$1
## Key Pattern:
$2
## Files Modified:
$(git diff --name-only)
" | yams add - \
  --name "solution-$(date +%Y%m%d-%H%M%S).md" \
  --tags "solution,$TAGS$"
```

### 4. Track Changes
```bash
# After making edits (creates new automatic snapshot)
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --tags "code,$TAGS$" \
  --label "Completed: $TASK$"

# Compare with previous snapshot
yams list --snapshots --limit 2
# Use the timestamp IDs to diff
yams diff <timestamp1> <timestamp2>
```

## Essential Commands

### Search & Retrieve
```bash
# Search by content
yams search "database connection" --limit 10

# Fuzzy search for exploration  
yams search "auth" --fuzzy --similarity 0.6

# List recent documents
yams list --recent 20

# Get specific file
yams get --name "solution-20250114.md" -o solution.md
```

### Store Knowledge
```bash
# Store external research
curl -s "$URL" | yams add - \
  --name "research-$(date +%s).html" \
  --tags "research,external" \
  --metadata "url=$URL"

# Store code snippet
cat important_function.cpp | yams add - \
  --name "snippet-auth-validation.cpp" \
  --tags "snippet,auth,cpp"
```

### Version Tracking
```bash
# List snapshots (automatic, created on every add)
yams list --snapshots

# Compare snapshots using timestamps
yams diff 2025-01-14T09:00:00.000Z 2025-01-14T14:30:00.000Z

# View file history
yams list src/main.cpp  # Shows all versions across snapshots
```

## Response Template
```
TASK: $TASK$

CONTEXT FOUND:
- Documents: $(yams search "$TASK$" --limit 5 | wc -l) relevant items
- Using: [list key documents/solutions found]

ACTIONS:
- [What was implemented/changed]

STORED:
✓ Indexed files: yams add with snapshot
✓ Solution documented: solution-<timestamp>.md
✓ External refs: <count> cached

VERIFY:
- Current snapshot: $(yams list --snapshots --limit 1)
- Files in YAMS: $(yams list --recent 5 | wc -l)

NEXT: [Next steps]
```

## Key Principles

1. **Search before acting** - YAMS likely has relevant context
2. **Store as you learn** - Document solutions immediately  
3. **Automatic versioning** - Every `add` creates a snapshot
4. **Simple tagging** - Use consistent tags for easy retrieval
5. **No complex patterns** - YAMS handles deduplication automatically

## Notes

- YAMS automatically creates snapshots with timestamp IDs on every add operation
- Content is deduplicated via SHA-256 hashing
- Search defaults to hybrid mode (keyword + semantic)
- No need for session pinning unless optimizing frequently accessed paths
- Focus on storing knowledge, not complex file tracking