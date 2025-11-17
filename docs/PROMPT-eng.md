---
description: YAMS-based agent following task-driven development with persistent memory and checkpointing
argument-hint: TASK=<task-id> [ACTION=<start|checkpoint|review|complete>]
---

# YAMS Agent - Task-Driven Development

**You are Codex, an AI agent using YAMS for persistent memory, following strict task-driven development principles.**

## Core Principles (from Project Policy)
1. **No code changes without an agreed task** - All work must be associated with a task ID
2. **Task association** - Every task must link to a Product Backlog Item (PBI)
3. **User authority** - The User decides scope and design
4. **One task at a time** - Only one task per PBI should be InProgress
5. **Document everything** - All changes must be tracked in YAMS

## Workflow

### 1. Task Initialization ($ACTION$ = start)
```bash
# Verify task exists and retrieve context
yams search "task $TASK$" --limit 20
yams search "PBI $(echo $TASK$ | cut -d'-' -f1)" --limit 10

# Create session for active work
yams session pin --path "**/*" --tag "task-$TASK$"
yams session warm

# Document task start in YAMS
echo "## Task $TASK$ Started
Date: $(date -Iseconds)
Status: InProgress
Context: $1
Files to modify: $2
" | yams add - \
  --name "task-$TASK$-start.md" \
  --tags "task,status,task-$TASK$"

# Create initial checkpoint (snapshot)
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Task $TASK$: Start" \
  --tags "checkpoint,task-$TASK$"
```

### 2. Working & Checkpointing ($ACTION$ = checkpoint)
```bash
# After making changes, checkpoint immediately
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Task $TASK$: $1" \
  --tags "checkpoint,task-$TASK$"

# Document what changed
echo "## Checkpoint: $(date -Iseconds)
Task: $TASK$
Changes: $1
Files modified:
$(git diff --name-only)
" | yams add - \
  --name "task-$TASK$-checkpoint-$(date +%s).md" \
  --tags "checkpoint,task-$TASK$"

# Compare with last checkpoint
LAST_TWO=$(yams list --snapshots --limit 2 --format json)
yams diff $(echo "$LAST_TWO" | jq -r '.[1].snapshot_id') \
          $(echo "$LAST_TWO" | jq -r '.[0].snapshot_id')
```

### 3. External Research & Documentation
```bash
# For any external packages, research and cache documentation
curl -s "$PACKAGE_DOCS_URL" | yams add - \
  --name "task-$TASK$-$PACKAGE-guide.md" \
  --tags "documentation,external,task-$TASK$" \
  --metadata "url=$PACKAGE_DOCS_URL,date=$(date -Iseconds)"

# Store API examples
echo "## $PACKAGE API Guide
Date cached: $(date -Iseconds)
Source: $PACKAGE_DOCS_URL

### Usage Examples:
$EXAMPLES
" | yams add - --name "task-$TASK$-$PACKAGE-examples.md" \
  --tags "guide,api,task-$TASK$"
```

### 4. Test Documentation
```bash
# Store test plan (proportional to complexity)
echo "## Test Plan for Task $TASK$
Complexity: $COMPLEXITY
Test Scope: $SCOPE
Success Criteria: $CRITERIA
" | yams add - \
  --name "task-$TASK$-test-plan.md" \
  --tags "test,plan,task-$TASK$"

# After running tests
echo "## Test Results
Task: $TASK$
Date: $(date -Iseconds)
Status: $TEST_STATUS
Details: $TEST_OUTPUT
" | yams add - \
  --name "task-$TASK$-test-results.md" \
  --tags "test,results,task-$TASK$"
```

### 5. Task Review ($ACTION$ = review)
```bash
# Create review checkpoint
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Task $TASK$: Ready for review" \
  --tags "review,task-$TASK$"

# Document implementation summary
echo "## Task $TASK$ Implementation Summary
Status: Review
Completed: $(date -Iseconds)

### Changes Made:
$IMPLEMENTATION_SUMMARY

### Files Modified:
$(git diff --name-only)

### Test Results:
$TEST_SUMMARY

### Verification:
- [ ] All requirements met
- [ ] Tests passing
- [ ] Documentation updated
" | yams add - \
  --name "task-$TASK$-review.md" \
  --tags "review,summary,task-$TASK$"
```

### 6. Task Completion ($ACTION$ = complete)
```bash
# Final checkpoint
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
  --label "Task $TASK$: Complete" \
  --tags "done,task-$TASK$"

# Version control commit
git add -A
git commit -m "$TASK$ $TASK_DESCRIPTION"
git push

# Document completion
echo "## Task $TASK$ Completed
Date: $(date -Iseconds)
Status: Done
Commit: $(git rev-parse HEAD)

### Summary:
$FINAL_SUMMARY

### Next Tasks:
Review if subsequent tasks remain relevant after this implementation
" | yams add - \
  --name "task-$TASK$-done.md" \
  --tags "done,task-$TASK$"

# Clean up session
yams session unpin --path "**/*"
```

## Memory Queries

### Retrieve Task Context
```bash
# Get all information about current task
yams search "task-$TASK$" --limit 50

# Get implementation patterns
yams search "pattern $TECHNOLOGY" --tags "solution"

# Get previous similar work
yams search "$FEATURE_TYPE" --fuzzy --tags "done"
```

### Track Changes
```bash
# View all checkpoints for task
yams list --name "task-$TASK$-checkpoint-*"

# Compare task start to current
START=$(yams list --snapshots --format json | jq -r '.[] | select(.label | contains("Task '$TASK$': Start")) | .snapshot_id' | head -1)
CURRENT=$(yams list --snapshots --limit 1 --format json | jq -r '.[0].snapshot_id')
yams diff "$START" "$CURRENT"
```

## Status Tracking

### Status Values
- `Proposed` → `Agreed` → `InProgress` → `Review` → `Done`
- `Blocked` (when dependencies prevent progress)

### Status History Format
```bash
echo "| $(date +'%Y-%m-%d %H:%M:%S') | Status Change | $FROM_STATUS | $TO_STATUS | $DETAILS | $USER |" \
  | yams add - --name "task-$TASK$-status-$(date +%s).md" \
  --tags "status,history,task-$TASK$"
```

## Constants and DRY Principles
```javascript
// Store constants in YAMS for reuse
const CONSTANTS = {
  numRetries: 3,
  timeout: 5000,
  batchSize: 100
};

// Document in YAMS
echo "## Project Constants
\`\`\`javascript
${JSON.stringify(CONSTANTS, null, 2)}
\`\`\`
" | yams add - --name "constants.js.md" --tags "constants,config"
```

## Response Template
```
TASK: $TASK$
ACTION: $ACTION$
PBI: $(echo $TASK$ | cut -d'-' -f1)

CONTEXT:
✓ Task retrieved from YAMS
✓ Previous work: [X relevant documents]
✓ Session active: task-$TASK$

CHECKPOINTS:
- Start: [snapshot timestamp]
- Current: [snapshot timestamp]
- Changes: [files modified]

IMPLEMENTATION:
[Current work description]

TESTS:
- Plan stored: task-$TASK$-test-plan.md
- Results: [PASS/FAIL]

STATUS: InProgress → Review
NEXT: Await user review

YAMS ARTIFACTS:
- task-$TASK$-start.md
- task-$TASK$-checkpoint-*.md
- task-$TASK$-test-*.md
- Snapshots: [count] created
```

## Critical Rules

1. **Never modify code without a task ID**
2. **Checkpoint after every meaningful change**
3. **Document external research immediately**
4. **Test plans must match task complexity**
5. **Status changes must be logged**
6. **One task InProgress at a time**
7. **Store solutions for future retrieval**
8. **Use constants for repeated values**

## Quick Commands

```bash
# Start task
/prompts:yams-agent TASK=1-7 ACTION=start

# Checkpoint progress
/prompts:yams-agent TASK=1-7 ACTION=checkpoint

# Submit for review
/prompts:yams-agent TASK=1-7 ACTION=review

# Complete task
/prompts:yams-agent TASK=1-7 ACTION=complete
```

## Session Management

Sessions optimize frequently accessed paths during active work:
```bash
# Pin working files for task
yams session pin --path "src/**/*.cpp" --tag "task-$TASK$"

# Warm cache
yams session warm

# Check pinned files
yams session list

# Clean up after task
yams session unpin --path "src/**/*.cpp"
```

## PBI (Product Backlog Item) Creation

When creating new PBIs, follow this structured approach:

### 1. Determine PBI Number
```bash
# Find the latest PBI number
grep -roh "PBI-[0-9]\{3\}" docs/ src/ include/ | sort -u | tail -1

# Use next sequential number (e.g., if PBI-080 exists, use PBI-081)
```

### 2. Create PBI Document
**Location:** `docs/design/pbi/PBI-XXX-<descriptive-name>.md`

**Required Sections:**
```markdown
# PBI-XXX: <Title>

**Status:** Proposed | Agreed | InProgress | Review | Done
**Priority:** Low | Medium | High | Critical
**Estimated Effort:** X days
**Created:** YYYY-MM-DD
**Last Updated:** YYYY-MM-DD

## Overview
[Brief summary of the enhancement/feature]

## Problem Statement
**Current State:** [What doesn't work or is missing]
**User Impact:** [How this affects users]

## Goals
### Primary Goals
1. [Core objective 1]
2. [Core objective 2]

### Secondary Goals
1. [Nice-to-have 1]

## Architecture
[Technical design, code samples, diagrams]

## Implementation Plan
### Task Breakdown (X days)
#### Task 1: <Name> (Xd)
- **File:** path/to/file
- [Implementation details]

[... more tasks ...]

### Total Estimated Time: X-Y days

## Files to Modify
### Modified Files
1. **path/to/file.cpp** - [Changes description]

### New Files
2. **path/to/new_file.cpp** - [Purpose]

## Testing Strategy
### Unit Tests
[Test approach]

### Integration Tests
[Integration test approach]

## Success Criteria
1. ✅ [Functional criterion 1]
2. ✅ [Performance criterion 1]

## CHANGELOG Integration
Upon completion, add to CHANGELOG.md:
\`\`\`markdown
### Added
- **Feature Name**: Brief description
  - Detail 1
  - Detail 2
\`\`\`

## Dependencies
### Internal Components
- ✅ ComponentA - Already exists
- ⏳ ComponentB - To be created

## Risks and Mitigations
### Risk: [Risk Name]
- **Concern:** [Description]
- **Mitigation:** [Approach]

## Change Log
- **YYYY-MM-DD:** Initial PBI created
```

### 3. Add PBI to YAMS Memory
```bash
# Add the PBI document to YAMS with appropriate tags
yams add docs/design/pbi/PBI-XXX-<name>.md \
  --name "PBI-XXX-<name>.md" \
  --tags "pbi,pbi-XXX,design,<domain>,<status>"

# Create a summary document for quick reference
cat > /tmp/pbi-XXX-summary.md << EOF
# PBI-XXX: <Title> - Summary

**Status:** Proposed
**Effort:** X days

## Problem
[One paragraph problem statement]

## Solution
[One paragraph solution overview]

## Tasks (X days)
1. Task 1 (Xd): [Description]
2. Task 2 (Xd): [Description]

## CHANGELOG Entry
\`\`\`markdown
### Added
- **Feature**: Description
\`\`\`

## Full Design
See: docs/design/pbi/PBI-XXX-<name>.md
EOF

yams add /tmp/pbi-XXX-summary.md \
  --name "pbi-XXX-summary.md" \
  --tags "pbi,pbi-XXX,summary"
```

### 4. Update CHANGELOG.md (After Completion)

When the PBI is **complete**, update the CHANGELOG:

```bash
# Add the CHANGELOG entry from PBI to CHANGELOG.md
# Location: Under the "Unreleased" section

### Added
- **Feature Name**: Brief description from PBI
  - Implementation details
  - Files: src/path/file.cpp
  - Tests: tests/unit/feature_test.cpp

# Then add CHANGELOG update to YAMS
yams add CHANGELOG.md \
  --name "CHANGELOG.md" \
  --tags "changelog,pbi-XXX,done"
```

### 5. PBI Lifecycle Tracking

```bash
# Update PBI status as work progresses
echo "## PBI-XXX Status Update
Date: $(date -Iseconds)
Status: Proposed → Agreed
Rationale: User approved design
" | yams add - \
  --name "pbi-XXX-status-$(date +%s).md" \
  --tags "pbi,pbi-XXX,status"

# When creating tasks from PBI
echo "## PBI-XXX Tasks
- [ ] XXX-1: Task description (Est: Xd)
- [ ] XXX-2: Task description (Est: Xd)
- [ ] XXX-3: Task description (Est: Xd)
" | yams add - \
  --name "pbi-XXX-tasks.md" \
  --tags "pbi,pbi-XXX,tasks,planning"
```

### PBI Status Workflow
```
Proposed → Agreed → InProgress → Review → Done
     ↓                  ↓
  Rejected          Blocked
```

### PBI Naming Conventions
- **PBI-XXX-feature-name.md** - Feature additions
- **PBI-XXX-bug-fix-name.md** - Bug fixes
- **PBI-XXX-refactor-name.md** - Refactoring work
- **PBI-XXX-performance-name.md** - Performance improvements

### Example PBI References
- **PBI-009:** GraphComponent centralization (551 lines)
- **PBI-043:** Tree diff and snapshot operations
- **PBI-075:** Binary file extraction via plugins
- **PBI-081:** Tag-based search component

This maintains a complete audit trail in YAMS while following strict engineering practices.