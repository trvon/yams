# Project Policy with Integrated Knowledge Management
[derived from](https://gist.github.com/auth/github/callback?return_to=https://gist.github.com/boxabirds/4a8a8a16b1f8431fd64a790209452380&browser_session_id=425c39000b6091fc8eca3f7d1541dfff32f68cfb0baf7296afb1225cbfd4d382&code=e528920a74cf435a099e&state=757eefde51a1ef31f553e54f006fd0a0e70b775b3f0bab1368968ad11f8e820e)

This policy provides a single, authoritative, and machine-readable source of truth for AI coding agents and humans, ensuring that all work is governed by clear, unambiguous rules and workflows. It integrates YAMS (Yet Another Memory System) as the primary knowledge management system for persistent storage and retrieval.

# 1. Introduction and Core Systems

> Rationale: Sets the context, actors, compliance requirements, and establishes YAMS as the primary knowledge management system.

## 1.1 Actors

> Rationale: Defines who is involved in the process and what their roles are.

- **User**: The individual responsible for defining requirements, prioritising work, approving changes, and ultimately accountable for all code modifications.
- **AI_Agent**: The delegate responsible for executing the User's instructions precisely as defined by PBIs and tasks, utilizing YAMS for knowledge persistence.

## 1.2 Knowledge Management System (YAMS)

> Rationale: Establishes YAMS as the primary system for persistent storage and retrieval, reducing reliance on web searches and building project knowledge over time.

### 1.2.1 YAMS Priority Rules

1. **Search First**: Before using web_search, ALWAYS check YAMS for relevant information:
   ```bash
   yams search "query terms" --limit 10
   yams search "specific topic" --fuzzy --similarity 0.8
   ```

2. **Store After Search**: After any web_search that returns useful information:
   ```bash
   yams add - --name "descriptive-name" --tags "source,topic,date" --metadata "url=<source_url>"
   ```

3. **Tag Consistently**: Use hierarchical tags for easy retrieval:
   - Format: `project/<project-name>/<module>/<feature>/<date>`
   - Examples: `project/myapp/auth/jwt/2024-01-15`, `external/api-docs/stripe/v2`

### 1.2.2 YAMS Usage Patterns

**Before Starting Any Task:**
```bash
# Check for existing context
yams search "task <task-id>" --limit 5
yams search "<feature-name> implementation" --fuzzy
yams list --recent 20 --tags "module/<module-name>"
```

**When Researching External APIs or Documentation:**
```bash
# First check if we have it cached
yams search "<package-name> documentation" --limit 10

# If not found, search web then cache
# After web_search returns results:
yams add - --name "<package>-docs-<date>" \
  --tags "external,docs,<package>,cached" \
  --metadata "source=<url>,cached_date=$(date +%Y%m%d)"
```

**Before Making Architectural Decisions:**
```bash
# Search for previous decisions
yams search "decision <topic>" --fuzzy
yams search "architecture <component>" --limit 10
```

## 1.3 Architectural Compliance

> Rationale: Ensures all work aligns with architectural standards and references, with YAMS as the knowledge backbone.

- **Policy Reference**: This document adheres to the AI Coding Agent Policy Document.
- **Knowledge Management**: All research, decisions, and external documentation must be stored in YAMS.
- **Includes**:
  - All tasks must be explicitly defined and agreed upon before implementation.
  - All code changes must be associated with a specific task.
  - All PBIs must be aligned with the PRD when applicable.
  - All external research must be cached in YAMS before use.

# 2. Fundamental Principles

> Rationale: Establishes the foundational rules that govern all work, including knowledge management practices.

## 2.1 Core Principles

1. **Knowledge-First Development**: Check YAMS before any external search. Build and maintain a comprehensive project knowledge base.
2. **Task-Driven Development**: No code shall be changed in the codebase unless there is an agreed-upon task explicitly authorising that change.
3. **PBI Association**: No task shall be created unless it is directly associated with an agreed-upon Product Backlog Item (PBI).
4. **PRD Alignment**: If a Product Requirements Document (PRD) is linked to the product backlog, PBI features must be sense-checked to ensure they align with the PRD's scope.
5. **User Authority**: The User is the sole decider for the scope and design of ALL work.
6. **User Responsibility**: Responsibility for all code changes remains with the User, regardless of whether the AI Agent performed the implementation.
7. **Prohibition of Unapproved Changes**: Any changes outside the explicit scope of an agreed task are EXPRESSLY PROHIBITED.
8. **Task Status Synchronisation**: The status of tasks in the tasks index must always match the status in the individual task file.
9. **Controlled File Creation**: The AI_Agent shall not create any files outside explicitly defined structures without User confirmation.
10. **External Package Research and Documentation**:
    - **YAMS First**: Check YAMS for existing package documentation before web research
    - **Web + Cache**: If not in YAMS, research via web then immediately cache in YAMS
    - **Document Format**: Create `<task-id>-<package>-guide.md` with cached API information
    - **YAMS Tags**: Tag as `external/package/<package-name>/<version>`
11. **Task Granularity**: Tasks must be defined to be as small as practicable while still representing a cohesive, testable unit of work.
12. **Don't Repeat Yourself (DRY)**: Information should be defined in a single location and referenced elsewhere.
13. **Use of Constants for Repeated Values**: Any value used more than once must be defined as a named constant.
14. **Technical Documentation for APIs**: Must be created/updated and stored in both the project and YAMS.

## 2.2 Knowledge Management Workflow

### 2.2.1 Research Workflow
1. **Check YAMS First**:
   ```bash
   yams search "<topic>" --limit 20
   yams search "<topic>" --fuzzy --similarity 0.7
   ```

2. **If Not Found in YAMS**:
   - Use web_search or other tools
   - Immediately cache results in YAMS:
     ```bash
     yams add - --name "<topic>-research-$(date +%Y%m%d)" \
       --tags "research,<topic>,<source>" \
       --metadata "url=<source_url>,query=<search_query>"
     ```

3. **Reference YAMS Content**:
   - When using cached information, reference the YAMS hash
   - Include retrieval command in documentation

### 2.2.2 Decision Storage
All architectural and implementation decisions must be stored in YAMS:
```bash
yams add - --name "decision-<topic>-$(date +%Y%m%d)" \
  --tags "decision,architecture,<module>" \
  --metadata "pbi=<pbi-id>,task=<task-id>"
```

# 3. Product Backlog Item (PBI) Management

> Rationale: Defines how PBIs are managed with integrated knowledge management.

## 3.1 Overview

This section defines rules for Product Backlog Items (PBIs), ensuring clarity, consistency, and effective management throughout the project lifecycle with YAMS integration.

## 3.2 PBI Knowledge Management

Before creating or modifying any PBI:
1. Search YAMS for related PBIs and decisions:
   ```bash
   yams search "pbi <feature-area>" --limit 10
   yams search "requirement <topic>" --fuzzy
   ```

2. Store PBI decisions and research:
   ```bash
   yams add - --name "pbi-<id>-research" \
     --tags "pbi,<id>,research,<feature>" \
     --metadata "status=<status>,created=$(date +%Y%m%d)"
   ```

[Continue with existing PBI sections 3.3-3.6, adding YAMS integration notes where relevant]

# 4. Task Management with Knowledge Persistence

> Rationale: Defines how tasks are documented, executed, and tracked with YAMS integration for knowledge persistence.

## 4.1 Task Documentation and Knowledge

> Rationale: Specifies the structure and content required for task documentation with YAMS integration.

- **Pre-Task YAMS Check**:
  ```bash
  # Before starting any task
  yams search "task <task-id>" --limit 5
  yams search "<feature> implementation" --fuzzy
  ```

- **Task Knowledge Storage**:
  ```bash
  # Store task context and decisions
  yams add - --name "task-<task-id>-context" \
    --tags "task,<task-id>,<pbi-id>,<module>" \
    --metadata "status=<status>,updated=$(date +%Y%m%d)"
  ```

## 4.2 Principles

1. Each task must have its own dedicated markdown file.
2. Task files must follow the specified naming convention.
3. All required sections must be present and properly filled out.
4. When adding a task to the tasks index, its markdown file MUST be created immediately and linked.
5. Individual task files must link back to the tasks index.
6. **YAMS Integration**: All task-related research, decisions, and implementations must be stored in YAMS with appropriate tags.

## 4.3 Task Workflow with Knowledge Management

> Rationale: Describes the allowed status values and transitions for tasks with integrated YAMS operations.

### 4.3.1 Pre-Task YAMS Operations

Before starting any task work:
```bash
# Check for similar tasks or implementations
yams search "task similar to <description>" --fuzzy --limit 10

# Check for related feature implementations
yams search "<feature-name> implementation" --limit 20

# Review recent work in the module
yams list --recent 30 --tags "module/<module-name>"
```

## 4.4 Task Status Synchronisation

To maintain consistency across the codebase:

1. **Immediate Updates**: When a task's status changes, update both the task file and the tasks index in the same commit.
2. **Status History**: Always add an entry to the task's status history when changing status.
3. **Status Verification**: Before starting work on a task, verify its status in both locations.
4. **Status Mismatch**: If a status mismatch is found, immediately update both locations to the most recent status.
5. **YAMS Status Storage**: Store status changes in YAMS for audit trail:
   ```bash
   yams add - --name "task-<task-id>-status-change" \
     --tags "task,status-change,<task-id>" \
     --metadata "from=<old-status>,to=<new-status>,timestamp=$(date +%Y%m%d-%H%M%S)"
   ```

Example of a status update with YAMS storage:
```bash
# After updating task file and index
echo "Task 1-7 status changed from Proposed to InProgress" | \
  yams add - --name "task-1-7-status-$(date +%Y%m%d-%H%M%S)" \
  --tags "task,1-7,status-change" \
  --metadata "from=Proposed,to=InProgress,user=Julian"
```

## 4.5 Status Definitions
- **task_status(Proposed)**: The initial state of a newly defined task.
- **task_status(Agreed)**: The User has approved the task description and its place in the priority list.
- **task_status(InProgress)**: The AI Agent is actively working on this task.
- **task_status(Review)**: The AI Agent has completed the work and it awaits User validation.
- **task_status(Done)**: The User has reviewed and approved the task's implementation.
- **task_status(Blocked)**: The task cannot proceed due to an external dependency or issue.

## 4.6 Event Transitions with YAMS Integration

- **event_transition on "user_approves" from Proposed to Agreed**:
  1. Verify task description is clear and complete.
  2. Ensure task is properly prioritized in the backlog.
  3. Create task documentation file following the _template.md pattern and link it.
  4. **YAMS Operations**:
     ```bash
     # Store task approval
     yams add - --name "task-<task-id>-approved" \
       --tags "task,<task-id>,approved,<pbi-id>" \
       --metadata "approved_by=<user>,date=$(date +%Y%m%d)"

     # Search for similar approved tasks for patterns
     yams search "task approved <feature-area>" --limit 10
     ```
  5. Log status change in task history.

- **event_transition on "start_work" from Agreed to InProgress**:
  1. Verify no other tasks are InProgress for the same PBI.
  2. **YAMS Pre-Work Check**:
     ```bash
     # Check for implementation patterns
     yams search "<task-description> implementation" --fuzzy

     # Store work commencement
     yams add - --name "task-<task-id>-started" \
       --tags "task,<task-id>,started,work-log" \
       --metadata "started=$(date +%Y%m%d-%H%M%S)"
     ```
  3. Create a new branch for the task if using version control.
  4. Log start time and assignee in task history.

- **event_transition on "submit_for_review" from InProgress to Review**:
  1. Ensure all task requirements are met.
  2. Run all relevant tests and ensure they pass.
  3. **YAMS Implementation Storage**:
     ```bash
     # Store implementation summary
     yams add - --name "task-<task-id>-implementation" \
       --tags "task,<task-id>,implementation,<module>" \
       --metadata "files_modified=<count>,tests_added=<count>"

     # Store key code snippets if novel patterns
     yams add - --name "task-<task-id>-code-pattern" \
       --tags "code-pattern,<task-id>,<pattern-type>"
     ```
  4. Update task documentation with implementation details.
  5. Create a pull request or mark as ready for review.
  6. Log submission for review in task history.

- **event_transition on "approve" from Review to Done**:
  1. Verify all acceptance criteria are met.
  2. **YAMS Completion Storage**:
     ```bash
     # Store completion record
     yams add - --name "task-<task-id>-completed" \
       --tags "task,<task-id>,completed,<pbi-id>" \
       --metadata "completed=$(date +%Y%m%d),reviewer=<user>"

     # Store lessons learned or patterns discovered
     yams add - --name "task-<task-id>-learnings" \
       --tags "learnings,<task-id>,<module>"
     ```
  3. Merge changes to the main branch if applicable.
  4. Update task documentation with completion details.
  5. **Review Next Task Relevance**: Check YAMS for related tasks before proceeding.
  6. Archive task documentation as needed.

- **event_transition on "reject" from Review to InProgress**:
  1. Document the reason for rejection in task history.
  2. **YAMS Rejection Storage**:
     ```bash
     # Store rejection feedback for learning
     yams add - --name "task-<task-id>-rejection" \
       --tags "task,<task-id>,rejection,feedback" \
       --metadata "rejected_by=<user>,reason=<category>"
     ```
  3. Update task documentation with review feedback.
  4. Search YAMS for similar rejections to identify patterns.

- **event_transition on "mark_blocked" from InProgress to Blocked**:
  1. Document the reason for blocking in task history.
  2. **YAMS Blocker Storage**:
     ```bash
     # Store blocker information
     yams add - --name "task-<task-id>-blocked" \
       --tags "task,<task-id>,blocked,<blocker-type>" \
       --metadata "blocked_by=<dependency>,date=$(date +%Y%m%d)"

     # Search for similar blockers and their resolutions
     yams search "blocked <blocker-type> resolved" --limit 10
     ```
  3. Identify dependencies causing the block.
  4. Consider creating new tasks to address blockers.

## 4.7 One In Progress Task Limit

Only one task per PBI should be 'InProgress' at any given time. Before starting a new task:
```bash
# Check for other in-progress tasks
yams search "task status InProgress pbi <pbi-id>" --limit 5
```

## 4.8 Task History Log with YAMS

> Rationale: Specifies how all changes to tasks are recorded in both markdown files and YAMS.

- **Location Description**: Task change history is maintained in:
  1. The task's markdown file under 'Status History' section
  2. YAMS for searchable history across all tasks

- **YAMS History Storage Pattern**:
  ```bash
  # For every history entry, also store in YAMS
  yams add - --name "task-history-<task-id>-$(date +%Y%m%d-%H%M%S)" \
    --tags "task-history,<task-id>,<event-type>" \
    --metadata "from=<from-status>,to=<to-status>,user=<user>"
  ```

### 4.8.1 Format Example

```markdown
| Timestamp | Event Type | From Status | To Status | Details | User |
|-----------|------------|-------------|-----------|---------|------|
| 2025-05-16 15:30:00 | Status Change | Proposed | Agreed | Task approved by Product Owner | johndoe |
```

Corresponding YAMS storage:
```bash
echo "Task approved by Product Owner" | \
  yams add - --name "task-1-1-history-20250516-153000" \
  --tags "task-history,1-1,status-change,approval" \
  --metadata "from=Proposed,to=Agreed,user=johndoe,timestamp=2025-05-16T15:30:00"
```

### 4.8.2 Task Validation Rules with YAMS

> Rationale: Ensures all tasks adhere to required standards with YAMS verification.

1. **Core Rules**:
   - All tasks must be associated with an existing PBI
   - Task IDs must be unique within their parent PBI
   - **YAMS Verification**: Before creating a task, search YAMS to ensure uniqueness:
     ```bash
     yams search "task id <task-id>" --limit 1
     ```
   - Tasks must follow the defined workflow states
   - All task knowledge must be stored in YAMS

2. **Pre-Implementation Checks**:
   - Verify the task exists and is in correct status
   - **YAMS Implementation Search**:
     ```bash
     # Search for similar implementations
     yams search "<feature> implementation example" --fuzzy

     # Check for known issues or gotchas
     yams search "<technology> common issues" --limit 10
     ```
   - Document the task ID in all related changes
   - List all files that will be modified

3. **Error Prevention**:
   - If unable to access required files, stop and report
   - **YAMS Error Pattern Search**:
     ```bash
     # Search for similar errors and solutions
     yams search "error <error-type> solution" --fuzzy --limit 10
     ```
   - For protected files, provide changes in applicable format
   - Document all status checks in task history

4. **Change Management**:
   - Reference the task ID in all commit messages
   - **YAMS Change Storage**:
     ```bash
     # Store significant changes
     yams add - --name "task-<task-id>-change-<type>" \
       --tags "task,<task-id>,change,<change-type>" \
       --metadata "files=<count>,lines=<count>"
     ```
   - Update task status according to workflow
   - Ensure all changes are properly linked

### 4.9 Version Control for Task Completion

> Rationale: Ensures consistent version control practices with YAMS integration.

1. **Commit Message Format**:
   - When a task moves from `Review` to `Done`, create a commit:
     ```
     <task_id> <task_description>
     ```
   - **YAMS Commit Storage**:
     ```bash
     # Store commit information
     yams add - --name "task-<task-id>-commit" \
       --tags "task,<task-id>,commit,completed" \
       --metadata "commit_hash=<hash>,branch=<branch>"
     ```

2. **Pull Request**:
   - Title: `[<task_id>] <task_description>`
   - Include link to task in description
   - **YAMS PR Storage**:
     ```bash
     # Store PR information for future reference
     yams add - --name "task-<task-id>-pr" \
       --tags "task,<task-id>,pull-request" \
       --metadata "pr_number=<number>,url=<pr-url>"
     ```

3. **Automation**:
   - When task marked as `Done`, run:
     ```bash
     git acp "<task_id> <task_description>"

     # Also store in YAMS
     echo "Task <task_id> completed and pushed" | \
       yams add - --name "task-<task-id>-deployment" \
       --tags "task,<task-id>,deployed"
     ```

4. **Verification**:
   - Ensure commit appears in task history
   - Confirm task status updated in both locations
   - **YAMS Verification**:
     ```bash
     # Verify task completion is stored
     yams search "task <task-id> completed" --limit 1
     ```

## 4.10 Task Index File

> Rationale: Defines the standard structure for PBI-specific task index files with YAMS integration.

*   **Location Pattern**: `docs/delivery/<PBI-ID>/tasks.md`
*   **Purpose**: To list all tasks associated with a specific PBI
*   **YAMS Synchronization**:
    ```bash
    # When creating/updating task index, also store in YAMS
    yams add docs/delivery/<PBI-ID>/tasks.md \
      --name "pbi-<PBI-ID>-task-index" \
      --tags "pbi,<PBI-ID>,task-index,current" \
      --metadata "task_count=<count>,last_updated=$(date +%Y%m%d)"
    ```

*   **Required Sections and Content**:
    1.  **Title**: `# Tasks for PBI <PBI-ID>: <PBI Title>`
    2.  **Introduction Line**: `This document lists all tasks associated with PBI <PBI-ID>.`
    3.  **Link to Parent PBI**: `**Parent PBI**: [PBI <PBI-ID>: <PBI Title>](./prd.md)`
    4.  **Task Summary Section Header**: `## Task Summary`
    5.  **Task Summary Table** with proper formatting

*   **YAMS Task List Query**:
    ```bash
    # To verify task list completeness
    yams search "task pbi <PBI-ID>" --limit 50
    ```

# 5. Testing Strategy with Knowledge Base

> Rationale: Ensures testing approaches are documented and retrievable via YAMS.

## 5.1 Test Knowledge Management

1. **Check for Existing Test Patterns**:
   ```bash
   yams search "test <feature>" --limit 10
   yams search "test pattern <type>" --fuzzy
   ```

2. **Store Test Strategies**:
   ```bash
   yams add - --name "test-strategy-<pbi-id>" \
     --tags "test,strategy,<pbi-id>,<test-type>" \
     --metadata "coverage=<percentage>,type=<unit|integration|e2e>"
   ```

[Continue with existing testing sections]

# 6. YAMS Command Reference (Quick Access)

## 6.1 Essential Commands for Development

### Search Operations (Always Do First)
```bash
# General search
yams search "<query>" --limit 20

# Fuzzy search for approximate matches
yams search "<query>" --fuzzy --similarity 0.7

# List recent entries
yams list --recent 20
```

### Storage Operations (After Web Searches)
```bash
# Store web search results
yams add - --name "web-<topic>-$(date +%Y%m%d)" \
  --tags "web,cache,<topic>" \
  --metadata "source=<url>"

# Store file
yams add <file> --tags "<tags>"

# Store with custom metadata
yams add - --metadata "key1=value1" --metadata "key2=value2"
```

### Retrieval Operations
```bash
# Get specific content
yams get <hash>

# Get to file
yams get <hash> -o output.txt
```

## 6.2 Development Workflow Integration

### Before Any External Search:
1. `yams search "<topic>" --limit 20`
2. `yams search "<topic>" --fuzzy` (if exact match fails)
3. Only use web_search if YAMS returns no relevant results

### After Any External Search:
1. Store the results immediately in YAMS
2. Tag appropriately for future retrieval
3. Include source URL in metadata

### Project Context Storage:
```bash
# Store current project state
yams add - --name "project-state-$(date +%Y%m%d)" \
  --tags "project,state,checkpoint" < current_state.md

# Store working implementations
yams add <file> --tags "working,<module>,v<version>"
```
