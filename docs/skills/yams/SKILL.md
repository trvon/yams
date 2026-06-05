---
name: yams
description: Code indexing, exact/semantic search, graph-assisted code navigation, and project memory
license: GPL-3.0
compatibility: opencode
metadata:
  tools: cli, mcp
  categories: search, indexing, memory, knowledge-graph
---

# YAMS Skill (agent.md)

## Quick Reference

```bash
# Status & Health
yams status                    # Check daemon and index status
yams daemon start              # Start background daemon
yams doctor                    # Diagnose issues

# Indexing
yams add <file>                # Index single file
yams add . -r --include "*.py" # Index directory recursively
yams watch                     # Auto-index on file changes

# Search (use grep first, search for semantic)
yams grep "pattern" --cwd .    # Code pattern search scoped to current project
yams grep -e "--flag" --cwd .  # Explicit pattern for leading '-' text
yams grep -g "*.cpp" "TODO"   # rg-style glob filtering
yams search "query"            # Semantic/hybrid search

# Graph
yams graph --explore <query>   # Agent context: symbols, relationships, snippets
yams graph --name <file>       # Raw file relationships
yams graph --list-types        # List node types with counts
yams graph --relations         # List relation types with counts
yams graph --search "pattern"  # Search nodes by label

# Agent storage
yams list --format json        # Scriptable list output
yams list --show-metadata      # Include metadata for work item
```

## Agent Memory Workflow

**YAMS is the single source of truth for agent memory and work item.**

### Required Metadata (Task Tracking)

Attach metadata to every `yams add`.

- `task` - short task slug (e.g., `list-json-refresh`)
- `phase` - `start` | `checkpoint` | `complete`
- `owner` - agent or author
- `source` - `code` | `note` | `decision` | `research`

### Index Project Files

```bash
# Index specific file types
yams add . -r --include "*.ts,*.tsx,*.js"

# Index with exclusions
yams add . -r --include "*.py" --exclude "venv/**,__pycache__/**"

# Index with metadata for tracking
yams add src/ -r --metadata "task=list-json-refresh,phase=checkpoint,owner=codex,source=code"
```

### Auto-Index with Watch

```bash
yams watch                     # Start watching current directory
yams watch --interval 2000     # Custom interval (ms)
yams watch --stop              # Stop watching
```

### Verify Indexing

```bash
yams status                    # Shows indexed file count
yams list --limit 10           # Recent indexed files
```

## Search Patterns

### Decision Tree

1. **Code patterns** → `yams grep` (fast, regex/literal; use `--cwd .` for repo scoping)
2. **Semantic/concept** → `yams search` (embeddings/hybrid)
3. **Codebase shape / blast radius** → `yams graph --explore` from a search/grep hit
4. **No results from grep** → Try `yams search`, then follow `graph_explore_hint` when present

### grep (Code Search)

```bash
# Exact pattern
yams grep "function authenticate"

# Regex pattern
yams grep "async.*await.*fetch"

# With context lines
yams grep "TODO" -A 2 -B 2

# Filter by extension
yams grep "import" --ext py

# rg-style glob filter (repeatable)
yams grep -g "src/**/*.cpp" "TODO"

# Scope to current working directory or an explicit directory
yams grep "TODO" --cwd .
yams grep "TODO" --cwd src/daemon

# Literal text (no regex)
yams grep "user?.name" -F

# Pattern starts with '-': use -- or explicit -e/--regexp
yams grep -- "--tags|foo" --include="docs/**/*.md"
yams grep --regexp "--tags|foo" -g "docs/**/*.md"
```

### search (Semantic Search)

```bash
# Concept search
yams search "error handling patterns"

# Hybrid search (default)
yams search "authentication flow" --type hybrid

# Limit results
yams search "database connection" --limit 5

# Filter by file type
yams search "API endpoint" --ext ts
```

### search (Metadata-Only)

```bash
# Force metadata/FTS path for structured metadata
yams search "task=example-task" --type keyword --limit 10

# Unique task selection (avoid collisions)
yams search "task=example-task" --type keyword --limit 20
# 2) List all used task values with counts

# Tag filters (tags are stored as metadata keys: tag:<name>)
yams search "plan" --type keyword --tags plan --limit 10
yams search "tagged logic" --type keyword --tags plan --limit 20
```

## Agent Storage

### Store Research

```bash
# Index documentation
curl -s "https://docs.example.com/api" | yams add - --name "api-docs.md" \
  --metadata "task=docs-cache,phase=checkpoint,owner=codex,source=research"

# Store with metadata
yams add notes.md --metadata "task=research-auth,phase=checkpoint,owner=codex,source=research"
```

### Store Decisions

```bash
# Pipe decision record
echo "## Decision: Use JWT for auth

### Context
Need stateless authentication for microservices.

### Decision
JWT with RS256, 15min expiry, refresh tokens.

### Rationale
Stateless, scalable, industry standard.
" | yams add - --name "decision-jwt-auth.md" \
  --metadata "task=auth-decision,phase=checkpoint,owner=codex,source=decision"
```

### Retrieve Knowledge

```bash
# Find related decisions
yams search "authentication decision"

# Find by metadata (JSON list is the source of truth)
yams list --format json --show-metadata \
  | jq '.documents[] | select(.metadata.task=="example-task")'

# Metadata + tags are separate in JSON output
yams list --format json --show-metadata \
  | jq '.documents[] | {name,metadata,tags}'
```

## Session Management

### Create Work Sessions

```bash
# Start named session
yams session start --name "feature-auth"

# List sessions
yams session ls

# Switch session
yams session use "feature-auth"

# Show current session
yams session show --json
```

### Session Scope

```bash
# Add files to session scope
yams session add --path "src/auth/**"

# Warm session cache (faster searches)
yams session warm --limit 100

# Search within session
yams search "login" --session
```

### Session Lifecycle

```bash
# Save session state
yams session save

# Load previous session
yams session load --name "feature-auth"

# Clear session cache
yams session clear

# End session
yams session close
```

## Graph Queries

Use graph after search/grep finds a likely entry point. Graph answers "what is connected to this?" and should guide local reads, not replace them.

### Agent Graph Context

```bash
# Preferred follow-up after search/grep hints: ranked symbols + line-numbered snippets
yams graph --explore "authenticateUser" --max-files 3

# Explore a file path when the result path is more useful than a symbol name
yams graph --explore src/auth/login.ts --max-files 8

# JSON for tool consumers
yams graph --explore "RequestHandler" --json
```

Notes:
- `yams search` and `yams grep` results may emit `graph_explore_hint`; run that exact command before broad local search.
- `--explore` is budgeted for agents: entry symbols, related files, relationship summaries, and line-numbered snippets.
- If `--explore` fails or looks stale, fall back to raw traversal plus local reads.

### Raw Graph Structure

```bash
# List available node types and relation types
yams graph --list-types
yams graph --relations

# Search nodes by label pattern (wildcards: * any chars, ? single char)
yams graph --search "*Controller*"
yams graph --search "auth*"
yams graph --search "handle?Request"

# List scoped node types
yams graph --list-type function --scope-cwd --limit 50
```

### File / Symbol Relationships

```bash
# Show file dependencies and symbols
yams graph --name src/auth/login.ts --depth 2 --limit 50

# Filter by relation type when doing blast-radius review
yams graph --name src/main.ts --relation includes --depth 1
yams graph --node-key "func:authenticate" --relation calls --depth 2

# Output as JSON or DOT
yams graph --name src/auth/login.ts --format json
yams graph --name src/auth/login.ts --format dot > graph.dot
```

Common relations: `calls`, `includes`, `contains`, `defined_in`, `located_in`, `has_version`, `semantic_neighbor`.

### Topology Navigation

```bash
# Find subsystem clusters and their medoid/bridge/core files
yams graph --topology-snapshots
yams graph --topology-clusters
yams graph --cluster <cluster-id>
```

Use topology when entering an unfamiliar subsystem. Start with medoids for representative files, bridges for cross-subsystem coupling, and core files for local implementation detail.

## MCP Integration

YAMS exposes tools via Model Context Protocol for programmatic access.

### Start MCP Server

```bash
yams serve                     # Start MCP server (quiet mode)
yams serve --verbose           # With logging
```

### Available MCP Tools (Code Mode)

The MCP server exposes a small composite tool surface.

| Tool | Purpose |
|------|---------|
| `query` | Read-only pipeline: `search`, `grep`, `list`, `list_collections`, `list_snapshots`, `graph`, `get`, `status`, `describe` |
| `execute` | Write batch: `add`, `update`, `delete`, `restore`, `download` |
| `session` | Session lifecycle: `start`, `stop`, `pin`, `unpin`, `watch` (extensions enabled) |
| `mcp.echo` | Echo utility |

Notes:
- `query` and `execute` accept arrays (`steps` / `operations`) and run them in order.
- Each `query.steps[i]` result is available to later steps via `$prev` (e.g., `$prev.results[0].hash`).
- Use `describe` to discover the parameter schema for an operation at runtime.

Example (pipeline: search -> get):

```json
{
  "name": "query",
  "arguments": {
    "steps": [
      {"op": "search", "params": {"query": "auth middleware", "limit": 1}},
      {"op": "get", "params": {"hash": "$prev.results[0].hash", "include_content": true}}
    ]
  }
}
```

### MCP Configuration

```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"]
    }
  }
}
```

### MCP Environment Toggles

| Variable | Effect |
|----------|--------|
| `YAMS_DISABLE_EXTENSIONS=1` | Disable YAMS extensions (removes `session`, disables some methods like `logging/setLevel`) |
| `YAMS_DAEMON_SOCKET=/path.sock` | Override daemon socket path used by MCP server |

## Troubleshooting

```bash
# Check daemon status
yams daemon status -d

# View daemon logs
yams daemon log -n 50

# Full diagnostic
yams doctor

# Repair index
yams doctor repair --all

# Fix embedding dimensions
yams doctor --fix-config-dims
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `YAMS_DATA_DIR` | Storage directory |
| `YAMS_DAEMON_SOCKET` | Daemon socket path override |
| `YAMS_LOG_LEVEL` | Logging verbosity |
| `YAMS_SESSION_CURRENT` | Default session |
docs/skills/yams/SKILL.md
