# MCP Server

YAMS runs as a Model Context Protocol server over stdio (NDJSON, JSON-RPC 2.0), giving AI assistants read + write access to your content-addressed store.

## Run

```bash
yams serve                              # stdio transport
yams serve --verbose                    # logs on stderr
docker run -i ghcr.io/trvon/yams:latest serve
```

## Client configuration

| Client           | Config location                                               |
|------------------|---------------------------------------------------------------|
| Desktop MCP clients | Client-specific MCP server configuration                  |
| Continue.dev     | IDE config — `contextProviders` entry                         |
| Cursor / Zed / custom | Any MCP-compliant stdio client                           |

### Generic stdio client

```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"],
      "env": { "YAMS_STORAGE": "/path/to/your/data" }
    }
  }
}
```

### Continue.dev

```json
{
  "contextProviders": [
    {
      "name": "mcp",
      "params": { "serverCommand": "yams", "serverArgs": ["serve"] }
    }
  ]
}
```

Restart the client after editing its config.

### Docker

```bash
docker run -i --rm \
  -v ~/.local/share/yams:/data -e YAMS_STORAGE=/data \
  ghcr.io/trvon/yams:latest serve
```

`-i` is required — stdio transport needs an attached stdin.

## Tools

The server exposes **3 composite tools**. All operations are reached through these; this keeps AI context windows tight (~800 tokens vs. ~4,000 for one-tool-per-op designs).

| Tool      | Purpose             | Operations |
|-----------|---------------------|------------|
| `query`   | Read pipeline       | `search`, `grep`, `list`, `list_collections`, `list_snapshots`, `suggest_context`, `semantic_dedupe`, `graph`, `get`, `status`, `describe` |
| `execute` | Write batch         | `add`, `update`, `delete`, `restore`, `download` |
| `session` | Session lifecycle   | `start`, `stop`, `pin`, `unpin`, `watch` |

Discover operation parameter schemas at runtime with `describe`:

```json
{"name": "query", "arguments": {"steps": [{"op": "describe", "params": {"target": "search"}}]}}
```

Full operation schemas: [docs/api/mcp_tools.md](../api/mcp_tools.md). Protocol implementation: [`include/yams/mcp/mcp_server.h`](../../include/yams/mcp/mcp_server.h).

### `query` examples

```json
// search
{"name": "query", "arguments": {
  "steps": [{"op": "search", "params": {"query": "configuration management", "limit": 5}}]}}

// grep with regex + context
{"name": "query", "arguments": {
  "steps": [{"op": "grep", "params": {"pattern": "class\\s+\\w+Handler", "line_numbers": true, "context": 2}}]}}

// pipeline: search → fetch top hit (uses $prev)
{"name": "query", "arguments": {
  "steps": [
    {"op": "search", "params": {"query": "auth middleware", "limit": 1}},
    {"op": "get", "params": {"hash": "$prev.results[0].hash", "include_content": true}}
  ]}}

// advisory retrieval — ranked snapshot suggestions + supporting hits
{"name": "query", "arguments": {
  "steps": [{"op": "suggest_context", "params": {"query": "auth token refresh flow", "limit": 3}}]}}

// persisted semantic duplicate groups
{"name": "query", "arguments": {
  "steps": [{"op": "semantic_dedupe", "params": {"limit": 10}}]}}
```

`suggest_context` inside a `session` suppresses already-served snapshot sets for the same normalized query, so long-running sessions don't re-suggest the same context each turn.

### `execute` example

```json
{"name": "execute", "arguments": {
  "operations": [{"op": "add", "params": {"path": "/tmp/notes.md", "tags": ["meeting"]}}]}}
```

Stops on first error unless `continueOnError: true`.

### `session` example

```json
{"name": "session", "arguments": {"action": "start", "params": {"label": "review"}}}
```

## Prompt templates

`prompts/list` and `prompts/get` expose built-in templates (`search_codebase`, `summarize_document`, `rag/*`) plus any file-backed templates you drop in:

1. `YAMS_MCP_PROMPTS_DIR` (env override)
2. `[mcp_server].prompts_dir` in `config.toml`
3. `$XDG_DATA_HOME/yams/prompts` (`~/.local/share/yams/prompts`)
4. `./docs/prompts` when running from the repo

Filename `PROMPT-<name>.md` is exposed as `<name>` with dashes → underscores (e.g. `PROMPT-research-mcp.md` → `research_mcp`).

## Hot / cold modes

Control daemon path selection on server startup:

| Flag                  | Env                     | Values                              |
|-----------------------|-------------------------|-------------------------------------|
| `--list-mode`         | `YAMS_LIST_MODE`        | `hot_only` · `cold_only` · `auto`   |
| `--grep-mode`         | `YAMS_GREP_MODE`        | `hot_only` · `cold_only` · `auto`   |
| `--retrieval-mode`    | `YAMS_RETRIEVAL_MODE`   | `hot_only` · `cold_only` · `auto`   |

`paths_only: true` on list/search operations engages hot paths where supported and shrinks responses.

## Testing

```bash
# initialize
echo '{"jsonrpc":"2.0","method":"initialize","params":{"clientInfo":{"name":"test","version":"1.0"}},"id":1}' \
  | yams serve

# list tools — expect query, execute, session
echo '{"jsonrpc":"2.0","method":"tools/list","id":2}' | yams serve

# one-shot search via query
echo '{"jsonrpc":"2.0","method":"tools/call","id":3,"params":{"name":"query","arguments":{"steps":[{"op":"search","params":{"query":"test"}}]}}}' \
  | yams serve
```

Verbose trace:

```bash
export SPLOG_LEVEL=debug
yams serve --verbose 2>debug.log
```

Minimal Python and Node.js clients: drive the `yams serve` subprocess with newline-delimited JSON on stdin and read responses line-by-line from stdout. See [docs/api/mcp_tools.md](../api/mcp_tools.md) for end-to-end examples.

## Troubleshooting

| Symptom                              | Fix                                                          |
|--------------------------------------|--------------------------------------------------------------|
| Client times out on connect          | Pre-warm daemon: `yams status`; check protocol version (supported 2024-11-05 … 2025-06-18) |
| No `tools/list` response             | Client isn't sending `notifications/initialized` after `initialize` |
| Client doesn't list YAMS             | Check config path/JSON; use absolute `yams` path; restart the client |
| Docker container exits immediately   | Missing `-i` — stdio needs an attached stdin                 |
| `Ctrl+C` doesn't stop server         | Update to latest; else `kill -TERM <pid>`                    |

Readiness: during startup the daemon may report initializing. `yams status` and server responses indicate not-ready services and progress. If model preloading prolongs init, switch to lazy loading and re-check with `yams stats -v`.

## Security

- Stdio runs with the calling process's permissions — no network surface.
- Docker: pin a version tag (not `latest`), run non-root, use read-only mounts, and cap resources.
- Set `YAMS_STORAGE` explicitly to avoid surprises from per-user defaults.

## Performance

- Pre-index with `yams add` before exposing to an assistant.
- Use `paths_only: true` and tight `limit` values for large corpora.
- Pick the search type that fits: `keyword` for exact, `fuzzy` for approximate, `hybrid` for default.

## References

- [CLI reference](cli.md) · [Tool schemas](../api/mcp_tools.md) · [Protocol header](../../include/yams/mcp/mcp_server.h)
- Issues: https://github.com/trvon/yams/issues
