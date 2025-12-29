# YAMS Tutorials & Examples

A index of task-oriented tutorials for YAMS. Short, practical, copy-paste friendly.

## Prerequisites
- Installed YAMS: see [Installation](../installation.md)
- Know the basics: [CLI Reference](../cli.md)

## TL;DR (instant demo)
```bash
# Initialize (interactive with grammar download prompts)
yams init

# Or auto mode (downloads recommended grammars)
yams init --auto

# Or use a per-project storage root (keeps data isolated)
export YAMS_STORAGE="$PWD/.yams"
yams init --non-interactive

# Import all Markdown files under the repo
yams add . --recursive --include="*.md"

# Explore (table, json, csv, minimal)
yams list --format table --limit 20

# Search (keyword or fuzzy)
yams search "configuration"
yams search "config file" --fuzzy --similarity 0.65 --limit 10

# Retrieve and delete
yams get <hash> -o ./restored.bin
yams delete <hash> --force
```

Pro tips:
- Full verbose help: `yams --help-all`
- Per-command verbose help: `yams <command> --help --verbose`
- Switch storage roots per shell/session by changing `YAMS_STORAGE`.

---

## Tutorials (Index)

These guides are short, practical walkthroughs. They’ll be added incrementally; meanwhile, use the TL;DR above and the CLI reference for complete flags.

1) **Quickstart: Add, Search, Retrieve** (coming soon)
   - Goal: ingest a folder of docs, list with rich metadata, search and retrieve.
   - Concepts: storage init, bulk add, list formats, keyword/fuzzy search.

2) **Bulk Import with Filters** (coming soon)
   - Goal: import only certain file types/extensions and tag them on the way in.
   - Concepts: `--include`, `--recursive`, tagging, metadata KV pairs.

3) **Scripting YAMS with JSON & jq** (coming soon)
   - Goal: pipe machine-readable results into jq or scripts for automation.
   - Concepts: `--format json`, selecting fields, composing commands.

4) **Fuzzy Search & Snippets** (coming soon)
   - Goal: approximate matches with readable snippets and highlights.
   - Concepts: `--fuzzy`, `--similarity`, result interpretation.

5) **Semantic & Hybrid Search** (coming soon)
   - Goal: meaning-based discovery and hybrid strategies.
   - Concepts: vector search configuration and query best practices.

6) **Per-Project Storage Isolation** (coming soon)
   - Goal: maintain independent data roots for different repos/projects.
   - Concepts: `YAMS_STORAGE`, environment scoping, cleanup patterns.

7) **Symbol Extraction Setup** (coming soon)
   - Goal: set up tree-sitter grammars for code symbol extraction.
   - Concepts: `yams init --auto`, grammar downloads, supported languages.

8) **MCP Server: Claude Integration** (coming soon)
   - Goal: configure YAMS as an MCP server for Claude Desktop.
   - Concepts: `yams serve`, claude_desktop_config.json, tools.

9) **Docker Deployment** (coming soon)
   - Goal: run YAMS in a container with persistent storage.
   - Concepts: volume mounts, environment variables, MCP over stdio.

---

## Patterns you can use today

**Tagging and metadata on ingest:**
```bash
yams add ./doc.pdf --tags "research,paper" --metadata "author=Jane Doe" --metadata "topic=IR"
```

**Batch indexing source code:**
```bash
yams add src/ --recursive --include="*.cpp,*.hpp,*.h" --tags "code,source"
yams add include/ --recursive --include="*.hpp,*.h" --tags "code,headers"
```

**Minimal lists for scripting:**
```bash
yams list --format minimal --limit 100 | while read -r h; do
  echo "Found: $h"
done
```

**Per-project isolation:**
```bash
YAMS_STORAGE="$PWD/.yams-projectA" yams init --non-interactive
YAMS_STORAGE="$PWD/.yams-projectA" yams add ./notes.txt
```

**MCP server for AI assistants:**
```bash
# Start MCP server (stdio transport)
yams serve

# Test with JSON-RPC
echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | yams serve
```

---

## See also
- CLI Reference: [../cli.md](../cli.md)
- MCP Guide: [../mcp.md](../mcp.md)
- Embeddings: [../embeddings.md](../embeddings.md)
- Admin/Operations: [../../admin/operations.md](../../admin/operations.md)

---

## Links
- **Docs:** [yamsmemory.ai](https://yamsmemory.ai)
- **GitHub:** [github.com/trvon/yams](https://github.com/trvon/yams)
- **Discord:** [discord.gg/rTBmRHdTEc](https://discord.gg/rTBmRHdTEc)

---

## Contributing a Tutorial
- Keep it brief (goal → steps → verification).
- Prefer copy-paste blocks.
- Link to CLI reference instead of duplicating long flag lists.
- Submit new tutorials as `docs/user_guide/tutorials/<topic>.md` and add them to the index above.