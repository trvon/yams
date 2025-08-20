# YAMS Tutorials & Examples

A index of task‑oriented tutorials for YAMS. Short, practical, copy‑paste friendly.

## Prerequisites
- Installed YAMS: see [Installation](../installation.md)
- Know the basics: [CLI Reference](../cli.md)
- Search concepts: [Search Guide](../search_guide.md), [Vector/Semantic Search](../vector_search_guide.md)

## TL;DR (instant demo)
```bash
# Use a per-project storage root (keeps data isolated)
export YAMS_STORAGE="$PWD/.yams"

# Initialize once (idempotent)
yams init --non-interactive

# Import all Markdown files under the repo
find . -type f -name '*.md' -print0 | xargs -0 -I {} yams add "{}"

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

1) Quickstart: Add, Search, Retrieve (coming soon)
- Goal: ingest a folder of docs, list with rich metadata, search and retrieve.
- Concepts: storage init, bulk add, list formats, keyword/fuzzy search.

2) Bulk Import with Filters (coming soon)
- Goal: import only certain file types/extensions and tag them on the way in.
- Concepts: find/xargs, tagging, metadata KV pairs.

3) Scripting YAMS with JSON & jq (coming soon)
- Goal: pipe machine‑readable results into jq or scripts for automation.
- Concepts: `--format json`, selecting fields, composing commands.

4) Fuzzy Search & Snippets (coming soon)
- Goal: approximate matches with readable snippets and highlights.
- Concepts: `--fuzzy`, `--similarity`, result interpretation.

5) Semantic & Hybrid Search (coming soon)
- Goal: meaning‑based discovery and hybrid strategies.
- Concepts: vector search configuration and query best practices.

6) Per‑Project Storage Isolation (coming soon)
- Goal: maintain independent data roots for different repos/projects.
- Concepts: `YAMS_STORAGE`, environment scoping, cleanup patterns.

7) Backups & Verification (coming soon)
- Goal: back up storage safely and verify integrity.
- Concepts: snapshotting storage, verifying content and metadata.

8) Browse (TUI) Power‑User Tips (coming soon)
- Goal: speed navigation, search toggles, and power keybindings.
- Concepts: FTXUI controls, fuzzy toggles, quick actions.

9) MCP Server: Stdio Smoke Test (coming soon)
- Goal: run `yams serve`, connect with a simple client, exchange a message.
- Concepts: transports, minimal JSON, quick client tooling (websocat/wscat).

---

## Patterns you can use today

- Tagging and metadata on ingest:
```bash
yams add ./doc.pdf --tags "research,paper" --metadata "author=Jane Doe" --metadata "topic=IR"
```

- Minimal lists for scripting:
```bash
yams list --format minimal --limit 100 | while read -r h; do
  echo "Found: $h"
done
```

- Per‑project isolation:
```bash
YAMS_STORAGE="$PWD/.yams-projectA" yams init --non-interactive
YAMS_STORAGE="$PWD/.yams-projectA" yams add ./notes.txt
```

---

## See also
- CLI Reference: [../cli.md](../cli.md)
- Search Guides: [../search_guide.md](../search_guide.md), [../vector_search_guide.md](../vector_search_guide.md)
- Admin/Config: [../../admin/configuration.md](../../admin/configuration.md)
- MCP: [../mcp.md](../mcp.md)

---

## Contributing a Tutorial
- Keep it brief (goal → steps → verification).
- Prefer copy‑paste blocks.
- Link to CLI reference instead of duplicating long flag lists.
- Submit new tutorials as `docs/user_guide/tutorials/<topic>.md` and add them to the index above.