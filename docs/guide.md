# Guide

Day-to-day use of YAMS from the command line. Every command here also takes
`--help`, which is the authoritative reference for its options.

YAMS runs a background daemon that owns the corpus. Commands start it when they
need it; [Install and setup](install.md#running-the-daemon-as-a-service) shows how
to run it as a login service instead.

## Add content

```bash
yams add ./design.md --tags design
yams add ./src --recursive --include "*.cpp,*.h" --tags code
cat meeting.txt | yams add - --name meeting-2026-11-13.txt --tags meetings
```

Content is stored once by its SHA-256 hash, so adding the same file twice costs
nothing. Directory adds honour the directory's `.gitignore`.

In a Git checkout, `yams add --changed` indexes only the tracked files that
changed since they were last added, and removes the ones that are gone.

## Search

```bash
yams search "how does the cache expire" --limit 5
yams search "retry policy" --tags code --cwd
```

`search` is hybrid by default: full-text and semantic scores are fused, and the
knowledge graph adds related documents. `--cwd` limits results to the current
directory; `--paths-only` prints one path per line for scripts.

## Grep

```bash
yams grep "ResourceGovernor" --cwd .
yams grep -l "TODO" --cwd .        # matching files only
yams grep -c "TODO" --cwd .        # matching lines per file
yams grep -F "a.b(c)"              # literal text, not a regular expression
```

`grep` searches the stored text of every document, including files that are no
longer on disk. A scan that is not scoped may stop at its time budget on a large
corpus; when it does, `grep` says so on stderr and in JSON output
(`"incomplete": true`). Scope it with `--cwd`, `--include` or a path to search
everything.

## Retrieve

```bash
yams get <hash>                    # content to stdout
yams get --name design.md -o design.md
yams cat <hash> --lines 40:80
yams --json get --hash <hash>      # content with hash, metadata and timestamps
```

JSON `get` always returns the complete original document, so it rejects
`--max-bytes`, `--lines` and `--around`; use `--metadata-only` for just the
metadata.

## List

```bash
yams list --recent 20
yams list --tags meetings --since 7d
yams list --indexed-after 2026-11-01 --format json
```

Time filters take ISO 8601 dates, relative windows such as `24h` or `7d`, and
words such as `yesterday`. A value YAMS cannot read is an error, not an
unfiltered list.

## Keep a project indexed

```bash
cd ~/src/my-project
yams watch            # start watching the Git root (or this directory)
yams watch stop
```

`yams watch` creates a session for the project and turns watching on. The
daemon then polls the tree and indexes files as they change:

- **Every watched session is followed**, not only the current one. `yams watch`
  leaves the current session alone unless you pass `--use`.
- **`.gitignore` files apply** at every level of the tree, including `!pattern`
  re-includes.
- **Removal waits.** A file that disappears is removed from the index after it
  has been gone for ten minutes, so switching branches does not churn the index.
- **Restarts are cheap.** What the watcher has seen is kept in the data
  directory, so a daemon restart does not re-index the tree.
- **It backs off.** A project that is not changing is polled less often, and
  watching pauses while the machine is under memory or CPU pressure.
- **Size limit.** Trees with more than 50,000 files are not watched; watch the
  source directory, or add a `.gitignore` for generated content.

`yams session ls` lists sessions, and `yams session rm <name>` removes one.

## Explore relationships

```bash
yams graph --explore "LifecycleComponent"
yams graph --relations
```

The graph links documents to their paths, versions, extracted entities and
semantically similar neighbours.

## Check health

```bash
yams status            # daemon state, data directory, document count
yams doctor            # connectivity and plugin checks
yams daemon log -n 50
```
