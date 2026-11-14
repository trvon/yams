# YAMS

**Local, persistent memory for code, documents and the people and agents who
work with them.**

YAMS stores what you give it once, keeps it on your machine, and finds it again
through full-text, semantic, graph and grep search. It runs as a small daemon
behind a CLI and an MCP server, so the same corpus serves your shell, your
editor and your coding agents.

> **Pre-1.0.** YAMS changes quickly and may break compatibility between
> releases. Release notes say when it does.

## Why YAMS

- **Your memory stays yours.** The corpus lives in a data directory you choose,
  in SQLite and plain files you can inspect, export or delete. There is no
  account, hosted service or model provider between you and what you stored.
- **Fast without a GPU.** The default embedding backend, Simeon, needs no model
  download and encodes about 9,000 documents a second through the daemon on a
  CPU. A MiniLM model manages about 460 on the same path
  ([details](newsletter.md#2026-04-25-simeon-is-now-the-default-embedding-backend)).
- **Measured retrieval.** Default hybrid search reaches 67.3 nDCG@10 and 79.1
  recall@10 on BEIR scifact. Every number on the [benchmarks](benchmarks/index.md)
  page says how it was produced.
- **Keeps up with your work.** `yams watch` keeps a project indexed as you edit,
  and `yams grep` searches the stored text of every file you added.

## Start

Install with Homebrew, APT, DNF, pacman, Docker or the Windows MSI; see
[Install and setup](install.md) for each channel.

```bash
brew install trvon/yams/yams
yams init

yams add ./notes --recursive --tags notes
yams search "how does the cache expire"
yams grep "TODO" --cwd .
```

To keep a project indexed while you work:

```bash
cd ~/src/my-project
yams watch
```

The [guide](guide.md) walks through adding, searching, retrieving and watching.

## Use it from an agent

`yams serve` exposes the corpus over MCP to any compatible tool:

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

## Where to go next

| Page | What it covers |
|---|---|
| [Install and setup](install.md) | Every channel, where files live, the daemon service, common settings |
| [Guide](guide.md) | Day-to-day use: add, search, grep, get, list, watch |
| [Retrieval reference](cli-retrieval.md) | Bounded output, scoping and repository freshness |
| [P2P corpus sync](p2p.md) | Direct daemon-to-daemon sync, no central service |
| [Benchmarks](benchmarks/index.md) | Current quality and cost numbers, and how to reproduce them |
| [Build from source](BUILD.md) | Meson, Conan, tests and the local quality gate |

Command behaviour is documented by `yams --help` and `yams <command> --help`.

## Project

- Source: [GitHub](https://github.com/trvon/yams), with a
  [self-hosted mirror](https://git.trevon.dev/trevon/yams)
- Chat: [Discord](https://discord.gg/rTBmRHdTEc)
- Updates: [newsletter](newsletter.md) and [release feed](https://github.com/trvon/yams/releases.atom)
- License: GPL-3.0-or-later
