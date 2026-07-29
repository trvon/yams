# YAMS

YAMS is local-first, persistent memory for code, documents, applications, and
AI agents. It combines content-addressed storage with text, vector, graph, and
grep retrieval.

> **Experimental:** YAMS is pre-1.0 and may change without compatibility
> guarantees.

## Start

```bash
brew install trvon/yams/yams
yams init
echo "hello world" | yams add - --tags demo
yams search hello
```

For AI assistants:

```bash
yams serve
```

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

## Reference

- [Build from source](BUILD.md)
- [Benchmarks](benchmarks/README.md)
- [Roadmap](roadmap.md)
- [Newsletter](newsletter.md)

Command behavior is documented by `yams --help` and
`yams <command> --help`. Machine-readable interface contracts live with the
source.

## Project

- SourceHut: https://sr.ht/~trvon/yams/
- GitHub: https://github.com/trvon/yams
- Discord: https://discord.gg/rTBmRHdTEc
- License: GPL-3.0-or-later
