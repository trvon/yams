# Roadmap

YAMS is pre-1.0. The roadmap describes direction, not a compatibility promise.

## Principles

- Local-first: a corpus remains usable without an account or hosted service.
- Portable: users can inspect, export, move, and delete their data.
- Measured: retrieval and performance changes require reproducible evidence.
- Small surface: command help, schemas, tests, and benchmarks are canonical.
- Interoperable: CLI, MCP, mobile, and plugins share the same corpus semantics.

## Current focus

- **Complete grep.** An unscoped `yams grep` on a large corpus can stop at its
  time budget because it reads every candidate's stored text. A substring index
  would let it pick candidates first.
- **Learn from real queries.** The local query log records searches. Once it has
  enough, the topology router can be refit on real questions instead of
  document titles, and query rewriting (predicting a document vector from the
  query) can be tested against the same baseline.
- **Watching that can stay on.** Sessions and `yams watch` now follow every
  enabled project, persist what they have seen and back off when idle; next is
  measuring their cost on constrained machines.
- Make daemon lifecycle and recovery predictable under load and crashes.
- Keep Simeon as the primary embedding backend, and promote retrieval changes
  only when they beat the default across corpora.
- Harden mobile corpus access through the stable local C ABI.

## Peer-to-peer corpus sharing

Goal: direct, selective sharing between corpora, with no central service
owning either one. Whole-corpus sync between daemons shipped in 0.20.0
([p2p.md](p2p.md)). Still open:

- Collection-, tag- and path-scoped manifests with a dry-run preview.
- Peer discovery. Peers are added by hand with `yams p2p enroll`.
- Conflict-preserving metadata and graph merge. Sync is last-writer-wins.

## Before 1.0

- Define and test the stable corpus, plugin, and mobile ABI boundaries.
- Establish multi-corpus retrieval quality gates and release baselines.
- Complete export/import with tags, relationships, and provenance intact.
- Separate fast correctness tests from explicit stress and soak lanes.
- Ship reproducible artifacts for supported operating systems and architectures.

## Later

- Scoped peer-to-peer sharing between teams
- Cross-repository retrieval federation
- Offline mobile corpus import, inspection, search, export, and deletion
- User-controlled context exchange between agents and applications

Completed work belongs in release history, benchmarks, and source control—not
as a growing archive of roadmap prose.
