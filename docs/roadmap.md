# Roadmap

YAMS is pre-1.0. The roadmap describes direction, not a compatibility promise.

## Principles

- Local-first: a corpus remains usable without an account or hosted service.
- Portable: users can inspect, export, move, and delete their data.
- Measured: retrieval and performance changes require reproducible evidence.
- Small surface: command help, schemas, tests, and benchmarks are canonical.
- Interoperable: CLI, MCP, mobile, and plugins share the same corpus semantics.

## Current focus

- Make daemon lifecycle and recovery predictable under load and crashes.
- Improve search, graph, and grep retrieval quality per token returned.
- Keep Simeon as the primary embedding and retrieval optimization backend.
- Align candidate generation, fusion, and reranking with measured query classes.
- Harden mobile corpus access through the stable local C ABI.
- Reduce duplicated infrastructure, configuration, and documentation.

## Peer-to-peer corpus sharing

The long-term goal is direct, selective sharing between YAMS corpora without a
central service becoming the owner of either corpus.

The first usable slice should provide:

1. Content-addressed exchange so existing blobs are never transferred twice.
2. Explicit peer identity and confirmation before any corpus is shared.
3. Collection-, tag-, and path-scoped manifests with a dry-run preview.
4. Authenticated and encrypted transport.
5. Resumable synchronization with integrity verification.
6. Conflict-preserving metadata and graph merge semantics.
7. Revocation and local deletion that do not depend on a remote account.

Peer discovery, transport, and conflict policy remain design work. Until those
contracts are tested, YAMS will not claim cross-device synchronization.

## Before 1.0

- Define and test the stable corpus, plugin, and mobile ABI boundaries.
- Establish multi-corpus retrieval quality gates and release baselines.
- Complete export/import with tags, relationships, and provenance intact.
- Separate fast correctness tests from explicit stress and soak lanes.
- Ship reproducible artifacts for supported operating systems and architectures.

## Later

- Peer-to-peer corpus replication and selective team sharing
- Cross-repository retrieval federation
- Offline mobile corpus import, inspection, search, export, and deletion
- User-controlled context exchange between agents and applications

Completed work belongs in release history, benchmarks, and source control—not
as a growing archive of roadmap prose.
