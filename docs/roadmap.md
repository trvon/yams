# Roadmap

YAMS (`0.23.1`) is approaching **1.0**. This roadmap tracks the concrete gates
to 1.0 across the core engine (CLI, daemon, MCP) and the `YAMS Memory` mobile
apps.

## Principles

- **Local-first:** a corpus remains usable without an account or hosted service.
- **Portable:** users can inspect, export, move, and delete their data.
- **Measured:** retrieval and performance changes require reproducible evidence
  ([benchmarks](benchmarks/index.md)).
- **Small surface:** command help, schemas, tests, and benchmarks are canonical.
- **Interoperable:** CLI, MCP, mobile, and plugins share the same corpus semantics.

## Path to Core 1.0 (Engine, CLI & MCP)

Already in place on `main` (`0.23.1`): default Simeon PQ-ADC hybrid retrieval,
libSQL MVCC backend, whole-corpus P2P sync ([p2p.md](p2p.md)), reproducible
BEIR and microbenchmark baselines, and the unified mobile C ABI.

Remaining 1.0 gates:

- **Freeze stable ABI & schema boundaries.** Lock the corpus storage format,
  plugin ABI, and mobile C ABI (`yams_mobile`) with forward-compatibility tests
  and migration guarantees.
- **Complete grep at scale.** Add a substring candidate index so unscoped
  `yams grep` on large corpora avoids linear scans against its time budget.
- **Full-provenance export/import.** Round-trip documents, tags, graph
  relationships, and provenance metadata cleanly between machines.
- **Predictable daemon lifecycle & low-cost watch.** Keep daemon recovery
  deterministic under crashes/load and validate `yams watch` overhead on
  constrained hosts.
- **Multi-corpus retrieval quality gates.** Keep Simeon as the default backend
  and promote routing or query-rewriting changes only when they beat the
  baseline across corpora.

## Path to Mobile 1.0 (`YAMS Memory`)

`YAMS Memory` embeds the native YAMS engine (`0.23.1`) directly on-device with
a local-first PDF reader, full-text indexing, unified `PDF | Text | Graph`
reading, offline neural Text-to-Speech, opt-in Paperbridge research search, and
Obsidian vault bridging ([design language](design.md), [privacy policy](privacy.md)).

- **Android v1.0 (Google Play):**
  - Release signing (`keystore.properties`), Play Store metadata & Data Safety
    declaration, and signed `.aab` release verification.
  - Adaptive launcher icon, Light/Dark/System theme parity, and foldable/DeX
    workspace polish.
- **iOS v1.1 (Apple App Store):**
  - Native Swift bridges for `PDFKit` rendering & text extraction,
    `AVSpeechSynthesizer` offline read-aloud, and iOS Files / Share Extension
    integration for feature parity with Android.
- **Monetization:** Free local-first core (`$0`, zero ads, zero telemetry).
  Optional policy-compliant In-App Purchase Supporter Tip Jar in a follow-up
  release; optional encrypted Sync/Relay later without ever gating local data.

## Peer-to-peer corpus sharing

Whole-corpus sync between daemons shipped in `0.20.0` ([p2p.md](p2p.md)).
Post-1.0 / ongoing P2P work:

- Collection-, tag-, and path-scoped manifests with a dry-run preview.
- Peer discovery beyond manual `yams p2p enroll`.
- Conflict-preserving metadata and graph merge (currently last-writer-wins).

## Post-1.0

- Scoped peer-to-peer sharing between teams and mobile-to-desktop relay sync
- Cross-repository retrieval federation
- Learning topology routing and query rewriting from opt-in local query logs
- User-controlled context exchange between agents and applications

Completed work belongs in release history, benchmarks, and source control—not
as a growing archive of roadmap prose.
