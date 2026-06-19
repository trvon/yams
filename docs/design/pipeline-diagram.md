# YAMS Pipeline Architecture

This page gives a high-level view of how YAMS moves data through ingestion,
retrieval, and repair. It is intended as an orientation guide for operators and
contributors who want to understand the main components without reading the
implementation first.

## Ingestion Pipeline

`yams add` accepts a file, directory, or inline document and moves it through a
small set of durable stages. The content store owns bytes and reference counts,
the metadata repository owns document visibility, and post-ingest workers handle
secondary enrichment such as text extraction, graph updates, and embeddings.

```text
  yams add
      │
      ▼
  ┌──────────────────┐
  │ Request handling │
  │ CLI / daemon RPC │
  └──────────────────┘
      │
      ▼
  ┌──────────────────┐
  │  IngestService   │
  │ queue + routing  │
  └──────────────────┘
      │
      ▼
  ┌──────────────────┐
  │ DocumentService  │
  │ content + metadata
  └──────────────────┘
      │
      ├──▶ ContentStore
      │       chunks, manifests, reference counts
      │
      ├──▶ MetadataRepository
      │       document row, tags, snapshot/path metadata
      │
      ▼
  ┌──────────────────┐
  │ PostIngestQueue  │
  │ enrichment work  │
  └──────────────────┘
      │
      ├──▶ Content index / FTS
      ├──▶ Knowledge graph
      ├──▶ Symbol and entity extraction
      └──▶ Embeddings and semantic graph updates
```

### Ingestion guarantees

- Content bytes are stored before document metadata is published.
- Reference-count updates complete before content-store metadata is published.
- A successful synchronous store returns after the metadata row is visible.
- Post-ingest work is asynchronous where possible, so graph and embedding work can
overlap with later ingestion.
- Per-document options such as session tags and `--no-embeddings` are carried into
post-ingest routing.

## Retrieval Pipeline

Search combines lexical, vector, and graph-oriented signals. The exact mix depends
on the command, query options, available embeddings, and corpus state.

```text
  yams search / grep / get
      │
      ▼
  ┌──────────────────┐
  │ Query parsing    │
  │ filters + limits │
  └──────────────────┘
      │
      ├───────────────┬────────────────┐
      ▼               ▼                ▼
  ┌───────────┐  ┌────────────┐  ┌──────────────┐
  │ Lexical   │  │ Vector     │  │ Metadata /   │
  │ FTS5      │  │ embeddings │  │ path filters │
  └───────────┘  └────────────┘  └──────────────┘
      │               │                │
      └───────────────┴────────────────┘
                      │
                      ▼
              ┌──────────────────┐
              │ Result fusion    │
              │ scoring + merge  │
              └──────────────────┘
                      │
                      ▼
              ┌──────────────────┐
              │ Optional graph   │
              │ and topology use │
              └──────────────────┘
                      │
                      ▼
              ┌──────────────────┐
              │ Final results    │
              └──────────────────┘
```

### Retrieval behavior

- Lexical search provides exact and near-exact text matching through the content
index.
- Vector search contributes semantic matches when an embedding backend is available
and the corpus has indexed vectors.
- Metadata and path filters narrow candidates before or during ranking.
- Graph and topology data can improve navigation between related documents, path
versions, and semantic neighborhoods.

## Repair Pipeline

Repair jobs reconcile derived state when indexes, embeddings, graph data, or path
metadata need to be rebuilt. They reuse the same post-ingest infrastructure where
possible so repair behavior stays close to normal ingestion behavior.

```text
  yams doctor / yams repair
      │
      ▼
  ┌──────────────────┐
  │ RepairService    │
  │ plan + schedule  │
  └──────────────────┘
      │
      ├──▶ Health checks
      ├──▶ Repair plan builder
      ├──▶ Metadata/index repair
      └──▶ PostIngestQueue for enrichment rebuilds
```

Repair may enqueue work at higher priority than background ingestion when it is
serving an explicit user request. Long-running repair tasks should still preserve
normal daemon responsiveness and report progress through daemon status surfaces.

## Benchmark and Profiling Helpers

YAMS includes benchmark helpers for development and release validation. These are
not required for normal use, but they are useful when comparing ingestion or
retrieval behavior across changes.

| Helper | Purpose |
|--------|---------|
| `tests/benchmarks/search/ingestion_e2e_bench.cpp` | End-to-end ingestion pipeline benchmark |
| `tests/benchmarks/search/retrieval_quality_bench.cpp` | Retrieval quality and latency benchmark |
| `tests/benchmarks/scripts/live_benchmark_bundle.sh` | Collects benchmark logs, trace output, and run metadata |
| `tests/benchmarks/scripts/live_mirror_suite.sh` | Runs ingestion and retrieval checks as one evidence bundle |
| `tests/benchmarks/scripts/retrieval_opt_loop.sh` | Explores retrieval scoring candidates and records results |

Benchmark artifacts should be interpreted with their fidelity level in mind:

- `daemon-faithful` runs use real daemon services and production-like components.
- `live-ish` runs exercise most of the real path but may use smaller corpora or
controlled startup conditions.
- `synthetic` runs use mock or generated data and are best for fast comparisons,
not final performance claims.

## Operational Notes

- Ingestion throughput is usually limited by durable writes, reference-count
updates, metadata publication, and post-ingest content-index commits.
- Retrieval quality depends on corpus size, embedding availability, and whether
post-ingest enrichment has completed.
- Repair and ingestion share downstream enrichment paths, so large repair jobs can
affect queue depth and should be monitored like bulk ingestion.
- Performance claims should be based on repeated benchmark runs with comparable
configuration, corpus size, and backend fidelity.
