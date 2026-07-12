# Evidence search pipeline

## Status and rollback boundary

`ClassicSearchPipeline` and `ResultFusion` remain the product default and the behavioral reference.
The alternative is selected only with typed configuration:

```toml
[search]
candidate_pipeline = "evidence"

[search.topology]
evidence_weight = 0.02
```

Removing the key restores the classic candidate path. No product environment variable selects the
implementation. The xplan harness uses `YAMS_BENCH_CANDIDATE_PIPELINE` only to write this key into
an isolated per-arm config.

## Formulation

The evidence path keeps candidate generation and ranking as separate obligations:

1. lexical, vector, graph, path, tag, and metadata legs generate bounded candidates;
2. candidates are deduplicated by document identity while retaining typed source evidence;
3. source contributions are fused with a soft weighted reciprocal score;
4. the configured lexical rank floor is materialized as typed lexical evidence, so later rescue and
   rerank stages observe the same signal used by fusion;
5. an optional topology snapshot annotates candidates already present in the pool;
6. a reranker may change scores, but must preserve the candidate multiset;
7. top-k is the only stage allowed to discard a candidate.

This establishes executable no-invention and no-premature-removal contracts. It also prevents a
topology route from silently replacing global ANN in the new implementation.

## Topology behavior

When the evidence pipeline and topology assist are both enabled, the engine:

- queries global ANN exactly as it does without topology;
- preserves lexical and non-vector fallback candidates;
- obtains route membership from the cached, prevalidated topology snapshot;
- disables the old per-cluster exact member reranker;
- converts route score, relative boundary margin, seed coverage, membership, and medoid status into
  a bounded additive score adjustment;
- intersects those annotations with the existing candidate pool, so topology cannot invent or
  remove a document.

Low-confidence abstention and empty routes produce no topology evidence. The configured
`search.topology.vector_policy` still controls the classic implementation; evidence mode is always
non-destructive while its empirical route-risk certificate is unfinished.

## Tests and experiment

`evidence_search_pipeline_catch2_test.cpp` covers typed aggregation, candidate identity, bounded
topology annotations, reranker preservation, and final-only truncation. Search-engine tests verify
that the classic frontend remains in place while the candidate implementation is selected through
typed config.

`search_evidence_pipeline_multicorp` runs three repeated arms in one SciFact + NFCorpus index:

- `classic`: current product behavior;
- `evidence`: new aggregation/fusion with topology disabled;
- `evidence_topology`: the same pipeline with candidate-only topology evidence.

Promotion requires per-corpus quality non-regression and acceptable p50/p95 latency. The classic
implementation remains default regardless of pooled improvement.

The worker validates experiment identity from every query trace. An arm declaring
`candidate_pipeline = "evidence"` fails if the tuned engine silently executes the classic path.

### R3 result

The decision-grade `evidence-typed-lexical-r3-v4` run produced:

| arm | MRR | SciFact MRR | NFCorpus MRR | recall@10 | post-fusion recall | p50 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| classic | 0.5516 | 0.6032 | 0.5000 | 0.4247 | 0.5300 | 17.83 |
| evidence | 0.5629 | 0.6246 | 0.5013 | 0.4449 | 0.5643 | 17.17 |
| evidence + topology | 0.5651 | 0.6268 | 0.5033 | 0.4448 | 0.5643 | 18.83 |

Evidence-only improved MRR by 2.05%, recall by 4.78%, post-fusion recall by 6.49%, and reduced the
ranking-loss query rate from 0.1833 to 0.1667. Both corpora were non-regressing in this mixed-index
gate. The topology annotation added 0.38% MRR relative to evidence-only, changed recall by less than
0.04%, and added about 1.67 ms p50. Its snapshot cache hit rate was 1.0 and exact member-distance
evaluations remained zero, so the residual cost is route lookup/projection rather than member
rescoring.

This run justifies broader evidence-pipeline transfer gates; it does not justify enabling topology
by default. Classic remains the product default and rollback reference until the evidence path
passes those gates.

## Deliberate next boundary

The current annotator consumes one selected membership union. It does not yet use the complete
multiscale overlap, persistence, bridge, cohesion, or density fields in `TopologyArtifactBatch`.
The next implementation increment should first transfer-gate the evidence pipeline on additional
corpora, then expose those topology values directly from the immutable snapshot and replace the
fixed evidence formula with one typed, benchmarked lever at a time. Hard narrowing must remain
unavailable in this path until the calibration/evaluation split supplies the empirical miss-risk
certificate described by `Yams.Topology.SelectiveRouting`.
