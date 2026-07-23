# Theorem-guided candidate expansion

Status: experimental; product-default topology routing remains disabled
Updated: 2026-07-22

## Decision

YAMS uses topology only as an additive candidate producer. The search engine keeps the global
Simeon result set and may add candidates reached from query-admissible seeds through a persisted,
typed relation. It does not use embedding geometry, cluster shape, or route confidence as a recall
certificate.

The implementation has three modes:

- `disabled`: run the unchanged global search path;
- `shadow`: produce and score the expansion, record theorem diagnostics, but return the unchanged
  global path; and
- `augment`: union scored expansion candidates with the global candidates before ordinary fusion.

Hard narrowing is not part of the candidate-expansion experiment and cannot be promoted from these
results.

## Formal contract

The active theorem is in `formal/topology/Yams/Topology/CandidateExpansion.lean`.

For a query `q`, let:

- `B(q)` be the baseline candidate list;
- `A(q)` be the selected query-admissible seeds;
- `R` be the persisted protected relation;
- `E(q)` be the independently scored documents reached from `A(q)` through `R`; and
- `P(q)` be the relevant documents for the evaluated query class.

The implementation constructs the additive candidate list

```text
C(q) = augment(B(q), E(q))
```

where `augment` preserves baseline order and appends only documents not already present. The Lean
development proves:

1. every baseline candidate remains present after augmentation;
2. a missing relevant document reachable from an admissible seed is present after augmentation;
3. enrichment of a document already in the baseline is distinct from a novel-document rescue; and
4. if the output budget preserves the baseline relevant candidates and at least one reachable
   missing relevant document survives, budgeted candidate recall strictly improves.

The theorem is conditional. It does not prove that semantic-neighbor edges contain a missing
relevant document, that a chosen seed is query-admissible, or that fusion preserves a rescued
document. Those are empirical obligations.

## Runtime boundary

The production path is intentionally small:

```text
PostIngestQueue / EmbeddingService
            |
            v
persisted semantic_neighbor graph
            |
            v
query-admissible baseline seeds --bounded traversal--> relation candidates
            |
            v
independent Simeon scoring
            |
            v
shadow observation or additive union before fusion
```

The daemon owns ingestion, embeddings, graph construction, and graph repair. Benchmark code waits
for the production queues to drain and invokes the production graph rebuild seam; it does not build
a parallel retrieval graph.

The search trace distinguishes:

- attempted versus applied expansion;
- all scored expansion candidates;
- novel candidates versus evidence enrichment of baseline candidates;
- relevant documents reachable from the selected seeds;
- relevant expansions surviving fusion and returned top-k; and
- preservation of baseline relevant candidates at the evaluated budget.

These observations map directly to the theorem. Route geometry, oracle candidate injection, and
counterfactual rerank-window placement are intentionally outside this experiment.

## Deterministic construction identity

Every comparison must use the same persisted graph. The benchmark records the exact topology
construction fingerprint, document/node counts, semantic-edge counts, connected-document count,
isolates, and drained daemon queue state. Cross-arm or cross-repeat identity drift invalidates the
run.

A daemon race previously allowed background semantic backfill to interleave with clear-and-rebuild.
`EmbeddingService` now serializes streaming updates, backfill discovery/write, and full rebuild
through one mutation boundary. This fix is part of measurement validity, not a search-quality lever.

## Current evidence

The daemon-native shadow run at
`build/benchmarks/search_candidate_rescue_graph/20260722-daemon-native-serialized-r13/`
completed three valid SciFact repeats with the same construction fingerprint
`2fd5ab4da463e241`.

The decision pair at
`build/benchmarks/search_candidate_rescue_graph/20260722-daemon-native-serialized-r14/`
compared global Simeon `c32` with active graph rescue `c32+16`, three repeats per arm. Across 150
paired queries:

- baseline relevant preservation was `1.0`;
- the producer proposed 1,393 scored items: 457 novel documents and 936 evidence enrichments;
- no relevant document was reachable, novelly rescued, or enriched;
- MRR remained `0.6775`, nDCG@10 remained `0.7071`, and Recall@10 remained `0.8133`; and
- no relevant expansion reached fusion, so this is not evidence about reranking.

The r14 arm used direct graph neighbors from lexical candidates, but its Simeon vector-seed probe
was disabled (`topology_vector_seed_probe=0`). It therefore did not test whether baseline semantic
candidates were better admissible seeds.

The follow-up shadow run at
`build/benchmarks/search_candidate_rescue_graph/20260722-admissible-simeon-seeds-r15/`
enabled the existing 16-result Simeon ANN seed probe for three repeats. Every query used direct
seed-neighbor traversal, with 8.24 newly admitted vector seeds per query on average. Each repeat
scored 515 selected candidates (167 novel documents and 348 evidence enrichments). None of the
three baseline pre-fusion misses appeared in that bounded, post-filter, post-cap set, so no relevant
novel candidate was produced.

That observation did not establish that the raw one-hop relation lacked the missing documents. The
old `reachable` metric was populated from the selected 16-document allow-list after per-seed edge
fetch, eligibility filtering, global ranking, and capping. It therefore conflated the theorem's raw
relation premise with materialization and selection survival. The relation-level premise remained
unknown.

### Stage-ledger result

The three-repeat r16 shadow audit at
`build/benchmarks/search_candidate_rescue_graph/20260723-reachability-stage-ledger-r16/`
separated raw relation membership, bounded fetch, eligibility, graph selection, and scoped Simeon
output. All repeats agreed exactly:

| query | missing qrel | raw relation | fetched / eligible | graph rank | selected 16 | Simeon output |
|---|---:|---:|---:|---:|---:|---:|
| 0 | 31715818 | yes | yes | 392 | no | no |
| 37 | 30655442 | yes | yes | 37 | no | no |
| 46 | 14407673 | no | no | — | no | no |

The graph rank is after the bounded fetch and orders candidates by seed-hit count, then maximum edge
weight. Thus the earlier categorical relation-failure claim was false for two of the three misses.
Those misses were lost by graph ranking and the 16-document cap. The third miss genuinely failed
the raw one-hop relation premise for the observed seeds.

The three-repeat r17 full-relation shadow audit at
`build/benchmarks/search_candidate_rescue_graph/20260723-full-relation-simeon-shadow-r17/`
removed edge truncation and materialized every observed one-hop candidate (`max_docs=2048`). The two
reachable qrels entered allow-lists of 731 and 414 documents, respectively, but neither appeared in
the scoped Simeon result list. All repeats again agreed exactly. The metric named `missing_scored`
means absent from the scorer's returned result list; it does not claim that the backend skipped the
candidate's internal PQ/ADC evaluation.

This localizes two independent obligations:

- relation reachability currently holds for two of the three baseline misses;
- graph-cap materialization fails those two at 16, while full materialization succeeds;
- the scoped Simeon output then fails to select either reachable qrel; and
- the third miss needs a different seed/relation path.

No relevant expansion reached fusion in either audit. These runs therefore provide no evidence
against fusion or reranking, and the conditional Lean theorem remains correct but empirically
uninstantiated for this query sample.

### ANN vehicle decision

The three-repeat r18 audit at
`build/benchmarks/search_candidate_rescue_graph/20260723-ann-vs-exact-shadow-r18/`
kept global retrieval on Simeon PQ/ADC and added a trace-only exhaustive scorer over the identical
topology allow-list. The exhaustive control uses the stored full-precision embeddings; it does not
replace the product retrieval arm or alter returned results.

| query | missing qrel | full relation | selected relation | Simeon top 32 | exact top 32 |
|---|---:|---:|---:|---:|---:|
| 0 | 31715818 | yes | yes | no | no |
| 37 | 30655442 | yes | yes | no | no |
| 46 | 14407673 | no | no | no | no |

All repeats agreed. Increasing only the global Simeon result budget from 32 to 48 left Recall@10 at
`0.8133`; active 16-document graph augmentation also left MRR (`0.6775`), nDCG@10 (`0.7071`), and
Recall@10 unchanged. The full-relation shadow retained the two reachable qrels, but neither the
approximate nor exhaustive embedding scorer selected them in its top 32.

This rules out PQ/ADC approximation as the immediate cause of these misses. ANN remains an
appropriate acceleration mechanism for broad seed retrieval, but it is not the semantic witness
for the theorem's candidate-survival premise. Exact nearest-neighbor scoring is not that witness
either: both implement the same embedding-similarity order, and that order does not select the two
reachable qrels at the current budget.

The architecture should therefore distinguish:

- the protected relation \(R(s,d)\), which supplies a candidate superset;
- a query-conditional selection score \(g(q,d)\), which must preserve missing relevant documents
  under a bounded `Top_b`;
- an exact implementation of `Top_b(g)` used as the experimental reference; and
- ANN only as an optional approximation to that reference, with a separately measured containment
  obligation.

This matches adaptive corpus-graph reranking: the graph proposes neighbors, while online
query-dependent document scores decide which neighborhood to expand rather than relying on a
static graph score alone (MacAvaney, Tonellotto, and Macdonald, *Adaptive Re-Ranking with a Corpus
Graph*, CIKM 2022, DOI `10.1145/3511808.3557231`).

## Next experiment

The next experiment needs an unthresholded exhaustive rank for every relation-reachable judged
document, followed by a scorer ablation at the same relation and output budget. Compare exact
embedding similarity with an existing query-dependent lexical/relevance score and an adaptive
graph-feedback score. This will determine whether increasing the output window is sufficient or the
selection function itself must change. In parallel, query 46 is the bounded case for a different
typed relation or multi-hop path. Do not change fusion or reranking until one selection producer
actually carries a missing qrel into the candidate union.

The active xplan is `search_candidate_rescue_graph`. Add new arms to that plan only when they test a
single producer choice at equal budgets. Do not restore the superseded coordinate matrix or an
oracle-candidate path.

## Promotion gate

Candidate expansion remains experimental unless repeated multi-corpus runs show all of the
following:

- stable daemon-native construction identity;
- non-zero conditional recall of missing relevant documents from admissible seeds;
- complete baseline relevant preservation;
- positive strict budgeted candidate-improvement rate;
- positive post-fusion and returned rescue survival; and
- no material latency regression relative to the global Simeon baseline.

Failure of reachability means refine the seed/relation producer. Failure before the scoped Simeon
output means inspect materialization, output budget, or embedding-space ranking. A candidate present
in that output but absent after fusion implicates fusion; survival through fusion but not returned
top-k implicates final ranking. Keeping those stages separate prevents more search-engine machinery
from being added without evidence.
