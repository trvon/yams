# Retrieval contracts and measurement boundaries

The Lean provenance model and the bounded C++ retriever have different proof
obligations. `formal/topology/Yams/Topology/AllowedSetRetrieval.lean` models the
allowed-set operator used by fast narrowing. Its protected-document preservation
theorem explicitly requires scorer completeness after each route's quota. Neither
cover membership nor zero misses on held-out queries proves this condition for
every future query. The earlier filter-based model remains a specification of
post-filtering an existing global list.

Construction identity describes topology membership and structural inputs.
Calibration identity must additionally describe the representatives and effective
routing policy that selected the measured allowed sets. Publication timestamps
are not calibration inputs. A missing or mismatching policy identity leaves
calibration unavailable, including for legacy configs containing only a
construction fingerprint. Held-out observation counts are empirical evidence;
the identity of their dataset/split must accompany them.

Document result limits and vector-row budgets are separate quantities. Bounded
chunk retrieval can omit documents when one document contributes many high-scoring
chunks. Weighted top-k aggregation over retrieved chunks is not an exhaustive
document score. Refill can improve unique-document coverage, but bounded refill
does not establish exact ranking or completeness. Exact document-complete scoring
is the comparison oracle; it must not silently become the product latency baseline.

Simeon lexical work should be shared within one request and one selected scoring
recipe. A request-local result must not leak across queries, recipes, backend
instances, or corpus rebuilds. Benchmark the routed APIs used by the engine;
`score()`'s optional hot-query cache is a separate API and experiment.

## Benchmark controls

- Current product behavior is `hybrid_assist` with `shadow`.
- Topology disabled is the no-topology control.
- Traced shadow can run an additional exact document-complete search. Its timing
  is diagnostic; compare product latency using untraced requests.
- A PHSS candidate count is a rerank pool size, not HNSW traversal work. It is
  clamped to at least the raw vector-row request size. Record effective values.
- An experiment requiring narrowing must show that narrowing actually executed,
  that its calibration matches the current construction and policy, and that work
  counters are observed. A successful fallback is not evidence for narrowing.

The optional local xplan harness calls the tracked
`tests/benchmarks/retrieval_contract.py` preflight. Its historical
`topology_routing_budget_ablation` plan currently fails that contract: nominal
16/64 PHSS pools alias under default chunk overfetching, and its Vec0/stale
calibration setup does not validate fast certified narrowing. Do not use historical
reports from that plan to promote defaults. `search_shadow_cost` separates the
three baseline modes above. Certified narrowing plans remain parked until a
held-out calibration generator and worker artifact loader exist; an artifact
filename alone is not evidence. Replace the historical routing gate with a PQ
comparison only after verifying calibration identity and actual narrow actions
for every arm.

For promotion, use frozen binaries and immutable shared corpus state, at least
three repetitions on SciFact and NF-Corpus and a mixed index, and report quality
alongside latency, unique-document fill, route action rates, and actual work.

## Remaining foundational work

1. Define a versioned calibration artifact containing immutable construction,
   routing policy, embedding-space and held-out dataset/query-split identities,
   protected-document definitions, observed misses, and scorer/budget settings.
   Generate observations against the document-complete oracle on a calibration
   split that is disjoint from the evaluation queries. Test drift and malformed
   artifacts before adding the worker loader; never synthesize successful counts.
2. Require observed narrow actions and complete work counters in each PQ arm,
   then run the existing multi-corpus evaluation loop. Revalidate calibration
   when scorer, aggregation, or quota policy changes. The current route identity
   does not prove bounded scorer completeness.
3. Characterize a document-candidate batch seam with explicit completeness and
   work status using high chunk-skew fixtures. Compare the bounded refill to
   document-complete retrieval before deciding whether incremental backend
   selection is worth implementing. Measure single-batch lexical requests as
   well as expanded/direct-rescue requests as corpus size grows.

These are prerequisites for stronger completeness or default-promotion claims,
not properties established by the current provenance proofs or microbenchmarks.
