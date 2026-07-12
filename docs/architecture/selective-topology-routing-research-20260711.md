# Selective topology routing: formal and empirical research lane

## Claim boundary

The current evidence does not justify calling topology a better ANN index or
enabling it by default. It does justify a narrower research proposition:

> A training-free, multiscale overlapping cover can act as an index-independent
> route certificate that selects the least expensive of global ANN, additive
> topology augmentation, and topology-filtered ANN while preserving a declared
> per-corpus recall-risk floor.

This is stronger than cluster-first routing and more falsifiable than a generic
"topology improves search" claim. It preserves global ANN as the safe action,
uses topology as evidence about where exact work is useful, and treats a route
as invalid when it only improves a pooled metric by harming a smaller corpus.

The Lean model is `Yams.Topology.SelectiveRouting`.

## Mathematical object

At scale `s`, topology produces an overlapping cover

`U_s = {U_s,1, ..., U_s,m}`

of document IDs. A finer cover refines a coarse cover when every fine
neighborhood is contained in a coarse neighborhood. Clusters need not be
disjoint. The construction has three obligations:

1. **Coverage:** every indexed document occurs in at least one neighborhood.
2. **Locality:** protected semantic/KG/reciprocal-neighbor pairs co-occur in at
   least one neighborhood at some scale.
3. **Bounded overlap:** the number of neighborhoods containing one document is
   capped, preventing repeated candidate work from growing without bound.

This reframes fragmentation. Many small connected components are not inherently
bad; they are useful fine neighborhoods if a coarser level reconnects them and
the route can select across levels. A flat partition has no such recovery path.

For each query, the policy chooses an action from

`A = {global, augment, narrow}`.

The optimization target is

`argmin_a Work(a, q)` subject to `RiskUpper(a, class(q)) <= epsilon_corpus`.

Work is measured with ANN candidate count, visited rows, exact distance
evaluations, and exact reranks. Risk is calibrated from judged queries and is
kept corpus-stratified. A useful route score is therefore not merely centroid
similarity: it is evidence that a cheaper action stays inside the declared loss
budget.

## Formal contracts now represented

`formal/topology/Yams/Topology/SelectiveRouting.lean` proves:

- refinement cannot invent documents and propagates fine-cover coverage to a
  coarser cover;
- an empty rejected route is exactly the global path;
- a non-empty rejected route is additive and preserves global candidates;
- admitted narrowing cannot invent candidates and preserves protected
  candidates when its soundness obligation holds;
- admission implies the configured ANN/distance/rerank work caps;
- a single per-corpus recall regression rejects generalized-memory promotion,
  even if a pooled score improves.

The proof deliberately separates deterministic soundness from empirical risk
calibration. The `misses / protected-candidates` arithmetic in Lean is a
benchmark obligation, not a claim of distribution-free generalization.

## Existing xplan observables

The generalized-memory and routing-budget plans already expose most of the
required quantities:

| Formal quantity | xplan/debug observable |
|---|---|
| route evidence | `topology_route_boundary_score_margin`, seed coverage, routed clusters/docs |
| action | narrow, confidence-abstain, augmentation, filter, and partition-ANN rates |
| candidate work | `vector_candidate_work_budget_avg`, ANN candidate budget |
| exact work | total rows visited and total exact distance evaluations |
| candidate loss | per-corpus pre/post-fusion relevant recall and ranking-loss rate |
| generalized floor | per-corpus MRR/recall plus macro and minimum metrics |
| construction | cluster overlap, source purity/entropy, relevant-fragment coverage |

Use `search_generalized_memory_topology_gate` for the conjunctive product gate
and `topology_routing_budget_ablation` for equal-work mechanism checks.

The existing query-class analyzer now emits the Lean risk counts across all
matching repeats:

```bash
python3 tests/benchmarks/xplan/analyze_query_class.py \
  build/benchmarks/search_generalized_memory_topology_gate/<stamp> \
  --route-calibration global_ann_c32 routed_margin020_min1
```

### Retrospective read of the current R3 run

Applying that analysis to `steady-state-topology-stage-r3-v1` gives:

| Scope | Admitted queries | Protected vector-unique rescues | Missed | Misses / 1000 | Mean exact-work delta | Mean RR delta |
|---|---:|---:|---:|---:|---:|---:|
| all | 9 | 12 | 3 | 250.00 | +11.67 | 0.0000 |
| NFCorpus | 9 | 12 | 3 | 250.00 | +11.67 | 0.0000 |

This retrospective is diagnostic, not held-out calibration. It says the current
run has only 12 judged vector-unique rescue candidates among admitted routes,
all in NFCorpus, and loses 3 of them while spending more exact work. There is no
eligible SciFact evidence for calibrating this particular loss. This is too sparse
to certify hard narrowing and directly explains why a pooled quality gain cannot
promote the route. It points first to retaining vector-unique rescues, eliminating
duplicate member distance evaluation, and calibrating a corpus/route-class
abstention floor; it does not show that fragmented clustering is intrinsically
harmful. Final-window fusion/ranking remains part of the gate as well.

## Shadow experiment seam

The engine now supports a typed `TopologyVectorPolicy::Shadow` policy. It runs
the topology route against the same query as global ANN, projects the returned
vector candidates through the proposed allowed-document set, records the
retained and removed document IDs, and returns the original global result. It
does not run a second ANN search or an exact-distance pass. A focused Catch2
test requires document order and final scores to be identical with topology
disabled and in shadow mode.

The xplan worker and query-class analyzer now emit:

- query corpus and stable query identity;
- route scale, margin, seed hits, selected cover multiplicity, and allowed-doc
  count;
- vector-unique relevant rescues retained/lost before fusion relative to global
  ANN, while separately recording lexical fallback;
- returned reciprocal-rank delta;
- rows visited, exact distance evaluations, and exact reranks;
- action (`global`, `augment`, or `narrow`) and abstention reason.

The same-query counterfactual is available with:

```bash
python3 tests/benchmarks/xplan/analyze_query_class.py \
  build/benchmarks/search_generalized_memory_topology_gate/<stamp> \
  --shadow-calibration shadow_margin020_min1
```

### Current shadow R3 result

The shadow arm in `shadow-calibration-r3-v1` and the corrected global baseline
in `shadow-baseline-fixed-r3-v1` use the same two-corpus plan and budgets, with
three valid repeats per arm:

| Metric | Global ANN C32 | Shadow C32 | Read |
|---|---:|---:|---|
| macro MRR | 0.5524 ± 0.0005 | 0.5619 ± 0.0097 | within run noise |
| macro recall | 0.4290 ± 0.0047 | 0.4324 ± 0.0047 | within run noise |
| vector candidate budget | 32 | 32 | equal |
| distance-evaluation budget | 53.2 | 53.2 | equal |
| search latency p50 | 17.50 ms | 19.17 ms | +1.67 ms / +9.5% |
| search latency p95 | 29.67 ms | 32.33 ms | +2.67 ms / +9.0% |
| actual topology apply/narrow rate | 0 | 0 | shadow is non-mutating |
| proposed narrow rate | — | 0.5167 | diagnostic only |

The shadow counterfactual found 12 protected vector-unique relevant candidates
across 9 proposed narrow queries. The route retained 6 and would remove 6:
`500 misses / 1000 protected candidates`. All observed protected candidates
and losses are in NFCorpus. This rejects hard narrowing at the present route
threshold. It also gives a sharper diagnosis than comparing independent global
and routed arms: topology is not yet recognizing when a sparse-corpus relevant
neighbor lies outside the selected cover.

The shadow-only latency delta is route construction and projection overhead,
not a second vector search. Before promotion, that cost should move toward a
cached, prevalidated membership snapshot and a lightweight document-set
projection. The benchmark fixture also now waits for any automatic topology
rebuild before publishing its explicit source snapshot; this removes a startup
race that previously invalidated two global repeats.

Calibration and evaluation query sets must be disjoint. Thresholds should be a
small monotone table in typed `SearchEngineConfig`/`TopologyConfig`, not a new
environment-variable surface. Start training-free: isotonic bins or conservative
empirical upper bounds over route margin, seed coverage, overlap, and corpus.
Only test a learned gate if that baseline cannot transfer across corpora.

## Decision sequence

1. Run topology in shadow mode while returning global ANN. Record paired query
   loss and work without product risk.
2. Calibrate route classes on one split. A class with insufficient evidence
   cannot narrow; it may only augment or fall back.
3. Validate on held-out SciFact and NFCorpus in one index, repeats at least 3.
4. Require non-regression for every corpus and compare at equal exact-distance
   and candidate budgets.
5. Add a third, structurally different corpus before considering a default.
6. Only then test cold-start and read/write saturation with topology enabled.

## Literature bridge

- Angelopoulos et al., [Conformal Risk Control](https://arxiv.org/abs/2208.02814)
  motivates explicit monotone loss control and calibration, including false
  negatives and distribution shift. YAMS currently adopts only the separation
  of calibration from runtime selection; it does not yet claim a conformal
  guarantee.
- Lu, Xiao, and Ishikawa,
  [Probabilistic Routing for Graph-Based Approximate Nearest Neighbor Search](https://arxiv.org/abs/2402.11354)
  demonstrates that routing can skip exact neighbor evaluations under a stated
  probabilistic guarantee instead of relying only on a heuristic score.
- Chatzakis, Papakonstantinou, and Palpanas,
  [DARTH: Declarative Recall Through Early Termination](https://doi.org/10.1145/3749160)
  treats target recall as a declarative contract and adapts effort per query.
  This is the closest systems analogue to YAMS's proposed action selector.
- Har-Peled and Mendel,
  [Fast Construction of Nets in Low-Dimensional Metrics and Their Applications](https://doi.org/10.1137/S0097539704446281)
  supplies the geometric basis for bounded multiscale covers in doubling
  metrics. YAMS must measure whether its embedding/topology space has enough
  local structure for these assumptions to be useful.
- Alvarado et al.,
  [G-Mapper: Learning a Cover in the Mapper Construction](https://arxiv.org/abs/2309.06634)
  reinforces treating topology as an overlapping cover whose construction can
  be optimized, rather than as one final partition. Its learned construction is
  not required by the proposed training-free first pass.
- Vecchiato,
  [Learning Cluster Representatives for Approximate Nearest Neighbor Search](https://arxiv.org/abs/2412.05921)
  separates cluster construction from routing quality. YAMS can apply that
  lesson without training by optimizing representative/route objectives against
  paired held-out retrieval loss.

These papers support the research direction, not a state-of-the-art claim.
Advancement would require beating strong global ANN and filtered-ANN baselines
at equal recall and exact-work budgets across generalized-memory corpora.
