# Retrieval as routing through a trusted coordinate atlas

Status: research model, not a product-default claim
Updated: 2026-07-19

## Thesis and boundary

The useful idea is not that general topology gives YAMS one new global
coordinate system. General topology gives neighborhoods, overlap, refinement,
connectedness, and invariance across scale. It does not give a ranking distance
or make retrieval computationally cheap.

The stronger and more testable thesis is:

> Embeddings and graph-derived coordinates can define local charts over a
> multiscale overlapping cover. YAMS may route through a small set of charts
> only when their local distortion, uncertainty, retrieval loss, and work are
> bounded; otherwise it augments or falls back to global ANN.

This replaces the fragile picture of one globally faithful embedding space with
an **atlas**: several local coordinate systems whose overlap allows recovery
when one chart fragments, drifts, or poorly represents a query.

## Four objects that must not be conflated

| Object | What it contributes | What it does not establish |
|---|---|---|
| Topology | neighborhoods, covers, refinement, bridges, persistence | a distance or ranking |
| Geometry | coordinates, distance, local distortion, intrinsic dimension | relevance or confidence |
| Measure/statistics | density, uncertainty, calibrated miss risk | a fast index |
| Index/policy | candidate budget, work, fallback, action selection | semantic correctness |

An embedding is therefore a coordinate map, not proof that Euclidean or cosine
proximity preserves every retrieval-relevant relation. The property needed for
routing is weaker and measurable: protected local neighborhoods must be
preserved well enough inside the selected charts.

## Formal problem

Let `X` be a finite document corpus and `q` a query. Let `N*k(q)` denote the
unknown ideal relevant top-`k` set. The system observes several views:

- semantic embedding `phi_e : X -> R^d` and query vector `z_q`;
- sparse evidence `l(q, x)` from lexical retrieval;
- a weighted relation graph `G = (X, E, w)`;
- metadata or policy predicates `m(q, x)`.

At scale `s`, topology supplies an overlapping cover

```text
U_s = {U_s,1, ..., U_s,n}.
```

A local chart is

```text
C_i = (U_i, psi_i, mu_i, r_i, delta_i, u_i),
```

where `psi_i` is a bounded-dimensional coordinate map, `mu_i` is a routing
representative, `r_i` is observed local intrinsic dimension, `delta_i` is local
distortion on protected pairs, and `u_i` is route uncertainty. Charts may
overlap and fine charts may refine coarse charts.

The protected relation `P` is deliberately retrieval-specific. It can include
judged-relevant pairs, reciprocal semantic neighbors, KG edges, citations, and
vector-unique rescues. A chart is not trusted merely because its clusters look
cohesive. It needs enough protected observations and a bounded disagreement
quantity, for example

```text
delta_i = sum_{(x,y) in P intersect U_i^2} |w(x,y) - sim(psi_i(x), psi_i(y))|
          / |P intersect U_i^2|.
```

This is an empirical local certificate, not a claim that `psi_i` is a global
isometry.

## The retrieval coordinate

For a query and chart, define a normalized cost vector

```text
Psi(q, C_i) = [d_e, d_s, d_g, d_m, delta_i, u_i].
```

- `d_e`: semantic cost, initially `1 - max remapped-cosine affinity` to match
  the current router; use angular distance if a true metric is required;
- `d_s`: sparse cost, initially `1 - normalized seed mass`;
- `d_g`: graph/diffusion cost from query seeds to the chart;
- `d_m`: metadata mismatch or constraint cost;
- `delta_i`: observed local coordinate distortion;
- `u_i`: calibrated uncertainty for this chart and query class.

With nonnegative typed weights, charts can be ordered by the routing energy

```text
E(q, C_i) = w_e d_e + w_s d_s + w_g d_g + w_m d_m
          + w_delta delta_i + w_u u_i.
```

`E` is intentionally called an **energy**, not a metric: sparse evidence,
metadata mismatch, and query-conditioned uncertainty need not be symmetric or
satisfy the triangle inequality. This distinction lets the system use several
useful signals without making a false geometric claim.

The current C++ router is an early two-axis instance of this scalarization. It
combines normalized seed mass and remapped centroid cosine, then adds heuristic
persistence/cohesion terms and sometimes a size dampener. The new model treats
those constants as an uncalibrated ordering function. They cannot admit hard
narrowing by themselves.

## Lean formula: minimum work under two trust certificates

For an admissible chart set `A(q)`, route to chart subset `S` under work budget
`B`. The compact objective is

```text
S*(q; B) = argmin_{S subset A(q)} Work(q, S)

subject to
  RequiredEvidenceObserved(q, S),
  ChartRiskUpper(q, S) <= delta,
  MissRiskUpper(q, S, corpus(q)) <= epsilon_corpus,
  Work(q, S) <= B.
```

`ChartRiskUpper` covers support, local distortion, overlap, intrinsic dimension,
and uncertainty. `MissRiskUpper` covers protected-candidate and judged retrieval
loss on a disjoint calibration set. These are separate because a geometrically
clean chart can still be irrelevant, and a relevant-looking route can still be
geometrically unstable. `RequiredEvidenceObserved` requires positive observation
status for chart risk, route risk, and work; a zero-valued counter without such
status is unavailable evidence, not measured zero work.

The action remains three-way:

```text
narrow   if required evidence observed AND chart certificate
         AND route-risk certificate AND budget pass
augment  if a useful route exists but either certificate fails
global   if no useful route exists
```

This is represented in
`formal/topology/Yams/Topology/RetrievalCoordinates.lean`. The proved boundary is

```text
selectCoordinateRoutingAction(cfg, observation) = narrow
  iff RequiredCoordinateEvidenceObserved(observation)
  and CoordinateChartAdmissible(cfg.chart, observation.chart)
  and RouteCertificateAdmissible(cfg.route, observation.certificate).
```

The model also proves that an untrusted chart can never select hard narrowing.
It extends rather than replaces the existing cover/refinement, fallback,
protected-candidate, effort-budget, and per-corpus recall theorems.

### Geometry-to-recall theorem

The earlier preservation theorem assumed directly that the route certificate
covered every protected candidate. That assumption was safe but circular: it
did not explain why local geometry supplied the coverage. The coordinate model
now derives candidate-stage recall from three independently testable premises:

```text
RelevantSetProtected(q, P, R)
and PreservesLocalPairs(selectedCover, P)
and CertificateMaterializesCover(certificate, selectedCover)
────────────────────────────────────────────────────────────────
R ∩ GlobalCandidates(q) ⊆ CoordinateRoutedCandidates(q).
```

`RelevantSetProtected` says each relevant document is connected to the query
anchor by a protected relation. `PreservesLocalPairs` is the atlas-construction
obligation. `CertificateMaterializesCover` is the runtime obligation that every
document in the selected cover reaches the vector allow-list. None directly
assumes that the relevant candidates are already in that allow-list.

Lean proves this as
`coordinateRoute_relevantTopK_subset_routedCandidates`. It also proves
`coordinateRoute_admitted_strictlyFocusesRelevantTopK`: when an admitted chart
preserves the relevant top-k and at least one global candidate lies outside the
materialized chart, the route retains every relevant candidate, invents no
global candidate, and removes at least one non-selected candidate. This is a
strictly better-focused candidate set, not a theorem about final ranking,
latency, or whether the current atlas satisfies the premises.

## Atlas construction hypothesis

Start with a training-free, multiscale construction:

1. Build a reciprocal semantic-neighbor graph and retain lexical/KG relations
   as protected edges rather than blindly mixing all edge types.
2. Construct bounded overlapping neighborhoods at several scales using nets,
   connected refinements, or a Mapper-like cover.
3. Give each chart a semantic representative and optionally local spectral or
   diffusion coordinates.
4. Measure support, overlap, protected-pair disagreement, local intrinsic
   dimension, drift, and route uncertainty.
5. Rank admissible charts by `E`; calibrate action classes separately.

Do not introduce hyperbolic, diffusion, or learned coordinates as a new default
without evidence. They are alternative chart families. For example, hyperbolic
coordinates are plausible for hierarchy-heavy KG regions, while diffusion
coordinates are plausible where many paths define neighborhood similarity.
Neither is expected to dominate semantic coordinates for every query class.

## Falsifiable obligations and xplan observables

| Formal quantity | Required observation |
|---|---|
| semantic/sparse/graph costs | per-chart component values before scalarization |
| local chart support | documents and protected pairs per chart |
| local distortion | mean absolute protected-edge similarity disagreement and observation count |
| local intrinsic dimension | radial chart-level MLE and its support status |
| uncertainty | nearest competing route-score gap plus held-out upper bound by query class |
| atlas behavior | scale, overlap multiplicity, selected charts, uncovered query rate |
| miss risk | global-vector-unique relevant candidates retained/lost before fusion |
| work | rows visited, distance evaluations, candidates, exact reranks, latency |
| action | global, augment, narrow, plus abstention reason |

The next decision-grade experiment should not compare another cluster score.
It should log `Psi(q, C_i)` in shadow mode, pair every proposal with global ANN,
and answer:

1. Does low chart distortion predict low retrieval miss risk?
2. Does local intrinsic dimension identify queries where routing saves work?
3. Do overlapping multiscale charts recover relevant documents lost by a flat
   partition?
4. At the same exact-distance budget, does an admitted route preserve per-corpus
   recall while reducing work?

If the first two relationships do not transfer across SciFact, NFCorpus, and a
third structurally different corpus, the proposed coordinate certificate is not
useful for product routing.

### Implemented coordinate trace

The first observability seam is implemented in the sparse-guided router and
`recordTopologyRoutingDebug`. With stage tracing enabled, each query now emits:

```text
topology_route_coordinate_count
topology_route_coordinate_rows
```

`topology_route_coordinate_rows` is a JSON array in descending score order. Each row has
the cluster ID, semantic and sparse costs, persistence/cohesion/size penalties,
measured distortion, measured local intrinsic dimension, route uncertainty,
the scalar route score, score eligibility, and whether the chart lies in the
selected score prefix. Graph and metadata axes remain `null` because the current
router does not measure them. Distortion and local intrinsic dimension also remain
`null` for charts without their minimum observation support. Missing evidence is
never encoded as zero.

The detailed rows are emitted whenever stage tracing is enabled. In shadow mode
they do not affect candidate selection, scoring, or the returned result set. In
active routing experiments they describe the route that was actually applied;
consumers must check `topology_shadow_evaluated` before treating a row as a
counterfactual proposal. Existing `topology_routing_budget_ablation` arms enable
stage tracing, so their per-query `debug.jsonl` artifacts can carry the rows
without a new product or benchmark environment knob.

### First observation run

The debug-build `topology_routing_budget_ablation` run at
`build/benchmarks/topology_routing_budget_ablation/20260719T232313Z` exercised
both exact and ANN representative routing over three repeats. Across 300 routed
query records and 900 coordinate rows, declared counts matched, rows were in
descending score order, the selected-prefix field used the stable
`in_selected_prefix` name, and every unavailable axis remained `null`.

This run is exploratory rather than decision-grade. All five arms completed,
but the equal-work gate rejected every arm because actual vector rows visited,
exact-distance evaluations, and ANN candidate work were unavailable. The routed
arms also applied narrowing on every query, so they are active interventions,
not shadow counterfactuals.

At the router boundary, ANN representative selection reduced exact
representative evaluations from 1095 to 71.18 on average, but added 975.7 ANN
distance evaluations. Total representative work therefore moved only from
1095 to 1047, while routed p50 latency remained approximately 101 ms. Exact and
ANN routing selected the same top chart for 39 of 50 paired queries and produced
the same final ranking for 38; reciprocal rank was unchanged for all 50. This
does not establish a work win. It does establish the next admission rule:
missing work status must force `augment` or `global`, never `narrow`.

The first measured implementation defines chart distortion as mean absolute
disagreement between a protected semantic-edge score and the member-embedding
cosine. It estimates chart-level intrinsic dimension from non-zero
member-to-centroid cosine radii using a radial maximum-likelihood estimator.
Query-route uncertainty is `1 - clamp(gap, 0, 1)`, where `gap` is the nearest
competing route-score difference. Each value remains absent when its minimum
observation support is unavailable.

`search_generalized_memory_topology_gate` supplies the true shadow arm: topology
constructs and scores the proposed allowed set while the vector leg returns the
unchanged global ranking. Its current corpus gate covers SciFact, NFCorpus, and
FiQA. Active narrowing remains a separate arm and cannot inherit a promotion
claim from shadow quality alone.

### Three-corpus shadow observation

The three-repeat `shadow_margin020_min1` observation at
`build/benchmarks/search_generalized_memory_topology_gate/20260720T010711Z`
evaluated 150 queries per repeat, split evenly across SciFact, NFCorpus, and
FiQA. Shadow evaluation coverage was 1.0 and the counterfactual policy proposed
narrowing for 40.67% of queries (SciFact 48%, NFCorpus 46%, FiQA 28%) without
changing the global result set. All three vector-work statuses were observed on
every query. Mean global work was 17,261 rows visited and 47.68 exact distance
evaluations; these are measurements of the unchanged arm, not projected savings
from narrowing.

Coordinate rows covered every query, but chart support was sparse: distortion
was observed on 26.22% of rows and local intrinsic dimension on 19.33%, while
competitor-gap uncertainty was observed on 100%. The construction produced
5,130 charts with a 93.70% singleton rate and no overlap membership. This is the
dominant geometric result: the current connected-component atlas is too
fragmented to provide a broadly observed chart certificate.

The offline shadow calibration found only one unique query with a
global-vector-protected candidate in an admitted counterfactual route. Repeated
three times, it was an NFCorpus case and retained that candidate, but `n=1`
cannot calibrate miss risk and provides no SciFact or FiQA transfer evidence.
Consequently this run does not admit active narrowing or establish that
distortion or local intrinsic dimension predicts miss risk.

The xplan arm remains formally invalid: its quality pass completed, but early
warmup searches timed out under concurrent host load, leaving 143, 143, and 144
of 150 warmup queries complete. The measured rows are retained as an observation
artifact, not promoted as a decision-grade result. A rerun should occur on an
isolated host only after changing the atlas construction to increase
non-singleton protected-pair support and adding overlap; rerunning the same
fragmented atlas would not repair the missing calibration evidence.

## Literature map

The literature supports components of the model, not the complete YAMS claim.
Entries were checked through Paperbridge against primary publication metadata
and available abstracts.

### Metric covers and scalable neighborhood search

- Har-Peled and Mendel,
  [Fast Construction of Nets in Low-Dimensional Metrics and Their Applications](https://doi.org/10.1137/S0097539704446281),
  gives the basis for hierarchical nets in metrics with bounded doubling
  dimension. YAMS must measure local dimensionality; it cannot assume this
  favorable regime globally.
- Beygelzimer, Kakade, and Langford,
  [Cover Trees for Nearest Neighbor](https://doi.org/10.1145/1143844.1143857),
  supports multiscale metric indexing without requiring Euclidean coordinates.
  It is a candidate construction principle, not evidence that current semantic
  distance has a benign expansion constant.
- Malkov and Yashunin,
  [Efficient and Robust Approximate Nearest Neighbor Search Using Hierarchical
  Navigable Small World Graphs](https://doi.org/10.1109/TPAMI.2018.2889473),
  demonstrates the practical value of scale-separated navigation. HNSW remains
  the global ANN baseline that an atlas router must beat at equal recall/work.

### Coordinates from local geometry

- Tenenbaum, de Silva, and Langford,
  [A Global Geometric Framework for Nonlinear Dimensionality Reduction](https://doi.org/10.1126/science.290.5500.2319),
  shows how local metric observations can recover global manifold coordinates
  under stated manifold assumptions. Those assumptions are hypotheses to test,
  not properties granted to text embeddings.
- Coifman and Lafon,
  [Diffusion Maps](https://doi.org/10.1016/j.acha.2006.04.006), and Nadler et al.,
  [Diffusion Maps, Spectral Clustering and Reaction Coordinates](https://doi.org/10.1016/j.acha.2005.07.004),
  motivate graph-derived coordinates and diffusion distance when connectivity
  across many paths is more meaningful than one centroid.
- Aumüller and Ceccarello,
  [The Role of Local Dimensionality Measures in Benchmarking Nearest Neighbor
  Search](https://doi.org/10.1016/j.is.2021.101807),
  shows that local intrinsic dimensionality and relative contrast expose query
  difficulty and runtime behavior. This directly motivates LID as an abstention
  feature and stratification variable.

### Topological covers and stability

- Singh, Mémoli, and Carlsson,
  [Topological Methods for the Analysis of High Dimensional Data Sets and 3D
  Object Recognition](https://doi.org/10.2312/SPBG/SPBG07/091-100),
  introduces Mapper as partial clustering guided by filter functions and encoded
  as a simplicial complex. It supports overlapping, guided covers rather than a
  single final partition.
- Cohen-Steiner, Edelsbrunner, and Harer,
  [Stability of Persistence Diagrams](https://doi.org/10.1145/1064092.1064133),
  supplies the mathematical reason to prefer features that persist under small
  perturbations. YAMS still needs an operational drift test tied to retrieval,
  rather than treating persistence score as automatic relevance.
- Carlsson,
  [Topological Pattern Recognition for Point Cloud Data](https://doi.org/10.1017/S0962492914000051),
  gives a broader account of persistent homology on finite metric samples. Its
  role here is diagnostic chart stability, not online ranking.

### Risk-aware routing

- Lu, Xiao, and Ishikawa,
  [Probabilistic Routing for Graph-Based Approximate Nearest Neighbor Search](https://arxiv.org/abs/2402.11354),
  formulates skipping exact neighbor evaluations with a probabilistic routing
  guarantee. It is currently an arXiv result in this map, so it is a mechanism
  precedent rather than a settled product guarantee.
- Angelopoulos et al.,
  [Conformal Risk Control](https://arxiv.org/abs/2208.02814),
  motivates calibration of monotone loss and explicit risk control. YAMS adopts
  the calibration/runtime separation only; exchangeability and transfer across
  changing corpora must be tested before claiming a conformal guarantee.

## Non-claims

- A semantic embedding is not assumed to be an isometry of relevance.
- A low-dimensional visualization is not automatically a retrieval index.
- Persistent topology does not imply query relevance.
- A weighted multi-signal energy is not called a metric without metric axioms.
- A pooled quality gain cannot hide a corpus-specific recall loss.
- Synthetic data may test construction and throughput, but not rank retrieval
  coordinate systems for product use.
