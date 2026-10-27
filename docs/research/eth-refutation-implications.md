# ETH refutation and YAMS kernels

Status: 2026-10-26. Claim under external review. No code changes follow from it.

## Claim

OpenAI published "The Exponential Time Hypothesis is False" (manuscript dated
2026-10-25) with a Lean formalization:
<https://github.com/openai/math/blob/main/lean/docs/378.md>.

- Deterministic, constructive, polynomial space.
- Exactly counts satisfying assignments of k-CNF on n variables.
- Lean bound: `(L+1)^{O_k(1)} · 2^{O_k(n (log log n)^2 / sqrt(log n))}`.
- Manuscript bound: `2^{O_k(n log log n / sqrt(log n))}`; `2^{O_k(n / sqrt(log n))}` when
  clauses m <= D·n.

Review group: Impagliazzo, Paturi, Williams.

The bound is subexponential, not practical. With the hidden constant set to 1,
`log log n / sqrt(log n)` (base 2) is 1.05 at n = 10^3 and 0.97 at n = 10^6, so the
algorithm does not beat brute force `2^n` at any input size a solver sees.

## What changes

SETH implies ETH. ETH false makes SETH false. Every lower bound conditioned on ETH, SETH or
the Orthogonal Vectors conjecture (OV) loses its evidence.

Losing a conditional lower bound does not produce an algorithm. Those reductions map SAT
*to* the problem: a fast algorithm for the problem would give fast SAT. A fast SAT
algorithm gives nothing back. Faster edit distance, regex matching or batch
nearest-neighbour search would each need a separate result.

## YAMS kernels

| Kernel | Location | Conditional bound | YAMS regime | Effect |
|---|---|---|---|---|
| Edit distance (fuzzy) | `third_party/symspell/include/symspell/symspell.hpp:434` | n^(2-e) under SETH (Backurs-Indyk, doi:10.1145/2746539.2746612) | k <= 2, terms < 30 chars, early exit | None. Bound is for long strings, unbounded k. |
| Regex (grep) | `src/app/services/grep_regex.cpp:56` (RE2) | m·n under SETH (arXiv:1511.07070) | RE2 runs at O(m·n) | None in practice. RE2 already at the bound. |
| Regex fallback | `src/app/services/grep_regex.cpp:85`, `:122`; `src/cli/commands/grep_command.cpp:1284` | Backreferences: NP-hard, unconditional (arXiv:2601.03020) | `std::regex` backtracking | Exponential worst case (ReDoS). Unrelated to ETH. |
| ANN, single query | `third_party/sqlite-vec-cpp/include/sqlite-vec-cpp/index/hnsw.hpp:510` | Hardness for batch or all-pairs only (arXiv:1803.00904) | Heuristic graph search, ef = 50 | None. |
| Exact NN, batch | `src/vector/sqlite_vec_backend.cpp:4630` | OV: Q·N·d under SETH | Q·N·d loop | None. Constant-factor gains only (blocking, GEMM). |
| Medoid selection | `src/topology/topology_representatives.cpp:104` | All-pairs | m <= 256 | None. |
| Graph traversal | `src/app/services/graph_context_service.cpp:1268`, `src/search/kg_scorer_simple.cpp:599` | None (bounded BFS) | Depth and fanout capped | None. |
| Fusion, packing | `src/search/evidence_search_pipeline.cpp:49`, `src/app/services/graph_context_service.cpp:1032` | None | Linear, greedy with caps | None. |

YAMS has no SAT, SMT or constraint solver.

## Follow-up work

These are independent of the claim. Ranked by expected value:

1. Bound the `std::regex` fallback: put backreferences behind a flag and give them a
   step budget. (arXiv:1511.07070, arXiv:2601.03020)
2. Distance-adaptive beam search instead of fixed `ef_search`. (arXiv:2505.15636; worst-case
   limits: arXiv:2310.19126)
3. Filtered ANN instead of the brute-force fallback for filtered vec0 queries.
   (arXiv:2402.00943)
4. Batch exact search as a blocked matrix multiply. Same results, lower constant.

Search changes ship only if the retrieval bench shows a gain (search PR gate).

## Sources

- OpenAI, "Working with academic communities to build scientific understanding",
  2026-10-26.
- Backurs, Indyk. Edit distance cannot be computed in strongly subquadratic time (unless
  SETH is false). STOC 2015, doi:10.1145/2746539.2746612.
- Backurs, Indyk. Which regular expression patterns are hard to match? arXiv:1511.07070.
- Hardness of regular expression matching with extensions. arXiv:2601.03020.
- Rubinstein. Hardness of approximate nearest neighbor search. arXiv:1803.00904.
- Andoni, Nosatzki. Edit distance in near-linear time: it's a constant factor.
  arXiv:2005.07678.
- Distance adaptive beam search for provably accurate graph-based nearest neighbor
  search. arXiv:2505.15636.
- Worst-case performance of popular ANN implementations. arXiv:2310.19126.
- Approximate nearest neighbor search with window filters. arXiv:2402.00943.
