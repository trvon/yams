import Yams.Topology.Contracts

namespace Yams.Pipeline

/-- Fixed benchmark probes that connect measured search impact back to the
pipeline-searchability contract. -/
inductive SearchProbeKind where
  | keyword
  | semantic
  | graphRerank
  deriving Repr, BEq, DecidableEq

/-- One measured post-ingest search probe from the C++ ablation JSON. -/
structure SearchImpactObservation where
  kind : SearchProbeKind
  skipped : Bool := false
  ok : Bool := false
  totalCount : Nat := 0
  wallMs : Nat := 0
  deriving Repr, BEq

/-- Probe preconditions induced by enabled pipeline stages. These are the Lean
mirror of the C++ benchmark's `required_by_contract` field. -/
def probeRequiredByConfig (cfg : PipelineConfig) (kind : SearchProbeKind) : Prop :=
  match kind with
  | .keyword => cfg.textExtraction = true ∧ cfg.contentIndex = true
  | .semantic => cfg.textExtraction = true ∧ cfg.embeddings = true
  | .graphRerank =>
      cfg.textExtraction = true ∧ cfg.kgCore = true ∧ cfg.embeddings = true ∧
        cfg.semanticNeighborGraph = true ∧ cfg.graphRerank = true

/-- Capability guaranteed by a completed pipeline for one probe kind. -/
def probeCapability (cfg : PipelineConfig) (after : DocumentState)
    (kind : SearchProbeKind) : Prop :=
  match kind with
  | .keyword => keywordSearchable after = true
  | .semantic => semanticSearchable after = true
  | .graphRerank => graphRerankAvailable cfg after = true

/-- Observation contract: when a probe is required by the enabled pipeline stages
and the before-state was stored, the measured probe must not be skipped, must
succeed, and must return at least one result. Latency is intentionally separated
into an explicit threshold predicate below. -/
def SearchImpactObservationContract
    (cfg : PipelineConfig)
    (before after : DocumentState)
    (obs : SearchImpactObservation) : Prop :=
  before.stored = true →
    probeRequiredByConfig cfg obs.kind →
      probeCapability cfg after obs.kind ∧
      obs.skipped = false ∧ obs.ok = true ∧ obs.totalCount > 0

/-- Explicit measured latency guard used for before/after benchmark summaries.
Lean checks the arithmetic predicate; C++ benchmark JSON supplies the measured
values. -/
def LatencyWithinRegressionBudget
    (budgetMs : Nat)
    (baseline current : SearchImpactObservation) : Prop :=
  current.wallMs ≤ baseline.wallMs + budgetMs

/-- Compact result observation for search-path optimizations. `topIds` is the
stable top-k identity list emitted by benchmark probes. -/
structure SearchResultObservation where
  kind : SearchProbeKind
  totalCount : Nat := 0
  topIds : List String := []
  deriving Repr, BEq

/-- Count regression budget for result-set preserving or explicitly bounded
search-path optimizations. A zero budget requires no loss in total result count. -/
def BoundedCountRegression
    (budget : Nat)
    (baseline current : SearchResultObservation) : Prop :=
  baseline.totalCount ≤ current.totalCount + budget

/-- Search optimization contract, separated by probe kind.

Keyword optimizations are exact by default. Semantic and graph-reranked search
may use an explicit count-regression budget, and can optionally require exact
stable top-k identities when the fixture is deterministic enough. -/
def SearchResultEquivalenceContract
    (countBudget : Nat)
    (requireExactTopK : Bool)
    (baseline current : SearchResultObservation) : Prop :=
  baseline.kind = current.kind ∧
    match baseline.kind with
    | .keyword => current.topIds = baseline.topIds ∧ current.totalCount = baseline.totalCount
    | .semantic =>
        BoundedCountRegression countBudget baseline current ∧
          (requireExactTopK = true → current.topIds = baseline.topIds)
    | .graphRerank =>
        BoundedCountRegression countBudget baseline current ∧
          (requireExactTopK = true → current.topIds = baseline.topIds)

/-- Exact keyword result preservation satisfies the search equivalence contract. -/
theorem keywordResultEquivalence_exact
    {baseline current : SearchResultObservation}
    (hKindBase : baseline.kind = .keyword)
    (hKindCurrent : current.kind = .keyword)
    (hTop : current.topIds = baseline.topIds)
    (hCount : current.totalCount = baseline.totalCount) :
    SearchResultEquivalenceContract 0 true baseline current := by
  constructor
  · rw [hKindBase, hKindCurrent]
  · rw [hKindBase]
    exact ⟨hTop, hCount⟩

/-- Semantic result contracts make degradation explicit through a numeric budget. -/
theorem semanticResultEquivalence_withBudget
    {baseline current : SearchResultObservation}
    {budget : Nat}
    (hKindBase : baseline.kind = .semantic)
    (hKindCurrent : current.kind = .semantic)
    (hCount : BoundedCountRegression budget baseline current) :
    SearchResultEquivalenceContract budget false baseline current := by
  constructor
  · rw [hKindBase, hKindCurrent]
  · rw [hKindBase]
    exact ⟨hCount, by intro h; cases h⟩

/-- Completion contract implies the keyword probe capability whenever keyword
pipeline stages are enabled. -/
theorem completion_keywordProbeCapability
    {cfg : PipelineConfig}
    {before after : DocumentState}
    (h : PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hReq : probeRequiredByConfig cfg .keyword) :
    probeCapability cfg after .keyword := by
  exact completion_keywordSearchable h hStored hReq.1 hReq.2

/-- Completion contract implies the semantic probe capability whenever embedding
pipeline stages are enabled. -/
theorem completion_semanticProbeCapability
    {cfg : PipelineConfig}
    {before after : DocumentState}
    (h : PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hReq : probeRequiredByConfig cfg .semantic) :
    probeCapability cfg after .semantic := by
  exact completion_semanticSearchable h hStored hReq.1 hReq.2

/-- Completion contract implies graph/rerank probe capability whenever all graph
rerank preconditions are enabled. -/
theorem completion_graphRerankProbeCapability
    {cfg : PipelineConfig}
    {before after : DocumentState}
    (h : PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hReq : probeRequiredByConfig cfg .graphRerank) :
    probeCapability cfg after .graphRerank := by
  exact (h hStored).2.2.2 hReq.1 hReq.2.1 hReq.2.2.1 hReq.2.2.2.1 hReq.2.2.2.2

/-- A measured successful keyword probe with non-empty results satisfies the
observation contract under the completion contract. -/
theorem keywordObservation_contract
    {cfg : PipelineConfig}
    {before after : DocumentState}
    {obs : SearchImpactObservation}
    (hCompletion : PipelineCompletionContract cfg before after)
    (hKind : obs.kind = .keyword)
    (hNotSkipped : obs.skipped = false)
    (hOk : obs.ok = true)
    (hNonEmpty : obs.totalCount > 0) :
    SearchImpactObservationContract cfg before after obs := by
  intro hStored hReq
  rw [hKind] at hReq ⊢
  exact ⟨completion_keywordProbeCapability hCompletion hStored hReq, hNotSkipped, hOk,
    hNonEmpty⟩

/-- Semantic probes are not required when embeddings are disabled. -/
theorem semanticProbe_notRequired_withoutEmbeddings
    {cfg : PipelineConfig}
    (hEmbeddings : cfg.embeddings = false) :
    ¬ probeRequiredByConfig cfg .semantic := by
  intro hReq
  simp [probeRequiredByConfig, hEmbeddings] at hReq

/-- Graph/rerank probes are not required when KG core is disabled. -/
theorem graphProbe_notRequired_withoutKg
    {cfg : PipelineConfig}
    (hKg : cfg.kgCore = false) :
    ¬ probeRequiredByConfig cfg .graphRerank := by
  intro hReq
  simp [probeRequiredByConfig, hKg] at hReq

/-- Example threshold check over imported benchmark numbers. -/
theorem example_latencyWithinBudget :
    LatencyWithinRegressionBudget 10
      { kind := .keyword, wallMs := 2 }
      { kind := .keyword, wallMs := 7 } := by
  unfold LatencyWithinRegressionBudget
  decide

end Yams.Pipeline
