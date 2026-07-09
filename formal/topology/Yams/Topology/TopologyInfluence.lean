import Yams.Core
import Yams.Topology.Artifacts
import Yams.Topology.SearchEngine

namespace Yams.SearchEngine

/-- Product-level topology routing policy. The C++ benchmark can map typed config
or benchmark-only overrides onto these modes, while Lean states what each mode is
allowed to change. -/
inductive TopologyRoutingMode where
  | disabled
  | weakQueryOnly
  | hybridAssist
  | rerankOnly
  deriving Repr, BEq, DecidableEq

/-- Bounded topology influence configuration. -/
structure TopologyInfluenceConfig where
  mode : TopologyRoutingMode := .weakQueryOnly
  maxClusters : Nat := 2
  maxDocs : Nat := 64
  deriving Repr, BEq

/-- Minimal persisted topology route: a cluster and its member document IDs.
Uses Yams.Core types for document IDs. -/
structure TopologyCluster where
  id : Yams.Core.ClusterId
  docs : List Yams.Core.DocumentId := []
  deriving Repr, BEq

/-- Search-path observability for topology routing. This mirrors the benchmark
counters used to decide whether a measured A/B run actually exercised topology. -/
structure TopologyRouteObservation where
  enabled : Bool := false
  loadAttempted : Bool := false
  loadSucceeded : Bool := false
  applied : Bool := false
  addedCandidates : Nat := 0
  routedClusters : Nat := 0
  routedDocs : Nat := 0
  deriving Repr, BEq

/-- Modes that may expand the candidate set. Rerank-only is intentionally false:
it may use topology to alter scores, but not to add documents. -/
def topologyMayExpand (mode : TopologyRoutingMode) (weakTier1Query : Bool) : Bool :=
  match mode with
  | .disabled => false
  | .weakQueryOnly => weakTier1Query
  | .hybridAssist => true
  | .rerankOnly => false

/-- Modes that may load topology artifacts for either expansion or reranking. -/
def topologyMayLoad (mode : TopologyRoutingMode) (weakTier1Query : Bool) : Bool :=
  match mode with
  | .disabled => false
  | .weakQueryOnly => weakTier1Query
  | .hybridAssist => true
  | .rerankOnly => true

/-- Candidate-expansion cluster window. -/
def selectedTopologyClusters
    (cfg : TopologyInfluenceConfig) (clusters : List TopologyCluster) : List TopologyCluster :=
  clusters.take cfg.maxClusters

/-- Flatten selected cluster members before the document cap. -/
def selectedTopologyDocsUnbounded
    (cfg : TopologyInfluenceConfig) (clusters : List TopologyCluster) : List Yams.Core.DocumentId :=
  (selectedTopologyClusters cfg clusters).flatMap (fun c => c.docs)

/-- Bounded candidate-expansion document IDs. -/
def selectedTopologyDocs
    (cfg : TopologyInfluenceConfig) (clusters : List TopologyCluster) : List Yams.Core.DocumentId :=
  (selectedTopologyDocsUnbounded cfg clusters).take cfg.maxDocs

/-- Convert bounded topology documents into search candidates. The abstract model
uses the KG source for topology-expanded candidates; the provenance theorem below
is the important guard: every added doc must come from a selected cluster member. -/
def topologyCandidates
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (clusters : List TopologyCluster) : List ComponentCandidate :=
  if topologyMayExpand cfg.mode weakTier1Query then
    (selectedTopologyDocs cfg clusters).map
      (fun doc => { doc := doc, source := .knowledgeGraph, score := 0 })
  else
    []

/-- Topology sidecar vector candidates. This models the tightened C++ path:
topology-expanded hashes are queried through a separate graph-vector leg instead
of narrowing the ordinary vector leg. -/
def topologySidecarCandidates
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (clusters : List TopologyCluster) : List ComponentCandidate :=
  if topologyMayExpand cfg.mode weakTier1Query then
    (selectedTopologyDocs cfg clusters).map
      (fun doc => { doc := doc, source := .graphVector, score := 0 })
  else
    []

/-- Fusion-window policy for topology sidecar survival. Slots are off by default
in C++; when enabled, the fusion window reserves a bounded suffix for eligible
topology sidecar results instead of letting ordinary top-k saturation drop all of
them before final ranking metrics can observe them. -/
structure TopologySidecarSurvivalConfig where
  slots : Nat := 0
  deriving Repr, BEq

/-- Abstract topology sidecar survival reserve. `baseRanked` models the ordinary
fusion ranking with eligible sidecar results removed or displaced; `topologyRanked`
models eligible sidecar results ordered by topology-sidecar evidence. -/
def reserveTopologySidecarResults
    (limit : Nat)
    (cfg : TopologySidecarSurvivalConfig)
    (baseRanked topologyRanked : List SearchResult) : List SearchResult :=
  let reserve := Nat.min cfg.slots limit
  (baseRanked.take (limit - reserve)) ++ (topologyRanked.take reserve)

/-- The topology sidecar reserve remains bounded by the fusion-window limit. -/
theorem reserveTopologySidecarResults_respectsLimit
    (limit : Nat)
    (cfg : TopologySidecarSurvivalConfig)
    (baseRanked topologyRanked : List SearchResult) :
    (reserveTopologySidecarResults limit cfg baseRanked topologyRanked).length ≤ limit := by
  unfold reserveTopologySidecarResults
  let reserve := Nat.min cfg.slots limit
  have hReserveLe : reserve ≤ limit := Nat.min_le_right cfg.slots limit
  have hBase : (baseRanked.take (limit - reserve)).length ≤ limit - reserve := by
    rw [List.length_take]
    exact Nat.min_le_left (limit - reserve) baseRanked.length
  have hTopo : (topologyRanked.take reserve).length ≤ reserve := by
    rw [List.length_take]
    exact Nat.min_le_left reserve topologyRanked.length
  calc
    ((baseRanked.take (limit - reserve)) ++ (topologyRanked.take reserve)).length
        = (baseRanked.take (limit - reserve)).length +
          (topologyRanked.take reserve).length := by simp
    _ ≤ (limit - reserve) + reserve := Nat.add_le_add hBase hTopo
    _ = limit := Nat.sub_add_cancel hReserveLe

/-- If at least one sidecar result is eligible and a positive bounded slot exists,
the reserve admits a topology sidecar result into the bounded fusion window. -/
theorem reserveTopologySidecarResults_admitsSidecarWhenSlotAvailable
    {limit : Nat}
    {cfg : TopologySidecarSurvivalConfig}
    {baseRanked : List SearchResult}
    {topologyHead : SearchResult}
    {topologyTail : List SearchResult}
    (hLimit : 0 < limit)
    (hSlots : 0 < cfg.slots) :
    topologyHead ∈ reserveTopologySidecarResults limit cfg baseRanked
      (topologyHead :: topologyTail) := by
  unfold reserveTopologySidecarResults
  have hReservePos : 0 < Nat.min cfg.slots limit := by
    exact Nat.lt_min.mpr ⟨hSlots, hLimit⟩
  apply List.mem_append_right
  cases hReserve : Nat.min cfg.slots limit with
  | zero =>
      rw [hReserve] at hReservePos
      cases hReservePos
  | succ _ =>
      simp

/-- Search pipeline with topology candidate expansion. Rerank-only intentionally
uses the baseline pipeline here because this model separates candidate-set safety
from score-ordering safety. -/
def runSearchWithTopology
    (searchCfg : SearchConfig)
    (topologyCfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    (failed timedOut : List SearchSource := []) : SearchResponse :=
  runSearch searchCfg (baseCandidates ++ topologyCandidates topologyCfg weakTier1Query clusters)
    failed timedOut

/-- Additive sidecar topology pipeline: the ordinary component candidates are
kept unchanged, and graph-vector topology hits are appended as a separate leg. -/
def runSearchWithTopologySidecar
    (searchCfg : SearchConfig)
    (topologyCfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    (failed timedOut : List SearchSource := []) : SearchResponse :=
  runSearch searchCfg
    (baseCandidates ++ topologySidecarCandidates topologyCfg weakTier1Query clusters)
    failed timedOut

/-- The sidecar form cannot narrow away any pre-existing component candidate
before fusion: every baseline candidate is still present in the additive input. -/
theorem topologySidecar_preservesBaseCandidateMembership
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    {candidate : ComponentCandidate}
    (h : candidate ∈ baseCandidates) :
    candidate ∈ baseCandidates ++ topologySidecarCandidates cfg weakTier1Query clusters := by
  exact List.mem_append_left (topologySidecarCandidates cfg weakTier1Query clusters) h

/-- Benchmark observability model: applied requires a successful load and at
least one added candidate. -/
def observeTopologyRoute
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (loadSucceeded : Bool)
    (clusters : List TopologyCluster) : TopologyRouteObservation :=
  let mayLoad := topologyMayLoad cfg.mode weakTier1Query
  let added := topologyCandidates cfg weakTier1Query clusters
  { enabled := cfg.mode != .disabled,
    loadAttempted := mayLoad,
    loadSucceeded := mayLoad && loadSucceeded,
    applied := mayLoad && loadSucceeded && !added.isEmpty,
    addedCandidates := added.length,
    routedClusters := if mayLoad && loadSucceeded then (selectedTopologyClusters cfg clusters).length else 0,
    routedDocs := if mayLoad && loadSucceeded then (selectedTopologyDocs cfg clusters).length else 0 }

/-- Candidate expansion respects the configured document cap. -/
theorem topologyCandidates_respectsDocCap
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (clusters : List TopologyCluster) :
    (topologyCandidates cfg weakTier1Query clusters).length ≤ cfg.maxDocs := by
  unfold topologyCandidates selectedTopologyDocs
  by_cases h : topologyMayExpand cfg.mode weakTier1Query
  · simp [h]
    exact Nat.min_le_left cfg.maxDocs (selectedTopologyDocsUnbounded cfg clusters).length
  · simp [h]

/-- Selected route set respects the configured cluster cap. -/
theorem selectedTopologyClusters_respectsClusterCap
    (cfg : TopologyInfluenceConfig)
    (clusters : List TopologyCluster) :
    (selectedTopologyClusters cfg clusters).length ≤ cfg.maxClusters := by
  unfold selectedTopologyClusters
  rw [List.length_take]
  exact Nat.min_le_left cfg.maxClusters clusters.length

/-- Disabled topology cannot add candidates. -/
theorem topologyCandidates_disabled_empty
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (clusters : List TopologyCluster)
    (hMode : cfg.mode = .disabled) :
    topologyCandidates cfg weakTier1Query clusters = [] := by
  unfold topologyCandidates topologyMayExpand
  simp [hMode]

/-- Weak-query-only topology cannot add candidates for a strong tier-1 query. -/
theorem topologyCandidates_weakOnly_strong_empty
    (cfg : TopologyInfluenceConfig)
    (clusters : List TopologyCluster)
    (hMode : cfg.mode = .weakQueryOnly) :
    topologyCandidates cfg false clusters = [] := by
  unfold topologyCandidates topologyMayExpand
  simp [hMode]

/-- Rerank-only topology cannot add candidates. -/
theorem topologyCandidates_rerankOnly_empty
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (clusters : List TopologyCluster)
    (hMode : cfg.mode = .rerankOnly) :
    topologyCandidates cfg weakTier1Query clusters = [] := by
  unfold topologyCandidates topologyMayExpand
  simp [hMode]

/-- Every topology-added candidate comes from the bounded selected document list. -/
theorem topologyCandidates_fromSelectedDocs
    {cfg : TopologyInfluenceConfig}
    {weakTier1Query : Bool}
    {clusters : List TopologyCluster}
    {candidate : ComponentCandidate}
    (h : candidate ∈ topologyCandidates cfg weakTier1Query clusters) :
    candidate.doc ∈ selectedTopologyDocs cfg clusters ∧ candidate.source = .knowledgeGraph := by
  unfold topologyCandidates at h
  by_cases hExpand : topologyMayExpand cfg.mode weakTier1Query
  · simp [hExpand] at h
    rcases h with ⟨doc, hDoc, hCandidate⟩
    subst hCandidate
    exact ⟨hDoc, rfl⟩
  · simp [hExpand] at h

/-- Every topology-added candidate came from a selected cluster member before the
document cap. This forbids arbitrary document injection by topology routing. -/
theorem topologyCandidates_noArbitraryDocs
    {cfg : TopologyInfluenceConfig}
    {weakTier1Query : Bool}
    {clusters : List TopologyCluster}
    {candidate : ComponentCandidate}
    (h : candidate ∈ topologyCandidates cfg weakTier1Query clusters) :
    ∃ cluster, cluster ∈ selectedTopologyClusters cfg clusters ∧ candidate.doc ∈ cluster.docs := by
  have hSelected := (topologyCandidates_fromSelectedDocs h).1
  unfold selectedTopologyDocs selectedTopologyDocsUnbounded at hSelected
  have hUnbounded : candidate.doc ∈ selectedTopologyDocsUnbounded cfg clusters :=
    List.mem_of_mem_take hSelected
  simp [selectedTopologyDocsUnbounded] at hUnbounded
  exact hUnbounded

/-- Disabled topology is result-equivalent to the baseline search path. -/
theorem runSearchWithTopology_disabled_baseline
    (searchCfg : SearchConfig)
    (topologyCfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    (failed timedOut : List SearchSource)
    (hMode : topologyCfg.mode = .disabled) :
    runSearchWithTopology searchCfg topologyCfg weakTier1Query baseCandidates clusters failed timedOut =
      runSearch searchCfg baseCandidates failed timedOut := by
  unfold runSearchWithTopology
  rw [topologyCandidates_disabled_empty topologyCfg weakTier1Query clusters hMode]
  simp

/-- Rerank-only topology is candidate-set equivalent to the baseline search path
in this expansion model. Score/order effects should be modeled separately by a
rerank contract that cannot introduce document IDs. -/
theorem runSearchWithTopology_rerankOnly_baseline
    (searchCfg : SearchConfig)
    (topologyCfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    (failed timedOut : List SearchSource)
    (hMode : topologyCfg.mode = .rerankOnly) :
    runSearchWithTopology searchCfg topologyCfg weakTier1Query baseCandidates clusters failed timedOut =
      runSearch searchCfg baseCandidates failed timedOut := by
  unfold runSearchWithTopology
  rw [topologyCandidates_rerankOnly_empty topologyCfg weakTier1Query clusters hMode]
  simp

/-- Full topology-assisted pipeline still respects the user-visible top-k limit. -/
theorem runSearchWithTopology_respectsLimit
    (searchCfg : SearchConfig)
    (topologyCfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    (failed timedOut : List SearchSource) :
    (runSearchWithTopology searchCfg topologyCfg weakTier1Query baseCandidates clusters failed timedOut).results.length ≤
      searchCfg.limit := by
  unfold runSearchWithTopology
  exact runSearch_respectsLimit searchCfg
    (baseCandidates ++ topologyCandidates topologyCfg weakTier1Query clusters) failed timedOut

/-- Applied topology implies observability recorded a load attempt and success. -/
theorem topologyObservation_applied_implies_loaded
    {cfg : TopologyInfluenceConfig}
    {weakTier1Query loadSucceeded : Bool}
    {clusters : List TopologyCluster}
    (hApplied : (observeTopologyRoute cfg weakTier1Query loadSucceeded clusters).applied = true) :
    (observeTopologyRoute cfg weakTier1Query loadSucceeded clusters).loadAttempted = true ∧
      (observeTopologyRoute cfg weakTier1Query loadSucceeded clusters).loadSucceeded = true := by
  unfold observeTopologyRoute at hApplied ⊢
  simp only
  cases hMay : topologyMayLoad cfg.mode weakTier1Query <;>
    cases hLoad : loadSucceeded <;>
    simp [hMay, hLoad] at hApplied ⊢

end Yams.SearchEngine
