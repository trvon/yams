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

/-- Unbounded union of the documents represented by a collection of topology
fragments. Fragmentation is useful when each component is a small local memory
neighborhood; retrieval may select and union several such neighborhoods. -/
def fragmentUnionDocs (fragments : List TopologyCluster) : List Yams.Core.DocumentId :=
  fragments.flatMap (fun fragment => fragment.docs)

/-- `fragments` refine a coarse semantic region when they neither invent
documents outside it nor lose any document from it. No lower bound on fragment
size is required: singleton fragments are valid refinements. -/
def IsFragmentationOf (coarse : TopologyCluster) (fragments : List TopologyCluster) : Prop :=
  (∀ fragment ∈ fragments, ∀ doc ∈ fragment.docs, doc ∈ coarse.docs) ∧
    (∀ doc ∈ coarse.docs, ∃ fragment ∈ fragments, doc ∈ fragment.docs)

/-- Fragmentation does not lose recoverability: every document represented by
the coarse region remains present in the union of its fragments. -/
theorem fragmentation_preservesDocumentCoverage
    {coarse : TopologyCluster}
    {fragments : List TopologyCluster}
    {doc : Yams.Core.DocumentId}
    (hFragmentation : IsFragmentationOf coarse fragments)
    (hDoc : doc ∈ coarse.docs) :
    doc ∈ fragmentUnionDocs fragments := by
  rcases hFragmentation.2 doc hDoc with ⟨fragment, hFragment, hMember⟩
  simp [fragmentUnionDocs]
  exact ⟨fragment, hFragment, hMember⟩

/-- Refinement cannot invent a document outside the coarse region. -/
theorem fragmentation_noArbitraryDocuments
    {coarse : TopologyCluster}
    {fragments : List TopologyCluster}
    {doc : Yams.Core.DocumentId}
    (hFragmentation : IsFragmentationOf coarse fragments)
    (hDoc : doc ∈ fragmentUnionDocs fragments) :
    doc ∈ coarse.docs := by
  simp [fragmentUnionDocs] at hDoc
  rcases hDoc with ⟨fragment, hFragment, hMember⟩
  exact hFragmentation.1 fragment hFragment doc hMember

/-- Selecting another fragment is monotone before budget truncation: it can add
coverage but cannot remove a document already exposed by earlier fragments. -/
theorem fragmentUnionDocs_append_monotone
    (selected additional : List TopologyCluster)
    {doc : Yams.Core.DocumentId}
    (hDoc : doc ∈ fragmentUnionDocs selected) :
    doc ∈ fragmentUnionDocs (selected ++ additional) := by
  simp [fragmentUnionDocs] at hDoc ⊢
  exact Or.inl hDoc

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

/-- A bounded fragment route is safe for a protected set when the selected
document budget explicitly covers every protected document. The empirical router
must establish this premise; fragmentation itself does not make it false. -/
def FragmentBudgetCovers
    (cfg : TopologyInfluenceConfig)
    (clusters : List TopologyCluster)
    (protectedDocs : List Yams.Core.DocumentId) : Prop :=
  ∀ doc ∈ protectedDocs, doc ∈ selectedTopologyDocs cfg clusters

/-- Additive retrieval needs a weaker but useful certificate: at least one
protected/relevant document is exposed by the selected fragment budget. This is
not sufficient for replacing global ANN, but it can augment vector recall. -/
def FragmentBudgetHits
    (cfg : TopologyInfluenceConfig)
    (clusters : List TopologyCluster)
    (protectedDocs : List Yams.Core.DocumentId) : Prop :=
  ∃ doc ∈ protectedDocs, doc ∈ selectedTopologyDocs cfg clusters

/-- Every protected document survives a fragment budget that certifies coverage. -/
theorem selectedTopologyDocs_preservesProtected
    {cfg : TopologyInfluenceConfig}
    {clusters : List TopologyCluster}
    {protectedDocs : List Yams.Core.DocumentId}
    (hCoverage : FragmentBudgetCovers cfg clusters protectedDocs) :
    ∀ doc ∈ protectedDocs, doc ∈ selectedTopologyDocs cfg clusters := by
  exact hCoverage

/-- A fragment hit certificate yields an explicit protected document in the
bounded selected set. -/
theorem selectedTopologyDocs_hitsProtected
    {cfg : TopologyInfluenceConfig}
    {clusters : List TopologyCluster}
    {protectedDocs : List Yams.Core.DocumentId}
    (hHit : FragmentBudgetHits cfg clusters protectedDocs) :
    ∃ doc ∈ protectedDocs, doc ∈ selectedTopologyDocs cfg clusters := by
  exact hHit

/-- Confidence-bearing decision for the live hard-narrowing path. An unconfident
decision is an explicit abstention and must preserve the baseline candidate set. -/
structure TopologyNarrowingDecision where
  confident : Bool := false
  allowedDocs : List Yams.Core.DocumentId := []
  deriving Repr, BEq

/-- Restrict one candidate leg to topology-selected documents only when the
route is confident. This models the live vector-leg narrowing boundary. -/
def narrowCandidates
    (decision : TopologyNarrowingDecision)
    (candidates : List ComponentCandidate) : List ComponentCandidate :=
  if decision.confident then
    candidates.filter (fun candidate => decision.allowedDocs.contains candidate.doc)
  else
    candidates

/-- Abstention is candidate-set identity. This is the formal fallback contract
that an empirical confidence gate must preserve. -/
theorem narrowCandidates_abstain_identity
    (decision : TopologyNarrowingDecision)
    (candidates : List ComponentCandidate)
    (hAbstain : decision.confident = false) :
    narrowCandidates decision candidates = candidates := by
  simp [narrowCandidates, hAbstain]

/-- Hard narrowing cannot invent candidates. -/
theorem narrowCandidates_subset
    {decision : TopologyNarrowingDecision}
    {candidates : List ComponentCandidate}
    {candidate : ComponentCandidate}
    (h : candidate ∈ narrowCandidates decision candidates) :
    candidate ∈ candidates := by
  by_cases hConfident : decision.confident
  · have hFiltered : candidate ∈ candidates ∧ candidate.doc ∈ decision.allowedDocs := by
      simpa [narrowCandidates, hConfident] using h
    exact hFiltered.1
  · simpa [narrowCandidates, hConfident] using h

/-- Every candidate surviving a confident narrowing decision is explicitly in
the allowed-document set. -/
theorem narrowCandidates_confident_fromAllowed
    {decision : TopologyNarrowingDecision}
    {candidates : List ComponentCandidate}
    {candidate : ComponentCandidate}
    (hConfident : decision.confident = true)
    (h : candidate ∈ narrowCandidates decision candidates) :
    candidate.doc ∈ decision.allowedDocs := by
  simp [narrowCandidates, hConfident] at h
  exact h.2

/-- A protected candidate is retained when it was present before narrowing and
its document is covered by the confidence certificate's allowed set. Benchmarks
must establish which observable candidates deserve this protection. -/
theorem narrowCandidates_preservesProtected
    {decision : TopologyNarrowingDecision}
    {candidates protectedCandidates : List ComponentCandidate}
    (hConfident : decision.confident = true)
    (hPresent : ∀ candidate ∈ protectedCandidates, candidate ∈ candidates)
    (hCovered : ∀ candidate ∈ protectedCandidates, candidate.doc ∈ decision.allowedDocs) :
    ∀ candidate ∈ protectedCandidates, candidate ∈ narrowCandidates decision candidates := by
  intro candidate hProtected
  simp [narrowCandidates, hConfident, hPresent candidate hProtected,
    hCovered candidate hProtected]

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

/-- Topology vector-augmentation candidates. Routed hashes are scored by the query
embedding and unioned with global ANN under ordinary vector provenance. -/
def topologyVectorAugmentationCandidates
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (clusters : List TopologyCluster) : List ComponentCandidate :=
  if topologyMayExpand cfg.mode weakTier1Query then
    (selectedTopologyDocs cfg clusters).map
      (fun doc => { doc := doc, source := .vector, score := 0 })
  else
    []

/-- Unbounded candidate view of explicitly selected fragments. This separates
the semantic value of fragmentation from a later candidate-budget policy. -/
def fragmentCandidates (fragments : List TopologyCluster) : List ComponentCandidate :=
  (fragmentUnionDocs fragments).map
    (fun doc => { doc := doc, source := .vector, score := 0 })

/-- Additive fragment augmentation keeps the global ANN/component candidates and
appends query-scored local topology neighborhoods to the vector candidate stream. -/
def augmentWithFragments
    (baseCandidates : List ComponentCandidate)
    (fragments : List TopologyCluster) : List ComponentCandidate :=
  baseCandidates ++ fragmentCandidates fragments

/-- Fragment augmentation cannot remove a global ANN/component candidate. -/
theorem augmentWithFragments_preservesBase
    (baseCandidates : List ComponentCandidate)
    (fragments : List TopologyCluster)
    {candidate : ComponentCandidate}
    (hCandidate : candidate ∈ baseCandidates) :
    candidate ∈ augmentWithFragments baseCandidates fragments := by
  exact List.mem_append_left (fragmentCandidates fragments) hCandidate

/-- Adding more fragments is candidate-set monotone before a budget is applied. -/
theorem fragmentCandidates_append_monotone
    (selected additional : List TopologyCluster)
    {candidate : ComponentCandidate}
    (hCandidate : candidate ∈ fragmentCandidates selected) :
    candidate ∈ fragmentCandidates (selected ++ additional) := by
  rcases List.mem_map.mp hCandidate with ⟨doc, hDoc, rfl⟩
  apply List.mem_map.mpr
  exact ⟨doc, fragmentUnionDocs_append_monotone selected additional hDoc, rfl⟩

/-- If selected fragments refine a coarse region, additive augmentation exposes
every coarse-region document as a vector candidate, regardless of how many
or how small the fragments are. -/
theorem fragmentedAugmentation_recoversDocument
    (baseCandidates : List ComponentCandidate)
    {coarse : TopologyCluster}
    {fragments : List TopologyCluster}
    {doc : Yams.Core.DocumentId}
    (hFragmentation : IsFragmentationOf coarse fragments)
    (hDoc : doc ∈ coarse.docs) :
    ∃ candidate ∈ augmentWithFragments baseCandidates fragments, candidate.doc = doc := by
  let candidate : ComponentCandidate := { doc := doc, source := .vector, score := 0 }
  refine ⟨candidate, ?_, rfl⟩
  apply List.mem_append_right
  apply List.mem_map.mpr
  exact ⟨doc, fragmentation_preservesDocumentCoverage hFragmentation hDoc, rfl⟩

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

/-- Additive topology pipeline: ordinary candidates remain present while routed,
query-scored members are unioned into the normal vector candidate stream. -/
def runSearchWithTopologyAugmentation
    (searchCfg : SearchConfig)
    (topologyCfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    (failed timedOut : List SearchSource := []) : SearchResponse :=
  runSearch searchCfg
    (baseCandidates ++ topologyVectorAugmentationCandidates topologyCfg weakTier1Query clusters)
    failed timedOut

/-- The augmentation form cannot narrow away any pre-existing component candidate
before fusion: every baseline candidate is still present in the additive input. -/
theorem topologyAugmentation_preservesBaseCandidateMembership
    (cfg : TopologyInfluenceConfig)
    (weakTier1Query : Bool)
    (baseCandidates : List ComponentCandidate)
    (clusters : List TopologyCluster)
    {candidate : ComponentCandidate}
    (h : candidate ∈ baseCandidates) :
    candidate ∈ baseCandidates ++
      topologyVectorAugmentationCandidates cfg weakTier1Query clusters := by
  exact List.mem_append_left
    (topologyVectorAugmentationCandidates cfg weakTier1Query clusters) h

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
