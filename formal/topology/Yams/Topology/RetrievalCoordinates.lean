import Yams.Topology.SelectiveRouting

namespace Yams.SearchEngine

/-!
Retrieval coordinates for selective topology routing.

An embedding vector is not treated as a globally faithful coordinate system.
Instead, routing observes a query relative to one local chart in a multiscale,
overlapping atlas.  Semantic, sparse, graph, and metadata evidence locate the
query; measured distortion and uncertainty say whether that location is safe
enough to replace global ANN.

The scalar below is a routing energy, not a mathematical metric.  In
particular, sparse and metadata costs need not be symmetric or satisfy the
triangle inequality.  Metric claims belong to the individual coordinate maps
and must be validated on protected local pairs.
-/

/-- Normalized query-to-region costs. Lower is better on every axis. -/
structure RetrievalCoordinates where
  semanticCost : Rat := 0
  sparseCost : Rat := 0
  graphCost : Rat := 0
  metadataCost : Rat := 0
  distortionPenalty : Rat := 0
  uncertaintyPenalty : Rat := 0
  deriving Repr, BEq

/-- Typed scalarization of coordinate evidence. The product implementation
should keep these values in config and validate them per query class rather
than add another environment-variable surface. -/
structure RetrievalCoordinateWeights where
  semantic : Rat := 1
  sparse : Rat := 1
  graph : Rat := 0
  metadata : Rat := 0
  distortion : Rat := 1
  uncertainty : Rat := 1
  deriving Repr, BEq

def NonnegativeCoordinateWeights (weights : RetrievalCoordinateWeights) : Prop :=
  0 ≤ weights.semantic ∧
    0 ≤ weights.sparse ∧
    0 ≤ weights.graph ∧
    0 ≤ weights.metadata ∧
    0 ≤ weights.distortion ∧
    0 ≤ weights.uncertainty

/-- Query-to-region routing energy. This orders already-observed regions; it
does not by itself certify that hard narrowing is safe. -/
def routingEnergy
    (weights : RetrievalCoordinateWeights) (coordinates : RetrievalCoordinates) : Rat :=
  weights.semantic * coordinates.semanticCost +
    weights.sparse * coordinates.sparseCost +
    weights.graph * coordinates.graphCost +
    weights.metadata * coordinates.metadataCost +
    weights.distortion * coordinates.distortionPenalty +
    weights.uncertainty * coordinates.uncertaintyPenalty

/-- One observed local chart. Rates use integer counts to keep the formal core
independent of floating-point behavior. Distortion is evaluated only on the
protected semantic/KG/reciprocal-neighbor pairs relevant to retrieval. -/
structure RetrievalChartObservation where
  chartId : String := ""
  cover : TopologyCoverLevel := {}
  coordinateDimension : Nat := 0
  localIntrinsicDimensionMilli : Nat := 0
  supportDocuments : Nat := 0
  observedMaxOverlap : Nat := 0
  protectedPairs : Nat := 0
  distortionMilliSum : Nat := 0
  uncertaintyPerThousand : Nat := 1000
  deriving Repr, BEq

structure RetrievalChartConfig where
  minSupportDocuments : Nat := 1
  maxLocalIntrinsicDimensionMilli : Nat := 0
  maxObservedOverlap : Nat := 1
  maxMeanDistortionMilli : Nat := 0
  maxUncertaintyPerThousand : Nat := 0
  deriving Repr, BEq

/-- A chart with no protected-pair observations cannot certify local geometry.
`distortionMilliSum` is the sum of per-pair absolute similarity disagreement,
rounded into thousandths by the implementation. -/
def EmpiricalLocalDistortionWithin
    (maxMeanDistortionMilli : Nat) (chart : RetrievalChartObservation) : Prop :=
  0 < chart.protectedPairs ∧
    chart.distortionMilliSum ≤ maxMeanDistortionMilli * chart.protectedPairs

/-- Trust envelope for a local coordinate chart. A zero local-dimension cap is
the explicit "unset" value at this abstract boundary. -/
def CoordinateChartAdmissible
    (cfg : RetrievalChartConfig) (chart : RetrievalChartObservation) : Prop :=
  cfg.minSupportDocuments ≤ chart.supportDocuments ∧
    (cfg.maxLocalIntrinsicDimensionMilli = 0 ∨
      chart.localIntrinsicDimensionMilli ≤ cfg.maxLocalIntrinsicDimensionMilli) ∧
    chart.observedMaxOverlap ≤ cfg.maxObservedOverlap ∧
    EmpiricalLocalDistortionWithin cfg.maxMeanDistortionMilli chart ∧
    chart.uncertaintyPerThousand ≤ cfg.maxUncertaintyPerThousand

/-- A retrieval atlas is a finite family of local, possibly overlapping charts.
The cover level records topology; the coordinate observation records geometry
and its empirical trust envelope. -/
structure RetrievalAtlas where
  charts : List RetrievalChartObservation := []
  deriving Repr, BEq

def AtlasCoversCorpus
    (atlas : RetrievalAtlas) (corpus : List Yams.Core.DocumentId) : Prop :=
  ∀ doc ∈ corpus,
    ∃ chart ∈ atlas.charts, doc ∈ coverDocs chart.cover

/-- A document belongs to the query's protected neighborhood when a measured
relation connects it to the query anchor in either direction. The relation may
represent judged relevance, a reciprocal semantic edge, a citation, or another
retrieval-specific invariant. -/
def InProtectedQueryNeighborhood
    (anchor doc : Yams.Core.DocumentId)
    (pairs : List ProtectedDocumentPair) : Prop :=
  ∃ pair ∈ pairs,
    (pair.left = anchor ∧ pair.right = doc) ∨
      (pair.left = doc ∧ pair.right = anchor)

/-- Completeness assumption connecting unknown relevance to observable local
relations. This is an explicit construction/calibration obligation, not a
consequence of low coordinate distortion. -/
def RelevantSetProtected
    (anchor : Yams.Core.DocumentId)
    (pairs : List ProtectedDocumentPair)
    (relevantDocs : List Yams.Core.DocumentId) : Prop :=
  ∀ doc ∈ relevantDocs, InProtectedQueryNeighborhood anchor doc pairs

/-- Runtime materialization connects a selected chart cover to the document
allow-list consumed by the vector leg. It permits extra allowed documents. -/
def CertificateMaterializesCover
    (certificate : SelectiveRouteCertificate) (cover : TopologyCoverLevel) : Prop :=
  ∀ doc ∈ coverDocs cover, doc ∈ certificate.allowedDocs

/-- Candidate-stage membership in the declared relevant set. -/
def CandidateDocsWithin
    (candidates : List ComponentCandidate)
    (docs : List Yams.Core.DocumentId) : Prop :=
  ∀ candidate ∈ candidates, candidate.doc ∈ docs

/-- Pair preservation plus cover materialization derives relevant-document
coverage instead of assuming certificate coverage directly. -/
theorem preservedNeighborhood_materializedCover_coversRelevant
    {anchor : Yams.Core.DocumentId}
    {pairs : List ProtectedDocumentPair}
    {relevantDocs : List Yams.Core.DocumentId}
    {cover : TopologyCoverLevel}
    {certificate : SelectiveRouteCertificate}
    (hRelevant : RelevantSetProtected anchor pairs relevantDocs)
    (hPairs : PreservesLocalPairs cover pairs)
    (hMaterialized : CertificateMaterializesCover certificate cover) :
    ∀ doc ∈ relevantDocs, doc ∈ certificate.allowedDocs := by
  intro doc hDoc
  rcases hRelevant doc hDoc with ⟨pair, hPair, hEndpoints⟩
  rcases hPairs pair hPair with ⟨cluster, hCluster, hLeft, hRight⟩
  apply hMaterialized
  simp [coverDocs, fragmentUnionDocs]
  refine ⟨cluster, hCluster, ?_⟩
  rcases hEndpoints with ⟨_, hDocRight⟩ | ⟨hDocLeft, _⟩
  · simpa [hDocRight] using hRight
  · simpa [hDocLeft] using hLeft

/-- Evidence used to order a region and evidence used to admit narrowing remain
separate. This prevents a low routing energy from masquerading as a recall
certificate. -/
structure CoordinateRouteObservation where
  chart : RetrievalChartObservation := {}
  coordinates : RetrievalCoordinates := {}
  certificate : SelectiveRouteCertificate := {}
  chartRiskObserved : Bool := false
  routeRiskObserved : Bool := false
  workObserved : Bool := false
  deriving Repr, BEq

structure CoordinateRoutingConfig where
  chart : RetrievalChartConfig := {}
  route : SelectiveRouteConfig := {}
  weights : RetrievalCoordinateWeights := {}
  deriving Repr, BEq

/-- Hard narrowing requires positive observation status for every certificate
class. A zero-valued counter is evidence only when its producer marked that
counter as observed. -/
def RequiredCoordinateEvidenceObserved (observation : CoordinateRouteObservation) : Prop :=
  observation.chartRiskObserved = true ∧
    observation.routeRiskObserved = true ∧
    observation.workObserved = true

def CoordinateRouteAdmissible
    (cfg : CoordinateRoutingConfig) (observation : CoordinateRouteObservation) : Prop :=
  RequiredCoordinateEvidenceObserved observation ∧
    CoordinateChartAdmissible cfg.chart observation.chart ∧
    RouteCertificateAdmissible cfg.route observation.certificate

instance coordinateRouteAdmissibleDecidable
    (cfg : CoordinateRoutingConfig)
    (observation : CoordinateRouteObservation) :
    Decidable (CoordinateRouteAdmissible cfg observation) := by
  unfold CoordinateRouteAdmissible RequiredCoordinateEvidenceObserved CoordinateChartAdmissible
    EmpiricalLocalDistortionWithin RouteCertificateAdmissible
    EmpiricalMissRiskWithin EffortWithinBudget
  infer_instance

/-- The coordinate-aware policy keeps the existing global/augment/narrow
actions. Geometry can veto narrowing, but a useful rejected route may still
augment the global candidate set. -/
def selectCoordinateRoutingAction
    (cfg : CoordinateRoutingConfig)
    (observation : CoordinateRouteObservation) : SelectiveRoutingAction :=
  if CoordinateRouteAdmissible cfg observation then
    .narrow
  else if observation.certificate.allowedDocs.isEmpty then
    .global
  else
    .augment

/-- Candidate-set effect of coordinate-aware routing before ordinary fusion and
top-k truncation. -/
def applyCoordinateRoute
    (cfg : CoordinateRoutingConfig)
    (observation : CoordinateRouteObservation)
    (globalCandidates : List ComponentCandidate) : List ComponentCandidate :=
  match selectCoordinateRoutingAction cfg observation with
  | .global => globalCandidates
  | .augment =>
      globalCandidates ++ allowedDocumentCandidates observation.certificate.allowedDocs
  | .narrow =>
      globalCandidates.filter
        (fun candidate => observation.certificate.allowedDocs.contains candidate.doc)

/-- Geometry-to-recall bridge at the candidate stage. If the ideal relevant
top-k is present in global retrieval, belongs to the protected query
neighborhood, and the selected chart preserves and materializes that
neighborhood, coordinate routing cannot remove it. This theorem does not claim
that later fusion or top-k truncation preserves ranking. -/
theorem coordinateRoute_relevantTopK_subset_routedCandidates
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    {globalCandidates relevantTopK : List ComponentCandidate}
    {anchor : Yams.Core.DocumentId}
    {pairs : List ProtectedDocumentPair}
    {relevantDocs : List Yams.Core.DocumentId}
    (hGlobal : ∀ candidate ∈ relevantTopK, candidate ∈ globalCandidates)
    (hTopKRelevant : CandidateDocsWithin relevantTopK relevantDocs)
    (hRelevant : RelevantSetProtected anchor pairs relevantDocs)
    (hPairs : PreservesLocalPairs observation.chart.cover pairs)
    (hMaterialized :
      CertificateMaterializesCover observation.certificate observation.chart.cover) :
    ∀ candidate ∈ relevantTopK,
      candidate ∈ applyCoordinateRoute cfg observation globalCandidates := by
  intro candidate hTopK
  have hAllowed : candidate.doc ∈ observation.certificate.allowedDocs :=
    preservedNeighborhood_materializedCover_coversRelevant
      hRelevant hPairs hMaterialized candidate.doc (hTopKRelevant candidate hTopK)
  by_cases hAdmissible : CoordinateRouteAdmissible cfg observation
  · simp [applyCoordinateRoute, selectCoordinateRoutingAction, hAdmissible,
      hGlobal candidate hTopK, hAllowed]
  · by_cases hEmpty : observation.certificate.allowedDocs = []
    · simpa [applyCoordinateRoute, selectCoordinateRoutingAction, hAdmissible, hEmpty] using
        hGlobal candidate hTopK
    · simp [applyCoordinateRoute, selectCoordinateRoutingAction, hAdmissible, hEmpty,
        hGlobal candidate hTopK]

/-- Candidate-level meaning of a strictly better-focused route: it retains the
declared relevant set, invents nothing outside the global candidate set, and
removes at least one global candidate. This does not order the retained set. -/
def StrictlyFocusesCandidates
    (relevant baseline routed : List ComponentCandidate) : Prop :=
  (∀ candidate ∈ relevant, candidate ∈ routed) ∧
    (∀ candidate ∈ routed, candidate ∈ baseline) ∧
    ∃ candidate ∈ baseline, candidate ∉ routed

/-- An admitted coordinate route is strictly better-focused when the local
geometry preserves the relevant top-k and at least one global candidate lies
outside the materialized chart. -/
theorem coordinateRoute_admitted_strictlyFocusesRelevantTopK
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    {globalCandidates relevantTopK : List ComponentCandidate}
    {anchor : Yams.Core.DocumentId}
    {pairs : List ProtectedDocumentPair}
    {relevantDocs : List Yams.Core.DocumentId}
    {outside : ComponentCandidate}
    (hAdmitted : CoordinateRouteAdmissible cfg observation)
    (hGlobal : ∀ candidate ∈ relevantTopK, candidate ∈ globalCandidates)
    (hTopKRelevant : CandidateDocsWithin relevantTopK relevantDocs)
    (hRelevant : RelevantSetProtected anchor pairs relevantDocs)
    (hPairs : PreservesLocalPairs observation.chart.cover pairs)
    (hMaterialized :
      CertificateMaterializesCover observation.certificate observation.chart.cover)
    (hOutsideGlobal : outside ∈ globalCandidates)
    (hOutsideAllowed : outside.doc ∉ observation.certificate.allowedDocs) :
    StrictlyFocusesCandidates relevantTopK globalCandidates
      (applyCoordinateRoute cfg observation globalCandidates) := by
  refine ⟨coordinateRoute_relevantTopK_subset_routedCandidates
    hGlobal hTopKRelevant hRelevant hPairs hMaterialized, ?_, ?_⟩
  · intro candidate hRouted
    have hFiltered : candidate ∈ globalCandidates ∧
        candidate.doc ∈ observation.certificate.allowedDocs := by
      simpa [applyCoordinateRoute, selectCoordinateRoutingAction, hAdmitted] using hRouted
    exact hFiltered.1
  · refine ⟨outside, hOutsideGlobal, ?_⟩
    intro hRouted
    have hFiltered : outside ∈ globalCandidates ∧
        outside.doc ∈ observation.certificate.allowedDocs := by
      simpa [applyCoordinateRoute, selectCoordinateRoutingAction, hAdmitted] using hRouted
    exact hOutsideAllowed hFiltered.2

/-- Hard narrowing is selected exactly when both the local chart and the
empirical route certificate are admissible and their required observations
are available. -/
theorem coordinateRoute_narrow_iff_admissible
    (cfg : CoordinateRoutingConfig)
    (observation : CoordinateRouteObservation) :
    selectCoordinateRoutingAction cfg observation = .narrow ↔
      CoordinateRouteAdmissible cfg observation := by
  by_cases hAdmissible : CoordinateRouteAdmissible cfg observation
  · simp [selectCoordinateRoutingAction, hAdmissible]
  · by_cases hEmpty : observation.certificate.allowedDocs = []
    · simp [selectCoordinateRoutingAction, hAdmissible, hEmpty]
    · simp [selectCoordinateRoutingAction, hAdmissible, hEmpty]

/-- Coordinate admission exposes the chart trust obligation independently of
the miss-risk and work-budget certificate. -/
theorem coordinateRoute_admitted_chartTrusted
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    (hAdmitted : CoordinateRouteAdmissible cfg observation) :
    CoordinateChartAdmissible cfg.chart observation.chart := by
  exact hAdmitted.2.1

/-- Coordinate admission also preserves the existing route-risk and work
contract. -/
theorem coordinateRoute_admitted_routeCertified
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    (hAdmitted : CoordinateRouteAdmissible cfg observation) :
    RouteCertificateAdmissible cfg.route observation.certificate := by
  exact hAdmitted.2.2

/-- Missing required observations force the policy to abstain from hard
narrowing even if default-valued counters happen to satisfy numeric bounds. -/
theorem coordinateRoute_unobserved_neverNarrows
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    (hUnobserved : ¬ RequiredCoordinateEvidenceObserved observation) :
    selectCoordinateRoutingAction cfg observation ≠ .narrow := by
  intro hNarrow
  have hAdmissible :=
    (coordinateRoute_narrow_iff_admissible cfg observation).mp hNarrow
  exact hUnobserved hAdmissible.1

/-- Without a coordinate trust certificate, the action cannot be hard
narrowing. -/
theorem coordinateRoute_untrusted_neverNarrows
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    (hUntrusted : ¬ CoordinateChartAdmissible cfg.chart observation.chart) :
    selectCoordinateRoutingAction cfg observation ≠ .narrow := by
  intro hNarrow
  have hAdmissible :=
    (coordinateRoute_narrow_iff_admissible cfg observation).mp hNarrow
  exact hUntrusted hAdmissible.2.1

end Yams.SearchEngine
