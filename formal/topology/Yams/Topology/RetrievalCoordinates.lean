import Yams.Topology.SelectiveRouting

namespace Yams.SearchEngine

/-!
Protected-relation covers for selective topology routing.

Embeddings locate query anchors, but do not certify that a narrowed candidate
set preserves recall.  The proof boundary is instead an explicit overlapping
cover whose fibers retain observed retrieval relations.  Those relations may
come from judged relevance, reciprocal semantic edges, citations, links, or
other retrieval-specific producers.
-/

/-- One selected cover fiber and its observed relation-preservation counts. -/
structure ProtectedRelationCoverObservation where
  coverId : String := ""
  cover : TopologyCoverLevel := {}
  protectedPairs : Nat := 0
  preservedProtectedPairs : Nat := 0
  deriving Repr, BEq

/-- A cover can certify narrowing only when it has observed at least one
protected relation and preserved all observed relations. -/
def ProtectedRelationCoverAdmissible
    (observation : ProtectedRelationCoverObservation) : Prop :=
  0 < observation.protectedPairs ∧
    observation.preservedProtectedPairs = observation.protectedPairs

/-- A retrieval cover is a finite family of possibly overlapping relation fibers. -/
structure ProtectedRelationAtlas where
  fibers : List ProtectedRelationCoverObservation := []
  deriving Repr, BEq

def AtlasCoversCorpus
    (atlas : ProtectedRelationAtlas) (corpus : List Yams.Core.DocumentId) : Prop :=
  ∀ doc ∈ corpus,
    ∃ fiber ∈ atlas.fibers, doc ∈ coverDocs fiber.cover

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
consequence of embedding proximity. -/
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

/-- A protected document is fiber-represented when some document in the
selected cover has an observed protected relation to it. -/
def ProtectedFibersRepresented
    (protectedRelation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop)
    (cover : TopologyCoverLevel)
    (protectedDocs : List Yams.Core.DocumentId) : Prop :=
  ∀ doc ∈ protectedDocs,
    ∃ representative ∈ coverDocs cover, protectedRelation representative doc

/-- A materialized certificate is saturated over protected-relation fibers
when selecting one representative cannot discard a related protected document. -/
def CertificateSaturatesProtectedFibers
    (protectedRelation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop)
    (certificate : SelectiveRouteCertificate)
    (protectedDocs : List Yams.Core.DocumentId) : Prop :=
  ∀ representative ∈ certificate.allowedDocs,
    ∀ doc ∈ protectedDocs,
      protectedRelation representative doc → doc ∈ certificate.allowedDocs

/-- Fiber representation, complete cover materialization, and fiber saturation
jointly cover the protected set. No geometric premise is used. -/
theorem represented_materialized_saturatedFibers_coverProtected
    {protectedRelation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {cover : TopologyCoverLevel}
    {certificate : SelectiveRouteCertificate}
    {protectedDocs : List Yams.Core.DocumentId}
    (hRepresented : ProtectedFibersRepresented protectedRelation cover protectedDocs)
    (hMaterialized : CertificateMaterializesCover certificate cover)
    (hSaturated :
      CertificateSaturatesProtectedFibers protectedRelation certificate protectedDocs) :
    ∀ doc ∈ protectedDocs, doc ∈ certificate.allowedDocs := by
  intro doc hProtected
  rcases hRepresented doc hProtected with ⟨representative, hCover, hRelation⟩
  exact hSaturated representative (hMaterialized representative hCover)
    doc hProtected hRelation

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

/-- Route selection evidence and narrowing admission remain separate. An
embedding-selected cover cannot masquerade as a recall certificate. -/
structure CoordinateRouteObservation where
  relationCover : ProtectedRelationCoverObservation := {}
  certificate : SelectiveRouteCertificate := {}
  coordinateSpaceAlignmentObserved : Bool := false
  protectedRelationCoverageObserved : Bool := false
  protectedFibersRepresentedObserved : Bool := false
  protectedFiberSaturationObserved : Bool := false
  routeRiskObserved : Bool := false
  workObserved : Bool := false
  deriving Repr, BEq

structure CoordinateRoutingConfig where
  route : SelectiveRouteConfig := {}
  deriving Repr, BEq

/-- Hard narrowing requires positive observation status for every certificate
class. A zero-valued counter is evidence only when its producer marked that
counter as observed. -/
def RequiredCoordinateEvidenceObserved (observation : CoordinateRouteObservation) : Prop :=
  observation.coordinateSpaceAlignmentObserved = true ∧
    observation.protectedRelationCoverageObserved = true ∧
    observation.protectedFibersRepresentedObserved = true ∧
    observation.protectedFiberSaturationObserved = true ∧
    observation.routeRiskObserved = true ∧
    observation.workObserved = true

def CoordinateRouteAdmissible
    (cfg : CoordinateRoutingConfig) (observation : CoordinateRouteObservation) : Prop :=
  RequiredCoordinateEvidenceObserved observation ∧
    ProtectedRelationCoverAdmissible observation.relationCover ∧
    RouteCertificateAdmissible cfg.route observation.certificate

instance coordinateRouteAdmissibleDecidable
    (cfg : CoordinateRoutingConfig)
    (observation : CoordinateRouteObservation) :
    Decidable (CoordinateRouteAdmissible cfg observation) := by
  unfold CoordinateRouteAdmissible RequiredCoordinateEvidenceObserved
    ProtectedRelationCoverAdmissible RouteCertificateAdmissible
    EmpiricalMissRiskWithin EffortWithinBudget
  infer_instance

/-- The relation-cover policy keeps the existing global/augment/narrow
actions. A useful uncertified route may still augment the global candidate set. -/
def selectCoordinateRoutingAction
    (cfg : CoordinateRoutingConfig)
    (observation : CoordinateRouteObservation) : SelectiveRoutingAction :=
  if CoordinateRouteAdmissible cfg observation then
    .narrow
  else if observation.certificate.allowedDocs.isEmpty then
    .global
  else
    .augment

/-- Candidate-set effect of relation-cover routing before ordinary fusion and
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

/-- Protected-relation bridge at the candidate stage. If the ideal relevant
top-k is present in global retrieval, belongs to the protected query
neighborhood, and the selected cover preserves and materializes that
neighborhood, routing cannot remove it. This theorem does not claim
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
    (hPairs : PreservesLocalPairs observation.relationCover.cover pairs)
    (hMaterialized :
      CertificateMaterializesCover observation.certificate observation.relationCover.cover) :
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

/-- Alternative protected-relation fiber bridge for the candidate stage.
Representation supplies a cover witness and saturation closes every related
protected fiber touched by the materialized certificate. -/
theorem coordinateRoute_fiberProtectedTopK_subset_routedCandidates
    {protectedRelation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    {globalCandidates protectedTopK : List ComponentCandidate}
    {protectedDocs : List Yams.Core.DocumentId}
    (hGlobal : ∀ candidate ∈ protectedTopK, candidate ∈ globalCandidates)
    (hTopKProtected : CandidateDocsWithin protectedTopK protectedDocs)
    (hRepresented :
      ProtectedFibersRepresented
        protectedRelation observation.relationCover.cover protectedDocs)
    (hMaterialized :
      CertificateMaterializesCover observation.certificate observation.relationCover.cover)
    (hSaturated :
      CertificateSaturatesProtectedFibers
        protectedRelation observation.certificate protectedDocs) :
    ∀ candidate ∈ protectedTopK,
      candidate ∈ applyCoordinateRoute cfg observation globalCandidates := by
  intro candidate hTopK
  have hAllowed : candidate.doc ∈ observation.certificate.allowedDocs :=
    represented_materialized_saturatedFibers_coverProtected
      hRepresented hMaterialized hSaturated candidate.doc
        (hTopKProtected candidate hTopK)
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

/-- An admitted route is strictly better-focused when the relation cover
preserves the relevant top-k and at least one global candidate lies outside
the materialized cover. -/
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
    (hPairs : PreservesLocalPairs observation.relationCover.cover pairs)
    (hMaterialized :
      CertificateMaterializesCover observation.certificate observation.relationCover.cover)
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

/-- Hard narrowing is selected exactly when both the relation cover and the
empirical route certificate are admissible and their required observations are available. -/
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

/-- Admission exposes the protected-relation obligation independently of the
miss-risk and work-budget certificate. -/
theorem coordinateRoute_admitted_relationCoverCertified
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    (hAdmitted : CoordinateRouteAdmissible cfg observation) :
    ProtectedRelationCoverAdmissible observation.relationCover := by
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

/-- Without a protected-relation cover certificate, the action cannot be hard narrowing. -/
theorem coordinateRoute_uncoveredRelation_neverNarrows
    {cfg : CoordinateRoutingConfig}
    {observation : CoordinateRouteObservation}
    (hUntrusted : ¬ ProtectedRelationCoverAdmissible observation.relationCover) :
    selectCoordinateRoutingAction cfg observation ≠ .narrow := by
  intro hNarrow
  have hAdmissible :=
    (coordinateRoute_narrow_iff_admissible cfg observation).mp hNarrow
  exact hUntrusted hAdmissible.2.1

end Yams.SearchEngine
