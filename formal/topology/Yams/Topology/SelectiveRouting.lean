import Yams.Topology.TopologyInfluence

namespace Yams.SearchEngine

/-!
Selective topology routing for generalized-memory search.

The model deliberately separates three obligations that are easy to conflate in
an implementation:

* topology construction exposes an overlapping, multiscale cover;
* offline observations calibrate an empirical miss-risk certificate;
* the runtime policy may narrow only when both that certificate and a work
  budget are satisfied, otherwise it augments or falls back to global ANN.

The deterministic theorems below are conditional on certificate soundness. The
empirical predicates are benchmark obligations; they are not presented as a
distribution-free statistical proof.
-/

/-- One resolution of an overlapping topology cover. `scale` is an ordinal
level, not a metric distance: callers may map it to graph radius, kNN depth, or
another construction-specific resolution. -/
structure TopologyCoverLevel where
  scale : Nat := 0
  clusters : List TopologyCluster := []
  deriving Repr, BEq

/-- Documents represented at one cover level. Repetition is intentional because
a document may be a member of several local neighborhoods. -/
def coverDocs (cover : TopologyCoverLevel) : List Yams.Core.DocumentId :=
  fragmentUnionDocs cover.clusters

/-- A cover represents every document in the corpus. Unlike a partition, this
does not require cluster disjointness. -/
def CoversCorpus
    (cover : TopologyCoverLevel) (corpus : List Yams.Core.DocumentId) : Prop :=
  ∀ doc ∈ corpus, doc ∈ coverDocs cover

/-- `fine` refines `coarse` when every fine neighborhood is contained in some
coarse neighborhood. Overlap is allowed at both levels. -/
def RefinesCover (fine coarse : TopologyCoverLevel) : Prop :=
  ∀ fineCluster ∈ fine.clusters,
    ∃ coarseCluster ∈ coarse.clusters,
      ∀ doc ∈ fineCluster.docs, doc ∈ coarseCluster.docs

/-- A finite overlap cap keeps a cover from degenerating into unbounded repeated
candidate work. -/
def BoundedCoverOverlap (cover : TopologyCoverLevel) (maxOverlap : Nat) : Prop :=
  ∀ doc,
    (cover.clusters.filter (fun cluster => cluster.docs.contains doc)).length ≤ maxOverlap

/-- Abstract local relation that a topology construction promises to preserve.
It can represent reciprocal graph edges, semantic neighbors, citations, or
cross-corpus links. -/
structure ProtectedDocumentPair where
  left : Yams.Core.DocumentId
  right : Yams.Core.DocumentId
  deriving Repr, BEq

/-- Every protected local relation has a witnessing neighborhood in the cover. -/
def PreservesLocalPairs
    (cover : TopologyCoverLevel) (pairs : List ProtectedDocumentPair) : Prop :=
  ∀ pair ∈ pairs,
    ∃ cluster ∈ cover.clusters,
      pair.left ∈ cluster.docs ∧ pair.right ∈ cluster.docs

/-- Refinement cannot invent documents outside the coarser cover. -/
theorem refinement_noArbitraryDocuments
    {fine coarse : TopologyCoverLevel}
    (hRefines : RefinesCover fine coarse)
    {doc : Yams.Core.DocumentId}
    (hDoc : doc ∈ coverDocs fine) :
    doc ∈ coverDocs coarse := by
  simp [coverDocs, fragmentUnionDocs] at hDoc ⊢
  rcases hDoc with ⟨fineCluster, hFineCluster, hFineDoc⟩
  rcases hRefines fineCluster hFineCluster with
    ⟨coarseCluster, hCoarseCluster, hContained⟩
  exact ⟨coarseCluster, hCoarseCluster, hContained doc hFineDoc⟩

/-- If a fine cover represents the corpus and refines a coarse cover, the coarse
cover represents the corpus as well. -/
theorem refinement_preservesCorpusCoverage
    {fine coarse : TopologyCoverLevel}
    {corpus : List Yams.Core.DocumentId}
    (hRefines : RefinesCover fine coarse)
    (hCovers : CoversCorpus fine corpus) :
    CoversCorpus coarse corpus := by
  intro doc hCorpus
  exact refinement_noArbitraryDocuments hRefines (hCovers doc hCorpus)

/-- Distance work is kept explicit so routed and global ANN can be compared at
equal candidate and exact-evaluation budgets. -/
structure SearchEffort where
  annCandidates : Nat := 0
  distanceEvaluations : Nat := 0
  exactReranks : Nat := 0
  deriving Repr, BEq

structure SearchEffortBudget where
  maxAnnCandidates : Nat := 0
  maxDistanceEvaluations : Nat := 0
  maxExactReranks : Nat := 0
  deriving Repr, BEq

def EffortWithinBudget (budget : SearchEffortBudget) (effort : SearchEffort) : Prop :=
  effort.annCandidates ≤ budget.maxAnnCandidates ∧
    effort.distanceEvaluations ≤ budget.maxDistanceEvaluations ∧
    effort.exactReranks ≤ budget.maxExactReranks

/-- Offline validation of a route class. Miss risk is represented as an integer
rate per 1000 protected candidates so the core model needs no floating-point or
probability dependency. -/
structure RouteRiskObservation where
  calibrationQueries : Nat := 0
  protectedCandidates : Nat := 0
  missedProtectedCandidates : Nat := 0
  deriving Repr, BEq

structure SelectiveRouteConfig where
  minRouteMargin : Nat := 0
  minSeedHits : Nat := 0
  minCalibrationQueries : Nat := 1
  maxMissesPerThousand : Nat := 0
  effortBudget : SearchEffortBudget := {}
  deriving Repr, BEq

/-- Evidence available to the routing boundary. `risk` comes from an offline
query class or corpus-stratified calibration run; it is not recomputed from
unknown relevance judgments at query time. -/
structure SelectiveRouteCertificate where
  allowedDocs : List Yams.Core.DocumentId := []
  routeMargin : Nat := 0
  seedHits : Nat := 0
  risk : RouteRiskObservation := {}
  effort : SearchEffort := {}
  deriving Repr, BEq

/-- Empirical miss-risk threshold. Non-empty protected evidence prevents a route
class with no relevance observations from being certified accidentally. -/
def EmpiricalMissRiskWithin
    (maxMissesPerThousand : Nat) (obs : RouteRiskObservation) : Prop :=
  0 < obs.protectedCandidates ∧
    obs.missedProtectedCandidates * 1000 ≤
      maxMissesPerThousand * obs.protectedCandidates

/-- Complete admission predicate for hard narrowing. -/
def RouteCertificateAdmissible
    (cfg : SelectiveRouteConfig) (certificate : SelectiveRouteCertificate) : Prop :=
  cfg.minRouteMargin ≤ certificate.routeMargin ∧
    cfg.minSeedHits ≤ certificate.seedHits ∧
    cfg.minCalibrationQueries ≤ certificate.risk.calibrationQueries ∧
    EmpiricalMissRiskWithin cfg.maxMissesPerThousand certificate.risk ∧
    EffortWithinBudget cfg.effortBudget certificate.effort

instance routeCertificateAdmissibleDecidable
    (cfg : SelectiveRouteConfig)
    (certificate : SelectiveRouteCertificate) :
    Decidable (RouteCertificateAdmissible cfg certificate) := by
  unfold RouteCertificateAdmissible EmpiricalMissRiskWithin EffortWithinBudget
  infer_instance

/-- Selective routing is intentionally three-way. Weak evidence with a useful
route augments global ANN; no useful route falls back; only admitted evidence
may replace global candidates. -/
inductive SelectiveRoutingAction where
  | global
  | augment
  | narrow
  deriving Repr, BEq, DecidableEq

def selectRoutingAction
    (cfg : SelectiveRouteConfig)
    (certificate : SelectiveRouteCertificate) : SelectiveRoutingAction :=
  if RouteCertificateAdmissible cfg certificate then
    .narrow
  else if certificate.allowedDocs.isEmpty then
    .global
  else
    .augment

/-- Convert allowed documents into query-score placeholders. The production
implementation must replace the zero score with query similarity before fusion. -/
def allowedDocumentCandidates
    (allowedDocs : List Yams.Core.DocumentId) : List ComponentCandidate :=
  allowedDocs.map (fun doc => { doc := doc, source := .vector, score := 0 })

def applySelectiveRoute
    (cfg : SelectiveRouteConfig)
    (certificate : SelectiveRouteCertificate)
    (globalCandidates : List ComponentCandidate) : List ComponentCandidate :=
  match selectRoutingAction cfg certificate with
  | .global => globalCandidates
  | .augment => globalCandidates ++ allowedDocumentCandidates certificate.allowedDocs
  | .narrow =>
      globalCandidates.filter
        (fun candidate => certificate.allowedDocs.contains candidate.doc)

/-- An inadmissible empty route is exactly global ANN. -/
theorem selectiveRoute_emptyFallback_identity
    {cfg : SelectiveRouteConfig}
    {certificate : SelectiveRouteCertificate}
    {globalCandidates : List ComponentCandidate}
    (hRejected : ¬ RouteCertificateAdmissible cfg certificate)
    (hEmpty : certificate.allowedDocs = []) :
    applySelectiveRoute cfg certificate globalCandidates = globalCandidates := by
  simp [applySelectiveRoute, selectRoutingAction, hRejected, hEmpty]

/-- An inadmissible non-empty route is additive and cannot remove a global ANN
candidate before the ordinary fusion budget is applied. -/
theorem selectiveRoute_augmentation_preservesGlobal
    {cfg : SelectiveRouteConfig}
    {certificate : SelectiveRouteCertificate}
    {globalCandidates : List ComponentCandidate}
    {candidate : ComponentCandidate}
    (hRejected : ¬ RouteCertificateAdmissible cfg certificate)
    (hNonEmpty : certificate.allowedDocs ≠ [])
    (hCandidate : candidate ∈ globalCandidates) :
    candidate ∈ applySelectiveRoute cfg certificate globalCandidates := by
  simp [applySelectiveRoute, selectRoutingAction, hRejected, hNonEmpty]
  exact Or.inl hCandidate

/-- Hard narrowing cannot invent a candidate absent from global ANN. -/
theorem selectiveRoute_narrow_subset
    {cfg : SelectiveRouteConfig}
    {certificate : SelectiveRouteCertificate}
    {globalCandidates : List ComponentCandidate}
    {candidate : ComponentCandidate}
    (hAdmitted : RouteCertificateAdmissible cfg certificate)
    (hCandidate : candidate ∈ applySelectiveRoute cfg certificate globalCandidates) :
    candidate ∈ globalCandidates := by
  have hFiltered : candidate ∈ globalCandidates ∧
      candidate.doc ∈ certificate.allowedDocs := by
    simpa [applySelectiveRoute, selectRoutingAction, hAdmitted] using hCandidate
  exact hFiltered.1

/-- Runtime soundness obligation for a protected candidate set. Calibration may
justify trusting a route class, but the deterministic preservation theorem needs
the selected allowed-document set to cover the protected candidates. -/
def CertificateCoversProtected
    (certificate : SelectiveRouteCertificate)
    (protectedCandidates : List ComponentCandidate) : Prop :=
  ∀ candidate ∈ protectedCandidates, candidate.doc ∈ certificate.allowedDocs

/-- The three-way policy preserves every protected global candidate when any
admitted hard route satisfies its coverage obligation. Rejected routes are safe
by construction because they augment or fall back. -/
theorem selectiveRoute_preservesProtected
    {cfg : SelectiveRouteConfig}
    {certificate : SelectiveRouteCertificate}
    {globalCandidates protectedCandidates : List ComponentCandidate}
    (hPresent : ∀ candidate ∈ protectedCandidates, candidate ∈ globalCandidates)
    (hSound : RouteCertificateAdmissible cfg certificate →
      CertificateCoversProtected certificate protectedCandidates) :
    ∀ candidate ∈ protectedCandidates,
      candidate ∈ applySelectiveRoute cfg certificate globalCandidates := by
  intro candidate hProtected
  by_cases hAdmitted : RouteCertificateAdmissible cfg certificate
  · simp [applySelectiveRoute, selectRoutingAction, hAdmitted,
      hPresent candidate hProtected, hSound hAdmitted candidate hProtected]
  · by_cases hEmpty : certificate.allowedDocs = []
    · simpa [applySelectiveRoute, selectRoutingAction, hAdmitted, hEmpty] using
        hPresent candidate hProtected
    · simp [applySelectiveRoute, selectRoutingAction, hAdmitted, hEmpty,
        hPresent candidate hProtected]

/-- Admission exposes each equal-budget comparison obligation directly. -/
theorem admissibleRoute_respectsEffortBudget
    {cfg : SelectiveRouteConfig}
    {certificate : SelectiveRouteCertificate}
    (hAdmitted : RouteCertificateAdmissible cfg certificate) :
    EffortWithinBudget cfg.effortBudget certificate.effort := by
  exact hAdmitted.2.2.2.2

/-- Per-corpus recall observations use counts rather than rounded floating-point
metrics. Cross multiplication allows baseline and current runs to have different
numbers of relevance judgments. -/
structure CorpusRecallObservation where
  corpus : String
  relevantJudgments : Nat := 0
  retrievedRelevant : Nat := 0
  deriving Repr, BEq

def ValidCorpusRecallObservation (obs : CorpusRecallObservation) : Prop :=
  0 < obs.relevantJudgments ∧ obs.retrievedRelevant ≤ obs.relevantJudgments

def RecallAtLeast
    (baseline current : CorpusRecallObservation) : Prop :=
  baseline.retrievedRelevant * current.relevantJudgments ≤
    current.retrievedRelevant * baseline.relevantJudgments

structure CorpusRecallComparison where
  baseline : CorpusRecallObservation
  current : CorpusRecallObservation
  deriving Repr, BEq

/-- A product-promotion gate is conjunctive over corpora: an aggregate gain may
not hide a regression on a smaller memory domain. -/
def PreservesCorpusRecall (comparison : CorpusRecallComparison) : Prop :=
  comparison.baseline.corpus = comparison.current.corpus ∧
    ValidCorpusRecallObservation comparison.baseline ∧
    ValidCorpusRecallObservation comparison.current ∧
    RecallAtLeast comparison.baseline comparison.current

def GeneralizedMemoryRecallFloor
    (comparisons : List CorpusRecallComparison) : Prop :=
  ∀ comparison ∈ comparisons, PreservesCorpusRecall comparison

/-- One failing corpus is enough to reject default promotion, even if a macro or
pooled metric improved. -/
theorem generalizedMemoryFloor_rejectsCorpusRegression
    {comparisons : List CorpusRecallComparison}
    {comparison : CorpusRecallComparison}
    (hMember : comparison ∈ comparisons)
    (hRegression : ¬ PreservesCorpusRecall comparison) :
    ¬ GeneralizedMemoryRecallFloor comparisons := by
  intro hFloor
  exact hRegression (hFloor comparison hMember)

/-- With equal relevance judgments, preserving retrieved-relevant count is
sufficient for the rational recall floor. -/
theorem recallAtLeast_of_equalJudgments_and_hits
    {baseline current : CorpusRecallObservation}
    (hJudgments : baseline.relevantJudgments = current.relevantJudgments)
    (hHits : baseline.retrievedRelevant ≤ current.retrievedRelevant) :
    RecallAtLeast baseline current := by
  unfold RecallAtLeast
  rw [← hJudgments]
  exact Nat.mul_le_mul_right baseline.relevantJudgments hHits

end Yams.SearchEngine
