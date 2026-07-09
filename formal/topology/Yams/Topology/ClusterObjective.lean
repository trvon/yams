import Yams.Topology.Artifacts

namespace Yams.Topology

/-- Per-cluster quality signals used to compare topology algorithms without
assuming one concrete clustering implementation. Higher cohesion, separation,
stability, and route relevance are better; lower size/expansion is enforced by
caps rather than folded into the score. -/
structure ClusterQualityObservation where
  clusterId : ClusterId
  memberCount : Nat := 0
  expansionDocs : Nat := 0
  cohesion : Rat := 0
  separation : Rat := 0
  stability : Rat := 0
  routeRelevance : Rat := 0
  deriving Repr, BEq

/-- Weights and hard admission thresholds for a benchmark/theory objective. -/
structure ClusterObjectiveConfig where
  maxMemberCount : Nat := 64
  maxExpansionDocs : Nat := 64
  minCohesion : Rat := 0
  minSeparation : Rat := 0
  minStability : Rat := 0
  minRouteRelevance : Rat := 0
  cohesionWeight : Rat := 1
  separationWeight : Rat := 1
  stabilityWeight : Rat := 1
  routeRelevanceWeight : Rat := 1
  deriving Repr, BEq

/-- Hard safety/admission envelope for one cluster. These predicates isolate
"clusters too broad/noisy" before any retrieval metric is inspected. -/
def ValidClusterForObjective
    (cfg : ClusterObjectiveConfig) (cluster : ClusterQualityObservation) : Prop :=
  cluster.memberCount ≤ cfg.maxMemberCount ∧
    cluster.expansionDocs ≤ cfg.maxExpansionDocs ∧
    cfg.minCohesion ≤ cluster.cohesion ∧
    cfg.minSeparation ≤ cluster.separation ∧
    cfg.minStability ≤ cluster.stability ∧
    cfg.minRouteRelevance ≤ cluster.routeRelevance

/-- Scalar objective for comparing already-valid clusters or algorithms. -/
def clusterObjectiveScore
    (cfg : ClusterObjectiveConfig) (cluster : ClusterQualityObservation) : Rat :=
  cfg.cohesionWeight * cluster.cohesion +
    cfg.separationWeight * cluster.separation +
    cfg.stabilityWeight * cluster.stability +
    cfg.routeRelevanceWeight * cluster.routeRelevance

/-- A cluster scheme is a whole artifact-level algorithm choice plus measured
quality summaries for its produced clusters. -/
structure ClusterSchemeObservation where
  algorithm : String := ""
  clusters : List ClusterQualityObservation := []
  deriving Repr, BEq

/-- Scheme validity: every cluster produced by the algorithm satisfies the hard
admission envelope. -/
def ValidClusterScheme
    (cfg : ClusterObjectiveConfig) (scheme : ClusterSchemeObservation) : Prop :=
  ∀ cluster, cluster ∈ scheme.clusters → ValidClusterForObjective cfg cluster

/-- Candidate cluster dominance for Pareto checks. A cluster dominates another
when it is no worse on all positive quality dimensions and has at least one
strictly better quality dimension while staying no larger. -/
def DominatesCluster (a b : ClusterQualityObservation) : Prop :=
  a.memberCount ≤ b.memberCount ∧
    a.expansionDocs ≤ b.expansionDocs ∧
    b.cohesion ≤ a.cohesion ∧
    b.separation ≤ a.separation ∧
    b.stability ≤ a.stability ∧
    b.routeRelevance ≤ a.routeRelevance ∧
    (b.memberCount < a.memberCount ∨ b.expansionDocs < a.expansionDocs ∨
      b.cohesion < a.cohesion ∨ b.separation < a.separation ∨
      b.stability < a.stability ∨ b.routeRelevance < a.routeRelevance)

/-- Pareto optimality over a finite candidate set. Useful when different
algorithms trade off cluster tightness, stability, and route relevance. -/
def ParetoOptimalCluster
    (candidates : List ClusterQualityObservation)
    (chosen : ClusterQualityObservation) : Prop :=
  chosen ∈ candidates ∧ ∀ other, other ∈ candidates → ¬ DominatesCluster other chosen

/-- Argmax optimality under one scalarized objective. This is the Lean hook for
benchmark-side grid search over algorithm/parameter choices. -/
def ArgmaxCluster
    (cfg : ClusterObjectiveConfig)
    (candidates : List ClusterQualityObservation)
    (chosen : ClusterQualityObservation) : Prop :=
  chosen ∈ candidates ∧
    ∀ other, other ∈ candidates → clusterObjectiveScore cfg other ≤ clusterObjectiveScore cfg chosen

/-- Route-stage observation isolates whether the router picked weak clusters even
when the cluster formation itself was valid. -/
structure RouteQualityObservation where
  clusterId : ClusterId
  routeScore : Rat := 0
  selected : Bool := false
  addedDocs : Nat := 0
  relevantAddedDocs : Nat := 0
  deriving Repr, BEq

structure RouteObjectiveConfig where
  minRouteScore : Rat := 0
  maxAddedDocs : Nat := 64
  minRelevantAddedDocs : Nat := 0
  deriving Repr, BEq

/-- Admission envelope for a selected route. -/
def ValidSelectedRoute
    (cfg : RouteObjectiveConfig) (route : RouteQualityObservation) : Prop :=
  route.selected = true →
    cfg.minRouteScore ≤ route.routeScore ∧
    route.addedDocs ≤ cfg.maxAddedDocs ∧
    cfg.minRelevantAddedDocs ≤ route.relevantAddedDocs

/-- Expansion/fusion observation isolates whether topology candidates are too
numerous or displace baseline evidence. -/
structure ExpansionFusionObservation where
  addedDocs : Nat := 0
  displacedBaselineDocs : Nat := 0
  relevantAddedDocs : Nat := 0
  relevantDisplacedDocs : Nat := 0
  deriving Repr, BEq

structure ExpansionFusionConfig where
  maxAddedDocs : Nat := 64
  maxDisplacedBaselineDocs : Nat := 0
  requireRelevantGain : Bool := false
  deriving Repr, BEq

/-- Expansion/fusion validity: topology may add docs only within budget, may not
exceed the displacement budget, and can optionally require at least one relevant
added document before candidate expansion is considered useful. -/
def ValidExpansionFusion
    (cfg : ExpansionFusionConfig) (obs : ExpansionFusionObservation) : Prop :=
  obs.addedDocs ≤ cfg.maxAddedDocs ∧
    obs.displacedBaselineDocs ≤ cfg.maxDisplacedBaselineDocs ∧
    (cfg.requireRelevantGain = true → obs.relevantAddedDocs > 0)

/-- A valid scheme has no over-broad clusters by construction. -/
theorem validScheme_noOverbroadClusters
    {cfg : ClusterObjectiveConfig} {scheme : ClusterSchemeObservation}
    (hValid : ValidClusterScheme cfg scheme)
    {cluster : ClusterQualityObservation}
    (hMember : cluster ∈ scheme.clusters) :
    cluster.memberCount ≤ cfg.maxMemberCount := by
  exact (hValid cluster hMember).1

/-- A valid scheme also respects per-cluster expansion caps. -/
theorem validScheme_respectsExpansionCaps
    {cfg : ClusterObjectiveConfig} {scheme : ClusterSchemeObservation}
    (hValid : ValidClusterScheme cfg scheme)
    {cluster : ClusterQualityObservation}
    (hMember : cluster ∈ scheme.clusters) :
    cluster.expansionDocs ≤ cfg.maxExpansionDocs := by
  exact (hValid cluster hMember).2.1

/-- A valid scheme excludes clusters below the configured cohesion threshold. -/
theorem validScheme_respectsCohesionFloor
    {cfg : ClusterObjectiveConfig} {scheme : ClusterSchemeObservation}
    (hValid : ValidClusterScheme cfg scheme)
    {cluster : ClusterQualityObservation}
    (hMember : cluster ∈ scheme.clusters) :
    cfg.minCohesion ≤ cluster.cohesion := by
  exact (hValid cluster hMember).2.2.1

/-- Any argmax cluster is drawn from the candidate set; optimality cannot invent a
cluster that the algorithm did not produce. -/
theorem argmaxCluster_fromCandidates
    {cfg : ClusterObjectiveConfig}
    {candidates : List ClusterQualityObservation}
    {chosen : ClusterQualityObservation}
    (h : ArgmaxCluster cfg candidates chosen) :
    chosen ∈ candidates := by
  exact h.1

/-- A selected route that is valid cannot exceed its added-doc budget. -/
theorem validSelectedRoute_addedDocsBound
    {cfg : RouteObjectiveConfig} {route : RouteQualityObservation}
    (hValid : ValidSelectedRoute cfg route)
    (hSelected : route.selected = true) :
    route.addedDocs ≤ cfg.maxAddedDocs := by
  exact (hValid hSelected).2.1

/-- Valid expansion/fusion cannot displace more baseline docs than allowed. -/
theorem validExpansionFusion_displacementBound
    {cfg : ExpansionFusionConfig} {obs : ExpansionFusionObservation}
    (hValid : ValidExpansionFusion cfg obs) :
    obs.displacedBaselineDocs ≤ cfg.maxDisplacedBaselineDocs := by
  exact hValid.2.1

/-- If relevant gain is required, a valid expansion/fusion observation must show
at least one relevant topology-added document. -/
theorem validExpansionFusion_requiresRelevantGain
    {cfg : ExpansionFusionConfig} {obs : ExpansionFusionObservation}
    (hValid : ValidExpansionFusion cfg obs)
    (hReq : cfg.requireRelevantGain = true) :
    obs.relevantAddedDocs > 0 := by
  exact hValid.2.2 hReq

end Yams.Topology
