import Std
import Yams.Core

namespace Yams.Topology

/-- Mirrors `TopologyBuildMode` in `include/yams/topology/topology_artifacts.h`. -/
inductive TopologyBuildMode where
  | exact
  | incremental
  | approximate
  deriving Repr, BEq, DecidableEq

/-- Mirrors `TopologyInputKind`. -/
inductive TopologyInputKind where
  | semanticNeighborGraph
  | embeddingNeighborhood
  | hybrid
  deriving Repr, BEq, DecidableEq

/-- Mirrors `DirtyRegionExpansionMode`. -/
inductive DirtyRegionExpansionMode where
  | neighborsOnly
  | priorClusterAndNeighbors
  | adaptive
  deriving Repr, BEq, DecidableEq

/-- Mirrors `DocumentTopologyRole`. -/
inductive DocumentTopologyRole where
  | core
  | bridge
  | medoid
  | outlier
  deriving Repr, BEq, DecidableEq

/-- Document hash used in topology. Uses Yams.Core.DocumentHash for unification. -/
abbrev DocumentHash := Yams.Core.DocumentHash
/-- File path. Uses Yams.Core.FilePath for unification. -/
abbrev FilePath := Yams.Core.FilePath
/-- Cluster identifier. Uses Yams.Core.ClusterId for unification. -/
abbrev ClusterId := Yams.Core.ClusterId
/-- Embedding vector. Uses Rat for now; consider transitioning to Yams.Core.EmbeddingVector (ℚ). -/
abbrev Embedding := List Rat

structure TopologyNeighbor where
  documentHash : DocumentHash
  score : Rat := 0
  reciprocal : Bool := false
  deriving Repr, BEq

structure TopologyDocumentInput where
  documentHash : DocumentHash
  filePath : FilePath := ""
  embedding : Embedding := []
  neighbors : List TopologyNeighbor := []
  metadata : List (String × String) := []
  deriving Repr, BEq

structure TopologyBuildConfig where
  mode : TopologyBuildMode := .incremental
  inputKind : TopologyInputKind := .hybrid
  maxDocuments : Nat := 0
  maxNeighborsPerDocument : Nat := 32
  maxDirtyRegionDocs : Nat := 256
  maxDirtyRegionDepth : Nat := 2
  maxDirtySeedCount : Nat := 64
  maxLevels : Nat := 3
  overlapLimit : Nat := 2
  fullRebuildDocThreshold : Nat := 4096
  minEdgeScore : Rat := 0
  reciprocalOnly : Bool := true
  allowOverlap : Bool := true
  emitBridgeAnnotations : Bool := true
  emitOutliers : Bool := true
  dirtyRegionExpansion : DirtyRegionExpansionMode := .priorClusterAndNeighbors
  kmeansK : Nat := 0
  kmeansMaxIterations : Nat := 10
  deriving Repr, BEq

structure TopologyDirtyRegion where
  seedDocumentHashes : List DocumentHash := []
  expandedDocumentHashes : List DocumentHash := []
  bfsDepthReached : Nat := 0
  includedPriorClusterMembers : Bool := false
  includedSemanticNeighbors : Bool := false
  coalesced : Bool := false
  exceededRegionBudget : Bool := false
  requiresWiderRebuild : Bool := false
  deriving Repr, BEq

structure DocumentClusterMembership where
  documentHash : DocumentHash
  clusterId : ClusterId
  parentClusterId : Option ClusterId := none
  clusterLevel : Nat := 0
  persistenceScore : Rat := 0
  cohesionScore : Rat := 0
  bridgeScore : Rat := 0
  role : DocumentTopologyRole := .core
  overlapClusterIds : List ClusterId := []
  deriving Repr, BEq

structure ClusterRepresentative where
  clusterId : ClusterId
  documentHash : DocumentHash
  filePath : FilePath := ""
  representativeScore : Rat := 0
  deriving Repr, BEq

structure ClusterArtifact where
  clusterId : ClusterId
  parentClusterId : Option ClusterId := none
  level : Nat := 0
  memberCount : Nat := 0
  persistenceScore : Rat := 0
  cohesionScore : Rat := 0
  bridgeMass : Rat := 0
  medoid : Option ClusterRepresentative := none
  memberDocumentHashes : List DocumentHash := []
  overlapClusterIds : List ClusterId := []
  centroidEmbedding : Embedding := []
  deriving Repr, BEq

/-- Mirrors `ClusterRoad`: one cluster's view of an undirected cluster-to-cluster link whose
weight sums the raw neighbor observations between members of the two clusters. Persisted in a
sidecar beside the snapshot, never inside the construction payload. -/
structure ClusterRoad where
  fromClusterId : ClusterId
  toClusterId : ClusterId
  weight : Rat := 0
  deriving Repr, BEq

structure TopologyArtifactBatch where
  snapshotId : String := ""
  algorithm : String := ""
  inputKind : TopologyInputKind := .hybrid
  generatedAtUnixSeconds : Nat := 0
  topologyEpoch : Nat := 0
  clusters : List ClusterArtifact := []
  memberships : List DocumentClusterMembership := []
  /-- Roads attached to the batch; empty when roads are off or the sidecar was dropped. -/
  roads : List ClusterRoad := []
  /-- Per-source bound the builder applied to `roads` (sidecar header `maxPerCluster`). -/
  roadMaxPerCluster : Nat := 0
  deriving Repr, BEq

structure TopologyUpdateStats where
  documentsProcessed : Nat := 0
  clustersCreated : Nat := 0
  clustersUpdated : Nat := 0
  membershipsUpdated : Nat := 0
  bridgeDocsTagged : Nat := 0
  medoidsSelected : Nat := 0
  dirtySeedCount : Nat := 0
  dirtyRegionDocs : Nat := 0
  coalescedDirtySets : Nat := 0
  fallbackFullRebuilds : Nat := 0
  deriving Repr, BEq

inductive RouteScoringMode where
  | current
  | sizeWeighted
  | seedCoverage
  deriving Repr, BEq, DecidableEq

structure TopologyRouteRequest where
  queryText : String := ""
  seedDocumentHashes : List DocumentHash := []
  limit : Nat := 8
  scoringMode : RouteScoringMode := .current
  queryEmbedding : Embedding := []
  sparseDenseAlpha : Rat := 1 / 2
  deriving Repr, BEq

structure ClusterRoute where
  clusterId : ClusterId
  medoidDocumentHash : Option DocumentHash := none
  routeScore : Rat := 0
  stabilityScore : Rat := 0
  memberCount : Nat := 0
  deriving Repr, BEq

/-- Structural invariant every produced cluster should satisfy. -/
def ClusterArtifact.memberCountMatches (c : ClusterArtifact) : Prop :=
  c.memberCount = c.memberDocumentHashes.length

/-- Structural invariant every membership should point at a produced cluster. -/
def membershipsReferenceClusters (batch : TopologyArtifactBatch) : Prop :=
  ∀ m ∈ batch.memberships, ∃ c ∈ batch.clusters, c.clusterId = m.clusterId

/-- Structural invariant every cluster member should have a corresponding membership row. -/
def clusterMembersHaveMemberships (batch : TopologyArtifactBatch) : Prop :=
  ∀ c ∈ batch.clusters, ∀ h ∈ c.memberDocumentHashes,
    ∃ m ∈ batch.memberships, m.documentHash = h ∧ m.clusterId = c.clusterId

/-- Main well-formedness target for topology batches, independent of the algorithm. -/
def WellFormedBatch (batch : TopologyArtifactBatch) : Prop :=
  (∀ c ∈ batch.clusters, c.memberCountMatches) ∧
  membershipsReferenceClusters batch ∧
  clusterMembersHaveMemberships batch

/-! ### Roads

Roads are retrieval hints layered over the certified cover. Their well-formedness mirrors the
soft validation `validateTopologyRoadSidecar` applies when a sidecar is loaded; a sidecar that
fails it is dropped, and the batch below keeps its construction identity unchanged. -/

/-- A cluster id names a cluster of the batch. -/
def namesCluster (batch : TopologyArtifactBatch) (id : ClusterId) : Prop :=
  ∃ c ∈ batch.clusters, c.clusterId = id

/-- Both road endpoints are clusters of the batch (`road_endpoint_not_cluster`). -/
def roadEndpointsAreClusters (batch : TopologyArtifactBatch) : Prop :=
  ∀ r ∈ batch.roads, namesCluster batch r.fromClusterId ∧ namesCluster batch r.toClusterId

/-- No road links a cluster to itself (`road_self_loop`). -/
def roadsHaveNoSelfLoop (batch : TopologyArtifactBatch) : Prop :=
  ∀ r ∈ batch.roads, r.fromClusterId ≠ r.toClusterId

/-- Road weights are non-negative (`road_weight_invalid`; finiteness is implicit over ℚ). -/
def roadWeightsNonNegative (batch : TopologyArtifactBatch) : Prop :=
  ∀ r ∈ batch.roads, 0 ≤ r.weight

/-- Roads leaving one source cluster. -/
def roadsFrom (batch : TopologyArtifactBatch) (source : ClusterId) : List ClusterRoad :=
  batch.roads.filter (fun r => r.fromClusterId == source)

/-- Every source keeps at most `roadMaxPerCluster` roads
(`road_count_exceeds_max_per_cluster`). -/
def roadsBoundedPerSource (batch : TopologyArtifactBatch) : Prop :=
  ∀ source : ClusterId, (roadsFrom batch source).length ≤ batch.roadMaxPerCluster

/-- Well-formedness of the road layer; `WellFormedBatch` does not depend on it. -/
def WellFormedRoads (batch : TopologyArtifactBatch) : Prop :=
  roadEndpointsAreClusters batch ∧
  roadsHaveNoSelfLoop batch ∧
  roadWeightsNonNegative batch ∧
  roadsBoundedPerSource batch

/-- A batch with roads removed, as the store yields when the sidecar is missing or rejected. -/
def TopologyArtifactBatch.withoutRoads (batch : TopologyArtifactBatch) : TopologyArtifactBatch :=
  { batch with roads := [], roadMaxPerCluster := 0 }

/-- A batch with a road layer attached, as the store yields when the sidecar loads. -/
def TopologyArtifactBatch.withRoads (batch : TopologyArtifactBatch)
    (roads : List ClusterRoad) (k : Nat) : TopologyArtifactBatch :=
  { batch with roads := roads, roadMaxPerCluster := k }

/-- The construction content a routing construction fingerprint is computed from: algorithm,
input kind, clusters and memberships. Roads are deliberately absent. -/
def constructionIdentity (batch : TopologyArtifactBatch) :
    String × TopologyInputKind × List ClusterArtifact × List DocumentClusterMembership :=
  (batch.algorithm, batch.inputKind, batch.clusters, batch.memberships)

/-- Attaching or replacing roads does not move the construction identity. -/
theorem constructionIdentity_withRoads (batch : TopologyArtifactBatch)
    (roads : List ClusterRoad) (k : Nat) :
    constructionIdentity (batch.withRoads roads k) = constructionIdentity batch := rfl

/-- Dropping roads does not move the construction identity. -/
theorem constructionIdentity_withoutRoads (batch : TopologyArtifactBatch) :
    constructionIdentity batch.withoutRoads = constructionIdentity batch := rfl

/-- The certified cover (cluster/membership well-formedness) is independent of roads: a batch is
well-formed with any road layer exactly when it is well-formed without one. -/
theorem wellFormedBatch_withRoads_iff (batch : TopologyArtifactBatch)
    (roads : List ClusterRoad) (k : Nat) :
    WellFormedBatch (batch.withRoads roads k) ↔ WellFormedBatch batch := Iff.rfl

/-- Dropping a rejected sidecar never breaks a well-formed cover. -/
theorem wellFormedBatch_withoutRoads (batch : TopologyArtifactBatch)
    (h : WellFormedBatch batch) : WellFormedBatch batch.withoutRoads := h

/-- The empty road layer is always well-formed, so a dropped sidecar leaves a valid batch. -/
theorem wellFormedRoads_withoutRoads (batch : TopologyArtifactBatch) :
    WellFormedRoads batch.withoutRoads := by
  refine ⟨?_, ?_, ?_, ?_⟩
  · intro r hr; simp [TopologyArtifactBatch.withoutRoads] at hr
  · intro r hr; simp [TopologyArtifactBatch.withoutRoads] at hr
  · intro r hr; simp [TopologyArtifactBatch.withoutRoads] at hr
  · intro source; simp [roadsFrom, TopologyArtifactBatch.withoutRoads]

/-- A well-formed road only ever leads to a cluster of the batch, so following it stays inside
the certified cover. -/
theorem wellFormedRoads_target_isCluster {batch : TopologyArtifactBatch}
    (h : WellFormedRoads batch) {r : ClusterRoad} (hr : r ∈ batch.roads) :
    namesCluster batch r.toClusterId :=
  (h.1 r hr).2

end Yams.Topology
