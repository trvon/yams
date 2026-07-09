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
  hdbscanMinPoints : Nat := 0
  hdbscanMinClusterSize : Nat := 0
  featureSmoothingHops : Nat := 0
  kmeansK : Nat := 0
  kmeansMaxIterations : Nat := 10
  minSimilarityToJoin : Rat := 45 / 100
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

structure TopologyArtifactBatch where
  snapshotId : String := ""
  algorithm : String := ""
  inputKind : TopologyInputKind := .hybrid
  generatedAtUnixSeconds : Nat := 0
  topologyEpoch : Nat := 0
  clusters : List ClusterArtifact := []
  memberships : List DocumentClusterMembership := []
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
  preferStableClusters : Bool := true
  weakQueryOnly : Bool := true
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

end Yams.Topology
