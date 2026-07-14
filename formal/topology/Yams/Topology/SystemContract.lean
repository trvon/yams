/-
End-to-end system contracts for the YAMS search engine.

These contracts connect storage, pipeline, topology artifacts, and search. They
are intentionally boundary-oriented: theorems protect candidate provenance and
publication safety while benchmarks decide quality/performance.
-/

import Yams.Core
import Yams.Topology.Artifacts
import Yams.Topology.Contracts
import Yams.Topology.Pipeline
import Yams.Topology.Storage
import Yams.Topology.SearchEngine
import Yams.Topology.TopologyInfluence
import Yams.Topology.Bridge

namespace Yams.Topology.SystemContract

/-- System state: the complete state of all modeled subsystems. -/
structure SystemState where
  storeState : Yams.Storage.StoreState
  pipelineStates : List Yams.Pipeline.DocumentState
  topologyBatch : Yams.Topology.TopologyArtifactBatch
  searchConfig : Yams.SearchEngine.SearchConfig
  deriving Repr, BEq

/-- System input: a batch of documents to be ingested. -/
structure SystemInput where
  docs : List Yams.Storage.StoreDoc
  deriving Repr, BEq

/-- System output: search results and topology-route observability. -/
structure SystemOutput where
  searchResponse : Yams.SearchEngine.SearchResponse
  topologyObservation : Yams.SearchEngine.TopologyRouteObservation
  deriving Repr, BEq

/-- End-to-end contract: storage/pipeline/search/topology provenance. -/
def FullSystemContract
    (input : SystemInput)
    (_before after : SystemState)
    (output : SystemOutput) : Prop :=
  Yams.Storage.PublishedSafe after.storeState ∧
  (∀ doc ∈ input.docs,
     ∃ s ∈ after.pipelineStates,
       s.documentId = doc.contentHash ∧ s.stored = true) ∧
  (∀ r ∈ output.searchResponse.results,
     r.doc ∈ after.storeState.metadataVisible) ∧
  Yams.Topology.WellFormedBatch after.topologyBatch ∧
  (∀ s ∈ after.pipelineStates,
     ∃ doc ∈ input.docs, s.documentId = doc.contentHash)

/-- Weaker contract: just storage-to-search connectivity. -/
def StorageToSearchContract
    (input : SystemInput)
    (_before after : SystemState)
    (output : SystemOutput) : Prop :=
  Yams.Storage.PublishedSafe after.storeState ∧
  (∀ doc ∈ input.docs, doc.contentHash ∈ after.storeState.metadataVisible) ∧
  (∀ r ∈ output.searchResponse.results, r.doc ∈ after.storeState.metadataVisible)

/-- Topology-assisted search contract. -/
def TopologyAssistedSearchContract
    (input : SystemInput)
    (before after : SystemState)
    (topologyCfg : Yams.SearchEngine.TopologyInfluenceConfig)
    (topologyClusters : List Yams.SearchEngine.TopologyCluster)
    (output : SystemOutput) : Prop :=
  StorageToSearchContract input before after output ∧
  (∀ tc ∈ topologyClusters,
     ∃ c ∈ after.topologyBatch.clusters, c.clusterId = tc.id) ∧
  (∀ tc ∈ topologyClusters, ∀ doc ∈ tc.docs,
     doc ∈ after.storeState.metadataVisible) ∧
  (∀ c ∈ Yams.SearchEngine.topologyCandidates topologyCfg true topologyClusters,
     ∃ tc ∈ topologyClusters, c.doc ∈ tc.docs)

/-- Batched commit preserves publication safety and makes input docs visible. -/
theorem batchedCommit_preservesStorageToSearch
    (input : SystemInput)
    (state : Yams.Storage.StoreState)
    (hSafe : Yams.Storage.PublishedSafe state) :
    Yams.Storage.PublishedSafe (Yams.Storage.batchedCommit input.docs state) ∧
    (∀ doc ∈ input.docs,
      doc.contentHash ∈ (Yams.Storage.batchedCommit input.docs state).metadataVisible) := by
  constructor
  · exact Yams.Storage.batchedCommit_publishedSafe input.docs state hSafe
  · intro doc hDoc
    simp [Yams.Storage.batchedCommit, Yams.Storage.publishMetadata,
      Yams.Storage.commitRefs, Yams.Storage.prepareDurableDocs, Yams.Storage.hashes]
    exact Or.inr ⟨doc, hDoc, rfl⟩

/-- Pipeline completion implies searchability for enabled text/vector sources. -/
theorem pipelineCompletion_impliesSourceSearchability
    (cfg : Yams.Pipeline.PipelineConfig)
    (before after : Yams.Pipeline.DocumentState)
    (hCompletion : Yams.Pipeline.PipelineCompletionContract cfg before after)
    (hStored : before.stored = true) :
    (cfg.textExtraction = true ∧ cfg.contentIndex = true →
       ∃ c, Yams.Topology.Bridge.keywordSearchableToCandidate after.documentId after = some c) ∧
    (cfg.textExtraction = true ∧ cfg.embeddings = true →
       ∃ c, Yams.Topology.Bridge.semanticSearchableToCandidate after.documentId after = some c) := by
  constructor
  · intro hKeyword
    exact Yams.Topology.Bridge.completionContract_implies_textCandidate cfg before after
      hCompletion hStored hKeyword.1 hKeyword.2
  · intro hSemantic
    exact Yams.Topology.Bridge.completionContract_implies_vectorCandidate cfg before after
      hCompletion hStored hSemantic.1 hSemantic.2

/-- Well-formed topology batch implies valid cluster memberships. -/
theorem wellFormedBatch_impliesValidMemberships
    (batch : Yams.Topology.TopologyArtifactBatch)
    (hWellFormed : Yams.Topology.WellFormedBatch batch) :
    ∀ c ∈ batch.clusters, ∀ docHash ∈ c.memberDocumentHashes,
      ∃ m ∈ batch.memberships, m.documentHash = docHash ∧ m.clusterId = c.clusterId := by
  intro c hc docHash hDoc
  exact hWellFormed.2.2 c hc docHash hDoc

/-- Convert ClusterArtifact to the search topology cluster abstraction. -/
def clusterArtifactToTopologyCluster
    (artifact : Yams.Topology.ClusterArtifact) : Yams.SearchEngine.TopologyCluster :=
  { id := artifact.clusterId
    docs := artifact.memberDocumentHashes }

/-- Cluster artifact conversion preserves IDs. -/
theorem clusterArtifactToTopologyCluster_preservesId
    (artifact : Yams.Topology.ClusterArtifact) :
    (clusterArtifactToTopologyCluster artifact).id = artifact.clusterId := by
  rfl

/-- Cluster artifact conversion preserves member docs. -/
theorem clusterArtifactToTopologyCluster_preservesDocs
    (artifact : Yams.Topology.ClusterArtifact) :
    (clusterArtifactToTopologyCluster artifact).docs = artifact.memberDocumentHashes := by
  rfl

/-- Convert entire batch to topology clusters for search. -/
def batchToTopologyClusters
    (batch : Yams.Topology.TopologyArtifactBatch) : List Yams.SearchEngine.TopologyCluster :=
  batch.clusters.map clusterArtifactToTopologyCluster

/-- Batch conversion preserves all cluster IDs. -/
theorem batchToTopologyClusters_preservesIds
    (batch : Yams.Topology.TopologyArtifactBatch) :
    (batchToTopologyClusters batch).map (fun c => c.id) =
      batch.clusters.map (fun c => c.clusterId) := by
  simp [batchToTopologyClusters, clusterArtifactToTopologyCluster]

/-- End-to-end document traceability theorem. -/
theorem endToEnd_documentTraceability
    (input : SystemInput)
    (before after : SystemState)
    (output : SystemOutput)
    (hContract : FullSystemContract input before after output) :
    ∀ doc ∈ input.docs,
      (∃ s ∈ after.pipelineStates, s.documentId = doc.contentHash ∧ s.stored = true) := by
  intro doc hDoc
  exact hContract.2.1 doc hDoc

end Yams.Topology.SystemContract
