/-
Bridge module connecting the YAMS pipeline model to the search engine model.

The key invariant: a document may become a search candidate only after it is
visible at the modeled pipeline/storage boundary for that source.
-/

import Yams.Core
import Yams.Topology.Pipeline
import Yams.Topology.SearchEngine
import Yams.Topology.Storage

namespace Yams.Topology.Bridge

/-- Convert a pipeline document state to a search candidate. -/
def pipelineStateToCandidate
    (docId : Yams.Core.DocumentId)
    (pipelineState : Yams.Pipeline.DocumentState)
    (source : Yams.SearchEngine.SearchSource) :
    Option Yams.SearchEngine.ComponentCandidate :=
  if pipelineState.stored = true then
    some { doc := docId, source := source, rank := 0, score := 0 }
  else
    none

/-- Predicate: a pipeline state can produce a candidate for a given source. -/
def CanProduceCandidate
    (pipelineState : Yams.Pipeline.DocumentState)
    (_source : Yams.SearchEngine.SearchSource) : Prop :=
  pipelineState.stored = true

/-- Stored documents can always produce candidates. -/
theorem storedState_canProduceCandidate
    (pipelineState : Yams.Pipeline.DocumentState)
    (source : Yams.SearchEngine.SearchSource)
    (hStored : pipelineState.stored = true) :
    CanProduceCandidate pipelineState source := by
  simpa [CanProduceCandidate]

/-- `pipelineStateToCandidate` succeeds for stored documents. -/
theorem pipelineStateToCandidate_succeeds
    (docId : Yams.Core.DocumentId)
    (pipelineState : Yams.Pipeline.DocumentState)
    (source : Yams.SearchEngine.SearchSource)
    (hStored : pipelineState.stored = true) :
    pipelineStateToCandidate docId pipelineState source =
      some { doc := docId, source := source, rank := 0, score := 0 } := by
  simp [pipelineStateToCandidate, hStored]

/-- Connect keyword-searchable pipeline state to text source candidates. -/
def keywordSearchableToCandidate
    (docId : Yams.Core.DocumentId)
    (pipelineState : Yams.Pipeline.DocumentState) :
    Option Yams.SearchEngine.ComponentCandidate :=
  if Yams.Pipeline.keywordSearchable pipelineState = true then
    some { doc := docId, source := Yams.SearchEngine.SearchSource.text, rank := 0, score := 0 }
  else
    none

/-- Connect semantic-searchable pipeline state to vector source candidates. -/
def semanticSearchableToCandidate
    (docId : Yams.Core.DocumentId)
    (pipelineState : Yams.Pipeline.DocumentState) :
    Option Yams.SearchEngine.ComponentCandidate :=
  if Yams.Pipeline.semanticSearchable pipelineState = true then
    some { doc := docId, source := Yams.SearchEngine.SearchSource.vector, rank := 0, score := 0 }
  else
    none

/-- Completion with keyword stages enabled produces a text candidate. -/
theorem completionContract_implies_textCandidate
    (cfg : Yams.Pipeline.PipelineConfig)
    (before after : Yams.Pipeline.DocumentState)
    (hCompletion : Yams.Pipeline.PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hText : cfg.textExtraction = true)
    (hContent : cfg.contentIndex = true) :
    ∃ c, keywordSearchableToCandidate after.documentId after = some c := by
  have hSearchable := Yams.Pipeline.completion_keywordSearchable
    hCompletion hStored hText hContent
  simp [keywordSearchableToCandidate, hSearchable]

/-- Completion with embedding stages enabled produces a vector candidate. -/
theorem completionContract_implies_vectorCandidate
    (cfg : Yams.Pipeline.PipelineConfig)
    (before after : Yams.Pipeline.DocumentState)
    (hCompletion : Yams.Pipeline.PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hText : cfg.textExtraction = true)
    (hEmbed : cfg.embeddings = true) :
    ∃ c, semanticSearchableToCandidate after.documentId after = some c := by
  have hSearchable := Yams.Pipeline.completion_semanticSearchable
    hCompletion hStored hText hEmbed
  simp [semanticSearchableToCandidate, hSearchable]

/-- Convert storage content hash to search document ID. -/
def storageHashToDocumentId (hash : Yams.Storage.ContentHash) : Yams.Core.DocumentId :=
  hash

/-- Storage hash converts to document ID transitively. -/
theorem storageHashToDocumentId_transitive
    (hash : Yams.Storage.ContentHash) :
    storageHashToDocumentId hash = hash := by
  rfl

/-- Connect storage visibility to search candidate availability. -/
def storageVisibleToCandidate
    (hash : Yams.Storage.ContentHash)
    (source : Yams.SearchEngine.SearchSource) :
    Yams.SearchEngine.ComponentCandidate :=
  { doc := storageHashToDocumentId hash, source := source, rank := 0, score := 0 }

/-- Every metadata-visible hash can become a search candidate. -/
theorem metadataVisible_implies_candidate
    (storeState : Yams.Storage.StoreState)
    (hash : Yams.Storage.ContentHash)
    (source : Yams.SearchEngine.SearchSource)
    (hVisible : hash ∈ storeState.metadataVisible) :
    (storageVisibleToCandidate hash source).doc ∈
      (storeState.metadataVisible.map storageHashToDocumentId) := by
  simpa [storageVisibleToCandidate, storageHashToDocumentId] using hVisible

/-- System-level contract: storage-to-search connection. -/
def StorageToSearchContract
    (storeState : Yams.Storage.StoreState)
    (searchCandidates : List Yams.SearchEngine.ComponentCandidate) : Prop :=
  (∀ c ∈ searchCandidates, c.doc ∈ storeState.metadataVisible.map storageHashToDocumentId) ∧
  Yams.Storage.PublishedSafe storeState

/-- If storage is safe and candidates come from visible metadata, the bridge holds. -/
theorem storageToSearch_contractHolds
    (storeState : Yams.Storage.StoreState)
    (searchCandidates : List Yams.SearchEngine.ComponentCandidate)
    (hSafe : Yams.Storage.PublishedSafe storeState)
    (hFromStorage : ∀ c ∈ searchCandidates,
      c.doc ∈ storeState.metadataVisible.map storageHashToDocumentId) :
    StorageToSearchContract storeState searchCandidates := by
  exact ⟨hFromStorage, hSafe⟩

/-- Pipeline-to-search contract: pipeline output is searchable. -/
def PipelineToSearchContract
    (_pipelineCfg : Yams.Pipeline.PipelineConfig)
    (_searchCfg : Yams.SearchEngine.SearchConfig)
    (pipelineStates : List Yams.Pipeline.DocumentState)
    (searchCandidates : List Yams.SearchEngine.ComponentCandidate) : Prop :=
  ∀ c ∈ searchCandidates,
    ∃ s ∈ pipelineStates,
      s.documentId = c.doc ∧
      (c.source = Yams.SearchEngine.SearchSource.text →
        Yams.Pipeline.keywordSearchable s = true) ∧
      (c.source = Yams.SearchEngine.SearchSource.vector →
        Yams.Pipeline.semanticSearchable s = true) ∧
      (c.source = Yams.SearchEngine.SearchSource.knowledgeGraph →
        Yams.Pipeline.graphNavigable s = true)

end Yams.Topology.Bridge
