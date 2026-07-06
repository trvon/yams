import Yams.Topology.Artifacts

namespace Yams.Pipeline

structure PipelineConfig where
  textExtraction : Bool := true
  contentIndex : Bool := true
  kgCore : Bool := true
  symbolGraph : Bool := true
  nlEntityGraph : Bool := true
  titleInference : Bool := true
  embeddings : Bool := true
  semanticNeighborGraph : Bool := true
  graphRerank : Bool := true
  deriving Repr, BEq

structure DocumentState where
  stored : Bool := false
  extracted : Bool := false
  contentIndexed : Bool := false
  kgDocumentNode : Bool := false
  symbolsIndexed : Bool := false
  nlEntitiesIndexed : Bool := false
  titleInferred : Bool := false
  embedded : Bool := false
  semanticEdges : Bool := false
  deriving Repr, BEq

def postIngestPass (cfg : PipelineConfig) (s : DocumentState) : DocumentState :=
  let extracted := s.stored && cfg.textExtraction
  let contentIndexed := extracted && cfg.contentIndex
  let kgDocumentNode := s.stored && cfg.kgCore
  let symbolsIndexed := kgDocumentNode && extracted && cfg.symbolGraph
  let nlEntitiesIndexed := kgDocumentNode && extracted && cfg.nlEntityGraph
  let titleInferred := extracted && cfg.titleInference
  let embedded := extracted && cfg.embeddings
  let semanticEdges := kgDocumentNode && embedded && cfg.semanticNeighborGraph
  { s with
    extracted := extracted
    contentIndexed := contentIndexed
    kgDocumentNode := kgDocumentNode
    symbolsIndexed := symbolsIndexed
    nlEntitiesIndexed := nlEntitiesIndexed
    titleInferred := titleInferred
    embedded := embedded
    semanticEdges := semanticEdges }

def keywordSearchable (s : DocumentState) : Bool :=
  s.stored && s.contentIndexed

def semanticSearchable (s : DocumentState) : Bool :=
  s.stored && s.embedded

def graphNavigable (s : DocumentState) : Bool :=
  s.kgDocumentNode

def graphRerankAvailable (cfg : PipelineConfig) (s : DocumentState) : Bool :=
  cfg.graphRerank && s.kgDocumentNode && (s.symbolsIndexed || s.nlEntitiesIndexed || s.semanticEdges)

def withoutKg (s : DocumentState) : DocumentState :=
  { stored := s.stored,
    extracted := s.extracted,
    contentIndexed := s.contentIndexed,
    kgDocumentNode := false,
    symbolsIndexed := false,
    nlEntitiesIndexed := false,
    titleInferred := s.titleInferred,
    embedded := s.embedded,
    semanticEdges := false }

theorem keyword_independent_of_kg (s : DocumentState)
    (hStored : s.stored = true) (hContent : s.contentIndexed = true) :
    keywordSearchable (withoutKg s) = true := by
  simp [keywordSearchable, withoutKg, hStored, hContent]

theorem semantic_search_requires_embedding (s : DocumentState)
    (h : semanticSearchable s = true) : s.embedded = true := by
  simp [semanticSearchable] at h
  exact h.right

theorem graph_navigation_requires_core (s : DocumentState)
    (h : graphNavigable s = true) : s.kgDocumentNode = true := by
  simpa [graphNavigable] using h

structure ObservedCost where
  extractionMs : Nat := 0
  contentIndexMs : Nat := 0
  kgMs : Nat := 0
  titleNlMs : Nat := 0
  embeddingMs : Nat := 0
  searchMs : Nat := 0
  deriving Repr, BEq

def totalMs (c : ObservedCost) : Nat :=
  c.extractionMs + c.contentIndexMs + c.kgMs + c.titleNlMs + c.embeddingMs + c.searchMs

def kgAblationWorthwhile (noiseFloorMs baseline noKg : Nat) : Bool :=
  baseline > noKg + noiseFloorMs

end Yams.Pipeline
