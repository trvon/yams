/-
A small Lean 4 model of the YAMS ingestion -> search pipeline.

Purpose: make the dependency seams explicit before optimizing the implementation.
This is not a performance model; benchmark output should instantiate the cost
fields outside Lean or be connected later through generated facts.
-/

namespace Yams.Pipeline

/-- Optional enrichment stages that are commonly conflated in discussion. -/
structure PipelineConfig where
  textExtraction : Bool := true
  contentIndex : Bool := true
  kgCore : Bool := true              -- document/path/blob graph nodes and edges
  symbolGraph : Bool := true         -- code symbols and references
  nlEntityGraph : Bool := true       -- GLiNER / NLP entities and aliases
  titleInference : Bool := true      -- GLiNER title inference
  embeddings : Bool := true
  semanticNeighborGraph : Bool := true
  graphRerank : Bool := true
  deriving Repr, BEq

/-- Abstract document state after zero or more pipeline stages. -/
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

/-- One idealized successful pass over the asynchronous post-ingest pipeline. -/
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

/-- FTS/keyword search does not depend on graph state once content is indexed. -/
def keywordSearchable (s : DocumentState) : Bool :=
  s.stored && s.contentIndexed

/-- Vector/semantic search depends on embeddings, not on KG document nodes. -/
def semanticSearchable (s : DocumentState) : Bool :=
  s.stored && s.embedded

/-- Graph navigation requires the core graph. Rich entity/symbol graphs are additive. -/
def graphNavigable (s : DocumentState) : Bool :=
  s.kgDocumentNode

/-- Search-time KG reranking needs both a configured reranker and graph content. -/
def graphRerankAvailable (cfg : PipelineConfig) (s : DocumentState) : Bool :=
  cfg.graphRerank && s.kgDocumentNode && (s.symbolsIndexed || s.nlEntitiesIndexed || s.semanticEdges)

/-- Disabling KG does not by itself prevent keyword searchability. -/
theorem keyword_independent_of_kg (s : DocumentState)
    (hStored : s.stored = true) (hContent : s.contentIndexed = true) :
    keywordSearchable { s with kgDocumentNode := false, symbolsIndexed := false,
      nlEntitiesIndexed := false, semanticEdges := false } = true := by
  simp [keywordSearchable, hStored, hContent]

/-- Semantic searchability needs embeddings, but not KG. -/
theorem semantic_search_requires_embedding (s : DocumentState)
    (h : semanticSearchable s = true) : s.embedded = true := by
  simp [semanticSearchable] at h
  exact h.right

/-- Graph navigation cannot work without at least the core KG document node. -/
theorem graph_navigation_requires_core (s : DocumentState)
    (h : graphNavigable s = true) : s.kgDocumentNode = true := by
  simpa [graphNavigable] using h

/-- If KG core is disabled in a fresh pass, graph reranking is unavailable. -/
theorem no_core_graph_no_graph_rerank (cfg : PipelineConfig) (s : DocumentState)
    (hStored : s.stored = true) :
    graphRerankAvailable { cfg with kgCore := false } (postIngestPass { cfg with kgCore := false } s) = false := by
  simp [graphRerankAvailable, postIngestPass, hStored]

/-- The benchmark question: compare total observed costs under two configs. -/
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

/-- A KG ablation is worth product work only when measured savings exceed noise. -/
def kgAblationWorthwhile (noiseFloorMs baseline noKg : Nat) : Bool :=
  baseline > noKg + noiseFloorMs

end Yams.Pipeline
