import Yams.Core
import Yams.Topology.SearchImpact

namespace Yams.SearchEngine

/-- Abstract search component sources in the live C++ search engine. -/
inductive SearchSource where
  | text
  | simeonText
  | path
  | vector
  | entityVector
  | knowledgeGraph
  | tag
  | metadata
  | graphText
  | graphVector
  deriving Repr, BEq, DecidableEq

/-- User-visible retrieval mode. -/
inductive SearchMode where
  | keyword
  | semantic
  | hybrid
  deriving Repr, BEq, DecidableEq

/-- Minimal architecture-level search configuration. -/
structure SearchConfig where
  mode : SearchMode := .hybrid
  limit : Nat := 10
  textEnabled : Bool := true
  simeonTextEnabled : Bool := true
  pathEnabled : Bool := true
  vectorEnabled : Bool := true
  entityVectorEnabled : Bool := true
  kgEnabled : Bool := true
  tagEnabled : Bool := true
  metadataEnabled : Bool := true
  graphExpansionEnabled : Bool := true
  graphRerankEnabled : Bool := true
  deriving Repr, BEq

/-- One component candidate before fusion. Uses DocumentId for identity. -/
structure ComponentCandidate where
  doc : Yams.Core.DocumentId
  source : SearchSource
  rank : Nat := 0
  score : Nat := 0
  deriving Repr, BEq

/-- One fused or reranked result. Uses DocumentId for identity. -/
structure SearchResult where
  doc : Yams.Core.DocumentId
  score : Nat := 0
  sources : List SearchSource := []
  deriving Repr, BEq

structure SearchResponse where
  results : List SearchResult := []
  skipped : List SearchSource := []
  failed : List SearchSource := []
  timedOut : List SearchSource := []
  degraded : Bool := false
  deriving Repr, BEq

def allSources : List SearchSource :=
  [.text, .simeonText, .path, .vector, .entityVector, .knowledgeGraph, .tag, .metadata, .graphText,
    .graphVector]

def sourceAvailable (cfg : SearchConfig) (source : SearchSource) : Bool :=
  match source with
  | .text => cfg.textEnabled
  | .simeonText => cfg.simeonTextEnabled
  | .path => cfg.pathEnabled
  | .vector => cfg.vectorEnabled
  | .entityVector => cfg.entityVectorEnabled
  | .knowledgeGraph => cfg.kgEnabled
  | .tag => cfg.tagEnabled
  | .metadata => cfg.metadataEnabled
  | .graphText => cfg.graphExpansionEnabled && cfg.textEnabled
  | .graphVector => cfg.graphExpansionEnabled && cfg.vectorEnabled

/-- Mode-level source gate. Keyword mode may not use vector-only sources. Semantic mode drops
the structural sources (path, tag, metadata, graphText) but keeps text and simeonText: they
still run, as capped evidence (`applySemanticOnlyWeights`). Measured on BEIR, banning them cost
22 nDCG@10 points on scifact (67.0 to 44.8) and 12 on nfcorpus. Hybrid admits all enabled
sources. -/
def sourceAllowedByMode (mode : SearchMode) (source : SearchSource) : Bool :=
  match mode, source with
  | .keyword, .vector => false
  | .keyword, .entityVector => false
  | .keyword, .graphVector => false
  | .semantic, .path => false
  | .semantic, .tag => false
  | .semantic, .metadata => false
  | .semantic, .graphText => false
  | _, _ => true

/-- Effective source gate used by fanout. -/
def sourceEnabled (cfg : SearchConfig) (source : SearchSource) : Bool :=
  sourceAvailable cfg source && sourceAllowedByMode cfg.mode source

/-- Candidate collection after mode/config gating. -/
def collectCandidates (cfg : SearchConfig) (candidates : List ComponentCandidate) :
    List ComponentCandidate :=
  candidates.filter (fun c => sourceEnabled cfg c.source)

/-- Disabled or mode-skipped component list for observability. -/
def skippedSources (cfg : SearchConfig) : List SearchSource :=
  allSources.filter (fun s => !sourceEnabled cfg s)

/-- Architecture-level fusion: collapse each candidate to a result. The C++ code
uses weighted/RRF fusion and deduplication; this abstraction keeps the property
that fusion cannot invent document IDs. -/
def fuse (candidates : List ComponentCandidate) : List SearchResult :=
  candidates.map (fun c => { doc := c.doc, score := c.score, sources := [c.source] })

/-- Graph rerank/boost abstraction. It may alter scores but not document IDs. -/
def graphRerank (cfg : SearchConfig) (results : List SearchResult) : List SearchResult :=
  results.map (fun r =>
    if cfg.graphRerankEnabled && cfg.kgEnabled then { r with score := r.score + 1 } else r)

/-- User-visible top-k boundary. -/
def topK (limit : Nat) (results : List SearchResult) : List SearchResult :=
  results.take limit

/-- Full abstract search engine pipeline. -/
def runSearch (cfg : SearchConfig) (candidates : List ComponentCandidate)
    (failed timedOut : List SearchSource := []) : SearchResponse :=
  let active := collectCandidates cfg candidates
  let fused := fuse active
  let reranked := graphRerank cfg fused
  { results := topK cfg.limit reranked,
    skipped := skippedSources cfg,
    failed := failed,
    timedOut := timedOut,
    degraded := (!failed.isEmpty) || (!timedOut.isEmpty) }

/-- A result list is sourced from a candidate list when every result doc came
from at least one candidate. -/
def ResultsComeFromCandidates
    (candidates : List ComponentCandidate) (results : List SearchResult) : Prop :=
  ∀ r, r ∈ results → ∃ c, c ∈ candidates ∧ c.doc = r.doc

/-- A result list is sourced from another result list when every output doc came
from an input result. -/
def ResultsComeFromResults (before after : List SearchResult) : Prop :=
  ∀ r, r ∈ after → ∃ b, b ∈ before ∧ b.doc = r.doc

/-- Top-k always respects the requested limit. -/
theorem topK_respectsLimit (limit : Nat) (results : List SearchResult) :
    (topK limit results).length ≤ limit := by
  unfold topK
  rw [List.length_take]
  exact Nat.min_le_left limit results.length

/-- Top-k cannot invent results. -/
theorem topK_subset {limit : Nat} {results : List SearchResult} {r : SearchResult}
    (h : r ∈ topK limit results) : r ∈ results := by
  exact List.mem_of_mem_take h

/-- Collected candidates all satisfy the effective source gate. -/
theorem collectCandidates_enabled
    {cfg : SearchConfig} {candidates : List ComponentCandidate} {c : ComponentCandidate}
    (h : c ∈ collectCandidates cfg candidates) :
    sourceEnabled cfg c.source = true := by
  simp [collectCandidates] at h
  exact h.2

/-- Collected candidates came from the original candidate set. -/
theorem collectCandidates_subset
    {cfg : SearchConfig} {candidates : List ComponentCandidate} {c : ComponentCandidate}
    (h : c ∈ collectCandidates cfg candidates) : c ∈ candidates := by
  simp [collectCandidates] at h
  exact h.1

/-- Fusion cannot invent document IDs. -/
theorem fuse_noInvent (candidates : List ComponentCandidate) :
    ResultsComeFromCandidates candidates (fuse candidates) := by
  intro r hr
  simp [fuse] at hr
  rcases hr with ⟨c, hc, hr⟩
  subst hr
  exact ⟨c, hc, rfl⟩

/-- Graph rerank cannot invent document IDs. -/
theorem graphRerank_noInvent (cfg : SearchConfig) (results : List SearchResult) :
    ResultsComeFromResults results (graphRerank cfg results) := by
  intro r hr
  simp [graphRerank] at hr
  rcases hr with ⟨before, hBefore, hR⟩
  cases hGraph : cfg.graphRerankEnabled <;> cases hKg : cfg.kgEnabled <;> simp [hGraph, hKg] at hR
  · subst hR
    exact ⟨before, hBefore, rfl⟩
  · subst hR
    exact ⟨before, hBefore, rfl⟩
  · subst hR
    exact ⟨before, hBefore, rfl⟩
  · subst hR
    exact ⟨before, hBefore, rfl⟩

/-- If graph rerank is disabled, the rerank stage is identity. -/
theorem graphRerank_disabled_identity
    (cfg : SearchConfig) (results : List SearchResult)
    (hDisabled : cfg.graphRerankEnabled = false) :
    graphRerank cfg results = results := by
  simp [graphRerank, hDisabled]

/-- Keyword mode cannot admit vector candidates. -/
theorem keywordMode_noVectorCandidates
    {cfg : SearchConfig} {candidates : List ComponentCandidate} {c : ComponentCandidate}
    (hMode : cfg.mode = .keyword)
    (h : c ∈ collectCandidates cfg candidates) :
    c.source ≠ .vector ∧ c.source ≠ .entityVector ∧ c.source ≠ .graphVector := by
  have hEnabled := collectCandidates_enabled h
  constructor
  · intro hv
    simp [sourceEnabled, sourceAllowedByMode, hMode, hv] at hEnabled
  constructor
  · intro hv
    simp [sourceEnabled, sourceAllowedByMode, hMode, hv] at hEnabled
  · intro hv
    simp [sourceEnabled, sourceAllowedByMode, hMode, hv] at hEnabled

/-- Semantic mode admits no structural candidate: path, tag, metadata and graphText
contribute nothing. -/
theorem semanticMode_noStructuralCandidates
    {cfg : SearchConfig} {candidates : List ComponentCandidate} {c : ComponentCandidate}
    (hMode : cfg.mode = .semantic)
    (h : c ∈ collectCandidates cfg candidates) :
    c.source ≠ .path ∧ c.source ≠ .tag ∧ c.source ≠ .metadata ∧ c.source ≠ .graphText := by
  have hEnabled := collectCandidates_enabled h
  refine ⟨?_, ?_, ?_, ?_⟩ <;>
    (intro hs; simp [sourceEnabled, sourceAllowedByMode, hMode, hs] at hEnabled)

/-- Semantic mode keeps lexical evidence: an enabled text or simeonText candidate reaches
fusion. Its influence is bounded by the weight caps below, not by the source gate. -/
theorem semanticMode_admitsLexicalEvidence
    {cfg : SearchConfig} {candidates : List ComponentCandidate} {c : ComponentCandidate}
    (hMode : cfg.mode = .semantic)
    (hc : c ∈ candidates)
    (hLexical : (c.source = .text ∧ cfg.textEnabled = true) ∨
      (c.source = .simeonText ∧ cfg.simeonTextEnabled = true)) :
    c ∈ collectCandidates cfg candidates := by
  simp only [collectCandidates, List.mem_filter]
  refine ⟨hc, ?_⟩
  rcases hLexical with ⟨hs, hEn⟩ | ⟨hs, hEn⟩ <;>
    simp [sourceEnabled, sourceAvailable, sourceAllowedByMode, hMode, hs, hEn]

/-! ### Semantic-mode fusion weights

Mirrors `applySemanticOnlyLayer` and `applySemanticOnlyConfigExtras` in
`src/search/tuning_pipeline.cpp`. Weights are hundredths (`20` stands for 0.20) so caps and
floors are exact naturals. The C++ layer runs before `WeightSlots::normalize`; the caps and
floors are stated at that point. -/

/-- Per-source fusion weights, in hundredths. -/
structure SourceWeights where
  text : Nat := 0
  simeonText : Nat := 0
  path : Nat := 0
  vector : Nat := 0
  entityVector : Nat := 0
  kg : Nat := 0
  tag : Nat := 0
  metadata : Nat := 0
  graphText : Nat := 0
  graphVector : Nat := 0
  deriving Repr, BEq

/-- Fusion weight of one source. -/
def SourceWeights.weight (w : SourceWeights) : SearchSource → Nat
  | .text => w.text
  | .simeonText => w.simeonText
  | .path => w.path
  | .vector => w.vector
  | .entityVector => w.entityVector
  | .knowledgeGraph => w.kg
  | .tag => w.tag
  | .metadata => w.metadata
  | .graphText => w.graphText
  | .graphVector => w.graphVector

/-- Semantic-mode cap on the text weight (0.20). -/
def semanticTextCap : Nat := 20
/-- Semantic-mode cap on the simeonText weight (0.05). -/
def semanticSimeonTextCap : Nat := 5
/-- Semantic-mode floor on the vector weight (0.45). -/
def semanticVectorFloor : Nat := 45
/-- Semantic-mode floor on the entityVector weight (0.15). -/
def semanticEntityVectorFloor : Nat := 15

/-- The semantic-only weight layer: cap text and simeonText, floor vector and entityVector,
zero the KG and graph-expansion weights. -/
def applySemanticOnlyWeights (w : SourceWeights) : SourceWeights :=
  { w with
    text := min w.text semanticTextCap
    simeonText := min w.simeonText semanticSimeonTextCap
    vector := max w.vector semanticVectorFloor
    entityVector := max w.entityVector semanticEntityVectorFloor
    kg := 0
    graphText := 0
    graphVector := 0 }

/-- Semantic-mode contract over a candidate list and the effective weights: structural
sources contribute no candidate, text and simeonText contribute only under their caps, and the
vector and entityVector weights meet their floors. -/
def SemanticModeContract
    (cfg : SearchConfig) (w : SourceWeights) (candidates : List ComponentCandidate) : Prop :=
  (∀ c ∈ collectCandidates cfg candidates,
    c.source ≠ .path ∧ c.source ≠ .tag ∧ c.source ≠ .metadata ∧ c.source ≠ .graphText) ∧
  (∀ c ∈ collectCandidates cfg candidates,
    (c.source = .text → w.weight c.source ≤ semanticTextCap) ∧
    (c.source = .simeonText → w.weight c.source ≤ semanticSimeonTextCap)) ∧
  semanticVectorFloor ≤ w.vector ∧
  semanticEntityVectorFloor ≤ w.entityVector

/-- The semantic-only layer caps the lexical weights. -/
theorem applySemanticOnlyWeights_lexicalCapped (w : SourceWeights) :
    (applySemanticOnlyWeights w).text ≤ semanticTextCap ∧
      (applySemanticOnlyWeights w).simeonText ≤ semanticSimeonTextCap := by
  simp only [applySemanticOnlyWeights]
  exact ⟨Nat.min_le_right _ _, Nat.min_le_right _ _⟩

/-- The semantic-only layer floors the vector weights. -/
theorem applySemanticOnlyWeights_vectorFloored (w : SourceWeights) :
    semanticVectorFloor ≤ (applySemanticOnlyWeights w).vector ∧
      semanticEntityVectorFloor ≤ (applySemanticOnlyWeights w).entityVector := by
  simp only [applySemanticOnlyWeights]
  exact ⟨Nat.le_max_right _ _, Nat.le_max_right _ _⟩

/-- Semantic mode with the semantic-only weight layer satisfies the semantic-mode contract
for every candidate list and every incoming weight vector. -/
theorem semanticMode_contract
    {cfg : SearchConfig} (hMode : cfg.mode = .semantic)
    (w : SourceWeights) (candidates : List ComponentCandidate) :
    SemanticModeContract cfg (applySemanticOnlyWeights w) candidates := by
  have hCap := applySemanticOnlyWeights_lexicalCapped w
  have hFloor := applySemanticOnlyWeights_vectorFloored w
  refine ⟨fun c hc => semanticMode_noStructuralCandidates hMode hc, ?_, hFloor.1, hFloor.2⟩
  intro c _hc
  refine ⟨fun hs => ?_, fun hs => ?_⟩
  · rw [hs]; exact hCap.1
  · rw [hs]; exact hCap.2

/-- Disabled sources are observable in the skipped set. -/
theorem disabledSource_isSkipped
    {cfg : SearchConfig} {source : SearchSource}
    (hKnown : source ∈ allSources)
    (hDisabled : sourceEnabled cfg source = false) :
    source ∈ skippedSources cfg := by
  simp [skippedSources, hKnown, hDisabled]

/-- A response is degraded exactly when failures or timeouts are present in this
model. -/
theorem runSearch_degradedFlag
    (cfg : SearchConfig) (candidates : List ComponentCandidate)
    (failed timedOut : List SearchSource) :
    (runSearch cfg candidates failed timedOut).degraded =
      ((!failed.isEmpty) || (!timedOut.isEmpty)) := by
  rfl

/-- Full pipeline no-invention theorem: every visible result came from an enabled
input candidate. This is the core architecture guard for fusion/rerank/top-k
refactors. -/
theorem runSearch_resultsComeFromEnabledCandidates
    (cfg : SearchConfig) (candidates : List ComponentCandidate)
    (failed timedOut : List SearchSource) :
    ResultsComeFromCandidates (collectCandidates cfg candidates)
      (runSearch cfg candidates failed timedOut).results := by
  intro r hr
  unfold runSearch at hr
  have hTop : r ∈ graphRerank cfg (fuse (collectCandidates cfg candidates)) :=
    topK_subset hr
  have hGraph := graphRerank_noInvent cfg (fuse (collectCandidates cfg candidates)) r hTop
  rcases hGraph with ⟨preGraph, hPreGraph, hDoc⟩
  have hFuse := fuse_noInvent (collectCandidates cfg candidates) preGraph hPreGraph
  rcases hFuse with ⟨c, hc, hDocPre⟩
  exact ⟨c, hc, Eq.trans hDocPre hDoc⟩

/-- Full pipeline limit theorem: the visible response obeys the user limit. -/
theorem runSearch_respectsLimit
    (cfg : SearchConfig) (candidates : List ComponentCandidate)
    (failed timedOut : List SearchSource) :
    (runSearch cfg candidates failed timedOut).results.length ≤ cfg.limit := by
  unfold runSearch
  exact topK_respectsLimit cfg.limit (graphRerank cfg (fuse (collectCandidates cfg candidates)))

end Yams.SearchEngine
