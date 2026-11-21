import Yams.Topology.Artifacts
import Yams.Topology.Connected
import Yams.Topology.SGC
import Yams.Topology.Pipeline

namespace Yams.Topology

/-- Generic list-subset relation used by topology contracts. -/
def ListSubset (xs ys : List DocumentHash) : Prop :=
  ∀ h, h ∈ xs → h ∈ ys

/-- Contract for one neighbor observation after SGC/topology edge filtering.

This mirrors the production boundary in `applySGCSmoothing`: keep only neighbors
whose hash exists in the corpus, are not self-edges, satisfy the reciprocal-only
mode when enabled, and meet the minimum edge score. -/
def NeighborEdgeContract
    (cfg : TopologyBuildConfig)
    (corpusHashes : List DocumentHash)
    (src : TopologyDocumentInput)
    (neighbor : TopologyNeighbor) : Prop :=
  neighbor.documentHash ∈ corpusHashes ∧
  neighbor.documentHash ≠ src.documentHash ∧
  (cfg.reciprocalOnly = true → neighbor.reciprocal = true) ∧
  cfg.minEdgeScore ≤ neighbor.score

/-- Filtered edges always point to a known corpus document. -/
theorem neighborEdge_knownCorpus
    {cfg : TopologyBuildConfig}
    {corpusHashes : List DocumentHash}
    {src : TopologyDocumentInput}
    {neighbor : TopologyNeighbor}
    (h : NeighborEdgeContract cfg corpusHashes src neighbor) :
    neighbor.documentHash ∈ corpusHashes := by
  exact h.1

/-- Filtered edges never point back to the source document. -/
theorem neighborEdge_noSelf
    {cfg : TopologyBuildConfig}
    {corpusHashes : List DocumentHash}
    {src : TopologyDocumentInput}
    {neighbor : TopologyNeighbor}
    (h : NeighborEdgeContract cfg corpusHashes src neighbor) :
    neighbor.documentHash ≠ src.documentHash := by
  exact h.2.1

/-- When reciprocal-only mode is enabled, filtered edges are reciprocal. -/
theorem neighborEdge_reciprocalWhenRequired
    {cfg : TopologyBuildConfig}
    {corpusHashes : List DocumentHash}
    {src : TopologyDocumentInput}
    {neighbor : TopologyNeighbor}
    (h : NeighborEdgeContract cfg corpusHashes src neighbor)
    (hReciprocalOnly : cfg.reciprocalOnly = true) :
    neighbor.reciprocal = true := by
  exact h.2.2.1 hReciprocalOnly

/-- Filtered edges respect the configured score threshold. -/
theorem neighborEdge_minScore
    {cfg : TopologyBuildConfig}
    {corpusHashes : List DocumentHash}
    {src : TopologyDocumentInput}
    {neighbor : TopologyNeighbor}
    (h : NeighborEdgeContract cfg corpusHashes src neighbor) :
    cfg.minEdgeScore ≤ neighbor.score := by
  exact h.2.2.2

/-- A component-size cap where `0` means unlimited (`maxComponentDocs`). -/
def WithinComponentCap (cap size : Nat) : Prop :=
  cap = 0 ∨ size ≤ cap

/-- An original connected component is oversized when a cap is set and the component exceeds
it; only such a component is split (`splitOversizedComponent`). -/
def OversizedComponent (cap size : Nat) : Prop :=
  cap ≠ 0 ∧ cap < size

/-- Retained edges cross published clusters only inside a split: both endpoints came from the
same original connected component (`componentOf`) and that component was oversized
(`componentSize` is the size of an original component). -/
def CrossEdgesOnlyFromSplit
    (edges : List UEdge)
    (cap : Nat)
    (clusterOf : Nat → ClusterId)
    (componentOf : Nat → Nat)
    (componentSize : Nat → Nat) : Prop :=
  ∀ e ∈ edges, clusterOf e.a ≠ clusterOf e.b →
    componentOf e.a = componentOf e.b ∧ OversizedComponent cap (componentSize (componentOf e.a))

/-- No original component exceeds the cap (always true when the cap is `0`). -/
def NoOversizedComponent (cap : Nat) (componentOf : Nat → Nat) (componentSize : Nat → Nat) :
    Prop :=
  ∀ x, ¬ OversizedComponent cap (componentSize (componentOf x))

/-- Contract for one published connected-component cluster under the anti-giant cap
`maxComponentDocs` (`0` = unlimited): its members are reachable from its root, it has at most
`cap` members, and a retained edge crosses clusters only between two pieces of one oversized
original component. -/
def ConnectedComponentContract
    (edges : List UEdge)
    (cap : Nat)
    (root : Nat)
    (members : List Nat)
    (clusterOf : Nat → ClusterId)
    (componentOf : Nat → Nat)
    (componentSize : Nat → Nat) : Prop :=
  ConnectedFrom edges root members ∧
    WithinComponentCap cap members.length ∧
    CrossEdgesOnlyFromSplit edges cap clusterOf componentOf componentSize

/-- Membership in a connected-component artifact implies graph reachability from
that component's root. -/
theorem componentMember_reachable
    {edges : List UEdge}
    {cap root x : Nat}
    {members : List Nat}
    {clusterOf : Nat → ClusterId}
    {componentOf componentSize : Nat → Nat}
    (h : ConnectedComponentContract edges cap root members clusterOf componentOf componentSize)
    (hx : x ∈ members) :
    Reachable edges root x := by
  exact h.1 x hx

/-- A published cluster has at most `cap` members when a cap is set. -/
theorem component_respectsCap
    {edges : List UEdge}
    {cap root : Nat}
    {members : List Nat}
    {clusterOf : Nat → ClusterId}
    {componentOf componentSize : Nat → Nat}
    (h : ConnectedComponentContract edges cap root members clusterOf componentOf componentSize)
    (hCap : 0 < cap) :
    members.length ≤ cap := by
  rcases h.2.1 with h0 | hle
  · omega
  · exact hle

/-- A retained edge that crosses clusters joins two pieces of one oversized component. -/
theorem component_crossEdgesOnlyFromSplit
    {edges : List UEdge}
    {cap root : Nat}
    {members : List Nat}
    {clusterOf : Nat → ClusterId}
    {componentOf componentSize : Nat → Nat}
    (h : ConnectedComponentContract edges cap root members clusterOf componentOf componentSize)
    {e : UEdge}
    (he : e ∈ edges)
    (hCross : clusterOf e.a ≠ clusterOf e.b) :
    componentOf e.a = componentOf e.b ∧
      cap ≠ 0 ∧ cap < componentSize (componentOf e.a) :=
  h.2.2 e he hCross

/-- When no original component exceeds the cap, no retained edge crosses clusters: the
pre-split contract is a corollary. -/
theorem component_noCrossEdges
    {edges : List UEdge}
    {cap root : Nat}
    {members : List Nat}
    {clusterOf : Nat → ClusterId}
    {componentOf componentSize : Nat → Nat}
    (h : ConnectedComponentContract edges cap root members clusterOf componentOf componentSize)
    (hNone : NoOversizedComponent cap componentOf componentSize) :
    NoCrossClusterEdges edges clusterOf := by
  intro e he
  by_cases hSame : clusterOf e.a = clusterOf e.b
  · exact hSame
  · exact absurd (component_crossEdgesOnlyFromSplit h he hSame).2 (hNone e.a)

/-- With the cap off (`0`), no component is split and no retained edge crosses clusters. -/
theorem component_noCrossEdges_uncapped
    {edges : List UEdge}
    {root : Nat}
    {members : List Nat}
    {clusterOf : Nat → ClusterId}
    {componentOf componentSize : Nat → Nat}
    (h : ConnectedComponentContract edges 0 root members clusterOf componentOf componentSize) :
    NoCrossClusterEdges edges clusterOf :=
  component_noCrossEdges h (fun _ hOver => hOver.1 rfl)

/-- Contract for dirty-region expansion.

Every seed must be included in the expanded region. The configured region budget
must hold unless the engine explicitly requests a wider rebuild. -/
def DirtyRegionContract (cfg : TopologyBuildConfig) (region : TopologyDirtyRegion) : Prop :=
  ListSubset region.seedDocumentHashes region.expandedDocumentHashes ∧
  (region.requiresWiderRebuild = false →
    region.expandedDocumentHashes.length ≤ cfg.maxDirtyRegionDocs)

/-- Dirty-region expansion never drops a changed seed. -/
theorem dirtyRegion_containsSeeds
    {cfg : TopologyBuildConfig}
    {region : TopologyDirtyRegion}
    (h : DirtyRegionContract cfg region) :
    ListSubset region.seedDocumentHashes region.expandedDocumentHashes := by
  exact h.1

/-- Dirty-region expansion either respects the configured budget or requests a
wider rebuild. -/
theorem dirtyRegion_budgetOrWiderRebuild
    {cfg : TopologyBuildConfig}
    {region : TopologyDirtyRegion}
    (h : DirtyRegionContract cfg region) :
    region.requiresWiderRebuild = true ∨
      region.expandedDocumentHashes.length ≤ cfg.maxDirtyRegionDocs := by
  cases hReq : region.requiresWiderRebuild with
  | false =>
      exact Or.inr (h.2 hReq)
  | true =>
      exact Or.inl rfl

/-- Router outputs may only name clusters present in the artifact batch. -/
def RoutesReferenceClusters
    (batch : TopologyArtifactBatch)
    (routes : List ClusterRoute) : Prop :=
  ∀ r ∈ routes, ∃ c ∈ batch.clusters, c.clusterId = r.clusterId

/-- Router contract: returned routes reference known clusters and obey the
request limit. -/
def RouteResultContract
    (request : TopologyRouteRequest)
    (batch : TopologyArtifactBatch)
    (routes : List ClusterRoute) : Prop :=
  RoutesReferenceClusters batch routes ∧ routes.length ≤ request.limit

/-- A contracted route always points at a known cluster artifact. -/
theorem routedCluster_known
    {request : TopologyRouteRequest}
    {batch : TopologyArtifactBatch}
    {routes : List ClusterRoute}
    {route : ClusterRoute}
    (h : RouteResultContract request batch routes)
    (hr : route ∈ routes) :
    ∃ c ∈ batch.clusters, c.clusterId = route.clusterId := by
  exact h.1 route hr

/-- A contracted route result never exceeds the caller's limit. -/
theorem routedCluster_limit
    {request : TopologyRouteRequest}
    {batch : TopologyArtifactBatch}
    {routes : List ClusterRoute}
    (h : RouteResultContract request batch routes) :
    routes.length ≤ request.limit := by
  exact h.2

end Yams.Topology

namespace Yams.Pipeline

/-- Contract for one document after post-ingest completion. It states the public
search/navigation capabilities implied by enabled pipeline stages. -/
def PipelineCompletionContract
    (cfg : PipelineConfig)
    (before after : DocumentState) : Prop :=
  before.stored = true →
    (cfg.textExtraction = true → cfg.contentIndex = true →
      keywordSearchable after = true) ∧
    (cfg.textExtraction = true → cfg.embeddings = true →
      semanticSearchable after = true) ∧
    (cfg.kgCore = true → graphNavigable after = true) ∧
    (cfg.textExtraction = true → cfg.kgCore = true → cfg.embeddings = true →
      cfg.semanticNeighborGraph = true → cfg.graphRerank = true →
      graphRerankAvailable cfg after = true)

/-- The model post-ingest pass satisfies the completion contract. -/
theorem postIngestPass_completionContract
    (cfg : PipelineConfig)
    (before : DocumentState) :
    PipelineCompletionContract cfg before (postIngestPass cfg before) := by
  intro hStored
  constructor
  · intro hText hContent
    simp [keywordSearchable, postIngestPass, hStored, hText, hContent]
  constructor
  · intro hText hEmbed
    simp [semanticSearchable, postIngestPass, hStored, hText, hEmbed]
  constructor
  · intro hKg
    simp [graphNavigable, postIngestPass, hStored, hKg]
  · intro hText hKg hEmbed hSemantic hRerank
    simp [graphRerankAvailable, postIngestPass, hStored, hText, hKg, hEmbed, hSemantic, hRerank]

/-- If the completion contract holds and text/content stages are enabled, the
post-ingest output is keyword searchable. -/
theorem completion_keywordSearchable
    {cfg : PipelineConfig}
    {before after : DocumentState}
    (h : PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hText : cfg.textExtraction = true)
    (hContent : cfg.contentIndex = true) :
    keywordSearchable after = true := by
  exact (h hStored).1 hText hContent

/-- If the completion contract holds and text/embedding stages are enabled, the
post-ingest output is semantic searchable. -/
theorem completion_semanticSearchable
    {cfg : PipelineConfig}
    {before after : DocumentState}
    (h : PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hText : cfg.textExtraction = true)
    (hEmbed : cfg.embeddings = true) :
    semanticSearchable after = true := by
  exact (h hStored).2.1 hText hEmbed

/-- If the completion contract holds and KG core is enabled, graph navigation is
available. -/
theorem completion_graphNavigable
    {cfg : PipelineConfig}
    {before after : DocumentState}
    (h : PipelineCompletionContract cfg before after)
    (hStored : before.stored = true)
    (hKg : cfg.kgCore = true) :
    graphNavigable after = true := by
  exact (h hStored).2.2.1 hKg

end Yams.Pipeline
