import Yams.Topology.Artifacts

namespace Yams.Topology

/-- Undirected edge over document indices in a finite input list. -/
structure UEdge where
  a : Nat
  b : Nat
  deriving Repr, BEq, DecidableEq

/-- Normalize an undirected edge so duplicate directed observations share one key. -/
def UEdge.normalized (e : UEdge) : UEdge :=
  if e.a ≤ e.b then e else { a := e.b, b := e.a }

/-- Edge endpoint membership. -/
def UEdge.incident (e : UEdge) (x : Nat) : Prop :=
  e.a = x ∨ e.b = x

/-- The C++ connected engine uses reciprocal/min-score filtered neighbor observations
    to build an undirected edge set. This model starts after that filtering. -/
inductive Reachable (edges : List UEdge) : Nat → Nat → Prop where
  | refl (x : Nat) : Reachable edges x x
  | step {x y z : Nat} (e : UEdge)
      (he : e ∈ edges)
      (hxy : (e.a = x ∧ e.b = y) ∨ (e.a = y ∧ e.b = x))
      (rest : Reachable edges y z) : Reachable edges x z

/-- A component is connected when every member is reachable from the root. -/
def ConnectedFrom (edges : List UEdge) (root : Nat) (members : List Nat) : Prop :=
  ∀ x ∈ members, Reachable edges root x

/-- No edge should cross two different connected-component clusters. -/
def NoCrossClusterEdges (edges : List UEdge) (clusterOf : Nat → ClusterId) : Prop :=
  ∀ e ∈ edges, clusterOf e.a = clusterOf e.b

/-- C++ tie-break for medoid selection: maximum weighted degree, then lexicographically
    smaller document hash. `hashRank` abstracts the lexicographic order. -/
def IsMedoidByScore (score : Nat → Rat) (hashRank : Nat → Nat)
    (members : List Nat) (m : Nat) : Prop :=
  m ∈ members ∧ ∀ x ∈ members,
    score x < score m ∨ (score x = score m ∧ hashRank m ≤ hashRank x)

/-- The singleton component is connected, matching the outlier case in C++. -/
theorem singleton_connected (edges : List UEdge) (x : Nat) :
    ConnectedFrom edges x [x] := by
  intro y hy
  simp at hy
  subst y
  exact Reachable.refl x

/-- Reachability is reflexive for every edge set. -/
theorem reachable_refl (edges : List UEdge) (x : Nat) : Reachable edges x x := by
  exact Reachable.refl x

/-- If a batch is well formed, every membership references a cluster. This theorem is
    intentionally trivial: it gives C++ tests and future extractors a named target. -/
theorem wellFormed_membershipsReferenceClusters
    (batch : TopologyArtifactBatch) (h : WellFormedBatch batch) :
    membershipsReferenceClusters batch := by
  exact h.2.1

/-- If a batch is well formed, cluster member counts are exact. -/
theorem wellFormed_memberCounts
    (batch : TopologyArtifactBatch) (h : WellFormedBatch batch) :
    ∀ c ∈ batch.clusters, c.memberCount = c.memberDocumentHashes.length := by
  intro c hc
  exact h.1 c hc

end Yams.Topology
