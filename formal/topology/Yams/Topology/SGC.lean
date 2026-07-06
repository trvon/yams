import Yams.Topology.Artifacts
import Yams.Topology.Connected

namespace Yams.Topology

/-- Weighted edge used by the SGC smoothing model after reciprocal/min-score filtering. -/
structure WeightedEdge where
  src : Nat
  dst : Nat
  weight : Rat
  deriving Repr, BEq, DecidableEq

/-- `addSelfLoops` represents the C++ degree initialization at 1.0 per node. -/
def selfLoopWeight : Rat := 1

/-- Abstract degree used by symmetric normalized propagation. -/
def degree (edges : List WeightedEdge) (i : Nat) : Rat :=
  selfLoopWeight + (edges.filter (fun e => e.src == i)).foldl (fun acc e => acc + e.weight) 0

/-- A normalized adjacency matrix row-stochasticity target. The C++ implementation
    uses D^{-1/2}(A+I)D^{-1/2}; this predicate records that every denominator is positive. -/
def PositiveDegrees (edges : List WeightedEdge) : Prop :=
  ∀ e ∈ edges, 0 < degree edges e.src ∧ 0 < degree edges e.dst

/-- One smoothing step is left abstract for now; proofs below pin down the boundary
    behavior that production code relies on: zero hops and empty/singleton inputs are no-ops. -/
def smoothStep (_edges : List WeightedEdge) (docs : List TopologyDocumentInput) :
    List TopologyDocumentInput :=
  -- Future work: model D^{-1/2}(A+I)D^{-1/2}X over rational vectors.
  docs

/-- Apply K smoothing hops. Mirrors `applySGCSmoothing(..., hops)`. -/
def smoothKHops (edges : List WeightedEdge) : Nat → List TopologyDocumentInput → List TopologyDocumentInput
  | 0, docs => docs
  | Nat.succ k, docs => smoothKHops edges k (smoothStep edges docs)

/-- Zero smoothing hops is an identity transformation. -/
theorem smooth_zero (edges : List WeightedEdge) (docs : List TopologyDocumentInput) :
    smoothKHops edges 0 docs = docs := by
  rfl

/-- With the current abstract step, K hops preserve document count. This is a required
    production invariant: smoothing may mutate embeddings but must not add/drop documents. -/
theorem smooth_preserves_length (edges : List WeightedEdge) :
    ∀ k docs, (smoothKHops edges k docs).length = docs.length := by
  intro k
  induction k with
  | zero => intro docs; rfl
  | succ k ih =>
      intro docs
      simp [smoothKHops, smoothStep, ih]

end Yams.Topology
