import Yams.Core
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

/-- The C++ SGC implementation must never divide by a non-positive degree. -/
def PositiveDegrees (edges : List WeightedEdge) : Prop :=
  ∀ e ∈ edges, 0 < degree edges e.src ∧ 0 < degree edges e.dst

/-- Document with embedding for SGC smoothing. -/
structure SGCDocument where
  documentHash : Yams.Core.DocumentHash
  embedding : Yams.Core.EmbeddingVector
  deriving Repr, BEq

/-- Convert TopologyDocumentInput to SGCDocument for smoothing. -/
def topologyDocToSGCDoc (doc : TopologyDocumentInput) : SGCDocument :=
  { documentHash := doc.documentHash
    embedding := doc.embedding }

/-- Convert SGCDocument back to a minimal TopologyDocumentInput. -/
def sgcDocToTopologyDoc (doc : SGCDocument) : TopologyDocumentInput :=
  { documentHash := doc.documentHash
    embedding := doc.embedding }

/-- Conservative rational square-root abstraction.

The executable C++ implementation uses floating-point `sqrt`; the Lean model
keeps only the safety contract needed by the topology pipeline: zero stays zero
and positive denominators are guarded by `PositiveDegrees`/runtime checks. -/
def ratSqrt (r : Rat) : Rat :=
  if r = 0 then 0 else 1

/-- One abstract SGC propagation step.

This model intentionally abstracts away floating-point vector arithmetic. It
preserves the important C++ contract: smoothing may mutate embeddings, but it
must not add/drop documents or rewrite document hashes. -/
def smoothStep (edges : List WeightedEdge) (docs : List SGCDocument) :
    List SGCDocument :=
  docs.mapIdx (fun i doc =>
    let deg := degree edges i
    let scale := if deg = 0 then 1 else deg
    { doc with embedding := doc.embedding.map (fun x => x / scale) })

/-- Apply K smoothing hops. Mirrors `applySGCSmoothing(..., hops)`. -/
def smoothKHops (edges : List WeightedEdge) : Nat → List SGCDocument → List SGCDocument
  | 0, docs => docs
  | Nat.succ k, docs => smoothKHops edges k (smoothStep edges docs)

/-- Update topology documents with smoothed embeddings while preserving metadata. -/
def updateTopologyEmbeddings (docs : List TopologyDocumentInput) (smoothed : List SGCDocument) :
    List TopologyDocumentInput :=
  (docs.zip smoothed).map (fun pair => { pair.fst with embedding := pair.snd.embedding })

/-- Apply K smoothing hops to TopologyDocumentInput. -/
def smoothKHopsTopology (edges : List WeightedEdge) (k : Nat)
    (docs : List TopologyDocumentInput) : List TopologyDocumentInput :=
  let smoothed := smoothKHops edges k (docs.map topologyDocToSGCDoc)
  updateTopologyEmbeddings docs smoothed

/-- Zero smoothing hops is an identity transformation. -/
theorem smooth_zero (edges : List WeightedEdge) (docs : List SGCDocument) :
    smoothKHops edges 0 docs = docs := by
  rfl

/-- `smoothStep` preserves each document's hash: the structure update only
touches `embedding`, never `documentHash`. -/
theorem smoothStep_preserves_hashes
    (edges : List WeightedEdge) (docs : List SGCDocument) (i : Nat) :
    (smoothStep edges docs)[i]?.map (fun (d : SGCDocument) => d.documentHash) =
      docs[i]?.map (fun (d : SGCDocument) => d.documentHash) := by
  simp only [smoothStep, List.getElem?_mapIdx, Option.map_map]
  rfl

/-- Smoothing preserves document count through the abstract SGCDocument path.

The induction generalizes `docs` so the inductive step can push the step
through `smoothStep edges docs`. -/
theorem smooth_preserves_length (edges : List WeightedEdge) :
    ∀ k docs, (smoothKHops edges k docs).length = docs.length := by
  intro k
  induction k with
  | zero => intro docs; rfl
  | succ k ih =>
      intro docs
      calc
        (smoothKHops edges (Nat.succ k) docs).length
            = (smoothKHops edges k (smoothStep edges docs)).length := rfl
        _ = (smoothStep edges docs).length := ih (smoothStep edges docs)
        _ = docs.length := by simp [smoothStep]

/-- Smoothing preserves document hashes through the SGCDocument path:

`smoothKHops` only mutates embeddings; the document identity is invariant
across all hops. -/
theorem smooth_preserves_hashes' (edges : List WeightedEdge) :
    ∀ k (docs : List SGCDocument) (i : Nat),
      (smoothKHops edges k docs)[i]?.map (fun (d : SGCDocument) => d.documentHash) =
        docs[i]?.map (fun (d : SGCDocument) => d.documentHash) := by
  intro k
  induction k with
  | zero => intro docs i; rfl
  | succ k ih =>
      intro docs i
      calc
        (smoothKHops edges (Nat.succ k) docs)[i]?.map
            (fun (d : SGCDocument) => d.documentHash)
            = (smoothKHops edges k (smoothStep edges docs))[i]?.map
                (fun (d : SGCDocument) => d.documentHash) := rfl
        _ = (smoothStep edges docs)[i]?.map (fun (d : SGCDocument) => d.documentHash) :=
              ih (smoothStep edges docs) i
        _ = docs[i]?.map (fun (d : SGCDocument) => d.documentHash) :=
              smoothStep_preserves_hashes edges docs i

/-- External-facing version `∀ i, ...` after binding `k` and `docs`. -/
theorem smooth_preserves_hashes (edges : List WeightedEdge) (k : Nat)
    (docs : List SGCDocument) :
    ∀ (i : Nat), (smoothKHops edges k docs)[i]?.map (fun (d : SGCDocument) => d.documentHash) =
          docs[i]?.map (fun (d : SGCDocument) => d.documentHash) :=
  smooth_preserves_hashes' edges k docs

/-- Topology version: smoothing preserves document count.

The wrapper zips the smoothed stream with the original topology documents and
keeps every prior field; length is preserved because both sides have the same
length (`smooth_preserves_length` plus `List.length_map`). -/
theorem smooth_preserves_length_topology (edges : List WeightedEdge) :
    ∀ k docs, (smoothKHopsTopology edges k docs).length = docs.length := by
  intro k docs
  simp only [smoothKHopsTopology, updateTopologyEmbeddings, List.length_map,
             List.length_zip]
  have hSmoothedLen :
      (smoothKHops edges k (docs.map topologyDocToSGCDoc)).length =
        (docs.map topologyDocToSGCDoc).length :=
    smooth_preserves_length edges k (docs.map topologyDocToSGCDoc)
  rw [hSmoothedLen, List.length_map]
  exact Nat.min_self docs.length

/-- Helper: `(l1.zip l2)[i]?` factors into a bind over both list accesses. -/
theorem zip_getElem?_bind {α β : Type} (l1 : List α) (l2 : List β) (i : Nat) :
    (l1.zip l2)[i]? =
      l1[i]?.bind (fun a => l2[i]?.map (fun b => (a, b))) := by
  induction l1 generalizing l2 i with
  | nil =>
    cases l2 <;> cases i <;> rfl
  | cons x xs ih =>
    cases l2 with
    | nil =>
      cases i with
      | zero => rfl
      | succ n =>
        change none = xs[n]?.bind (fun a => Option.map (fun b => (a, b)) none)
        cases xs[n]? <;> rfl
    | cons y ys =>
      cases i with
      | zero => rfl
      | succ n =>
        change (xs.zip ys)[n]? = xs[n]?.bind (fun a => ys[n]?.map (fun b => (a, b)))
        exact ih ys n

/-- Auxiliary: `updateTopologyEmbeddings` preserves each topology document's
hash because the zip+map only replaces `embedding`. Equal-length precondition
makes the two streams line up. -/
theorem updateTopologyEmbeddings_preserves_hashes
    (docs : List TopologyDocumentInput) (smoothed : List SGCDocument)
    (hLen : smoothed.length = docs.length) (i : Nat) :
    (updateTopologyEmbeddings docs smoothed)[i]?.map
        (fun (d : TopologyDocumentInput) => d.documentHash) =
      docs[i]?.map (fun (d : TopologyDocumentInput) => d.documentHash) := by
  by_cases h : i < docs.length
  · -- Positive case: both lists contain index i
    have hSmooth : i < smoothed.length := by omega
    have hDocsElem : docs[i]? = some docs[i] := List.getElem?_eq_getElem h
    have hSmoothElem : smoothed[i]? = some smoothed[i] := List.getElem?_eq_getElem hSmooth
    have hZip : (docs.zip smoothed)[i]? = some (docs[i], smoothed[i]) := by
      rw [zip_getElem?_bind, hDocsElem, hSmoothElem, Option.bind_some, Option.map_some]
    simp only [updateTopologyEmbeddings, List.getElem?_map, hZip, Option.map_some,
               Option.map_some, hDocsElem]
  · -- Negative case: both indices out of bounds
    have hNotSmooth : ¬ i < smoothed.length := by omega
    have hDocsElem : docs[i]? = none :=
      List.getElem?_eq_none (Nat.le_of_not_lt h)
    have hSmoothElem : smoothed[i]? = none :=
      List.getElem?_eq_none (Nat.le_of_not_lt hNotSmooth)
    have hZip : (docs.zip smoothed)[i]? = none := by
      rw [zip_getElem?_bind, hDocsElem, hSmoothElem]
      rfl
    simp only [updateTopologyEmbeddings, List.getElem?_map, hZip, hDocsElem]
    rfl

/-- Topology version: smoothing preserves document hashes.

The wrapper preserves original topology document identity (`filePath`, `metadata`,
`neighbors`, …); only the `embedding` field is updated with the smoothed stream. -/
theorem smooth_preserves_hashes_topology (edges : List WeightedEdge) (k : Nat)
    (docs : List TopologyDocumentInput) :
    ∀ (i : Nat), (smoothKHopsTopology edges k docs)[i]?.map
            (fun (d : TopologyDocumentInput) => d.documentHash) =
          docs[i]?.map (fun (d : TopologyDocumentInput) => d.documentHash) := by
  intro i
  have hSmoothedLen :
      (smoothKHops edges k (docs.map topologyDocToSGCDoc)).length = docs.length := by
    have := smooth_preserves_length edges k (docs.map topologyDocToSGCDoc)
    rw [List.length_map] at this
    exact this
  exact updateTopologyEmbeddings_preserves_hashes docs
    (smoothKHops edges k (docs.map topologyDocToSGCDoc)) hSmoothedLen i

end Yams.Topology