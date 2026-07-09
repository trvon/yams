/-
Core type definitions shared across all YAMS formal models.

This module establishes a consistent document identity model that connects:
- Storage (content hashes, manifests)
- Pipeline (document processing states)
- Topology (document hashes, clusters)
- Search (document identifiers in results)

The key invariant: DocumentId, ContentHash, and DocumentHash are all the same
underlying type (String), but we use distinct type abbreviations to make the
intended usage explicit at each boundary.
-/

namespace Yams.Core

/-- Universal document identifier. 

This is the abstract notion of a document's identity, independent of how it is
stored, processed, or retrieved. In the C++ codebase, this corresponds to the
content-addressable hash of a document.
-/
abbrev DocumentId := String

/-- Content hash from the storage layer. 

This is the identifier used by the storage system (StoreDoc, manifest entries).
It is definitionally equal to DocumentId but used to make storage-boundary usage explicit.
-/
abbrev ContentHash := DocumentId

/-- Document hash from the topology layer. 

This is the identifier used in topology artifacts (clusters, neighbors, edges).
It is definitionally equal to DocumentId but used for topology-specific contexts.
-/
abbrev DocumentHash := DocumentId

/-- File path identifier. 

Used for source file tracking in topology and search contexts.
-/
abbrev FilePath := String

/-- Cluster identifier. 

Used to name connected components or algorithmic clusters in topology.
-/
abbrev ClusterId := String

/-- Embedding vector represented as rational numbers.

This allows precise algebraic reasoning about vector operations in SGC
and similarity computations. Using Lean's `Rat` (rationals) instead of Float avoids precision issues in
formal proofs.
-/
abbrev EmbeddingVector := List Rat

/-- Score as a rational number for precise comparisons.

Used for edge scores, document scores, route scores, etc.
-/
abbrev Score := Rat

/-- Conversion lemmas to support interoperability with existing Nat-based scores.

These are provided for compatibility with the existing search engine model
which uses Nat for scores. In production, scores are typically floating-point,
but Nat suffices for ranking/ordering proofs in the abstract model.
-/

-- Nat can be embedded into Rat
def natToScore (n : Nat) : Score := n

-- Score can be truncated to Nat (floor)
def scoreToNat (s : Score) : Nat := s.num.natAbs

-- Embedding can be represented as Nat lists for compatibility
def embeddingVectorToNatList (e : EmbeddingVector) : List Nat :=
  e.map (fun q => q.num.natAbs)

end Yams.Core
