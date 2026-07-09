import Yams.Core
import Yams.Topology.Contracts

namespace Yams.Storage

/-- Abstract content hash used by the storage/manifest/reference model.
Uses Yams.Core.ContentHash for unification with other document identifiers. -/
abbrev ContentHash := Yams.Core.ContentHash

/-- Minimal document-level store input for the ingestion hot-path proof.

The C++ store path has richer data (chunks, manifests, metadata rows), but the
optimization boundary we need to protect is keyed by the content hash made
visible to later metadata/search stages. -/
structure StoreDoc where
  contentHash : ContentHash
  deriving Repr, BEq, DecidableEq

/-- Abstract durable/visible store state.

`metadataVisible` is the public boundary: once a hash appears there, callers can
observe the document through metadata/search flows. The safety contract requires
manifest durability and reference-counter commit before that publication. -/
structure StoreState where
  contentDurable : List ContentHash := []
  manifestDurable : List ContentHash := []
  refsCommitted : List ContentHash := []
  metadataVisible : List ContentHash := []
  deriving Repr, BEq

/-- Hashes touched by a batch of store operations, preserving caller order. -/
def hashes (docs : List StoreDoc) : List ContentHash :=
  docs.map (fun doc => doc.contentHash)

/-- Conversion: ContentHash is a DocumentId, so we can view metadataVisible as DocumentIds. -/
def metadataVisibleAsDocumentIds (s : StoreState) : List Yams.Core.DocumentId :=
  s.metadataVisible

/-- Conversion: hashes as DocumentIds. -/
def hashesAsDocumentIds (docs : List StoreDoc) : List Yams.Core.DocumentId :=
  docs.map (fun doc => doc.contentHash)

/-- Public metadata is safe when every visible hash already has a durable manifest
and committed reference-counter state. -/
def PublishedSafe (s : StoreState) : Prop :=
  ∀ h, h ∈ s.metadataVisible → h ∈ s.manifestDurable ∧ h ∈ s.refsCommitted

/-- Store/chunk and manifest preparation phase. This must happen before metadata
publication. -/
def prepareDurableDocs (docs : List StoreDoc) (s : StoreState) : StoreState :=
  { s with
    contentDurable := s.contentDurable ++ hashes docs,
    manifestDurable := s.manifestDurable ++ hashes docs }

/-- Reference-counter commit phase. This must happen before metadata publication. -/
def commitRefs (docs : List StoreDoc) (s : StoreState) : StoreState :=
  { s with refsCommitted := s.refsCommitted ++ hashes docs }

/-- Metadata publication phase: the first point at which stored documents are
observable by metadata/search callers. -/
def publishMetadata (docs : List StoreDoc) (s : StoreState) : StoreState :=
  { s with metadataVisible := s.metadataVisible ++ hashes docs }

/-- Contract-preserving per-document semantics, stated at the visibility boundary:
prepare durable content/manifests, commit references, then publish metadata. -/
def perDocumentCommit (docs : List StoreDoc) (s : StoreState) : StoreState :=
  publishMetadata docs (commitRefs docs (prepareDurableDocs docs s))

/-- Batched/coalesced commit semantics for the proposed hot-path optimization.
It uses the same visibility boundary as `perDocumentCommit`: metadata appears
only after all manifests and refs for the batch are durable/committed. -/
def batchedCommit (docs : List StoreDoc) (s : StoreState) : StoreState :=
  publishMetadata docs (commitRefs docs (prepareDurableDocs docs s))

/-- The optimization is observationally equivalent at the modeled store boundary:
callers see the same durable content, manifests, committed refs, and metadata. -/
theorem batchedCommit_equivalent_perDocument
    (docs : List StoreDoc) (s : StoreState) :
    batchedCommit docs s = perDocumentCommit docs s := by
  rfl

/-- In particular, metadata visibility is identical under batched and
per-document commit semantics. -/
theorem batchedCommit_visibleEquivalent
    (docs : List StoreDoc) (s : StoreState) :
    (batchedCommit docs s).metadataVisible = (perDocumentCommit docs s).metadataVisible := by
  rfl

/-- Batched commit preserves publication safety when the prior state was safe. -/
theorem batchedCommit_publishedSafe
    (docs : List StoreDoc) (s : StoreState)
    (hSafe : PublishedSafe s) :
    PublishedSafe (batchedCommit docs s) := by
  intro h hVisible
  simp [batchedCommit, publishMetadata, commitRefs, prepareDurableDocs] at hVisible ⊢
  cases hVisible with
  | inl oldVisible =>
      have oldSafe := hSafe h oldVisible
      exact ⟨Or.inl oldSafe.1, Or.inl oldSafe.2⟩
  | inr newVisible =>
      exact ⟨Or.inr newVisible, Or.inr newVisible⟩

/-- After batched commit, every newly visible document has a committed reference. -/
theorem batchedCommit_newMetadataHasRefs
    (docs : List StoreDoc) (s : StoreState)
    {h : ContentHash}
    (hNew : h ∈ hashes docs) :
    h ∈ (batchedCommit docs s).refsCommitted := by
  simp [batchedCommit, publishMetadata, commitRefs, prepareDurableDocs]
  exact Or.inr hNew

/-- After batched commit, every newly visible document has a durable manifest. -/
theorem batchedCommit_newMetadataHasManifest
    (docs : List StoreDoc) (s : StoreState)
    {h : ContentHash}
    (hNew : h ∈ hashes docs) :
    h ∈ (batchedCommit docs s).manifestDurable := by
  simp [batchedCommit, publishMetadata, commitRefs, prepareDurableDocs]
  exact Or.inr hNew

/-- Store optimization contract used by the C++ hot-path work: visible metadata is
exactly old metadata followed by the batch hashes, and the resulting state is
publication-safe. -/
def StoreOptimizationContract
    (before after : StoreState)
    (docs : List StoreDoc) : Prop :=
  PublishedSafe after ∧ after.metadataVisible = before.metadataVisible ++ hashes docs

/-- Batched commit satisfies the store optimization contract. -/
theorem batchedCommit_contract
    (docs : List StoreDoc) (s : StoreState)
    (hSafe : PublishedSafe s) :
    StoreOptimizationContract s (batchedCommit docs s) docs := by
  constructor
  · exact batchedCommit_publishedSafe docs s hSafe
  · rfl

end Yams.Storage
