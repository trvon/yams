import Yams.Core
import Yams.Topology.Connected

namespace Yams.Topology

/-- Literature-informed topology signal families. The constructors are deliberately
small and operational: each one can be mapped to a benchmark source/builder.

* `lexicalCohesion`: repeated or locally coherent terms/phrases inside a segment.
* `lexicalDisruption`: a boundary/gap signal between adjacent segments.
* `keyphrase`: exemplar phrase or topic label extracted from a document/segment.
* `entity`: named/concept entity extracted from text.
* `ontology`: normalized entity connected through background knowledge.
* `structure`: title, heading, section, or discourse-structure marker.
* `embedding`: vector-neighborhood observation used as a source signal.
-/
inductive TopologySignalKind where
  | lexicalCohesion
  | lexicalDisruption
  | keyphrase
  | entity
  | ontology
  | structure
  | embedding
  deriving Repr, BEq, DecidableEq

abbrev FeatureId := String

/-- Construction-time gates that should be checked before publishing topology
edges. These are intentionally abstract Nat thresholds so C++ benchmarks can map
BM25/PMI/SDM/entity-confidence/embedding scores onto monotone buckets without
making Lean reason about floating point. -/
structure ConstructionGateConfig where
  minSignalWeight : Nat := 1
  maxSignalDocumentFrequency : Nat := 64
  minNeighborScore : Nat := 1
  maxComponentDocs : Nat := 64
  deriving Repr, BEq

/-- A document or segment-level source signal used to justify a topology edge.
`documentFrequency` models hub suppression: broad features are allowed only when
below the configured DF cap. -/
structure DocumentSignal where
  doc : Nat
  feature : FeatureId
  kind : TopologySignalKind
  weight : Nat := 1
  documentFrequency : Nat := 1
  deriving Repr, BEq

/-- A directed neighbor observation before reciprocal/mutual filtering. -/
structure NeighborObservation where
  src : Nat
  dst : Nat
  score : Nat := 1
  deriving Repr, BEq

/-- A connected-component candidate emitted after graph construction. -/
structure TopologyComponent where
  root : Nat
  members : List Nat := []
  deriving Repr, BEq

/-- Per-signal admission: strong enough and not a high-frequency hub. -/
def SignalPassesGate (cfg : ConstructionGateConfig) (signal : DocumentSignal) : Prop :=
  cfg.minSignalWeight ≤ signal.weight ∧
    signal.documentFrequency ≤ cfg.maxSignalDocumentFrequency

/-- A topology edge is justified by two admitted signals of the same kind and
feature, one on each endpoint. This captures the useful-clustering hypothesis:
edges should be traceable to sparse, discriminative word/phrase/entity/structure
signals rather than arbitrary document similarity. -/
def EdgeHasSignalWitness
    (cfg : ConstructionGateConfig)
    (signals : List DocumentSignal)
    (edge : UEdge) : Prop :=
  ∃ left ∈ signals, ∃ right ∈ signals,
    left.doc = edge.a ∧
      right.doc = edge.b ∧
      left.feature = right.feature ∧
      left.kind = right.kind ∧
      left.doc ≠ right.doc ∧
      SignalPassesGate cfg left ∧
      SignalPassesGate cfg right

/-- Every published edge has a non-hub same-feature witness. -/
def GraphRespectsSignalGate
    (cfg : ConstructionGateConfig)
    (signals : List DocumentSignal)
    (edges : List UEdge) : Prop :=
  ∀ edge ∈ edges, EdgeHasSignalWitness cfg signals edge

/-- Reciprocal/mutual neighbor gate: an undirected edge is allowed only when both
directed observations exist and both pass the score threshold. -/
def ReciprocalNeighborWitness
    (cfg : ConstructionGateConfig)
    (observations : List NeighborObservation)
    (edge : UEdge) : Prop :=
  ∃ forward ∈ observations, ∃ reverse ∈ observations,
    forward.src = edge.a ∧
      forward.dst = edge.b ∧
      reverse.src = edge.b ∧
      reverse.dst = edge.a ∧
      cfg.minNeighborScore ≤ forward.score ∧
      cfg.minNeighborScore ≤ reverse.score

/-- Every published edge survived reciprocal/mutual filtering. -/
def GraphRespectsReciprocalGate
    (cfg : ConstructionGateConfig)
    (observations : List NeighborObservation)
    (edges : List UEdge) : Prop :=
  ∀ edge ∈ edges, ReciprocalNeighborWitness cfg observations edge

/-- Component admission cap. The cap is a construction-time guard against giant
components; retrieval-time `maxDocs` remains a separate search-path cap. -/
def ComponentRespectsCap (cfg : ConstructionGateConfig) (component : TopologyComponent) : Prop :=
  component.members.length ≤ cfg.maxComponentDocs

/-- Component candidates are both connected and capped. -/
def ComponentsConnectedAndCapped
    (cfg : ConstructionGateConfig)
    (edges : List UEdge)
    (components : List TopologyComponent) : Prop :=
  ∀ component ∈ components,
    ConnectedFrom edges component.root component.members ∧ ComponentRespectsCap cfg component

/-- A compact certificate for a topology-builder output. C++ source builders can
populate the analogous debug artifact and benchmark scripts can reject runs that
lack these witnesses. -/
structure TopologyConstructionCertificate where
  edges : List UEdge := []
  components : List TopologyComponent := []
  signals : List DocumentSignal := []
  observations : List NeighborObservation := []
  deriving Repr, BEq

/-- The full gate contract for topology construction: sparse signal witnesses,
reciprocal observations, and no oversized components. -/
def CertificateRespectsGates
    (cfg : ConstructionGateConfig)
    (cert : TopologyConstructionCertificate) : Prop :=
  GraphRespectsSignalGate cfg cert.signals cert.edges ∧
    GraphRespectsReciprocalGate cfg cert.observations cert.edges ∧
    ComponentsConnectedAndCapped cfg cert.edges cert.components

/-- Signal-gated graphs expose the feature and kind that justified each edge, and
both endpoint witnesses satisfy the configured DF cap. -/
theorem signalGate_edgesHaveNonHubWitness
    {cfg : ConstructionGateConfig}
    {signals : List DocumentSignal}
    {edges : List UEdge}
    {edge : UEdge}
    (hGraph : GraphRespectsSignalGate cfg signals edges)
    (hEdge : edge ∈ edges) :
    ∃ feature kind, ∃ left ∈ signals, ∃ right ∈ signals,
      left.doc = edge.a ∧
        right.doc = edge.b ∧
        left.feature = feature ∧
        right.feature = feature ∧
        left.kind = kind ∧
        right.kind = kind ∧
        left.documentFrequency ≤ cfg.maxSignalDocumentFrequency ∧
        right.documentFrequency ≤ cfg.maxSignalDocumentFrequency := by
  rcases hGraph edge hEdge with
    ⟨left, hLeftMem, right, hRightMem, hLeftDoc, hRightDoc, hFeature, hKind,
      _hDistinct, hLeftGate, hRightGate⟩
  exact ⟨left.feature, left.kind, left, hLeftMem, right, hRightMem,
    hLeftDoc, hRightDoc, rfl, by rw [← hFeature], rfl, by rw [← hKind],
    hLeftGate.2, hRightGate.2⟩

/-- Reciprocal-gated graphs expose both directed observations that justified an
undirected edge. -/
theorem reciprocalGate_edgesHaveBothDirections
    {cfg : ConstructionGateConfig}
    {observations : List NeighborObservation}
    {edges : List UEdge}
    {edge : UEdge}
    (hGraph : GraphRespectsReciprocalGate cfg observations edges)
    (hEdge : edge ∈ edges) :
    ∃ forward ∈ observations, ∃ reverse ∈ observations,
      forward.src = edge.a ∧
        forward.dst = edge.b ∧
        reverse.src = edge.b ∧
        reverse.dst = edge.a ∧
        cfg.minNeighborScore ≤ forward.score ∧
        cfg.minNeighborScore ≤ reverse.score := by
  exact hGraph edge hEdge

/-- If the component section of a certificate passes, every published component
respects the anti-giant cap. -/
theorem componentsConnectedAndCapped_impliesCap
    {cfg : ConstructionGateConfig}
    {edges : List UEdge}
    {components : List TopologyComponent}
    {component : TopologyComponent}
    (hComponents : ComponentsConnectedAndCapped cfg edges components)
    (hComponent : component ∈ components) :
    component.members.length ≤ cfg.maxComponentDocs := by
  exact (hComponents component hComponent).2

/-- Certificate-level projection: every edge has a non-hub source-signal witness. -/
theorem certificate_edgesHaveNonHubWitness
    {cfg : ConstructionGateConfig}
    {cert : TopologyConstructionCertificate}
    {edge : UEdge}
    (hCert : CertificateRespectsGates cfg cert)
    (hEdge : edge ∈ cert.edges) :
    ∃ feature kind, ∃ left ∈ cert.signals, ∃ right ∈ cert.signals,
      left.doc = edge.a ∧
        right.doc = edge.b ∧
        left.feature = feature ∧
        right.feature = feature ∧
        left.kind = kind ∧
        right.kind = kind ∧
        left.documentFrequency ≤ cfg.maxSignalDocumentFrequency ∧
        right.documentFrequency ≤ cfg.maxSignalDocumentFrequency := by
  exact signalGate_edgesHaveNonHubWitness hCert.1 hEdge

/-- Certificate-level projection: every edge survived reciprocal filtering. -/
theorem certificate_edgesHaveReciprocalWitness
    {cfg : ConstructionGateConfig}
    {cert : TopologyConstructionCertificate}
    {edge : UEdge}
    (hCert : CertificateRespectsGates cfg cert)
    (hEdge : edge ∈ cert.edges) :
    ∃ forward ∈ cert.observations, ∃ reverse ∈ cert.observations,
      forward.src = edge.a ∧
        forward.dst = edge.b ∧
        reverse.src = edge.b ∧
        reverse.dst = edge.a ∧
        cfg.minNeighborScore ≤ forward.score ∧
        cfg.minNeighborScore ≤ reverse.score := by
  exact reciprocalGate_edgesHaveBothDirections hCert.2.1 hEdge

/-- Certificate-level projection: no certified component exceeds the configured
component-size cap. -/
theorem certificate_componentsRespectCap
    {cfg : ConstructionGateConfig}
    {cert : TopologyConstructionCertificate}
    {component : TopologyComponent}
    (hCert : CertificateRespectsGates cfg cert)
    (hComponent : component ∈ cert.components) :
    component.members.length ≤ cfg.maxComponentDocs := by
  exact componentsConnectedAndCapped_impliesCap hCert.2.2 hComponent

end Yams.Topology
