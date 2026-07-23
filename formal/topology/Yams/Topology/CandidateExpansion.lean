import Yams.Topology.SearchEngine

namespace Yams.SearchEngine

/-!
Candidate rescue through bounded relation expansion.

The primary retrieval claim is candidate discovery, not geometric narrowing.
A relation expansion may improve candidate recall only when it preserves the
baseline's relevant documents and a previously missing relevant document
survives candidate selection.  Relevance is used only at the theorem and
benchmark boundary; a production generator observes query scores and typed
document relations instead of relevance judgments.
-/

/-- Candidate-stage document membership, independent of source and score. -/
def CandidateDocPresent
    (doc : Yams.Core.DocumentId) (candidates : List ComponentCandidate) : Prop :=
  ∃ candidate ∈ candidates, candidate.doc = doc

/-- A document has evidence from a particular retrieval component. This is
strictly finer than document membership: a lexical candidate may acquire routed
vector evidence without becoming a novel document. -/
def CandidateSourcePresent
    (doc : Yams.Core.DocumentId) (source : SearchSource)
    (candidates : List ComponentCandidate) : Prop :=
  ∃ candidate ∈ candidates, candidate.doc = doc ∧ candidate.source = source

/-- Additive expansion before deduplication or a candidate budget is applied. -/
def augmentCandidates
    (baseline expansion : List ComponentCandidate) : List ComponentCandidate :=
  baseline ++ expansion

/-- Every relevant document found by the baseline remains available after selection. -/
def PreservesBaselineRelevant
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline selected : List ComponentCandidate) : Prop :=
  ∀ doc ∈ relevantDocs,
    CandidateDocPresent doc baseline → CandidateDocPresent doc selected

/-- A strict candidate rescue is relevant, missing from the baseline, and present
after the expansion budget has been applied. -/
def RescuesMissingRelevant
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline selected : List ComponentCandidate) : Prop :=
  ∃ doc ∈ relevantDocs,
    ¬ CandidateDocPresent doc baseline ∧ CandidateDocPresent doc selected

/-- Candidate recall strictly improves as a set property. A later ranking theorem
is still required for MRR or nDCG uplift. -/
def StrictlyImprovesCandidateRecall
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline selected : List ComponentCandidate) : Prop :=
  PreservesBaselineRelevant relevantDocs baseline selected ∧
    RescuesMissingRelevant relevantDocs baseline selected

/-- Every observed neighbor of a selected seed is materialized as an expansion
candidate. The relation can represent explicit links, entities, reciprocal
semantic neighbors, or another typed producer. -/
def RelationExpansionMaterialized
    (relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop)
    (seeds : List Yams.Core.DocumentId)
    (expansion : List ComponentCandidate) : Prop :=
  ∀ seed ∈ seeds,
    ∀ doc, relation seed doc → CandidateDocPresent doc expansion

/-- Relation expansion materializes query-scored evidence from one component.
This captures the live routed-vector path independently of document novelty. -/
def RelationEvidenceMaterialized
    (relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop)
    (seeds : List Yams.Core.DocumentId)
    (source : SearchSource)
    (expansion : List ComponentCandidate) : Prop :=
  ∀ seed ∈ seeds,
    ∀ doc, relation seed doc → CandidateSourcePresent doc source expansion

/-- At least one relevant document missed by the baseline is connected to a
selected seed. This is an empirical producer/calibration obligation. -/
def MissingRelevantReachable
    (relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop)
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline : List ComponentCandidate)
    (seeds : List Yams.Core.DocumentId) : Prop :=
  ∃ doc ∈ relevantDocs,
    ¬ CandidateDocPresent doc baseline ∧
      ∃ seed ∈ seeds, relation seed doc

/-- A relevant document already present in the broad baseline lacks evidence
from `source`, but is relation-reachable. This is a buried-document evidence
obligation, not a missing-document recall obligation. -/
def ExistingRelevantMissingSourceReachable
    (relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop)
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline : List ComponentCandidate)
    (seeds : List Yams.Core.DocumentId)
    (source : SearchSource) : Prop :=
  ∃ doc ∈ relevantDocs,
    CandidateDocPresent doc baseline ∧
      ¬ CandidateSourcePresent doc source baseline ∧
        ∃ seed ∈ seeds, relation seed doc

/-- The augmented representation adds component evidence to an existing
relevant document. No ranking improvement follows without a separate selector
or fusion premise. -/
def EnrichesExistingRelevantEvidence
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline enriched : List ComponentCandidate)
    (source : SearchSource) : Prop :=
  ∃ doc ∈ relevantDocs,
    CandidateDocPresent doc baseline ∧
      ¬ CandidateSourcePresent doc source baseline ∧
        CandidateSourcePresent doc source enriched

/-- A feedback seed is both present in the scored baseline and accepted by a
query-dependent seed policy. The policy can encode a reranker threshold, score
margin, reciprocal-neighbor support, or another calibrated relevance proxy. -/
def QueryAdmissibleSeed
    (admissible : Yams.Core.DocumentId → Prop)
    (baseline : List ComponentCandidate)
    (seed : Yams.Core.DocumentId) : Prop :=
  CandidateDocPresent seed baseline ∧ admissible seed

/-- A missing relevant document is reachable from a query-admissible baseline
seed. This is stronger than static reachability from an arbitrary route medoid
and models adaptive corpus-graph expansion. -/
def MissingRelevantReachableFromAdmissibleSeed
    (relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop)
    (admissible : Yams.Core.DocumentId → Prop)
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline : List ComponentCandidate)
    (seeds : List Yams.Core.DocumentId) : Prop :=
  ∃ doc ∈ relevantDocs,
    ¬ CandidateDocPresent doc baseline ∧
      ∃ seed ∈ seeds,
        QueryAdmissibleSeed admissible baseline seed ∧ relation seed doc

/-- Candidate selection retains every relevant expansion rescue. This is the
budget/fusion boundary that an uncapped union alone cannot establish. -/
def RelevantExpansionSurvivesSelection
    (relevantDocs : List Yams.Core.DocumentId)
    (baseline expansion selected : List ComponentCandidate) : Prop :=
  ∀ doc ∈ relevantDocs,
    ¬ CandidateDocPresent doc baseline →
      CandidateDocPresent doc expansion →
        CandidateDocPresent doc selected

/-- Plain augmentation cannot remove a baseline candidate. -/
theorem augmentCandidates_preservesBaseline
    (baseline expansion : List ComponentCandidate)
    {doc : Yams.Core.DocumentId}
    (hPresent : CandidateDocPresent doc baseline) :
    CandidateDocPresent doc (augmentCandidates baseline expansion) := by
  rcases hPresent with ⟨candidate, hCandidate, hDoc⟩
  exact ⟨candidate, List.mem_append_left expansion hCandidate, hDoc⟩

/-- A materialized relation expansion exposes any reachable missing relevant
document before the candidate budget is applied. -/
theorem materializedRelationExpansion_exposesReachableRelevant
    {relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {relevantDocs : List Yams.Core.DocumentId}
    {baseline expansion : List ComponentCandidate}
    {seeds : List Yams.Core.DocumentId}
    (hReachable : MissingRelevantReachable relation relevantDocs baseline seeds)
    (hMaterialized : RelationExpansionMaterialized relation seeds expansion) :
    ∃ doc ∈ relevantDocs,
      ¬ CandidateDocPresent doc baseline ∧ CandidateDocPresent doc expansion := by
  rcases hReachable with ⟨doc, hRelevant, hMissing, seed, hSeed, hRelation⟩
  exact ⟨doc, hRelevant, hMissing, hMaterialized seed hSeed doc hRelation⟩

/-- Materialized routed evidence enriches a reachable relevant document that
was already present through another component. This theorem explains evidence
rescue while deliberately making no candidate-recall or ranking claim. -/
theorem materializedRelationEvidence_enrichesExistingRelevant
    {relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {relevantDocs : List Yams.Core.DocumentId}
    {baseline expansion : List ComponentCandidate}
    {seeds : List Yams.Core.DocumentId}
    {source : SearchSource}
    (hReachable :
      ExistingRelevantMissingSourceReachable
        relation relevantDocs baseline seeds source)
    (hMaterialized : RelationEvidenceMaterialized relation seeds source expansion) :
    EnrichesExistingRelevantEvidence
      relevantDocs baseline (augmentCandidates baseline expansion) source := by
  rcases hReachable with
    ⟨doc, hRelevant, hPresent, hMissingSource, seed, hSeed, hRelation⟩
  refine ⟨doc, hRelevant, hPresent, hMissingSource, ?_⟩
  rcases hMaterialized seed hSeed doc hRelation with
    ⟨candidate, hCandidate, hDoc, hSource⟩
  exact ⟨candidate, List.mem_append_right baseline hCandidate, hDoc, hSource⟩

/-- Query-admissible reachability supplies the generic reachability obligation.
The extra seed evidence is retained as an empirical certificate even though the
set-theoretic recall proof only needs the resulting relation edge. -/
theorem admissibleSeedReachability_impliesReachability
    {relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {admissible : Yams.Core.DocumentId → Prop}
    {relevantDocs : List Yams.Core.DocumentId}
    {baseline : List ComponentCandidate}
    {seeds : List Yams.Core.DocumentId}
    (hReachable :
      MissingRelevantReachableFromAdmissibleSeed
        relation admissible relevantDocs baseline seeds) :
    MissingRelevantReachable relation relevantDocs baseline seeds := by
  rcases hReachable with
    ⟨doc, hRelevant, hMissing, seed, hSeed, _hAdmissible, hRelation⟩
  exact ⟨doc, hRelevant, hMissing, seed, hSeed, hRelation⟩

/-- Uncapped additive relation expansion strictly improves candidate recall when
it reaches and materializes at least one missing relevant document. -/
theorem materializedRelationExpansion_strictlyImprovesUncappedRecall
    {relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {relevantDocs : List Yams.Core.DocumentId}
    {baseline expansion : List ComponentCandidate}
    {seeds : List Yams.Core.DocumentId}
    (hReachable : MissingRelevantReachable relation relevantDocs baseline seeds)
    (hMaterialized : RelationExpansionMaterialized relation seeds expansion) :
    StrictlyImprovesCandidateRecall relevantDocs baseline
      (augmentCandidates baseline expansion) := by
  constructor
  · intro doc hRelevant hPresent
    exact augmentCandidates_preservesBaseline baseline expansion hPresent
  · rcases materializedRelationExpansion_exposesReachableRelevant
      hReachable hMaterialized with ⟨doc, hRelevant, hMissing, candidate, hCandidate, hDoc⟩
    refine ⟨doc, hRelevant, hMissing, candidate, ?_, hDoc⟩
    exact List.mem_append_right baseline hCandidate

/-- Budgeted candidate rescue theorem. Besides reachability and materialization,
the selector must preserve baseline relevance and retain the new relevant
candidate. These premises map directly to displacement and rescue-survival
metrics in the search experiment arm. -/
theorem materializedRelationExpansion_strictlyImprovesBudgetedRecall
    {relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {relevantDocs : List Yams.Core.DocumentId}
    {baseline expansion selected : List ComponentCandidate}
    {seeds : List Yams.Core.DocumentId}
    (hPreservesBaseline : PreservesBaselineRelevant relevantDocs baseline selected)
    (hReachable : MissingRelevantReachable relation relevantDocs baseline seeds)
    (hMaterialized : RelationExpansionMaterialized relation seeds expansion)
    (hSurvives :
      RelevantExpansionSurvivesSelection relevantDocs baseline expansion selected) :
    StrictlyImprovesCandidateRecall relevantDocs baseline selected := by
  refine ⟨hPreservesBaseline, ?_⟩
  rcases materializedRelationExpansion_exposesReachableRelevant
      hReachable hMaterialized with ⟨doc, hRelevant, hMissing, hExpanded⟩
  exact ⟨doc, hRelevant, hMissing, hSurvives doc hRelevant hMissing hExpanded⟩

/-- Literature-aligned adaptive candidate-rescue theorem. A query-admissible
baseline result seeds relation expansion; a missing relevant neighbor is
materialized; and the budgeted selector preserves baseline relevance and keeps
the rescue. Under those obligations candidate recall strictly improves. -/
theorem admissibleSeedExpansion_strictlyImprovesBudgetedRecall
    {relation : Yams.Core.DocumentId → Yams.Core.DocumentId → Prop}
    {admissible : Yams.Core.DocumentId → Prop}
    {relevantDocs : List Yams.Core.DocumentId}
    {baseline expansion selected : List ComponentCandidate}
    {seeds : List Yams.Core.DocumentId}
    (hPreservesBaseline : PreservesBaselineRelevant relevantDocs baseline selected)
    (hReachable :
      MissingRelevantReachableFromAdmissibleSeed
        relation admissible relevantDocs baseline seeds)
    (hMaterialized : RelationExpansionMaterialized relation seeds expansion)
    (hSurvives :
      RelevantExpansionSurvivesSelection relevantDocs baseline expansion selected) :
    StrictlyImprovesCandidateRecall relevantDocs baseline selected := by
  exact materializedRelationExpansion_strictlyImprovesBudgetedRecall
    hPreservesBaseline
    (admissibleSeedReachability_impliesReachability hReachable)
    hMaterialized
    hSurvives

end Yams.SearchEngine
