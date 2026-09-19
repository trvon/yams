import Yams.Topology.RetrievalCoordinates

namespace Yams.SearchEngine

/-! The live fast path searches inside allowed sets; it does not filter an already
retrieved global list. These contracts describe that separate operator. Empirical
calibration does not discharge the scorer-completeness premise below. -/

structure RoutedRetrievalRequest where
  allowedDocs : List Yams.Core.DocumentId := []
  documentBudget : Nat := 0
  chunkBudget : Nat := 0
  deriving Repr, BEq

/-- Includes ANN, chunk aggregation, and bounded document selection. -/
abbrev AllowedSetRetriever := RoutedRetrievalRequest → List ComponentCandidate

/-- Balanced route quotas are separate retrieval requests. Overlap and short
routes can leave the union below the sum of the quotas. -/
def retrieveRoutes (retrieve : AllowedSetRetriever)
    (routes : List RoutedRetrievalRequest) : List ComponentCandidate :=
  routes.flatMap (fun request => (retrieve request).take request.documentBudget)

def RetrieverRespectsAllowedSet (retrieve : AllowedSetRetriever) : Prop :=
  ∀ request, ∀ candidate ∈ retrieve request, candidate.doc ∈ request.allowedDocs

theorem retrieveRoutes_noInvent
    {retrieve : AllowedSetRetriever} {routes : List RoutedRetrievalRequest}
    (hAllowed : RetrieverRespectsAllowedSet retrieve)
    {candidate : ComponentCandidate}
    (hCandidate : candidate ∈ retrieveRoutes retrieve routes) :
    ∃ request ∈ routes, candidate.doc ∈ request.allowedDocs := by
  simp [retrieveRoutes] at hCandidate
  rcases hCandidate with ⟨request, hRequest, hHit⟩
  exact ⟨request, hRequest, hAllowed request candidate (List.mem_of_mem_take hHit)⟩

/-- A protected document must survive actual scoring AND the route's quota.
Allowed-set membership by itself is insufficient. This is an obligation of the
retriever/aggregation boundary, not a consequence of geometric routing. -/
def RoutedScorerComplete (retrieve : AllowedSetRetriever)
    (routes : List RoutedRetrievalRequest) (protectedDocs : List Yams.Core.DocumentId) : Prop :=
  ∀ doc ∈ protectedDocs, ∃ request ∈ routes,
    ∃ candidate ∈ (retrieve request).take request.documentBudget, candidate.doc = doc

theorem routedScorerComplete_preservesProtected
    {retrieve : AllowedSetRetriever} {routes : List RoutedRetrievalRequest}
    {protectedDocs : List Yams.Core.DocumentId}
    (hComplete : RoutedScorerComplete retrieve routes protectedDocs) :
    ∀ doc ∈ protectedDocs,
      ∃ candidate ∈ retrieveRoutes retrieve routes, candidate.doc = doc := by
  intro doc hDoc
  rcases hComplete doc hDoc with ⟨request, hRequest, candidate, hCandidate, hId⟩
  refine ⟨candidate, ?_, hId⟩
  simp [retrieveRoutes]
  exact ⟨request, hRequest, hCandidate⟩

/-- Counterexample: a valid allowed-set retriever can return a document absent
from the bounded global list. Global-subset theorems must not be applied to it. -/
example :
    let global : List ComponentCandidate := [{ doc := "global", source := .vector }]
    let retrieve : AllowedSetRetriever := fun _ => [{ doc := "local", source := .vector }]
    let routes := [{ allowedDocs := ["local"], documentBudget := 1, chunkBudget := 1 }]
    (retrieveRoutes retrieve routes).map (·.doc) ≠ global.map (·.doc) := by decide

/-- Counterexample: complete cover membership does not prevent quota crowding. -/
example :
    let retrieve : AllowedSetRetriever := fun _ =>
      [{ doc := "crowding", source := .vector }, { doc := "protected", source := .vector }]
    let routes := [{ allowedDocs := ["crowding", "protected"], documentBudget := 1 }]
    ¬ ("protected" ∈ (retrieveRoutes retrieve routes).map (·.doc)) := by decide

end Yams.SearchEngine
