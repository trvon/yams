# Search Routing And Tuning Flow

This diagram captures the current search routing and tuning path after the router extraction and
policy resolver cleanup.

## Current Flow

```mermaid
flowchart TD
    A[Query + SearchParams] --> B[SearchTuner baseline config and params]
    B --> C[resolveQueryPolicy]
    C --> C1[QueryRouter.route]
    C1 --> C2[Intent label + optional community label]
    C2 --> C3[Zoom layer]
    C3 --> C4[Intent layer]
    C4 --> C5[Optional community blend]
    C5 --> C6[Optional semantic-only layer]
    C6 --> C7[Normalized final SearchEngineConfig]

    C7 --> D[Text / Path / KG / Vector component queries]
    D --> E[Fusion]
    E --> F[Graph rerank]
    F --> G[Cross rerank]
    G --> H[Semantic rescue / evidence rescue]
    H --> I[SearchResponse + debug stats]
    I --> J[Tuner runtime observation]
```

## Important Notes

- Routing classifies labels, not numeric tuning values.
- `resolveQueryPolicy()` is the seam where corpus prior, query route, and mode overrides are
  combined.
- `queryFullText()` now consumes the already-resolved query intent instead of reclassifying.

## Next Cleanup Targets

1. Move remaining route-to-profile helpers fully out of `search_engine.cpp`.
2. Collapse compatibility telemetry paths like `community_detection` vs `query_routing`.
3. Keep the router taxonomy separate from the semantic API taxonomy.
4. Continue measuring profile behavior on long-memory corpora before changing bundle contents.
