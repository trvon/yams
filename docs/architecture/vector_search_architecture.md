# Architecture: Vector Search

This document outlines the embedding and vector search subsystem: how embeddings are generated, stored, and used for retrieval and hybrid ranking.

## Responsibilities

- Generate dense embeddings for ingested text using a configured provider.
- Persist vectors in a local vector database, keyed to document or chunk IDs.
- Serve k-NN queries for hybrid searches and similarity operations.

## Data model and constraints

- The vector DB uses a fixed embedding dimension determined at initialization.
- The embedding generator reports its dimension; mismatches are rejected early.
- Records store: `{id, vector, metadata}` where metadata links back to source.

## Initialization and repair

- The system validates or infers the embedding dimension at startup.
- A repair utility can backfill embeddings for existing content.

## Code references

- Vector DB config and initialization: `src/repair/embedding_repair_util.cpp`
- Dimension resolver: `src/vector/dim_resolver.cpp`

See also
- API: Vector Search API (`docs/api/vector_search_api.md`)
- Admin: Vector Search Tuning (`docs/admin/vector_search_tuning.md`)

