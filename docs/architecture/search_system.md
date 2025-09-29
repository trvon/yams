# Architecture: Search System

This document mirrors the structure of the search implementation. References point to the exact source that performs each responsibility.

## Ingestion and Normalization (`src/search/chunk_coverage.cpp`, `src/vector/document_chunker.cpp`)

- `build_chunk_plan` computes Rabin-based chunk boundaries and associates metadata needed for deduplication.
- `DocumentChunker::chunkDocument` walks normalized text, emits chunk IDs, and records offsets consumed later by ranking.

## Metadata and Storage Setup (`src/search/search_engine_builder.cpp`)

- `SearchEngineBuilder::build` wires together metadata stores, FTS5 virtual tables, and vector backends based on configuration flags.
- The builder inspects the configured hybrid weights and toggles vector/KG engines accordingly.

## Query Parsing and Keyword Retrieval (`src/search/query_parser.cpp`, `src/search/query_tokenizer.cpp`, `src/search/bk_tree.cpp`)

- `QueryParser::parse` resolves lexical operators, field filters, and phrase constraints.
- `TokenizeQuery` prepares normalized tokens for both keyword and fuzzy matching.
- `BkTree::search` provides typo-tolerant lookups that feed into the keyword scorer.

## Hybrid Ranking (`src/search/hybrid_search_engine.cpp`, `src/search/result_ranker.cpp`, `src/search/kg_scorer_simple.cpp`)

- `HybridSearchEngine::search` orchestrates keyword retrieval, optional embedding generation, and KG scoring before fusing results.
- `ResultRanker::rank` merges candidate lists, applies tunable weightings, and emits per-document explanations when available.
- `KgScorerSimple::score` reads graph adapters registered by the plugin loader to apply graph-aware boosts.

## Vector Integration (`src/vector/vector_index_manager.cpp`, `src/vector/vector_database.cpp`, `src/repair/embedding_repair_util.cpp`)

- `VectorIndexManager::search` executes k-NN lookups used by the hybrid engine.
- `VectorDatabase::insert`/`search` persist embeddings and handle transactional access to SQLite Vec tables.
- `run_embedding_repair` backfills vectors for existing documents when configuration toggles embeddings after initial ingest.

All sections correspond to entries in the traceability artifact at `docs/delivery/038/artifacts/architecture-traceability.md`.
