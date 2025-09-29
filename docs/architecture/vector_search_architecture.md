# Architecture: Vector Search

Implementation-oriented overview of the embedding and vector retrieval subsystem.

## Embedding Providers (`src/vector/embedding_generator.cpp`, `src/vector/model_loader.cpp`)

- `EmbeddingGenerator::generate` selects providers (mock vs ONNX) and streams text through tokenizer/model pipelines.
- `ModelLoader::load` resolves model assets, handles cache directories, and returns provider instances consumed by the generator.

## Dimension Management (`src/vector/dim_resolver.cpp`)

- `DimResolver::ensureDimension` cross-checks requested dimensions with persisted metadata and raises errors when mismatches occur.
- Sentinel helpers write dimension metadata beside vector stores so future startups can detect incompatible configurations.

## Persistence (`src/vector/sqlite_vec_backend.cpp`, `src/vector/vector_database.cpp`)

- `SqliteVecBackend::initialize` opens the underlying SQLite database, configures vector tables, and surfaces schema versions.
- `VectorDatabase::insert` and `VectorDatabase::search` manage transactional writes and range queries for embedding vectors.

## Index Coordination (`src/vector/vector_index_manager.cpp`)

- `VectorIndexManager::start` wires the backend, cache, and executor threads; `VectorIndexManager::search` serves k-NN requests and merges filters supplied by higher layers.
- `VectorIndexManager::scheduleMaintenance` triggers background rebuilds when embedding dimensions change or vector corruption is detected.

## Repair Flows (`src/repair/embedding_repair_util.cpp`)

- `run_embedding_repair` iterates stored documents, regenerates embeddings when providers change, and updates vector tables without full re-ingest.

Use this module map alongside `docs/delivery/038/artifacts/architecture-traceability.md` when cross-referencing documentation with source.
