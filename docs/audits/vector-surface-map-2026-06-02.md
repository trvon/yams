# YAMS-facing vector library surface map — 2026-06-02

Purpose: identify the subset of `third_party/simeon` and `third_party/sqlite-vec-cpp` that the **YAMS vector path** actually depends on, so cleanup can focus on interface alignment instead of preserving every library API.

## Scope

This map is intentionally limited to the vector-path audit:

- `include/yams/vector/*`
- `src/vector/*`
- `src/embedding_simeon/*`
- vector-specific tests/benchmarks

Note: `simeon` has broader use in YAMS search (`src/search/simeon_lexical_backend.cpp`, `include/yams/search/simeon_lexical_backend.h`, `src/topology/topology_input_extractor.cpp`, `src/daemon/components/post_ingest_enrichment.cpp`). Those are real consumers, but they are outside the narrow vector-interface alignment work.

## 1. simeon usage from the YAMS vector path

### Direct headers used

- `simeon/simeon.hpp`
  - from `src/embedding_simeon/simeon_embedding_backend.cpp`
- `simeon/pq.hpp`
  - from `src/vector/sqlite_vec_backend.cpp`
  - from `src/vector/simeon_pq_persistence.h`

### Actual symbols used by YAMS vector code

#### Embedding backend glue (`src/embedding_simeon/simeon_embedding_backend.cpp`)
Used surface:
- `simeon::NGramMode`
- `simeon::ProjectionMode`
- `simeon::EncoderConfig`
- `simeon::Encoder`
- `simeon::active_simd_tier()`
- `simeon::simd_tier_name()`

Observed pattern:
- YAMS uses simeon here as a configurable in-process text encoder.
- YAMS does **not** use the broader retrieval, BM25, concept-mining, or fragment-geometry surface in this vector adapter.

#### PQ-backed vector search (`src/vector/sqlite_vec_backend.cpp`, `src/vector/simeon_pq_persistence.h`)
Used surface:
- `simeon::PQConfig`
- `simeon::ProductQuantizer`
- `simeon::PQQuery`
- `ProductQuantizer::m()`
- `ProductQuantizer::k()`
- `ProductQuantizer::dim()`
- `ProductQuantizer::is_trained()`
- `ProductQuantizer::codebooks()`
- `ProductQuantizer::import_codebooks(...)`
- `ProductQuantizer::train(...)`
- `ProductQuantizer::encode_batch(...)`
- `PQQuery::inner_product(...)`

Observed pattern:
- YAMS only needs the **PQ/ADC compression and scoring surface** here.
- This suggests a stable YAMS-facing sub-surface could be defined around:
  - encoder
  - PQ train/import/export/encode
  - PQ query scoring
- Everything else in simeon is extra from the vector audit point of view.

### Trimming implication for simeon

For vector-path cleanup, YAMS could conceptually depend on a much narrower subset:
- `encoder` module
- `pq` module
- SIMD tier reporting

That means future cleanup should avoid coupling YAMS vector code to unrelated simeon research/retrieval APIs.

## 2. sqlite-vec-cpp usage from the YAMS vector path

### Direct headers used

- `sqlite-vec-cpp/sqlite/registration.hpp`
- `sqlite-vec-cpp/distances/l2.hpp`
- `sqlite-vec-cpp/distances/cosine.hpp`
- `sqlite-vec-cpp/distances/inner_product.hpp`
- `sqlite-vec-cpp/index/hnsw_persistence.hpp` (migration compatibility only)

### Actual symbols used by YAMS vector code

#### sqlite function/module registration (`src/vector/sqlite_vec_backend.cpp`)
Used surface:
- `sqlite_vec_cpp::sqlite::register_all_functions(db_)`

Observed pattern:
- YAMS currently registers the **entire sqlite-vec-cpp SQL surface**.
- But for the vector backend, the real need is narrower:
  - `vec0` virtual table module
  - required distance/vector helper functions used by YAMS SQL paths and migration/tests

This is the clearest opportunity for surface reduction.

#### Distance kernels (`src/vector/sqlite_vec_backend.cpp`)
Used surface:
- `sqlite_vec_cpp::distances::l2_distance(...)`
- `sqlite_vec_cpp::distances::cosine_distance(...)`
- `sqlite_vec_cpp::distances::inner_product(...)`

Observed pattern:
- YAMS uses the distance kernels as plain C++ helpers, independent of the wider SQL API.

#### HNSW persistence compatibility (`src/vector/vector_schema_migration.cpp`)
Used surface:
- `sqlite_vec_cpp::index::create_hnsw_shadow_tables(...)`

Observed pattern:
- This is legacy migration residue, not an active data-plane dependency.
- It should be treated as compatibility code, and likely removed or isolated once HNSW migration support is cleaned up.

### Trimming implication for sqlite-vec-cpp

For the YAMS vector path, the practical dependency is:
- vec0 module registration
- a few distance kernels
- temporary migration compatibility shim

YAMS does **not** need to preserve the full sqlite-vec-cpp public interface in order to improve its own vector architecture.

## 3. Alignment recommendations

### Best near-term alignment target

Define and document a **YAMS-supported library subset**:

#### simeon subset for vectors
- Encoder config + encoder runtime
- PQ train/import/export/encode/query
- SIMD tier reporting

#### sqlite-vec-cpp subset for vectors
- vec0 registration
- exact distance helpers used in backend code/tests
- migration-only compatibility helper(s), ideally isolated

### Concrete cleanup directions

1. Add a YAMS-local wrapper around sqlite-vec registration
   - replace direct `register_all_functions()` dependency with a narrower wrapper
2. Isolate migration-only HNSW compatibility from the steady-state vector path
3. Keep YAMS vector code from reaching into non-vector simeon features
4. Split “owned library cleanup” from “YAMS integration contract” in future tasks

## 4. What this means for the audit

The vector audit should optimize for:
- stable YAMS-facing configuration defaults
- clean YAMS adapter contracts
- strong tests around the subset YAMS actually uses
- perf validation on YAMS-used paths

It should **not** require preserving or re-auditing every unused public API in either owned third-party library.
