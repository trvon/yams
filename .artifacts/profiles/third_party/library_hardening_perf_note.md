TASK: third-party-library-hardening-perf
MODE: engineering
PHASE: checkpoint
AGENT: opencode-third-party-library-hardening-perf

Actions:
- Resolved third-party OpenGrep local audit findings in `third_party/simeon` and `third_party/sqlite-vec-cpp`.
- Simeon: replaced direct string-byte reinterpret casts with byte views through `void`, and replaced integer-target unaligned memcpy loads in hot hash paths with byte-buffer + `std::bit_cast` helpers / explicit tail assembly.
- sqlite-vec-cpp: added typed byte-span helper for SQLite blobs, alignment check before `VectorView` over blob memory, checked HNSW node vector byte-size multiplication, bit_cast for HNSW float config serialization, safe entry-point blob read, typed HNSW random seed config instead of direct `YAMS_HNSW_RANDOM_SEED` read, and pre-sized batch distance output vectors.

Validation:
- Unscoped local OpenGrep rules: 0 findings for Simeon, 0 findings for sqlite-vec-cpp.
- Simeon Meson tests: 31/31 OK.
- sqlite-vec-cpp profile-build tests: 8/8 OK.

Perf notes:
- Simeon microbench remains healthy; FWHT 4096->384 around 10.9 us/doc (~91.5k docs/s), Achlioptas 4096->384 around 111.8 us/doc.
- sqlite batch pre-size optimization improved batch benchmark shape in the clean run: Batch_100x384 ~2497 ns CPU, Batch_1Kx384 ~25116 ns CPU, contiguous 1Kx384 ~23586 ns CPU.
- Distance kernels remain dominated by existing SIMD paths; no obvious low-risk kernel change made in this pass.

Artifacts:
- `.artifacts/opengrep/third_party_after/*.json`
- `.artifacts/profiles/third_party/simeon_microbench_final.log`
- `.artifacts/profiles/third_party/sqlite_batch_after_batch_presize.log`
- `.artifacts/profiles/third_party/sqlite_tests_final.log`
