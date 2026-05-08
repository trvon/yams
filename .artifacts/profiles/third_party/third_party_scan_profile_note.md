TASK: third-party-scan-profile
MODE: engineering
PHASE: checkpoint
AGENT: opencode-third-party-scan-profile

Scanned third_party/simeon and third_party/sqlite-vec-cpp with OpenGrep public profiles and unscoped local YAMS audit rules, then profiled their available tests/benchmarks.

Findings summary:
- Public OpenGrep registry profiles (`p/default`, `p/security-audit`, `p/trailofbits`, `p/c`) reported 0 findings in both libraries.
- Local YAMS rules are normally first-party path scoped, so default/audit scans skipped third_party. Temporary unscoped copies under `.artifacts/opengrep/third_party/rules-unscoped/` were used for third-party review.
- Simeon unscoped local: 7 audit-pattern findings, all in hash byte-load paths (`hash_crc32c.cpp`, `hash_tabulation.cpp`, `hash_xxh64.cpp`, `hasher.cpp`): 4 memcpy-to-integer, 3 reinterpret string bytes. These look intentional/low-risk but worth keeping in mind if rewriting hash code.
- sqlite-vec-cpp unscoped local: 16 audit-pattern findings: HNSW persistence bit/byte codecs, SQLite blob reinterpretation, one `YAMS_HNSW_RANDOM_SEED` env read, and 4 `.value()` calls in utility wrappers. Public rules found 0.

Profile summary:
- Simeon tests OK: ~5.55s wall. Microbench OK: ~5.40s. Accuracy bench OK: ~2.13s.
- sqlite-vec-cpp fresh profile build under `.artifacts/build/sqlite-vec-cpp-profile`: tests 8/8 OK (~25.6s).
- sqlite benchmarks: distance ~20.24s, batch distance ~9.08s, quantized search ~28.76s, vec0 ANN ~0.70s. HNSW benchmark timed out at 120s.

Artifacts:
- `.artifacts/opengrep/third_party/*.json` and `*.time`
- `.artifacts/profiles/third_party/summary.json`
- `.artifacts/profiles/third_party/sqlite_current_bench_summary.json`
- benchmark logs in `.artifacts/profiles/third_party/`
