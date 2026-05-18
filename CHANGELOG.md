# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.13.x archive: docs/changelogs/v0.13.md
- v0.12.x archive: docs/changelogs/v0.12.md
- v0.10.x archive: docs/changelogs/v0.10.md
- v0.9.x archive: docs/changelogs/v0.9.md
- v0.8.x archive: docs/changelogs/v0.8.md
- v0.7.x archive: docs/changelogs/v0.7.md
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

### Fixed

* download logic improvements and ci test fixes. Stagging download fixes seperate from search features ([2d5c662](https://github.com/trvon/yams/commit/2d5c6625eb4e794c878441bb052911baf91169e0))
Full changelog: [CHANGELOG.md](https://github.com/trvon/yams/blob/v0.14.1/CHANGELOG.md)

## [0.16.0](https://github.com/trvon/yams/compare/v0.15.0...v0.16.0) (2026-05-17)


### Added

* **search:** add Simeon bandit tuning ([#33](https://github.com/trvon/yams/issues/33)) ([6d8877a](https://github.com/trvon/yams/commit/6d8877a6d4034435a90538768f3cb3d1b22a6fb5))


### Fixed

* **config:** cross-validate backend + model at startup ([aec755e](https://github.com/trvon/yams/commit/aec755eaaa103ff578a3ff33caa86d70feeb0c96))
* **config:** skip simeon sentinels when ONNX backend is active ([cee859d](https://github.com/trvon/yams/commit/cee859df61aac19b43a14651e92317c075e77709))

## [0.15.0](https://github.com/trvon/yams/compare/v0.14.2...v0.15.0) (2026-05-15)


### Added

* add BM25 variant RRF fusion to lexical backend ([73fbe4c](https://github.com/trvon/yams/commit/73fbe4c842356e94b5930be598ccc910e5c3f974))
* add FAISS HNSW vector backend with ConfigResolver policy ([dc6aaa1](https://github.com/trvon/yams/commit/dc6aaa13cee61f9d35c28a6d79fdb20f087298d2))
* add RM3 pseudo-relevance feedback + adopt research-tuned config ([cbb8650](https://github.com/trvon/yams/commit/cbb86506d0e3f1c8cb98d759d40013d8f5c2a057))
* **doctor:** integrity checks for storage blobs and reference counts ([#30](https://github.com/trvon/yams/issues/30)) ([8d9de5f](https://github.com/trvon/yams/commit/8d9de5f91b3f86e1619dd5965edd9c9a219ae3c0))
* improved search fusion logic and tuning parameters. CI improvements, reverting to push with wrangler in CI. Experimenting with vector rebuild to address additional lost overhead in search ([8b7bfe5](https://github.com/trvon/yams/commit/8b7bfe575d50a36f272ca17ac7af85f660d6f7e6))
* integrate simeon concept mining into lexical backend ([102d5a1](https://github.com/trvon/yams/commit/102d5a1e50f2d74379c94e9c4cb3be76986dc050))
* **search:** add concept_count and concept_mining_enabled accessors ([a61d0fe](https://github.com/trvon/yams/commit/a61d0fe34efb4b3c6f404699e060d236ee48efa8))
* **search:** integrate simeon EntropyRouter strategy lane, corpus-state tuning ([8fd830e](https://github.com/trvon/yams/commit/8fd830e9a9f1f9326129524a441686060893fe5f))
* **search:** reduce graph noise, add edge-creation diagnostics ([1f9bc76](https://github.com/trvon/yams/commit/1f9bc767a010fd3ba2cb9abe8d5ceceaf67ac4b9))
* updating zyp pdf extractor to use v0.16.0 compat in plugin build ([bc100ef](https://github.com/trvon/yams/commit/bc100efcdc6216d7680a35a24d8e1741ceebe958))


### Fixed

* addressing build regressions where fiass needed openmp installed ([18b10dc](https://github.com/trvon/yams/commit/18b10dc444cd0e8da06828a642ec09cfd65132f6))
* **ci:** ci fixes ([61fc7d1](https://github.com/trvon/yams/commit/61fc7d1e70055787dfb6da155e3784e58194971c))
* **ci:** restore cache-hit guard for macOS/Windows warm step ([c3127d5](https://github.com/trvon/yams/commit/c3127d55c9d2807d583bcce4f32215206f8c4278))
* **ci:** revert Conan profile ccache wiring (breaks compiler_executables) ([07ad866](https://github.com/trvon/yams/commit/07ad8667ea460af234c2fa569067d7dd43cc918c))
* Copilot review feedback and gitignore for artifacts ([#26](https://github.com/trvon/yams/issues/26)) ([e27d5e3](https://github.com/trvon/yams/commit/e27d5e3e8496b2bd8ffc1a0fa57f3fa397433ddd))
* **daemon:** WAL flush on shutdown, corrupt-DB salvage, test hardening, ASAN fix ([#27](https://github.com/trvon/yams/issues/27)) ([867deed](https://github.com/trvon/yams/commit/867deed10190a1b37449cdd03f50f22a20efc75f))
* disable faiss on Apple Clang macOS, remove hardcoded /opt/homebrew paths ([8433d4f](https://github.com/trvon/yams/commit/8433d4f6fc17b60e4fd81554dd8a9650d34dd999))
* disable faiss on Apple Clang macOS, restore profile cache keys ([f14c7d6](https://github.com/trvon/yams/commit/f14c7d64ab9f04ffff0c6c3b80f8ec6858229b76))
* **docker:** add gfortran for openblas arm64 build ([8f5b0d0](https://github.com/trvon/yams/commit/8f5b0d0eb3e0a8a555e8a5b7fcf71dca5501da51))
* **docker:** add wget for Zig, disable faiss ([597aed4](https://github.com/trvon/yams/commit/597aed4fc9d52162de21dc5ee89726e467744c5f))
* **metadata:** deduplicate batch entries before UPSERT ([68a49e7](https://github.com/trvon/yams/commit/68a49e70dd18f57f14a8bd2e62fe3f0cc6ce32f2))
* **metadata:** retry constraint failures in executeQueryOnPool ([e971b56](https://github.com/trvon/yams/commit/e971b56aa9a97cb1e258c9e5c750ab7e7995691e))
* **recovery:** show 'salvaging' phase in daemon status, add verbose salvage logging ([0eeacbb](https://github.com/trvon/yams/commit/0eeacbb88d540fcb4b8330d37bc1f87bbfb75579))
* removed reduant corpus adapter ([ea6cd51](https://github.com/trvon/yams/commit/ea6cd51da317bd0b1eb1f47020065aad34a39ce0))
* **review:** change tryEnqueue to by-reference, prevent batch destruction ([c256458](https://github.com/trvon/yams/commit/c2564583a8e43e2a2d1c7988e61a82e4433bb32a))
* **salvage:** move salvage after pool init so status shows 'Salvaging' ([30d0894](https://github.com/trvon/yams/commit/30d08942bdac6baa267da739728ece41a6b16714))
* **salvage:** quick-check corrupt DBs, only block startup if docs needed ([86c39e3](https://github.com/trvon/yams/commit/86c39e36654866d87a18a36389d0aa76ca3b2aba))
* **search:** wire GLiNER concept extractor, fix PQ dirty race, add KG NER awareness ([6704e0b](https://github.com/trvon/yams/commit/6704e0bb76f052a268d1d8914b6768496403b41b))
* **shutdown:** move WriteCoordinator shutdown before io_context stop, abort in-flight batches ([ded04d2](https://github.com/trvon/yams/commit/ded04d272951f524cb898314e967733236f3764a))
* **test:** relax stuck-docs CHECK to &gt;= 0 (background loop may pre-process) ([70ff865](https://github.com/trvon/yams/commit/70ff86532ddb16e8d3ff7c9b0fca924e6e9ef3ea))
* **vector:** guard faiss backend init behind YAMS_HAS_FAISS ([bec8164](https://github.com/trvon/yams/commit/bec8164d4ce991fe93f11ad32acf066c6811b282))


### Security

* addressing codeql security finding and expanding opengrep rules ([2d84e44](https://github.com/trvon/yams/commit/2d84e44013bad6a2a1e7f27cdb14b0f84eb3ed75))


### Performance

* **ingest:** bypass GLiNER for zyp PDFs, preserve plugin metadata ([66c21c0](https://github.com/trvon/yams/commit/66c21c0b25a29c520ec70962b0cdc79fae67d2cc))
* **zyp:** true parallel PDF extraction + throughput benchmark ([6556415](https://github.com/trvon/yams/commit/65564158a9df0a2b6cf8580929e5cdee497b79dd))

## [0.14.0](https://github.com/trvon/yams/compare/v0.13.1...v0.14.0) (2026-05-04)

### Highlights

- Moved the search and embedding stack further toward the Simeon backend, including PHSS vector search support, topology and clustering work, and continued removal of legacy HNSW paths.
- Improved write-path coordination, repair and rebuild behavior, and daemon readiness under load.
- Added storage health checks, corruption detection, and broader instrumentation and tuning for memory-constrained systems.

### Breaking Changes

- Embeddings no longer center on ONNX Runtime by default. YAMS now prefers the lighter Simeon path, which reduces resource usage but requires AVX or NEON support.
- ONNX Runtime support was later reinstated as an alternate path, but Simeon remains the default direction in this release.

### Notable Fixes

- Fixed daemon status and readiness regressions, SocketServer loop fragility, MCP router issues, and DB optimize repair handling.
- Reduced memory pressure in repair and search paths and tightened batching for topology and rebuild work.
- Cleaned up release and packaging regressions, including build-artifact leakage and platform-specific CI issues that affected release confidence.

### Security

- Expanded Opengrep coverage and addressed storage correctness, underflow-related bugs, and repair subsystem scanning issues.
- Tightened related hook and script behavior used in repo security checks.

### Performance

- Improved idle memory reclamation, ingestion throughput, startup behavior, and compressed R2 transport behavior.

### Full Details

- See `CHANGELOG.md` in this PR for the full commit-level history.

---
This PR was generated with [Release Please](https://github.com/googleapis/release-please). See [documentation](https://github.com/googleapis/release-please#release-please).
