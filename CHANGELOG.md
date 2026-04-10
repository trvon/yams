# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.7.x archive: docs/changelogs/v0.7.md
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

## [0.13.0](https://github.com/trvon/yams/compare/v0.12.1...v0.13.0) (2026-04-10)


### Added

* additional optimizations and improvements for package validation and ingestion speed ([e6e0b39](https://github.com/trvon/yams/commit/e6e0b39fd8ef462a4a8067578ed49c4e8eddc86a))
* build improvements, docs updated and optimizations for cli latency ([8777a27](https://github.com/trvon/yams/commit/8777a27f3cf4c94255f4142840e1b32123c9b7c2))
* checkpoint for updated unit test for daemon communication / protobuf errors. Expanding untested surface with optimizations for communication and ingestion. Additional work for ci apt / rpm install improvements, systemd configuration for installs and improved validation testing of packaging logic to reduce gpg errors. ([13be8fd](https://github.com/trvon/yams/commit/13be8fdfa0887d8cdceb4cf2764b62d6f6ef1905))
* CLI request handling refactor for unified load and error handling. Continued refactor and improvement of search engine with scoped tuning ([48a955e](https://github.com/trvon/yams/commit/48a955e793deb5112ca36330e5c90e88a7b57e8e))
* CorpusStats-driven tuning accuracy + per-query community detection. Fixes for clion meson detection ([473274d](https://github.com/trvon/yams/commit/473274dea0fd49cf035bda8e7928de42dbfe64a9))
* Correctness fixes for flaky tests, improvements for linux build logic based on clion change. Improvement of tuning / governor daemon logic to simplify scaling logic ([2b2552e](https://github.com/trvon/yams/commit/2b2552e34ca379963fd3f017467d7335370e4908))
* expanding mobile abi compiled support ([8da76c9](https://github.com/trvon/yams/commit/8da76c9c47c99efc123a667d1147f45bc7503fe6))
* fixes for repair batching logic, hnsw rebuild and search refinements before graph building improvements land ([12e4b19](https://github.com/trvon/yams/commit/12e4b1943b19cb27aa34a51645836274fb381979))
* graph expansion logic improvements for search testing, repair improvements for embeddings ([a99efd1](https://github.com/trvon/yams/commit/a99efd13c856d2bf53cfd7f0388a4ad95081836e))
* improving repair logic and search engine metrics for tuning logic ([825d9aa](https://github.com/trvon/yams/commit/825d9aa66600af2f04228e2e447ca5e071d0e735))
* Improving search speed for hybrid engine. Trying best to balance speed and precision ([c475f10](https://github.com/trvon/yams/commit/c475f101b07b8095f1ae9b5dc778ef0809ae5c9c))
* imrpovements for repair and graph clustering logic and search usage ([ad8d7ac](https://github.com/trvon/yams/commit/ad8d7ac059c7c7624d6617751ba74cab9306859a))
* ingestion optimizations and regression test updates ([e13ff15](https://github.com/trvon/yams/commit/e13ff15493be89cce7328b0b99b288e68d950a8d))
* installation tests and build improvements. Test fixes related to optimizations for reranker ([883eb11](https://github.com/trvon/yams/commit/883eb1127ee9eefadffa4f865c0f8139e3def232))
* mobile binding improvements ([3540000](https://github.com/trvon/yams/commit/35400002c901e8070b60a1db061c07d05776ac31))
* optimizations for data returns and harden daemon status and normalize MCP/version responses ([9a1ed08](https://github.com/trvon/yams/commit/9a1ed082d4afd35ec58753510a8c30ef357f8901))
* optimizations for multiclient usage, updated benchmarks and ci correctness improvements to improve test runs collection ([53326b6](https://github.com/trvon/yams/commit/53326b624ec1b747600bf1ccd7f3607d8f56cf4c))
* profile for building mobile bindings ([09c8e0f](https://github.com/trvon/yams/commit/09c8e0f95986c1c5b230ace88d42c0fd73623d0b))
* removing unnecessary abstraction and adding some optimizations for embeddings generation ([814672f](https://github.com/trvon/yams/commit/814672f4ddf6ba4c140af651f4f80f3e56aed061))
* search / retrieval optimizations for performance and accuracy ([ce36736](https://github.com/trvon/yams/commit/ce36736e63074ea8078bb61a6bb34fa66d7f501c))
* search improvements, UBSan meson option, CI quality job, and fix clang-tidy findings ([677f67e](https://github.com/trvon/yams/commit/677f67e799474cc645ccad2003b2c0e93de24237))
* site update ([b6e7c7e](https://github.com/trvon/yams/commit/b6e7c7e90c393ed92248444b685dac808c3b7f77))
* tuning improvements for graph influence ([e1d2db3](https://github.com/trvon/yams/commit/e1d2db3f4f879efa62f9bdde09a47763971ffb23))


### Fixed

* additional repair logic improvements to unify repair system. Search improvements and graph building optimizations for retrieval performance improvement ([96eb95f](https://github.com/trvon/yams/commit/96eb95f53572ffd41bcd7f297d5f59d6d9ced908))
* address regressions in onnx runtime generation from previous commit. Addressing critical bugs ([fff1ca1](https://github.com/trvon/yams/commit/fff1ca19ac56eac4c9ee1834c8952be9d1b5d08d))
* clang-tidy / infer fixes. Fix for test.yml builds ([a56bc5d](https://github.com/trvon/yams/commit/a56bc5d126166c49968189def7c53ee094ac9140))
* cleaning up benchmark page and adding CmakeLists.txt ([f8b794d](https://github.com/trvon/yams/commit/f8b794d70ef50c1c12678d31f112796ea12701b1))
* cleaning up object references ([f69c388](https://github.com/trvon/yams/commit/f69c388e4b8645e5b7462e6cc0d7ba78c40539bf))
* fix issues with mobile bindings and improving abi alignment ([1962ff6](https://github.com/trvon/yams/commit/1962ff6a907745a8da1940c91ddc50a2175b3394))
* fixing rpm builds ([9b6461e](https://github.com/trvon/yams/commit/9b6461e273bf0522e7014336922e4a6dc398a931))
* fixing scaling logic of embedding generation that fallback to CPU ([120d98b](https://github.com/trvon/yams/commit/120d98bc77491e3f2a905c9028e0da7527be5507))
* improvements for embedding generation (embedding gemma specific issue) and Dockerfile regression fix ([e09778b](https://github.com/trvon/yams/commit/e09778b7fe30a3a9596a06a77c53c9024f983cce))
* improvements for embedding repair routing to hot model. Continued search / graph build retrieval improvements ([364a114](https://github.com/trvon/yams/commit/364a11476d1b8596e72720b9c29af2f715dcb241))
* improvements for laoding handling in UX response ([3b5fb8f](https://github.com/trvon/yams/commit/3b5fb8fba8f1e859bea1dc499eb3ce12f560dfc6))
* re-ranker embedding generation fix, test failures drift fix, search improvements for graph query expansion and gliner improvements for building graph ([e8a6296](https://github.com/trvon/yams/commit/e8a62967974e29a6ee1f8e1cfd5675443004d160))
* release please fix, improvements for embedding generation (check if model is loading an query that). continue work on search retrieval improvement related to on going graph improvements ([3f3ba05](https://github.com/trvon/yams/commit/3f3ba0582db7acf9ec8670ce2659f113bfdb66c9))
* repair fixes for embedding generations and stalls, mcp code mode improvements for smarter routing, test improvements and additional search / graph optimizations ([aee787d](https://github.com/trvon/yams/commit/aee787dbf464edc575cda52e9d83f7b3b143a3d9))
* test and ci fixes and search improvements. Adding a router for tuning logic ([01c72ec](https://github.com/trvon/yams/commit/01c72ec8282085855bace3ad3e6817026acfdafc))
* test fixes related to metadata optimizations and some search tuning checkpoints before refactor experiment ([d1225ab](https://github.com/trvon/yams/commit/d1225ab465aef5d11fa184e867d2cb4491e4ec10))
* tree-diff benchmark regression accounting and stabilize subtree fast-path measurement ([e41ec38](https://github.com/trvon/yams/commit/e41ec383775421210d722db4413fe5f6577ca16d))
* updating readme and linking benchmark pages ([1526545](https://github.com/trvon/yams/commit/1526545f797e466a82f5c0000db09124091584b0))
* vector and embedding count fixes related to repair functionality. Improvements for search and tuning scope of candidates ([d25cc39](https://github.com/trvon/yams/commit/d25cc39e12b0d7bc53be7014e372ab38bcf596d7))

## [0.12.1](https://github.com/trvon/yams/compare/v0.11.0...v0.12.0) (2026-04-02)

### Added

* adding fuzz test for new download logic, removing r2 endpoint from tests, decoupling embedding / onnxruntime logic for work from previous commit ([2e20416](https://github.com/trvon/yams/commit/2e20416e96cc592f13d63d5cacb74629e54756c7))
* additional dev leakage cleanup for query and tick logic ([106aa0b](https://github.com/trvon/yams/commit/106aa0b1c85197ff7f61559aa228ad2f5a850062))
* ann improvements ([c00aded](https://github.com/trvon/yams/commit/c00aded0ab9c065f8e95299238ee55c9938d2e78))
* benchmark and validation testing against original system before swap ([5715ae9](https://github.com/trvon/yams/commit/5715ae95a6d68ebad8a77b93c0e6b29520515ac3))
* benchmark fixes and instrumentation ([39037c0](https://github.com/trvon/yams/commit/39037c0f3c5c80417694aa215fb1126057e6d770))
* cleaning up codebase and improving treesitter plugin ([2381c90](https://github.com/trvon/yams/commit/2381c90c861436a71dc066ac36b3260b583c0809))
* homebrew fixes for nightly formula install ([58a8cf8](https://github.com/trvon/yams/commit/58a8cf8efe633c7ab6e6a3b70c915b678217bc54))
* massive commit. Download logic updates with expanding controls (stop, start, resume etc.) from daemon control. This is used in my archive openclaw agent. This commit also includes work to harden onnxruntime abi access. this should allow loading newer abi's when builds are pinned to 1.23.0. ([86ec015](https://github.com/trvon/yams/commit/86ec015eeab4e0dda4bebc766310751a57a8046a))
* moving changelogs to archive, implementing TurboQuant then will integrate in vector engine ([147f5ca](https://github.com/trvon/yams/commit/147f5ca0522d4e0c4e41c1ddccefcf95fad58107))
* search improvements, test correctness fixes and bug fixes ([9c201af](https://github.com/trvon/yams/commit/9c201afae7175659f3397286cc293e0dfbc36c34))
* search optimizations (expanding re-ranking work to recovery items that are not strong candidates based on how they are retrieved). Test improvements and TSAN/ASAN fixes. ([d353e53](https://github.com/trvon/yams/commit/d353e53f263535d14d197ff7166218c7afd90685))
* test cleanups ([0beaf58](https://github.com/trvon/yams/commit/0beaf58b184d1e3a0203df32bf76f6ba6ac6561d))
* test coverage improvements, list hardening for wildcard usage, search and benchmark optimizations for retrievals ([9e918b7](https://github.com/trvon/yams/commit/9e918b723e1e64d7679d56d4922b495df2978cac))
* test improvements, doc updates for download expansion ([f3de749](https://github.com/trvon/yams/commit/f3de749787867a68b9b36e56acaa802ee492ed5c))
* test suite improvements. Migrating remaining tests to catch2. Improving hnsw for usage in benchmarks ([15e049d](https://github.com/trvon/yams/commit/15e049daee9c1977c4baa9358019bcca5334ffcf))
* TurboQuant imrpovements ([1bc6b7d](https://github.com/trvon/yams/commit/1bc6b7d6d146b239b6e5c4d7d9df17a275aecab1))
* TurboQuant integration for re-ranking, starting with benchmarks, then moving to integration ([1692306](https://github.com/trvon/yams/commit/169230645a2cd13016f2961a437863aff394ba88))
* TurboQuant now supports end-to-end packed-vector storage, reranking, and direct compressed ANN traversal with persisted per-coordinate calibration and validated multi-dimension performance. ([8b39304](https://github.com/trvon/yams/commit/8b393049ba8e2ab7df328ba08f39060a1ceb8f43))
* TurboQuant search and ANN compression improvements. CLi client regression fixes ([6874b22](https://github.com/trvon/yams/commit/6874b2220e7bb8cf4bea0366ae9997c86dccbd30))
* updating staging ([c182221](https://github.com/trvon/yams/commit/c182221b48a4b21f26d5e52c98842cfa5aea5ee9))

### Fixed

* bug fixes from test regressions and test infrastructure improvements ([f4b3576](https://github.com/trvon/yams/commit/f4b3576e8efeca2d25be5197c562877423083a14))
* bypass broken Gemma CoreML path and restore ANN/TurboQuant search ([a0b4915](https://github.com/trvon/yams/commit/a0b4915bcd03df235a5d306b6a0de642a1105a55))
* ci fixes, coverage improvements and test improvements and bugs fixes ([9091a9f](https://github.com/trvon/yams/commit/9091a9fa5db6a7648a9dd9d50a7b5b8c0d89489a))
* cleaning up ENV leakage from debugging ([49a106a](https://github.com/trvon/yams/commit/49a106aeef8358b0488c03ae56b2833b1751b4fc))
* cleaning up macos build detection logic ([970f253](https://github.com/trvon/yams/commit/970f25338039783fd72c9e63cdf933764737727f))
* cleanup storage override logic ([29ef276](https://github.com/trvon/yams/commit/29ef276f32a4235be0f2754c810f29ae34f65f00))
* cli regression fixes and turboquant improvements for storage retrieval logic ([2922835](https://github.com/trvon/yams/commit/2922835242521e4925dfc81ece6d2956467008ef))
* code cleanup and bug fixes related to plugin system trust ([782561b](https://github.com/trvon/yams/commit/782561b385645d5b2a6f9be7cc4a108d26c57c7c))
* continued turboquant work. added additional api's and improved testing / perf tests ([7609d63](https://github.com/trvon/yams/commit/7609d6355b05a50fd97def3ff3a1a597d18e3318))
* correct fixes in the search engine and windows build improvements ([a4f4756](https://github.com/trvon/yams/commit/a4f47568c2acb335974ec3fa883cb0b1344e72a3))
* correctness fixes for stale sqlite handles (apart of previous work), improvements in test suite and continue work for search optimizations ([9c496e6](https://github.com/trvon/yams/commit/9c496e674e5d6e5f4cdb1a15b14baf82a2d6caaf))
* create dir helper cleanup and windows build fix ([7a71bf7](https://github.com/trvon/yams/commit/7a71bf78a4c16c3457d9a99dda35d52ceb4b6eda))
* expanding utf-8 sanitization and expand search tracing and test coverage ([81cc38a](https://github.com/trvon/yams/commit/81cc38a3cfee67c360a7159d91a028617ad07d4d))
* fix cli commands and working on retrieval optimizations ([dd93cf8](https://github.com/trvon/yams/commit/dd93cf8fd2ab628e1113414dc1effea0d89f12b7))
* fixing search regression when fuzzy search triggered and testing improvements ([943393a](https://github.com/trvon/yams/commit/943393af6d3a035062c49c250e333ea4b033edbd))
* macos homebrew packaging fixes ([7a55178](https://github.com/trvon/yams/commit/7a55178e7bdd7f4e736035b77d73acddc99156ee))
* make Gemma CoreML fallback automatic ([d5967de](https://github.com/trvon/yams/commit/d5967deedf82738772b7a882a5fa5b97967197bc))
* test coverage updates and address protobuf bug in search ([554bf47](https://github.com/trvon/yams/commit/554bf47366ffd2012bcc9fb41e0cdb98937bded3))
* **tests:** update turboquant_bench docs and fix unused-variable warning ([7c3b4c0](https://github.com/trvon/yams/commit/7c3b4c0f8255592e72d9ffe89f23abb0fb21cb47))
* tsan fixes, test instrumentation improvements and improved sql handler logic ([2fd9164](https://github.com/trvon/yams/commit/2fd9164bdabcfccebb401737bef1c180de9e85a5))
* windows build fixes and test instrumentation to resolve ANN precision issues ([c32db6e](https://github.com/trvon/yams/commit/c32db6e34d6ed30184723cc4799e3e2dad012316))

### Changed

* **build:** centralize dependency discovery and trim Meson duplication ([0261b90](https://github.com/trvon/yams/commit/0261b90a7bb449dafefec0212532605e54f2517d))
* **vector:** remove dead thread-local TurboQuant plumbing from vector_index_manager ([4ac90c7](https://github.com/trvon/yams/commit/4ac90c7573fc9da403182abff410d7f4120a7b94))
