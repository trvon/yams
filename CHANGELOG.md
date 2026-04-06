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

