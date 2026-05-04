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

## [0.14.1](https://github.com/trvon/yams/compare/v0.14.0...v0.14.1) (2026-05-04)


### Fixed

* download logic improvements and ci test fixes. Stagging download fixes seperate from search features ([2d5c662](https://github.com/trvon/yams/commit/2d5c6625eb4e794c878441bb052911baf91169e0))

## [0.14.0](https://github.com/trvon/yams/compare/v0.13.1...v0.14.0) (2026-05-04)


### Added

* add hint for graph to search and grep files ([52fa720](https://github.com/trvon/yams/commit/52fa720dc6d4c67437074e2d7c0af6cfa743b5ca))
* adding instrumentation and config options for build and repair system. Bumping simeon and improving topology system with experimental findings ([8de19b8](https://github.com/trvon/yams/commit/8de19b84f0ff75c17dd1f295523244a8bb42cd40))
* adding simeon https://github.com/trvon/simeon ([d7f6ad4](https://github.com/trvon/yams/commit/d7f6ad4fc64a718f03d58abe04f5aac75f003c49))
* adding storage check, db corruption check and tests ([eae3c5a](https://github.com/trvon/yams/commit/eae3c5afbb45fddbd83d2dfbf3987fed44d2efcf))
* adding support for phss vector search, test fixes, daemon status regression fix, ci fixes for docker builds with new third-party library ([e88578a](https://github.com/trvon/yams/commit/e88578a9fc085f0c88ff15c0788a6551582a1a43))
* additional config imrpovements and deeper integrations of simeon in yams search ([f27077b](https://github.com/trvon/yams/commit/f27077bc67cfc75ccc0f15cbb968c778468adba6))
* address test drift, removing hnsw remants for simeon backend implementation, continued refactor of vector backend ([d8cb076](https://github.com/trvon/yams/commit/d8cb0764d8386bac3cdee769f04aa96d73cdac00))
* bench harness data_dir reuse + richer telemetry, doc updates ([cc49704](https://github.com/trvon/yams/commit/cc49704791ee188792e871c3b2d215ca9b2f2d63))
* **breaking:** breaking change. removing onnxruntime for embeddings in favor of simeon. embeddings will now use less system resources, run faster but require avx/neon support per library ([2b60bed](https://github.com/trvon/yams/commit/2b60bed67bc71d37ca173017ae6cb22181ae1689))
* bumping simeon module and linking new fragmented strategy ([0eeed2e](https://github.com/trvon/yams/commit/0eeed2ed41506606107ed7615ab39ec9c9dd2861))
* bumping simeon, improving graph building, clustering, configuration tuning of said system and updating benchmark files. Additional fixes for daemon readiness and status return underload. ([a01bd76](https://github.com/trvon/yams/commit/a01bd767ce03e38b9fa1e804b09a384a67c69522))
* ci/test regressions updates, reinstated onnxruntime (simeon will still be default), expanding topology instrumentation in search, newsletter update with throughput comparison ([f825bc3](https://github.com/trvon/yams/commit/f825bc3cf5a1cfe4bbab29d5c8c8baa1048c1b05))
* cleaning up comments and migrating search to simeon ([8914169](https://github.com/trvon/yams/commit/8914169bd53cf004db6b0dfe32eb3d00df446113))
* cleaning up request path and fixing checkpoint logic underload ([082c381](https://github.com/trvon/yams/commit/082c38168737993386edf3eae572f3d0f0504d3f))
* config cleanup and expansion ([8bdad0c](https://github.com/trvon/yams/commit/8bdad0c7b30ddd383d222ba81c381164b37c8fec))
* continued extraction from serach engine and vector improvements. This allows for better testing and experimentation ([6174b13](https://github.com/trvon/yams/commit/6174b13cafd28d74d6aea6ea7453daabc9b106b5))
* **daemon:** adaptive physical-walk TTL + statvfs fast-path, hot-tier getenv→TuningSnapshot, RepairScheduling/RepairTuning slice extractions ([f4ffb3d](https://github.com/trvon/yams/commit/f4ffb3d8c5c2dcc7cb7e3321600c05ca2f243cec))
* expanding topology instrumentation to expose quality of clusters, cli ux improvements and expanded modifiers ([1fcb7cf](https://github.com/trvon/yams/commit/1fcb7cf6df365a48e3d42a254254975cf7f58eb9))
* expanding topology system and search for better testing and simeon integration ([8c313fc](https://github.com/trvon/yams/commit/8c313fc023c90247e2fe6ae5fe72d4ec8dd5160d))
* extending instrumentation for memory and cluster profiling. COntinue UX improvements for grep and search cli commands and mcp ([8417340](https://github.com/trvon/yams/commit/8417340289b25e4cc5d98e55ada52cec9f177cde))
* extractor reranker and helpers from search engine. Establishing dual paths for simeon and onnxruntime to present heavy vs light system trade offs ([e44c28b](https://github.com/trvon/yams/commit/e44c28b2ff920db2d44a1459c381f9eaab5762f1))
* further removing hnsw references and centralizing on simeon pq implementation ([4617bf4](https://github.com/trvon/yams/commit/4617bf4d8fdf8582716e5f27a6f1a9d836ac23e6))
* graph clustering improvements for topology system. Checkpoint before experiment with better clustering systems and simeon module bump ([3ac9bd6](https://github.com/trvon/yams/commit/3ac9bd68b850acc2fc802f5544e5bdd79a55aaab))
* improving tuning and expanding instrumentation for memory allocation ([06b4992](https://github.com/trvon/yams/commit/06b4992c10fc9346e7393df16a935dc3156383fc))
* **linter:** running linter on codebase ([94c8e6a](https://github.com/trvon/yams/commit/94c8e6a9dbc8ccd049c355920524fea7046edb6c))
* memory optimizations, and topology tuning improvements ([926ef8a](https://github.com/trvon/yams/commit/926ef8a6404e2c5d2bc3cea7ede11839474de85f))
* migrating additional subsystems to write coordinator. Addressing symspell holding write pool and blocking ingestion ([9b69495](https://github.com/trvon/yams/commit/9b694953674f589f0ce16e57e35baee2e01a1380))
* profiling improvements ([9b774d6](https://github.com/trvon/yams/commit/9b774d607305ae6debdd7baff839e89aee20d20b))
* refactor vector engine to reduce monoliths in codebase ([be0feac](https://github.com/trvon/yams/commit/be0feac6058f4c41cbb21d89699b0593e2b93905))
* reinforcing proxy path for quick ux, adding topology tuning engine for corpus cluster usage in search, simeon memory management improvements ([2ced7a7](https://github.com/trvon/yams/commit/2ced7a7e5cb4cb8b87de662e84bc603f6afe6382))
* removing experiments and unifying compression in embeddings storage. ([d669c00](https://github.com/trvon/yams/commit/d669c009d0c36fc438f89dec56a0cccc50d0365a))
* removing isolated and deadcode from graph command ([3c8d60a](https://github.com/trvon/yams/commit/3c8d60addb2a3b5601221d7cdf2d9c064a4c0e28))
* removing other graph clustering algorithms for hdbscan with sgc ([6fa8b03](https://github.com/trvon/yams/commit/6fa8b037b0c56df75ff409a730ffdbcdd6177416))
* repair logic improvements, expanding config options to disable rebuilding/repair for memory constrained systems ([7de4d95](https://github.com/trvon/yams/commit/7de4d95a4381a2bdcc15ed87b471b56de68a7f78))
* test fixes, metric expansion, build improvements for gliner ([df0c78c](https://github.com/trvon/yams/commit/df0c78cf29c4dfe6bc7203657d8a9239c7d223f5))
* tuning topology system to work better with simeon via configuration ([e4a8639](https://github.com/trvon/yams/commit/e4a8639ef6d79a13326c1bfe6c193da13d9907b7))
* updating git module to https ([d7e55e0](https://github.com/trvon/yams/commit/d7e55e0dad12e2ed2ce0b2106bc783de553a99ce))
* vendoring hdbscan for clustering of documents, refactor duplicate code logic in cli and daemon paths, topology instrumentation ([6cdba45](https://github.com/trvon/yams/commit/6cdba45660aaa1e7dc6531a1a015c958d71405ea))
* write coordinator ([dae95f4](https://github.com/trvon/yams/commit/dae95f4267fd075b7a8fd72c047a7db691359f46))


### Fixed

* addressing code smell from previous commits ([debfbca](https://github.com/trvon/yams/commit/debfbcaa2654cea169cdb84c8b6212e38de0cef1))
* addressing gap where graph nodes not populated at ingestion ([25214af](https://github.com/trvon/yams/commit/25214af1576296e6e67440452424fae42fe954de))
* addressing gaps and todo in storage implementation. Improving tests ([d0e33f4](https://github.com/trvon/yams/commit/d0e33f4fe086b590fd0fbef82bb826e3cd365337))
* addressing unoptimal copies of vectors in repair path ([a3f1d52](https://github.com/trvon/yams/commit/a3f1d529c9b955d15c27a42b81f9a790e9748473))
* **asan:** Addressing asan test regressions and suite failures from recent changes ([0204072](https://github.com/trvon/yams/commit/0204072397f7791f362cfd185c85fbb2cf365b51))
* ci build fix for pinned version change. checkpoiting write improvements before move all writes to single component ([438572c](https://github.com/trvon/yams/commit/438572cc1a2f8b7f2107311c2ea892f7139949ea))
* ci fixes and experiment for search / cluster routing ([c55ce54](https://github.com/trvon/yams/commit/c55ce54b87b51ddf55ea93774036edaef09be015))
* **ci:** ci build fixes for windows, linux, macos tests lanes ([49b40d5](https://github.com/trvon/yams/commit/49b40d5471ccbee46775fb87d4d57cd9ccf98d6d))
* **ci:** ci test and bug fixes. Improvements for testing topology construction ([42ee44c](https://github.com/trvon/yams/commit/42ee44cef40115671f04c4e5f0b24244880c7e93))
* **ci:** test fixes ([7d29f96](https://github.com/trvon/yams/commit/7d29f96df5d69f2abe77dad0e435133cc4bd8fac))
* **ci:** test fixes and newsletter numbers updates ([42406b9](https://github.com/trvon/yams/commit/42406b99b090b98fb775559ca9de032be844548b))
* cleaning up startup logic and timers as corpus grows ([2ee0a75](https://github.com/trvon/yams/commit/2ee0a753ffb0e1dbeb06863289fb230c373cfa01))
* cli clean up of old comments and duplicate code. search improvements as we migrate yams to simeon. ([6c2c31f](https://github.com/trvon/yams/commit/6c2c31f2bc88757bc2cc047fcf9affb9b614278f))
* correctness fix for abalation test, fixes for daemon response (regression is present underload/repair) ([4f92b4e](https://github.com/trvon/yams/commit/4f92b4ed39b256f9726512d593e96950a409a9dd))
* **docs,ci:** addressing breaking changes in zig v0.16.x in Dockerfile, docs update and code cleanup ([8772cc8](https://github.com/trvon/yams/commit/8772cc8e54cd3c7b403ec2743f5c74b8e4977213))
* fix bad version update ([c3d3ca4](https://github.com/trvon/yams/commit/c3d3ca4c70eaf6560156f5e8bed3237ab5f4b614))
* fixing apt update, re-rolling gpg public key ([76f9170](https://github.com/trvon/yams/commit/76f917051f2efd8678e5cbef78db36072299251b))
* fixing fragiling SocketServer loop ([e5eaaf8](https://github.com/trvon/yams/commit/e5eaaf82923276723c6304ae61e09765d79c37c3))
* fixing leakage of build artifacts in release, unifying repair path, adding cache to tuning path and bumping simeon version ([127bc9e](https://github.com/trvon/yams/commit/127bc9e9e0fe1764f73371316a0aa1f04b3f5463))
* fixing neon build and memory pressure issues ([0975e3e](https://github.com/trvon/yams/commit/0975e3e7402ec705842f3bc473bd79d1c932f1c4))
* fixing onnx model logic so that memory does not hold indefinitely. Removing re-ranker logic from plugin ([801980c](https://github.com/trvon/yams/commit/801980ce11c1bd3c289e709f9b535f2f2711bd65))
* HNSW and topology rebuilds are now batched in repair. Cleaning up from previous migration work ([bdcff5b](https://github.com/trvon/yams/commit/bdcff5b63641546f9c5d27d4236d601e517a24a8))
* mcp router fix ([2f505b0](https://github.com/trvon/yams/commit/2f505b009e84e62bfb7f215da267387ac93349bf))
* **perf:** pushing newsletter, and embedding lock update ([fe8cbd1](https://github.com/trvon/yams/commit/fe8cbd1471fad62ac86f2427c71b04bf59dd0ca8))
* prune fixes for build artifacts, readiness improvements and hnsw api unification ([1b6f137](https://github.com/trvon/yams/commit/1b6f137132cf417c1ef5f27680435e74d4a1bba3))
* refining communication path in daemon ([c510d55](https://github.com/trvon/yams/commit/c510d5574a0e5096623800eb45c8c7d8f7b7e258))
* repair fix in db optimize ([ba9030b](https://github.com/trvon/yams/commit/ba9030bce8d7af96c060fa540a2476198bd49dc5))
* simeon bump and double lock addressed in search ([3615254](https://github.com/trvon/yams/commit/36152546b0019737d12c63a0b0af62538266f830))
* text fixes, connection handingly fixes and continued refactor work ([f381597](https://github.com/trvon/yams/commit/f381597c0f2b1250403cb69e99628adb908c1628))
* topology fixes improvements, alignment in daemon readiness checkpoint ([1f9387e](https://github.com/trvon/yams/commit/1f9387e5aacefd622ae38dbf8b7ad511b45d2da8))


### Security

* adding opengrep config to githook with rules. Addressing additional concerns ([3dd0e2b](https://github.com/trvon/yams/commit/3dd0e2b1f693bf438e81c9c0c492d3d1902e44b7))
* additional security fixes ([faee34e](https://github.com/trvon/yams/commit/faee34ef79f66a84562561db7bceb8216adb567c))
* additional security fixes and improvements for storage correctness ([5581fa0](https://github.com/trvon/yams/commit/5581fa08e0fe3c074b6dc4d826b17ec0dcf6963c))
* addressing before underflow ([32f458b](https://github.com/trvon/yams/commit/32f458b38e37f37fe73da8dda79395353601c549))
* dev scripts and hooks update, perf update, security fixes for quality issues ([5adb8f1](https://github.com/trvon/yams/commit/5adb8f153f0b70372ace2798a09eaca489aec8e9))
* expanding opengrep suite and addressing issues ([c25a4ad](https://github.com/trvon/yams/commit/c25a4ad94d5a94f491104e65970136196a07b9f1))
* storage implementation improvements ([886c9c0](https://github.com/trvon/yams/commit/886c9c04305e8b481097f2edba0275bf2f54dd10))
* updates to opengrep rules, repair subsystem scanning logic and windows build logic ([f3ffb92](https://github.com/trvon/yams/commit/f3ffb922648617ae48fd4ae42d86aa95125b3287))


### Performance

* additional optimizations for freeing memory when idle and processing files ([ff24452](https://github.com/trvon/yams/commit/ff244528f018098e9e3d4ec3489bef910c98d539))
* cleaning up search experiments, moving to new routing system for search for larger corpus improvements, repair system now triggers semantic / graph repair ([ae1f0c1](https://github.com/trvon/yams/commit/ae1f0c10d203034898e62e8f07cf7c16296936e2))
* improve compress behavior for r2. Compress locally and transmit and retrieve compressed data ([0ef3f8a](https://github.com/trvon/yams/commit/0ef3f8aeac753773d24f68fff20e299e298be74e))
* instrumentation and ingestion optimizations ([611d770](https://github.com/trvon/yams/commit/611d77054ce026bd8110d8dab3a57075a2afacd9))
* startup optimizations ([9ac4513](https://github.com/trvon/yams/commit/9ac45139a3729a0c98a92c1af1cce7054bd6d759))
