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

## [0.13.1](https://github.com/trvon/yams/compare/v0.13.0...v0.13.1) (2026-04-18)


### Fixed

* clang warning fixes and daemon startup wal guard and fallback regression fix ([c2a9d32](https://github.com/trvon/yams/commit/c2a9d322adfb26f0cdea2c41bc9c75afca4e12fb))
* enforce resource caps ([d86eb06](https://github.com/trvon/yams/commit/d86eb06c3c9d7f24392833d62596ee45414b00eb))
* fixing ci push to repo ([8b9a443](https://github.com/trvon/yams/commit/8b9a443280b68afd65fc8636992e60d81be1dba6))
* fixing mcp code routing and paramater normalization with a light refactor ([ce56050](https://github.com/trvon/yams/commit/ce560509bfa432c3c7fbc02014bf8557219a62f6))
* small fixes for benchmarks and documentation update ([d7ab484](https://github.com/trvon/yams/commit/d7ab4844a1c4b67cbe6957932d9f0b6a68185884))

## [0.13.0](https://github.com/trvon/yams/compare/v0.12.1...v0.13.0) (2026-04-17)


### Added

* adding yams corpus tuner ([a97952e](https://github.com/trvon/yams/commit/a97952eb76c54e59e30a21f79f2f668ff81c2b83))
* additional locks patterns reduced ([bb651d8](https://github.com/trvon/yams/commit/bb651d82156e6fed87a5f6fbca464069d33cde9d))
* additional optimizations and improvements for package validation and ingestion speed ([e6e0b39](https://github.com/trvon/yams/commit/e6e0b39fd8ef462a4a8067578ed49c4e8eddc86a))
* additional test clean up and expansion for benchmarks ([b6b6ba9](https://github.com/trvon/yams/commit/b6b6ba93843d2f4d3faf8e6d0633c2c87434fb09))
* address search gaps ([46d4c2c](https://github.com/trvon/yams/commit/46d4c2c17a651f978ea97cd09973ccd460273bc8))
* auto complete improvements and topology refactor to improve graph rebuilding on ingestion ([58cba44](https://github.com/trvon/yams/commit/58cba447a4361c94111a34af96a6fe2a3929d615))
* build improvements, docs updated and optimizations for cli latency ([8777a27](https://github.com/trvon/yams/commit/8777a27f3cf4c94255f4142840e1b32123c9b7c2))
* checkpoint for updated unit test for daemon communication / protobuf errors. Expanding untested surface with optimizations for communication and ingestion. Additional work for ci apt / rpm install improvements, systemd configuration for installs and improved validation testing of packaging logic to reduce gpg errors. ([13be8fd](https://github.com/trvon/yams/commit/13be8fdfa0887d8cdceb4cf2764b62d6f6ef1905))
* cleaning up additional locks in service manager ([ff110ac](https://github.com/trvon/yams/commit/ff110ace56d52b42809ed8d06f05a97daa2c5e95))
* cleaning up test suite ([a930776](https://github.com/trvon/yams/commit/a930776655e584d9f5492352c2dd0d1e7d76fe32))
* CLI request handling refactor for unified load and error handling. Continued refactor and improvement of search engine with scoped tuning ([48a955e](https://github.com/trvon/yams/commit/48a955e793deb5112ca36330e5c90e88a7b57e8e))
* completion fixes and reduce bottleneck of ingestion on add from cli ([4c9ce1e](https://github.com/trvon/yams/commit/4c9ce1e1e1d8b435a0bc97dd490e320b1508cc59))
* completitions fixes. Starting topology routing work for semantic clusters ([0c74dcb](https://github.com/trvon/yams/commit/0c74dcb34edcee2471e67bda571e9f1ace83907d))
* continued corectness improvements, ANN / vectordb improvements ([ef3d925](https://github.com/trvon/yams/commit/ef3d92579720ddfb2af06fc25dc8883ecd95cdda))
* CorpusStats-driven tuning accuracy + per-query community detection. Fixes for clion meson detection ([473274d](https://github.com/trvon/yams/commit/473274dea0fd49cf035bda8e7928de42dbfe64a9))
* Correctness fixes for flaky tests, improvements for linux build logic based on clion change. Improvement of tuning / governor daemon logic to simplify scaling logic ([2b2552e](https://github.com/trvon/yams/commit/2b2552e34ca379963fd3f017467d7335370e4908))
* correctness improvements and bumping sqlite_vec_backend lib ([c9ad8f5](https://github.com/trvon/yams/commit/c9ad8f5dc843cdf05c8c12d16a36f167ac878fe1))
* daemon lifecycle improvements for supervisor mode, search now uses topology logic, homebrew formula launches yams in foreground mode and test updates ([036e5e7](https://github.com/trvon/yams/commit/036e5e7d9efae38a45fc6a92905fb39f10f7c6ec))
* expanding mobile abi compiled support ([8da76c9](https://github.com/trvon/yams/commit/8da76c9c47c99efc123a667d1147f45bc7503fe6))
* expansion of topology system into repair and ingestion pipelines ([3c2f61b](https://github.com/trvon/yams/commit/3c2f61b863013e6691f54241c5186479fdbb7fdd))
* extracted public and categorized methods from service manager. ([a9bc8a7](https://github.com/trvon/yams/commit/a9bc8a755e7c369f212e9836614e9a261035a4cb))
* fixes for repair batching logic, hnsw rebuild and search refinements before graph building improvements land ([12e4b19](https://github.com/trvon/yams/commit/12e4b1943b19cb27aa34a51645836274fb381979))
* fixing brittleness in CI pip usage that breaks under package updates and system delayed updates. Improving graph repair process and reducing locks in ingestion pipeline ([a1253d1](https://github.com/trvon/yams/commit/a1253d1f696e5755ec58f15c80bb8a8835b58205))
* further homebrew bug fixes and improvements ([902f75c](https://github.com/trvon/yams/commit/902f75c379e19736c9be0c544faaba221c687a49))
* further search improvements and daemon sleep fixes. Commit before service manager refactor/cleanup ([569ffae](https://github.com/trvon/yams/commit/569ffaed8e61fcd658ef55fc251520e83a3a1739))
* graph expansion logic improvements for search testing, repair improvements for embeddings ([a99efd1](https://github.com/trvon/yams/commit/a99efd13c856d2bf53cfd7f0388a4ad95081836e))
* improved online metrics for corpus ([9db8d79](https://github.com/trvon/yams/commit/9db8d79b6a15293516c1cfabf9970b69ecbbd648))
* improvement for topology rebuilds on ingestion and in repair ([3183dae](https://github.com/trvon/yams/commit/3183dae14c23a506fcf448f6690e1677b3436213))
* improving benchmark by adding to doctor command with persistence to tune search engine, improving topology epoch tracking and connecting adaptive tuner ([463a377](https://github.com/trvon/yams/commit/463a377d50be9053e4eaff76a13088bfafad6eca))
* improving packaging logic for homebrew and addressing pip install regression in tests ([3cccde2](https://github.com/trvon/yams/commit/3cccde2902949dca640758d042a7a68e8771f303))
* improving repair logic and search engine metrics for tuning logic ([825d9aa](https://github.com/trvon/yams/commit/825d9aa66600af2f04228e2e447ca5e071d0e735))
* Improving search speed for hybrid engine. Trying best to balance speed and precision ([c475f10](https://github.com/trvon/yams/commit/c475f101b07b8095f1ae9b5dc778ef0809ae5c9c))
* imrpovements for repair and graph clustering logic and search usage ([ad8d7ac](https://github.com/trvon/yams/commit/ad8d7ac059c7c7624d6617751ba74cab9306859a))
* ingestion optimizations and regression test updates ([e13ff15](https://github.com/trvon/yams/commit/e13ff15493be89cce7328b0b99b288e68d950a8d))
* installation tests and build improvements. Test fixes related to optimizations for reranker ([883eb11](https://github.com/trvon/yams/commit/883eb1127ee9eefadffa4f865c0f8139e3def232))
* instrumentation improvements for benchmarks, shell completition improvements, packaging updates for completions and docs updates ([f4fffd7](https://github.com/trvon/yams/commit/f4fffd73a03aaa42c3ea7e5d3af3b9948a0cf02e))
* introducing epochs for search rebuilding ([a4fdb70](https://github.com/trvon/yams/commit/a4fdb70e6c08218075c7fda34c2b10c2a380e9f3))
* large commit, new tune command, search tuning and improved corpus learning with persistent values ([9c23fcf](https://github.com/trvon/yams/commit/9c23fcf86943ef392c7c5564e310fc8ebebe7212))
* mobile binding improvements ([3540000](https://github.com/trvon/yams/commit/35400002c901e8070b60a1db061c07d05776ac31))
* optimizations for data returns and harden daemon status and normalize MCP/version responses ([9a1ed08](https://github.com/trvon/yams/commit/9a1ed082d4afd35ec58753510a8c30ef357f8901))
* optimizations for multiclient usage, updated benchmarks and ci correctness improvements to improve test runs collection ([53326b6](https://github.com/trvon/yams/commit/53326b624ec1b747600bf1ccd7f3607d8f56cf4c))
* profile for building mobile bindings ([09c8e0f](https://github.com/trvon/yams/commit/09c8e0f95986c1c5b230ace88d42c0fd73623d0b))
* reduce sleeps in daemon service manager, ingestion pipeline and moving towards event driven design ([8d48496](https://github.com/trvon/yams/commit/8d484963fe575d09c973d236428fd4066cea2309))
* refining readiness strings with constexpr. Final release testing for v0.13.0 ([5eab5b9](https://github.com/trvon/yams/commit/5eab5b9d4866ce9ba2fc9226a07f3892934278f4))
* removing unnecessary abstraction and adding some optimizations for embeddings generation ([814672f](https://github.com/trvon/yams/commit/814672f4ddf6ba4c140af651f4f80f3e56aed061))
* search / grep freshness and query intent router improvements. Test improvements ([55a7f5a](https://github.com/trvon/yams/commit/55a7f5afcd7a380098eac7237002f36d5cafad91))
* search / retrieval optimizations for performance and accuracy ([ce36736](https://github.com/trvon/yams/commit/ce36736e63074ea8078bb61a6bb34fa66d7f501c))
* search improvements, UBSan meson option, CI quality job, and fix clang-tidy findings ([677f67e](https://github.com/trvon/yams/commit/677f67e799474cc645ccad2003b2c0e93de24237))
* site update ([b6e7c7e](https://github.com/trvon/yams/commit/b6e7c7e90c393ed92248444b685dac808c3b7f77))
* test imrpovements, infer fixes in search changes from previous commits ([4a3dce8](https://github.com/trvon/yams/commit/4a3dce881f050ac55bb22d4ec9ee4e6be8b48ba5))
* tuning improvements for graph influence ([e1d2db3](https://github.com/trvon/yams/commit/e1d2db3f4f879efa62f9bdde09a47763971ffb23))
* vector optimizations. Most performance gains in search felt on macos ([bb0d33a](https://github.com/trvon/yams/commit/bb0d33a436a1df7ebfa6caec46a7234cf4a721e1))
* vendoring and migrating correctly to use tinyfsm ([fdac636](https://github.com/trvon/yams/commit/fdac6368029b35ef400963dcf310055b5b5a4238))


### Fixed

* additional correctness issues in repairservic, backgroundtask manager, tuning advisor and daemon ([cbdef02](https://github.com/trvon/yams/commit/cbdef02884612c930ef8c792e7b375be5ea71afb))
* additional repair logic improvements to unify repair system. Search improvements and graph building optimizations for retrieval performance improvement ([96eb95f](https://github.com/trvon/yams/commit/96eb95f53572ffd41bcd7f297d5f59d6d9ced908))
* address ci source venv and graph repair timeouts ([e8ddb31](https://github.com/trvon/yams/commit/e8ddb31145edbe3acdda4519999af548e295c6b6))
* address daemon starvation/deadlock paths and aligning ownership of run loop lifecycle ([61d92c9](https://github.com/trvon/yams/commit/61d92c9f5a8fa157cdc111cd89e7fbeb18eeafd7))
* address regressions in onnx runtime generation from previous commit. Addressing critical bugs ([fff1ca1](https://github.com/trvon/yams/commit/fff1ca19ac56eac4c9ee1834c8952be9d1b5d08d))
* address test failures and improve daemon status ux ([a3be3c6](https://github.com/trvon/yams/commit/a3be3c62b28938a058223fa6ad6f1748cf2551ec))
* addressing UX issue with daemon status readiness ([11ef101](https://github.com/trvon/yams/commit/11ef10141fce2dc233e99614be31e25c6d4b3a13))
* CI fix ([625c573](https://github.com/trvon/yams/commit/625c573773cb99aee6f569188b2d515fd772493c))
* clang-tidy / infer fixes. Fix for test.yml builds ([a56bc5d](https://github.com/trvon/yams/commit/a56bc5d126166c49968189def7c53ee094ac9140))
* cleaning up benchmark page and adding CmakeLists.txt ([f8b794d](https://github.com/trvon/yams/commit/f8b794d70ef50c1c12678d31f112796ea12701b1))
* cleaning up object references ([f69c388](https://github.com/trvon/yams/commit/f69c388e4b8645e5b7462e6cc0d7ba78c40539bf))
* concurrency and race condition fixes ([57b7e20](https://github.com/trvon/yams/commit/57b7e20657f789e4ee6e930c85f6188e38154113))
* continued correctness improvements. Added vec0 search mode and benchmark plumbing for YAMS retrieval ([5795a0f](https://github.com/trvon/yams/commit/5795a0f36006fb7123e3d9bdf8569799554d6e8d))
* fix config snapshot logic in CompressionMontor, RecoveryManager, TranscationManager and CompressionsError handler ([6ee7c57](https://github.com/trvon/yams/commit/6ee7c57515a11b3778d6a9b246fa4ff29deac01a))
* fix for test messing up config ([422d857](https://github.com/trvon/yams/commit/422d85700b33685fb16aa70cac69178c88568a87))
* fix issues with mobile bindings and improving abi alignment ([1962ff6](https://github.com/trvon/yams/commit/1962ff6a907745a8da1940c91ddc50a2175b3394))
* fixing regressions with serach engien start ([3f03eb2](https://github.com/trvon/yams/commit/3f03eb2b934dcd12d3ece531ee4becb75e6911d2))
* fixing rpm builds ([9b6461e](https://github.com/trvon/yams/commit/9b6461e273bf0522e7014336922e4a6dc398a931))
* fixing scaling logic of embedding generation that fallback to CPU ([120d98b](https://github.com/trvon/yams/commit/120d98bc77491e3f2a905c9028e0da7527be5507))
* fixing stale issue related to repair manager, mcp suggestions logic that leads to looped behavior in long sessions and ci test failures ([1c9f03c](https://github.com/trvon/yams/commit/1c9f03c486d665687929809b97ae3e9ea1b4a0b1))
* improve stats from embeddings used to trigger search rebuild: ([e2772f2](https://github.com/trvon/yams/commit/e2772f2b6a81000bfb3647b569962843e7376abd))
* improved plugin locking around onnx session state and ownership improvements ([2d0df86](https://github.com/trvon/yams/commit/2d0df863b7626735ec1d7039281b286165de864a))
* improvements for embedding generation (embedding gemma specific issue) and Dockerfile regression fix ([e09778b](https://github.com/trvon/yams/commit/e09778b7fe30a3a9596a06a77c53c9024f983cce))
* improvements for embedding repair routing to hot model. Continued search / graph build retrieval improvements ([364a114](https://github.com/trvon/yams/commit/364a11476d1b8596e72720b9c29af2f715dcb241))
* improvements for laoding handling in UX response ([3b5fb8f](https://github.com/trvon/yams/commit/3b5fb8fba8f1e859bea1dc499eb3ce12f560dfc6))
* improving repair check ([6eb1635](https://github.com/trvon/yams/commit/6eb16351456cb391001bd79ea6cccf334533926e))
* metadata optimizations ([2a0f020](https://github.com/trvon/yams/commit/2a0f02069d70a2a32bcc51106b474f5ef5ed72d4))
* re-ranker embedding generation fix, test failures drift fix, search improvements for graph query expansion and gliner improvements for building graph ([e8a6296](https://github.com/trvon/yams/commit/e8a62967974e29a6ee1f8e1cfd5675443004d160))
* readiness ux fix ([a008438](https://github.com/trvon/yams/commit/a00843852d4f8de9d454c8ea382506612c0a2412))
* release please fix, improvements for embedding generation (check if model is loading an query that). continue work on search retrieval improvement related to on going graph improvements ([3f3ba05](https://github.com/trvon/yams/commit/3f3ba0582db7acf9ec8670ce2659f113bfdb66c9))
* removing gtest and continued cleanup work of test suite ([e2c8ba0](https://github.com/trvon/yams/commit/e2c8ba043b917d383d2e1fe37ba71b13075a5a1a))
* repair fixes for embedding generations and stalls, mcp code mode improvements for smarter routing, test improvements and additional search / graph optimizations ([aee787d](https://github.com/trvon/yams/commit/aee787dbf464edc575cda52e9d83f7b3b143a3d9))
* reranker and internal benchmark fixes to address updates and test regressions ([32bac6d](https://github.com/trvon/yams/commit/32bac6d0af931661222161260b62230f56686ee2))
* suspected deadlock fixes in repair, bugs addressed in request logic. Continued triage from infer output and test failures ([e261420](https://github.com/trvon/yams/commit/e2614203510be93a5df8826fabb4c4e19f5a4cb6))
* tear down fixes for tests ([991e047](https://github.com/trvon/yams/commit/991e04783b8ed686366eadee418e4ce7793fdd2b))
* test and ci fixes and search improvements. Adding a router for tuning logic ([01c72ec](https://github.com/trvon/yams/commit/01c72ec8282085855bace3ad3e6817026acfdafc))
* test build error fixes ([bd20aee](https://github.com/trvon/yams/commit/bd20aee2a55650435f94a15077cc099e562339fa))
* test fixes related to metadata optimizations and some search tuning checkpoints before refactor experiment ([d1225ab](https://github.com/trvon/yams/commit/d1225ab465aef5d11fa184e867d2cb4491e4ec10))
* test regression and build fixes ([ed80f97](https://github.com/trvon/yams/commit/ed80f970eb78b3b4b739412eb629d3c4f3d5ff76))
* tree-diff benchmark regression accounting and stabilize subtree fast-path measurement ([e41ec38](https://github.com/trvon/yams/commit/e41ec383775421210d722db4413fe5f6577ca16d))
* updating readme and linking benchmark pages ([1526545](https://github.com/trvon/yams/commit/1526545f797e466a82f5c0000db09124091584b0))
* vector and embedding count fixes related to repair functionality. Improvements for search and tuning scope of candidates ([d25cc39](https://github.com/trvon/yams/commit/d25cc39e12b0d7bc53be7014e372ab38bcf596d7))

## [Unreleased]

### Added

* New top-level `yams tune` command for interactive relevance tuning over the user's own corpus. Subcommands: default (interactive labeling), `status`, `reset [--policy rules|contextual|all]`, and `replay` (R6 stub). See [docs/cli/tune.md](docs/cli/tune.md) and [docs/guides/interactive-tuning.md](docs/guides/interactive-tuning.md).

### Deprecated

* `yams doctor tune` now prints a deprecation notice on stderr and forwards to `yams tune`. The alias will be removed in a future release.

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
