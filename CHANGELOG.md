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

## [0.10.0](https://github.com/trvon/yams/compare/yams-v0.9.0...yams-v0.10.0) (2026-02-23)


### Added

* adding additional benchmark and validating onnxruntime model usage for embedding generations. Should be much faster ([3101780](https://github.com/trvon/yams/commit/3101780fe2fc4453518166653ebf614c7440c870))
* adding additional tests, making the aggressive profile scaling better ([a9bad18](https://github.com/trvon/yams/commit/a9bad18fa41748dafb020325236ab54568d8a3fb))
* adding adidtional tasks ([c7c5b63](https://github.com/trvon/yams/commit/c7c5b633176c4c7ebf49fb00aef00977e6935756))
* adding drain to constrexpr metrics ([4d6c163](https://github.com/trvon/yams/commit/4d6c163c9d64231e2fc19946b1fcec8d8b0dfe87))
* adding fuzzing updates and fixes in ipc ([c64a0b2](https://github.com/trvon/yams/commit/c64a0b239dd5391c350ac1d5782a3e5ade18dba4))
* additional improvements to reduce read and request contention ([226cea7](https://github.com/trvon/yams/commit/226cea7ca2f2c560b8b75cd7ab12a96a4b78882b))
* additional optimizations for faster responses and ASAN fixes ([8e4b0df](https://github.com/trvon/yams/commit/8e4b0df4776f17c579a0a256934be4ad58b93029))
* additional test improvements, improved logic for ingestion queue to increase throughput and client stability ([be77f1f](https://github.com/trvon/yams/commit/be77f1f4d37f080dd6842991548023cc63235eba))
* address bugs with scaling, batching in daemon processing ([1d255d5](https://github.com/trvon/yams/commit/1d255d59ce863c16e6256b361c393068384bc0da))
* benchmark expansion and normalization. Imrpoving metrics for repair and models ([f4f3c05](https://github.com/trvon/yams/commit/f4f3c055fe691fcae59c8416a226209fa8eaaedd))
* benchmark, model testing / enhancements, insertion performance updates ([c831103](https://github.com/trvon/yams/commit/c83110319d9ff6d96e122efb9180f3fd732124eb))
* bug fixes in daemon lifecycle, cleaning up tests and resolving test failures ([2360828](https://github.com/trvon/yams/commit/2360828eb7f568cd4f49b49b827668a1a66b8449))
* bump submodule ([90dc507](https://github.com/trvon/yams/commit/90dc5079518b5749004790bc7bb0b0c73bcc6da9))
* expanding graph usage in search ([8401db1](https://github.com/trvon/yams/commit/8401db189f13eabbbf71d3cd76209d384bfd906b))
* expanding mcp UI support, improvements for slots for component usage and other socket server stabilizations ([9254264](https://github.com/trvon/yams/commit/925426426aac11df0dc21d648fcef9365abbd996))
* fixing bug with multiple processes access gpu, read/write pools now added in metadata repository, and updated plugins for opencode and vscode ([94a4947](https://github.com/trvon/yams/commit/94a49472f16e8b6b412fab0811a6fb4d8780e41a))
* fixing TSAN issues in MCP. implementing cloudflare code mode https://blog.cloudflare.com/code-mode-mcp/ ([91e2123](https://github.com/trvon/yams/commit/91e212389c62aaad9965c355c349eceba3d5ecb5))
* fuzzing and config expansion. Moving to optimizing ingestion. Adding tunable embedding (chunk size etc.) ([44a94ea](https://github.com/trvon/yams/commit/44a94eabe1152959e55b0829e6e217da21e7bfa5))
* fuzzing tooling ([907cddf](https://github.com/trvon/yams/commit/907cddf6d3d7a247d462ba8bd901d01cfed5c2fd))
* **governance:** implement Netflix Gradient2 adaptive concurrency limiters ([4b2185d](https://github.com/trvon/yams/commit/4b2185dbcf1627c46ccd452328e2858d51d48ce2))
* Hardening moodel logic for embedings. Adding trace-linked feedback event persistence checkpoint ([8256917](https://github.com/trvon/yams/commit/8256917e5e7b061e0de9a2b5e7053790d5daef5c))
* implementing embedding chunking strategy and two staged ingestion that processes then distributes document data ([a44c2fb](https://github.com/trvon/yams/commit/a44c2fb4af591983a9583ce541185a8192310943))
* improvements for repair logic ([2b1e498](https://github.com/trvon/yams/commit/2b1e498494b4250590d1476ee2c9c7fccec273a5))
* improving ingestion speed with kg batch and updating metrics to use constexpr ([0c8ffea](https://github.com/trvon/yams/commit/0c8ffea3210ca3e847df906a12a809bbad71bb88))
* improving sql access under contention and expanding tuning logic with graph ([37b95bf](https://github.com/trvon/yams/commit/37b95bf6eb6b1ae2b7e9b6c3bdca7db01eb5af94))
* instrumented embedding for deeper insight into hotspots. Fixed CoreML scaling that resulted in high ram usage due to VRAM calculation. Expanded testing for embedding and draining of work ([cc28b8a](https://github.com/trvon/yams/commit/cc28b8a343a49e4d46b0a646da1432db6c5468a3))
* mcp refactor and improvements. reverting list hydration patch from previous commit ([e23d4b3](https://github.com/trvon/yams/commit/e23d4b37a9ebae96cd4bb457da43e0629aea40c5))
* mcp ui work and stabiliizations ([96f19e2](https://github.com/trvon/yams/commit/96f19e2cd2390a22acfdcf1a64bb94c4cbb57105))
* merge and site update ([1d7b15d](https://github.com/trvon/yams/commit/1d7b15d41dea50da0505ae2ef28086f47b624406))
* merge vscode settings ([c973a24](https://github.com/trvon/yams/commit/c973a24ff5cf812bf664b7870aa2da4a3c078f2b))
* mobile bindings expansion, update mcp for graph operations and download tool hardening ([c4ae922](https://github.com/trvon/yams/commit/c4ae922b7a2f05d57373b48a876287c25eb97bd4))
* move newsletter to sourcehut ([de6a5b1](https://github.com/trvon/yams/commit/de6a5b1e62fda1baec1feb4442eca986196d05df))
* optimizing query logic to reduce hangs with large corpuses ([2c8664b](https://github.com/trvon/yams/commit/2c8664b327adec3f31a86ba252e4732afbc60d32))
* prompt update for code mode mcp ([cb20b94](https://github.com/trvon/yams/commit/cb20b9436737ba09ff5d2f55ec6f23c10f3d35bc))
* recovery test improvements ([00c3a76](https://github.com/trvon/yams/commit/00c3a76771d4e97bc1890093e6222f26ebb31403))
* refactoring mcp and metadata_repository further. ([fb76e2f](https://github.com/trvon/yams/commit/fb76e2f1ed3aa4444fc8b623600b8f7c9cf6ddee))
* release please ([f9c94e8](https://github.com/trvon/yams/commit/f9c94e805c9fe060a40d72574391d40ec27c55ae))
* **search:** implement parallel post-processing (PBI-001 Phase 3) ([c1d6a99](https://github.com/trvon/yams/commit/c1d6a992fc50aaaf81550be83943cd640d9a1b6e))
* stability improvements for document ingestion ([cd7c698](https://github.com/trvon/yams/commit/cd7c698e0732970404ff45b6fdedf3a98c8eb3eb))
* staging plugin and brew update and mcp connection bug fix ([6b6bd6b](https://github.com/trvon/yams/commit/6b6bd6ba4bfea5cc71f58bf1db1f0071311879c8))
* streamlining tuning and pool logic in daemon ([dea5315](https://github.com/trvon/yams/commit/dea5315ae39bf806862f40001bce1ac27141e6d6))
* test coverage improvements. Scaling bug addresses ([b071112](https://github.com/trvon/yams/commit/b0711128364cee44c1bc062c528b7ed381b1196e))
* test hardening, added benchmark for multiclient and improve stability to support up to 16 clients before performance degrades, improved parallization and request handling in daemon ingestion pipeline ([33821d9](https://github.com/trvon/yams/commit/33821d93f1df6f035fe0016b21d543254adb0715))
* test improvements, ASAN fixes, and onnx model performance improvements ([6a90f8c](https://github.com/trvon/yams/commit/6a90f8c934fc541d42e343ece9bf50093150945e))
* tuning improvements with expanded db metrics, package hardening and updated tests ([8516a1b](https://github.com/trvon/yams/commit/8516a1b779bb5157de0b2e53c5fdc6d0bc05744e))
* updating embedding default checking to paragraph ([11ba59f](https://github.com/trvon/yams/commit/11ba59f4bb0a8b63c485ece32bd72f69fc34f63b))
* updating metrics for search and retrieval for future learning system. Additional tuning for search performance ([8f38ee7](https://github.com/trvon/yams/commit/8f38ee76be8958b2961dd1224cd2ff9a82b0a100))
* versioning bumping opencode plugin ([7ee2091](https://github.com/trvon/yams/commit/7ee209112f5ea8fa61b1c6044247c838efae5776))


### Fixed

* adding cap at onnx concurrency limit ([0c8a5ee](https://github.com/trvon/yams/commit/0c8a5ee451652d5d62e3d3bbaac0b209377c496f))
* additional merge fixes and search improvements ([55b2dc5](https://github.com/trvon/yams/commit/55b2dc517f5de2d34fd2f5bfcb0eaee96d78e648))
* additional update optimizations for plugin ([1fdb890](https://github.com/trvon/yams/commit/1fdb89082508c2ae01aafe67b1a350c8385e7a79))
* addressing test compilation issues ([c638d9a](https://github.com/trvon/yams/commit/c638d9aad4c95f44b596f23937266bf06583f1e8))
* apply tuning logic for profiles to GPU ([04b55be](https://github.com/trvon/yams/commit/04b55be44e32efc9b0e0a5d2d6f6f21736991efe))
* bug fixes for resource limiting and onnxruntime model shapes for embeddings ([b40b60f](https://github.com/trvon/yams/commit/b40b60f054a617110fd68e15603fff1bcf918270))
* ci and repo updates ([f7819d0](https://github.com/trvon/yams/commit/f7819d08d461054bac3f509bdd255d4927d14786))
* ci fixes for test compilation ([a8a9c74](https://github.com/trvon/yams/commit/a8a9c74b31d47cb64cba2b2dd6a7841b9c81e1e1))
* cleaning up postingest pipeline ([8b79d8a](https://github.com/trvon/yams/commit/8b79d8afafd01aa08521cec5803ee75b2e313e0d))
* code mode fixes and tests update for ci ([49f0f90](https://github.com/trvon/yams/commit/49f0f901b7b03c12cd71cf6825077465473fe546))
* compiler issues addressed and homebrew updates ([73b7d1e](https://github.com/trvon/yams/commit/73b7d1eabb50aa239e842bb5b33669c120d32d54))
* docker build fixes addition clamping logic for onbound memory group ([90d28d7](https://github.com/trvon/yams/commit/90d28d722e5890c35174e7db680853b0bb8029c0))
* embeddings optimizations ([d960839](https://github.com/trvon/yams/commit/d960839e5416bd7f01d38ad1d7172e6bca5a1a61))
* expose gpu usage, add command imrpovments and other bug fixes ([a3522b8](https://github.com/trvon/yams/commit/a3522b8b224861e43b2d9f038a3281f3d770e686))
* extract bug fixes and cleaning up unused session logic ([7ca1afb](https://github.com/trvon/yams/commit/7ca1afb954c5740751232c7a0fa752b8f457f276))
* fixing bind issue when generating large batches in repair ([1e0ca15](https://github.com/trvon/yams/commit/1e0ca1566c7478c32d6e3ce8e5e2e4b621b01925))
* Fixing issues that break windows release build. This should allow new msvc version Working on debug builds. ([a0fd82e](https://github.com/trvon/yams/commit/a0fd82e25c224bccf1c2d82c269003c1817037db))
* fixing macos coreml detection ([567ee32](https://github.com/trvon/yams/commit/567ee32a999d07521db6611551fb56778f7729a6))
* fixing mcp download bug, updating socket server configs for more client connections ([247f337](https://github.com/trvon/yams/commit/247f3374a28efe56504ef0bc744ba8e3579c24e7))
* fixing metrics system and removing deadcode ([fc4b5ce](https://github.com/trvon/yams/commit/fc4b5ceffb915ba91068caeff56217ca3ab90b23))
* fixing pipeline ingestion of kg and embeddings. Still finding stalls and will continue to investigate ([cc1212b](https://github.com/trvon/yams/commit/cc1212bb1f9f81b058286db1941013e630a67298))
* fixing repair regression ([e39273a](https://github.com/trvon/yams/commit/e39273a207b980c2b050392043402b102da2cf8c))
* fixing repairmanager logic, windows build issues, linting, resource scaling ([448f5e4](https://github.com/trvon/yams/commit/448f5e47b49539cca85f68b940a607e06d4a5d43))
* fixing rocm builds, removing dead code and some optimizations ([e061229](https://github.com/trvon/yams/commit/e0612298d375cf4aba92109d722fb1a956efd641))
* fixing update logic for matches based on plugin ([bb6c4b1](https://github.com/trvon/yams/commit/bb6c4b1f34f727bbe8afc6ac092bc9b79d68d330))
* fixing windows head ordering, reducing complexity in mcp server (debugging opencode, claude, droid failed to list tools issues) ([33b83ae](https://github.com/trvon/yams/commit/33b83aeb530e276fc7f828946f08402ce38167b2))
* hardening socket timeout logic ([bf65ad8](https://github.com/trvon/yams/commit/bf65ad86054b597417babc539536f82d766346ba))
* **homebrew:** update template with correct install paths ([435bb14](https://github.com/trvon/yams/commit/435bb147454195317d515b9b979a16818da3d6ea))
* **homebrew:** update template with working install paths ([a597c6c](https://github.com/trvon/yams/commit/a597c6cf9814d4967b376ff05b7214da144d3788))
* **homebrew:** use pre-built binaries and update workflow ([8c6913d](https://github.com/trvon/yams/commit/8c6913d655787606f7f91b5e3afc458211051c7d))
* house cleaning ([a177027](https://github.com/trvon/yams/commit/a177027320af979ded88001c0823e3ebeccfb794))
* improving graph repair ([1a0667d](https://github.com/trvon/yams/commit/1a0667d28fc2cdf0e57aed17d9c2a2d42d68c9a8))
* improving socket and chunking reliability and robustness. Added better regression tests and errror logging logic for state connections ([ff77c6b](https://github.com/trvon/yams/commit/ff77c6b304fd58d9fd4b134e25c9e0c0486603d2))
* license header file updates, mcp improvements, windows build fixes ([2946b23](https://github.com/trvon/yams/commit/2946b231dd48dad1f9f0f999e07ff41cf75ab220))
* lifecycle regression fix and repair improvements ([677756b](https://github.com/trvon/yams/commit/677756b25a7a3dae0dabb3fd26f027d7d50cfaec))
* mcp improvements for grep and search, improving helper usage ([29de453](https://github.com/trvon/yams/commit/29de453020e98d695b0cecb806d9221818b6796a))
* mcp update ([747adbc](https://github.com/trvon/yams/commit/747adbc62c3633331147447a4398f72bcac5915d))
* moved SocketServer read/write into dedicated IOCoordinator and fixed proxy socket paths resolution logic. Fixed TSAN race in SymSpell init ([4ecdbe7](https://github.com/trvon/yams/commit/4ecdbe7d733da6a762350f458d5e23f62518b4ee))
* moving submodule to https ([e197573](https://github.com/trvon/yams/commit/e1975730eb14517d0d66206c3eb81274d42640c4))
* nightly fixes for homebrew, daemon fixes and install logic improvements ([d6d9268](https://github.com/trvon/yams/commit/d6d926801bee1fd0428e8780deed5a334b618241))
* onnx pool model scaling improvements, test fixes and UX improvements ([0614229](https://github.com/trvon/yams/commit/0614229387a02dbcc870cb53ad66302361880745))
* **onnx:** ensure GPU provider defines are passed to plugin ([5b06ca7](https://github.com/trvon/yams/commit/5b06ca7a2c5357fcc06a53f60906759e7bfe9d58))
* performance tuning improvements, windows build fixes and other improvements ([46aa12c](https://github.com/trvon/yams/commit/46aa12c0fd22e27d450aa7339eafadd0d50c75a9))
* performance updates, v0.8.2 pre-staging ([f6251f6](https://github.com/trvon/yams/commit/f6251f698254735463c1c0b482ac7855de5bb621))
* pipeline scaling fixes, post ingestion benchmark updates ([2184993](https://github.com/trvon/yams/commit/218499386b485871a146b74acbb9073f41d944ae))
* refining ingestion logic ([52303af](https://github.com/trvon/yams/commit/52303afbe1369b205dc9fe6c242e3fa9917f5348))
* reintroducing list hydration for all docs, updating repair logic ([cc814df](https://github.com/trvon/yams/commit/cc814df065f9319b29691743747c3347a7382973))
* **release:** initialize yams-repo submodule for R2 uploads ([2fc5067](https://github.com/trvon/yams/commit/2fc5067a4d89dc3133beb9310a9fef0f8ebbacce))
* removing claude/skills ([1c800e0](https://github.com/trvon/yams/commit/1c800e086b6e898b12a64b9043b0917fe39c00ff))
* removing duplicate shim logic ([0703a28](https://github.com/trvon/yams/commit/0703a281883e3d0d16c7e27c4fe92b4f712e909a))
* removing mobile bindings from builds ([8835d67](https://github.com/trvon/yams/commit/8835d6704eab68bd86d710af2248682c91ce9653))
* repair command improvements and optimizations ([3fd243f](https://github.com/trvon/yams/commit/3fd243f68773577490db84de4d0070c8827c3cea))
* repair improvements and socketserver hardening ([3c09bec](https://github.com/trvon/yams/commit/3c09becde555e261fc8cf8418d18a92fed14575a))
* **repair:** fix SIGSEGV crash, embedding failures, and runaway repair loops ([ab06614](https://github.com/trvon/yams/commit/ab0661450542b3a46d959a4ff75ca818bebc579e))
* repairmanager, post ingest and pipeline fixes ([c4a7992](https://github.com/trvon/yams/commit/c4a79921227da83d47eaed482471617aac2822be))
* resolving TSAN issues and expanding graph building to use gliner data ([fc284f3](https://github.com/trvon/yams/commit/fc284f3b37bb3cba557256e256dc53e3323fd3c7))
* reverted bad change. moved embedding generation to repair coordinator. Still tuning resource usage and governors ([8a06575](https://github.com/trvon/yams/commit/8a06575a0c340ca512d238d5fbc7fdcb8499b937))
* ROCM/MIGraphX onnxruntime test validate improvements ([7764a0c](https://github.com/trvon/yams/commit/7764a0c3c9495ba64ec7c455b39d98ba986064d5))
* SocketServer slots for proxy and cli requests, fixed tests and increase doctor request handling ([430057f](https://github.com/trvon/yams/commit/430057f4bfadffcbc26a1e281178ff4129c9ff97))
* staging ROCM improvements ([fd32c51](https://github.com/trvon/yams/commit/fd32c519c51d218d78f8351bd709122842d0eb59))
* stop lifecycle improvements ([36ac123](https://github.com/trvon/yams/commit/36ac1235ea69c042efee294ab7d1288fdf13069c))
* test fixes annd windows build downgrade to c++20 ([79ea83e](https://github.com/trvon/yams/commit/79ea83eac8452dba02f6941669307af0dcf5c5c0))
* test improvements ([de5abde](https://github.com/trvon/yams/commit/de5abde3374f8b0ad05382e2857b106650f4aa02))
* test updates and benchmark updates for grep ([29b5172](https://github.com/trvon/yams/commit/29b5172e07ca84e9012dbd25824a5b54c4b99855))
* testing improvements and bug fixes in daemon logic for lifecycle management ([014e12c](https://github.com/trvon/yams/commit/014e12c9750fa7ff8457ca4ee3f2609afb8f5209))
* tuning improvements and better tests. Now working address memory issue of huge queue when ingesting allot of files ([df1348f](https://github.com/trvon/yams/commit/df1348f3c7efdcc42384605e6b47323ce26b0aa0))
* tuning manage logic improvements, optimizations for ingestion and throughput optimizations ([d0fcd2a](https://github.com/trvon/yams/commit/d0fcd2a1c299fc7fbfddf2dbb363cd6809a964ff))
* updating conanfile for onnxruntime to respect system version, updated tests for windows compat, update vectors to enforce wal check point so files does not explode in size ([d86ac66](https://github.com/trvon/yams/commit/d86ac665cde71e24b11bd43e0d0b4d14377cdc31))
* updating prompts, hydration logic and update mcp prompts ([3bc6b54](https://github.com/trvon/yams/commit/3bc6b54c8906e1730d87ea7f762cde50a9dfb187))
* windows build fixes ([2e966c4](https://github.com/trvon/yams/commit/2e966c4f1bdf77645b08c469f1e0545ff11e4c3e))
* windows build fixes ([0049dbe](https://github.com/trvon/yams/commit/0049dbe900107d2b7ed1914c2488733d08279bc5))
* windows test fixes ([901337b](https://github.com/trvon/yams/commit/901337b135e66e513f62033534080eefd3c8e8ed))


### Documentation

* update changelog; ignore local artifacts ([27881ec](https://github.com/trvon/yams/commit/27881ecfbd2b15620530746b8a864e1d24a51c5e))
* update README with Homebrew installation and simplified init ([f7d559b](https://github.com/trvon/yams/commit/f7d559b2a9df9fc1ef162f017b1429b33bee5e35))
* updating changelog ([af6fdbd](https://github.com/trvon/yams/commit/af6fdbd5c35e2264e1c177abd51c7c057b9ddf90))


### Maintenance

* update submodules ([d229d9f](https://github.com/trvon/yams/commit/d229d9f20b3249e4092335c60ae744b2f4b91fef))

## [v0.8.3] - Unreleased

### Fixed
- **MCP download retrievability contract**: `download` no longer returns a non-retrievable hash when post-index ingest fails. On ingest failure, response now sets `indexed=false` and clears `hash` to prevent follow-up `get/cat` calls from hammering daemon CAS with repeated "File not found" lookups.
- **Stuck-doc recovery enqueue**: Fixed repair stuck-document recovery enqueueing `PostIngestTask`s to an unused InternalEventBus channel. Recovery now enqueues to `post_ingest` (consumed by PostInestQueue), so re-extraction actually runs.
- **Doctor FTS5 reindex SQLite TOOBIG**: Added best-effort truncation retry for oversized extracted text and report `truncated=<n>` in output.

- **Embedding plugin output-shape compatibility**: ONNX provider output handling now accepts provider-padded batch dimensions (`output_B >= requested_B`) and uses the requested prefix rows, preventing false shape-mismatch failures under MIGraphX/accelerated providers.
- **Embedding dim hint precedence**: ONNX plugin no longer lets stale config hints override graph-inferred embedding dimensions when model metadata is available, reducing startup/runtime mismatches.
- **GPU escape hatch for stability triage**: Added ONNX env controls to force CPU execution (`YAMS_ONNX_FORCE_CPU=1` / `YAMS_ONNX_DISABLE_GPU=1`) for benchmark/repro stability when GPU EPs are unstable.
- **Cross-process MIGraphX execution guard**: ONNX MIGraphX now uses a process-level device lock (default: enabled) so multiple daemon processes do not concurrently execute on the same GPU and trigger ROCm/HSA instability. Configure with `YAMS_MIGRAPHX_PROCESS_LOCK` (`0` to disable) and optional `YAMS_MIGRAPHX_LOCK_PATH`.
- **Gradient limiter clamp safety**: Prevented invalid limiter bounds (`minLimit > maxLimit`) in post-ingest stage limiter setup when a stage cap resolves to unset/zero, fixing debug-build aborts from `std::clamp` assertions during title stage completion.
- **Snapshot propagation consistency across add paths**: `yams add` now propagates one effective snapshot ID per invocation across file, stdin, and directory ingest paths (daemon + local fallback), and preserves snapshot label/id when daemon directory ingest is translated to indexing requests.
- **Non-directory snapshot persistence parity**: File/stdin document stores now upsert first-class `tree_snapshots` records (with label/collection metadata) so snapshot timelines no longer depend only on per-document tags.

- **ONNX dynamic tensor padding**: Pads input tensors to the aligned actual token length instead of the model's `max_seq_len` (512). Short texts now skip hundreds of zero-padded positions, yielding **~70-85x throughput improvement** for short inputs (93 texts/sec vs 1.6 texts/sec on CPU with `nomic-embed-text-v1.5`). Enabled by default; disable with `YAMS_ONNX_DYNAMIC_PADDING=0`. Sequence lengths are 8-byte aligned to balance SIMD efficiency with padding reduction.
- **ROCm / MIGraphX compiled-model caching**: Enable save/load of compiled MIGraphX artifacts to avoid paying multi-minute `compile_program` costs when sessions are recreated (e.g., after scale-down/eviction). Defaults to caching hashed `*.mxr` artifacts under the model directory. Configure with `YAMS_MIGRAPHX_COMPILED_PATH` (directory; if a `.mxr` path is provided, its parent directory is used), `YAMS_MIGRAPHX_SAVE_COMPILED`, and `YAMS_MIGRAPHX_LOAD_COMPILED`.
- **ONNX batch warmup and auto-tuning**: `warmupModel()` runs a 4-text batch embedding immediately after model load, pre-warming the ONNX Runtime session and measuring throughput. When `YAMS_EMBED_DOC_CAP` is unset, an auto-tuning sweep (batch sizes 4→8→16→32→64) selects the batch size with peak texts/sec as the default cap via `TuneAdvisor::setEmbedDocCap()`. Disable auto-tuning with `YAMS_EMBED_AUTOTUNE=0`.
- **ONNX session reuse**: Pool-managed sessions now show **25x latency reduction** on warm cycles (259ms cold → 10ms warm), confirming session creation cost is amortized after the first inference.
- **Hardware-adaptive pool sizing**: `TuneAdvisor::onnxSessionsPerModel()` dynamically sizes the session pool based on available CPU cores and GPU state, preventing over-subscription on constrained hardware.

### Diagnostics
- **ONNX ROCm diagnostic output**: GPU diagnostic now reports cold vs warm embedding timing to distinguish first-run compilation from steady-state inference.

### Changed
- **Balanced/base daemon scaling defaults**: Updated no-env defaults for multi-client stability and fairness: `YAMS_IO_THREADS` default `2 -> 6`, `YAMS_CONN_SLOTS_MIN` default `64 -> 256`, `YAMS_SERVER_MAX_INFLIGHT` default `2048 -> 256`, `YAMS_SERVER_QUEUE_BYTES_CAP` default `256MiB -> 128MiB`, `YAMS_IPC_TIMEOUT_MS` default `5000 -> 15000`, `YAMS_MAX_IDLE_TIMEOUTS` default `3 -> 12`, and server writer budget fallback now defaults to `8MiB` per turn.
- **MCP graph tool consolidation**: Unified KG ingest under `graph` via `action="ingest"` and removed separate `kg_ingest` tool exposure to keep the MCP tool surface smaller and cleaner.
- **Benchmark reporting terminology**: Normalized benchmark docs to use neutral **retrieval result** wording (instead of subjective "significant" labels), and explicitly tagged the current baseline as the SciFact benchmark result.

- **Post-ingest embedding fan-out (phased rollout)**:
  - **Phase 1**: Added centralized embedding selection policy (strategy + mode + caps/boosts) via `ConfigResolver` with config/env precedence.
  - **Phase 2**: Moved embed preparation upstream so post-ingest can queue prepared embed payloads; embedding service consumes prepared-doc fast-path.
  - **Phase 3**: Added extraction utility that returns both extracted text and optional content bytes, enabling downstream stage reuse and reducing duplicate content-store reads.
  - **Phase 4**: Added selection strategy toggle (`ranked` vs `intro_headings`) and queue observability counters for prepared-doc/chunk vs hash-only embed dispatch.

- **Embedding selection defaults codified**: default strategy `ranked`, mode `budgeted`, `max_chunks_per_doc=8`, `max_chars_per_doc=24000`, `heading_boost=1.25`, `intro_boost=0.75`.
- **Embedding chunking defaults codified**: default chunk strategy `paragraph`; default config favors sentence boundary flexibility in embedding pipeline (`preserve_sentences=false`, `use_token_count=false` unless overridden).

#### ONNX Embedding Benchmarks (CPU-only, macOS M3 Max, `nomic-embed-text-v1.5` 768-dim)

| Benchmark | Metric | Result |
|-----------|--------|--------|
| Batch size sweep (64 texts) | Peak throughput | 55.9 texts/sec @ batch=16 |
| Dynamic padding ON vs OFF (short texts) | Speedup | ~70-85x (111 vs 1.5 texts/sec) |
| Session reuse (5 cycles) | Cold vs warm | 259ms → 10ms (25.9x) |
| Concurrent inference (4 threads) | Throughput | 83.7 texts/sec |

## [v0.8.2] - February 2, 2026

### Fixed
- MCP stdio: Improved OpenCode compatibility during handshake/tool discovery (initialize capabilities schema, strict JSON-RPC batch responses, more robust NDJSON parsing).
- MCP tools: Fixed `mcp.echo` tool `inputSchema` shape so tool schemas validate correctly.
- **FTS5 orphan detection**: Fixed bug where orphan scan used synthetic hashes (e.g., `orphan_id_12345`) that never matched actual documents. Orphans were detected but never removed because the removal query used non-existent hashes. Now passes document rowids directly via `Fts5Job.ids` and calls `removeFromIndex(docId)` which deletes by FTS5 rowid.

### Added
- **MCP grep tag filtering**: `grep` tool now accepts `tags` and `match_all_tags` parameters, aligning MCP grep with CLI `yams grep --tags` capabilities.
- **Blackboard search tools**: Three new OpenCode blackboard tools for discovering content:
  - `bb_search_tasks` — semantic search for tasks (mirrors `bb_search_findings`).
  - `bb_search` — unified cross-entity semantic search returning both findings and tasks.
  - `bb_grep` — regex/pattern search across all blackboard content with optional entity filtering.
- MCP stdio debug/compat toggles: `YAMS_MCP_HANDSHAKE_TRACE` (trace handshake events) and `YAMS_MCP_MINIMAL_TOOLS` (expose only `mcp.echo` for client compatibility debugging).

### Performance
- **Stdin routed through daemon**: `yams add -` (stdin) now sends content directly to the daemon via inline content support instead of falling back to local service initialization (~40s startup penalty eliminated). Blackboard plugin also drops unnecessary `--sync` flag for further latency reduction.
- **Adaptive sync polling**: `--sync` extraction polling now uses exponential backoff (5ms → 100ms) instead of fixed 100ms intervals. Small documents that extract via fast-track are detected on the first poll (~5ms) instead of waiting a full 100ms cycle.
- **Unified async add pipeline with parallel batching**: `yams add` with multiple files now processes up to 4 files concurrently via `addBatch()` instead of sequentially. Single shared `DaemonClient` is reused across all add operations (CLI and MCP), eliminating per-file client construction overhead.
- **`addViaDaemonAsync` coroutine**: New async entry point for all add operations. Replaces promise/future-per-attempt pattern with direct `co_await`, reducing overhead. MCP `handleStoreDocument`, `handleAddDirectory`, and download post-index all route through this single path.
- **Batch FTS5 orphan removal**: New `removeFromIndexByHashBatch()` wraps all per-hash SELECT+DELETE operations in a single transaction with cached prepared statements. Replaces N individual autocommit transactions with 1 transaction for N hashes. Eliminates prolonged DB lock contention during orphan scans (~29k orphans previously caused ~58k individual transactions), which blocked CLI requests (`yams stats`, `yams list`) and caused timeouts/segfaults.
- **`yams update` skip CLI-side name resolution for daemon path**: `yams update --name` no longer resolves names to hashes CLI-side before sending to the daemon. Eliminates ~40s `ensureStorageInitialized()` penalty and 1-4 redundant daemon round-trips. Name resolution now only occurs in the local fallback path.


## [v0.8.1] - January 31, 2026

### Added
- `yams list --metadata-values` for showing unique metadata values with counts (useful for PBI discovery).
- **Post-ingest file/directory tracking**: New metrics for tracking files and directories added/processed through the ingestion pipeline (`filesAdded`, `directoriesAdded`, `filesProcessed`, `directoriesProcessed`).
- **OpenCode Blackboard Plugin** (`external/opencode-blackboard/`): Multi-agent blackboard architecture plugin for OpenCode using YAMS as shared memory. Enables agent-to-agent communication through structured findings, task coordination, and context grouping. Requires YAMS v0.8.1+.
- **Per-stage queue depth exposure**: Real-time queue depth metrics for KG, symbol, entity, and title extraction stages accessible via daemon status.
- **Progress bars in CLI**: Visual progress bars for queue utilization, worker pool, memory pressure, and pipeline stages in `yams daemon status` and `yams status` commands.
- **Unified status UI**: `yams status` daemon-connected display now uses consistent section headers, row rendering, and status indicators matching `yams daemon status`.
- Unique PBI selection guidance in AGENTS workflow (metadata search + list values).
- **Data-dir single-instance enforcement**: Prevents multiple daemons from sharing the same data directory via flock-based `.yams-lock` file. Newer daemon requests shutdown of existing daemon and takes over, enabling seamless upgrades/restarts.

### Performance
- **Reduced status tick interval**: Governor metrics now update every 50ms (was 250ms) for more responsive CLI status output. Configurable via `YAMS_STATUS_TICK_MS`.
- **Batch snapshot info API**: New `batchGetSnapshotInfo()` method eliminates N+1 query pattern in `yams list --snapshots`. Reduces 3N queries to 1 query for N unknown snapshots.
- **Enumeration query caching**: `getSnapshots()`, `getSnapshotLabels()`, `getCollections()`, and `getAllTags()` now use a 60-second TTL cache with signal-based invalidation. Repeated calls return cached results, reducing database scans.
- **CPU-aware throttling**: ResourceGovernor now monitors CPU usage alongside memory pressure. Admission control rejects new work when CPU exceeds threshold (default 70%, configurable via `YAMS_CPU_HIGH_PCT`). Prevents CPU saturation during large batch adds.

### Fixed
- Post-ingest tuning reconciles per-stage concurrency targets to the overall budget.
- Post-ingest stage throttling now respects pause states and stage availability when computing TuneAdvisor budgets.
- Post-ingest pollers back off when a stage is paused or has a zero concurrency cap to avoid runaway CPU.
- Added a post-ingest stage snapshot log (active/paused/limits) at startup for easier tuning verification.
- Grep integration tests create the ingest directory before daemon startup to avoid missing queue paths.
- Post-ingest jobs reuse content bytes for KG/symbol/entity stages to avoid repeated content loads.
- Post-ingest KG stage no longer triggers duplicate symbol extraction when the symbol pipeline is active.
- External entity extraction reuses a single base64 payload per document across batches.
- CLI snippet formatting is now shared between search and grep for consistent output.
- `yams list` now uses the shared snippet formatter for previews.
- `yams grep` honors `--ext` filters, accepts `--cwd` with an optional path, and treats `**/*` patterns as matching direct children.
- `yams list --metadata-values` now aggregates counts in the database and respects list filters, avoiding large client-side scans.
- Added metadata aggregation indexes to speed up key/value count queries.

### Documentation
- Updated YAMS skill guide with unique PBI discovery and tagged search examples.

## [v0.8.0] - January 24, 2026

### Breaking
- **Vector database migration required**: sqlite-vec-cpp HNSW rewrite invalidates existing vector indices. After upgrading, run:
  ```bash
  yams doctor repair --embeddings   # Regenerate all embeddings
  ```
  Without this, search will fall back to FTS5-only (no semantic search).
- sqlite-vec-cpp submodule: HNSW API changes and third-party library removal (soft deletion, multi-threading, fp16 quantization, incremental persistence, pre-filtering).

### Added
- MCP `graph` tool for knowledge graph queries (parity with CLI `yams graph`).
- Graph: snapshot-scoped version nodes, `contains` edges for file→symbol, `--dead-code-report`.
- Graph prune policy (`daemon.graph_prune`) to keep latest snapshot versions.
- Download CLI: progress streaming (human/json) via DownloadService callbacks.
- Symbol-aware search ranking: definitions rank higher than usages (`YAMS_SYMBOL_WEIGHT`).
- Zig language support: functions, structs, enums, unions, fields, imports, calls.
- ColBERT MaxSim reranking when the preferred model is a ColBERT variant.
- Added support for the [mxbai-edge-colbert-v0-17m](https://huggingface.co/mixedbread-ai/mxbai-edge-colbert-v0-17m) model (embedding + MaxSim reranking, max-pooled and L2-normalized embeddings).
- Vector DB auto-rebuild on embedding dimension mismatch (`daemon.auto_rebuild_on_dim_mismatch`).
- Init now prompts for a tuning profile (efficient/balanced/aggressive) and writes `tuning.profile`.
- Search config supports a dedicated reranker model (`search.reranker_model`) with CLI helpers (`yams config search reranker`).
- **WEIGHTED_MAX fusion strategy**: Takes maximum weighted score per document instead of sum.
  Prevents "hub" documents from dominating via multi-component consensus boost. Used by
  SCIENTIFIC tuning profile for benchmark corpora.

### Performance

#### IPC & Daemon
- IPC latency reduced from ~8-28ms to ~2-5ms (connection pooling, async timers, cached config).
- Daemon startup throttling: PathTreeRepair via RepairCoordinator, Fts5Job startup delay (2s), reduced batch sizes (1000→100).

#### Ingestion & Storage
- Post-ingest throughput: dedicated worker pool, adaptive backoff, batched directory ingests.
- In-memory chunking for `storeBytes()` - avoids temp file I/O for large documents.

#### Database & Metadata
- **KGWriteQueue**: Batched, serialized writes to KnowledgeGraphStore via async writer coroutine.
  Eliminates "database is locked" errors during high-throughput ingestion by queueing KG operations
  (nodes, edges, aliases, doc entities) and committing in batches. Both symbol extraction and NL
  entity extraction now use deferred batching with nodeKey→nodeId resolution at commit time.
- Prepared statement caching for SQLite queries - reduces SQL compilation overhead on repeated operations. Cached methods: `setMetadata`, `setMetadataBatch`, `getMetadata`, `getAllMetadata`, `getContent`, `getDocument`, `getDocumentByHash`, `updateDocument`, `deleteDocument`, `insertContent`.
- `setMetadataBatch()` API for bulk metadata updates - 4x faster than individual calls.

#### Search & Retrieval
- Batch vector/KG lookups, flat_map for cache-friendly access, branch hints, memory pre-allocation.
- Concept boost post-processing now caps scan count and uses SIMD-accelerated matching with CPU
  feature auto-detect (fallback to scalar), reducing latency for large result sets.

#### Throughput Benchmarks (Debug, macOS M3 Max)

| Benchmark | Oct 2025 | Jan 2026 | Change |
|-----------|----------|----------|--------|
| Ingestion_SmallDocument | 2,771 ops/s | 2,821 ops/s | ~same |
| Ingestion_MediumDocument | 56 ops/s | 57 ops/s | ~same |
| Ingestion_E2E (100 docs) | - | 9.2 docs/s | new (KGWriteQueue) |
| Metadata_SingleUpdate | 10,537 ops/s | 17,794 ops/s | **+69%** |
| Metadata_BulkUpdate(500) | 7,823 ops/s | 50,473 ops/s | **+6.5x** |
| IPC_StreamingFramer | - | 3,732 ops/s | new |
| IPC_UnaryFramer | - | 10,088 ops/s | new |

### Experimental
- **libSQL backend**: Default database backend with concurrent write support via MVCC.
  Enables up to 4x write throughput during heavy indexing. Configure with meson option
  `database-backend` (choices: `libsql` [default], `sqlite`).

  **Installation**: If Rust toolchain is available, libsql builds automatically from source
  via the meson subproject. Otherwise falls back to SQLite.
  ```bash
  # Ensure Rust is installed (for automatic build)
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  # Or disable libsql to use standard SQLite
  meson configure -Ddatabase-backend=sqlite
  ```
  See [libSQL](https://github.com/tursodatabase/libsql) for details.

### Documentation
- **Embedding model recommendations**: Added model comparison table to README. 384-dim models
  (e.g., `all-MiniLM-L6-v2`) recommended for best speed/quality tradeoff.

### Changed
- **Reranking**: Score-based reranking is now the default. Uses geometric mean of text and
  vector scores to boost documents with multi-component consensus. No external model needed.
  Cross-encoder model reranking is opt-in via `enableModelReranking` config option.
- Tuning profile multipliers updated: efficient 0.5x, balanced 1.0x, aggressive 1.5x.

### Fixed
- **FTS5 natural language queries**: OR fallback now correctly triggers when AND query returns
  zero results. Previously, long queries like scientific abstracts would fail because the AND
  query returned nothing and the OR fallback condition was never met.
- **ONNX multi-threading on Linux/macOS**: Removed forced single-threaded execution that was
  only needed for Windows. Non-Windows platforms now use `intra_op_threads=4` by default,
  improving inference speed for 768-dim and larger models by 2-4x.
- Hybrid search fusion: fallback to non-empty `filePath` when vector results have empty paths
  (hash→path lookup failures no longer cause result mismatches).
- TSAN race in `daemon_search()`: pass `DaemonSearchOptions` by value to avoid stack reference
  escaping to coroutine thread.
- TSAN race in `handle_streaming_request()`: check `connection_closing_` before `socket.is_open()`
  to avoid race with `handle_connection` closing the socket.
- Compression stats now persist across daemon restarts (`Storage Logical Bytes` vs
  `CAS Unique Raw Bytes` now show correct values).
- CLI rejects ambiguous subcommands (e.g., `yams search graph` → use `--query`).
- `--paths-only` search now returns results correctly.
- `yams watch` waits for daemon ready; always ignores `.git` contents.
- Expanded prune patterns for build artifacts and language caches.
- Fixed ONNX model loading deadlock on Windows (single-flight pattern, recursive mutex).
- Streaming: 30s chunk timeout, backpressure stops producer on queue overflow.
- `yams add` returns immediately; hash computed async during ingestion.
- Replaced experimental Boost.Asio APIs with stable `async_initiate` (fixes TSAN races).
- File history now records snapshot metadata for single-file adds, not just directory snapshots.
