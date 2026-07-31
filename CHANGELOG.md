# Changelog

## [0.19.0](https://github.com/trvon/yams/compare/v0.18.1...v0.19.0) (2026-07-31)


### ⚠ BREAKING CHANGES

* **ingest:** callers using connectAsync or legacy MessageFramer and FrameReader helpers must migrate to connect, frame_message_into, feed, and get_frame.
* **search:** removes query parser, result ranker, search filter, and SearchResults container APIs from yams_search.

### Added

* **search:** make topology-assisted retrieval the default ([#57](https://github.com/trvon/yams/issues/57)) ([a3d065d](https://github.com/trvon/yams/commit/a3d065dd8d8871a863badfc3fb4443df13cffaf4))
* **search:** trace bounded topology rescue ([9618370](https://github.com/trvon/yams/commit/9618370d1fd9a90c1625a44c7c5b0b63cbcb333d))


### Fixed

* **build:** restore MSVC and Apple libc++ portability ([25bce22](https://github.com/trvon/yams/commit/25bce22f15af44ce079a079f8b0f04ca230b1a10))
* **ci:** preserve staged hook inputs ([7a86afe](https://github.com/trvon/yams/commit/7a86afebbee37bc43d8f96760c21597576cf8f18))
* **ci:** stabilize sanitizer test paths ([75ceff2](https://github.com/trvon/yams/commit/75ceff2975f575736f582eca6e0c35459dd29354))
* **cli:** harden argument count arithmetic ([a6117de](https://github.com/trvon/yams/commit/a6117de7614161e94c24138e3ceecdd8676cbfd5))
* **daemon:** make watcher and restart reliable ([f53d063](https://github.com/trvon/yams/commit/f53d063a5042fb00623fe41311f813aeb28868ff))
* **daemon:** stop shutdown log storm ([bb8fe66](https://github.com/trvon/yams/commit/bb8fe66dece0cabc96747bf2ecb1f2c2df9bb346))
* **daemon:** verify lifecycle process identity ([dabb424](https://github.com/trvon/yams/commit/dabb424596bddb9daaa0730c0d811d3395dd33bc))
* **graph:** improve agent navigation ([11b0241](https://github.com/trvon/yams/commit/11b02411e681ea38abd4a88b049db5c5ee0440bf))
* **graph:** propagate workspace scope through daemon ([0b3317b](https://github.com/trvon/yams/commit/0b3317b757a98772c504f642e27a52ef3297bb44))
* **graph:** scope exploration to workspace ([70d0236](https://github.com/trvon/yams/commit/70d02362b60c643cf409bd43f42cf9a03f3eb943))
* **graph:** scope explore to working tree ([2345462](https://github.com/trvon/yams/commit/2345462016c02e6047478c00cb26be154d213940))
* **grep:** bound final result window ([8d447f1](https://github.com/trvon/yams/commit/8d447f1a89092dada9bdc41fe6d93999663db4b8))
* harden buffer and parser contracts ([4e566f6](https://github.com/trvon/yams/commit/4e566f6387ee16d63f59963a69c91c888f9cc980))
* **prune:** prioritize extensionless signatures ([6e84723](https://github.com/trvon/yams/commit/6e8472382272db40afa7eee02b19452fffa2dc34))
* **release:** harden runtime boundaries ([cee3adc](https://github.com/trvon/yams/commit/cee3adc1cceea4e02ddb79fa45a986ac5895f174))
* **release:** repair package repo publish ([80738eb](https://github.com/trvon/yams/commit/80738eb3e8c5c93374b01b755472e58407665738))
* **rm:** make deletion safe and observable ([91fb1a3](https://github.com/trvon/yams/commit/91fb1a32d91b9919805fac90b7fcce7f83665736))
* **search:** cancel disconnected requests ([10ff335](https://github.com/trvon/yams/commit/10ff33532c3678dea97a787d20e3605e81ce13b2))
* **search:** correct code navigation ([8ad4ab2](https://github.com/trvon/yams/commit/8ad4ab29e0efc7ab47a9e01c2a0203dd41478934))
* **services:** saturate add deadline budget ([8cc1533](https://github.com/trvon/yams/commit/8cc1533fa889efeba767c204b2f31e5c27047e3e))
* **storage:** harden manifest and graph cleanup ([b31de9c](https://github.com/trvon/yams/commit/b31de9ce99db841ad57cbbed31c70d7a4aa7bf0a))
* synchronize daemon service publication ([9855c7b](https://github.com/trvon/yams/commit/9855c7bff83d39ee671b56df9dea02b04a8425bb))
* **test:** use compat jthread shim in deadline test ([4750aa5](https://github.com/trvon/yams/commit/4750aa59c92cd95a8b62625eef0cbd791f2a1d5f))


### Performance

* **ingest:** bound store batching ([ed8492b](https://github.com/trvon/yams/commit/ed8492bb65dbbd3b514a6b6ec1febee189628bf9))
* **ingest:** harden daemon batching and shutdown ([#58](https://github.com/trvon/yams/issues/58)) ([ae15bf3](https://github.com/trvon/yams/commit/ae15bf317a0b7ab5b613bedf58a0b168564a8035))
* **search:** bound topology routing and harden runtime paths ([#59](https://github.com/trvon/yams/issues/59)) ([7df64b1](https://github.com/trvon/yams/commit/7df64b1161ec51743ea6db4e2d69c953957d68c6))

## Changelog

This file is retained for release automation. Published releases and their
generated notes are available at:

- https://github.com/trvon/yams/releases
- https://sr.ht/~trvon/yams/

Curated project updates and benchmark commentary live in
[`docs/newsletter.md`](docs/newsletter.md).
