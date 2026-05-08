TASK: storage-compression-audit
MODE: engineering
PHASE: checkpoint
AGENT: opencode-storage-compression-audit

Scope:
- Audited `src/storage`, `include/yams/storage`, `src/compression`, and `include/yams/compression`.
- Ran local OpenGrep audit/default rules and public OpenGrep profiles (`p/default`, `p/security-audit`, `p/trailofbits`, `p/c`).

Findings fixed:
1. Storage path traversal/key validation gap.
   - `StorageEngine` accepted any 64-byte key, not necessarily hex. A key beginning `..` could influence sharded paths.
   - Added `isValidStorageKey()` requiring regular hashes and manifest base hashes to be 64 hex chars.
   - Added regression test for regular and manifest traversal-shaped keys.
2. Compressed storage false-positive header detection.
   - `CompressedStorageEngine` treated any block beginning with `KRNC` as compressed.
   - Replaced magic-only detection with full `CompressionHeader::parse()` plus exact stored-size validation.
   - Added regression test for raw data beginning with the compression magic.
3. Compression header / block arithmetic hardening.
   - Avoided `uncompressedSize * 2` overflow in header validation.
   - Avoided `offset + metadataSize` overflow in extended header parsing.
   - Switched header parse/serialize to `std::bit_cast` with a trivially-copyable static assertion.
4. Compression storage net-savings hardening.
   - If compressed payload plus header is not smaller than original, store original bytes.
   - Uses configured `compressionThreshold` together with policy `neverCompressBelow`.
5. Compression utils alignment hardening.
   - Replaced unaligned `reinterpret_cast<const uint32_t*>` magic read with `memcpy`.

Validation:
- TSAN build: `meson compile -C build/tsan -j4 catch2_compression_submodule catch2_storage_submodule` OK.
- TSAN tests: `compression_submodule` and `storage_submodule` OK.
- Public OpenGrep profiles over storage/compression: 0 findings.
- Local OpenGrep storage/compression findings reduced from 38 to 36; remaining are mostly low-signal `.value()` after explicit guards, FFI/byte IO casts, env overlays, and fixed PRAGMAs.

Artifacts:
- `.artifacts/audit/storage_compression/opengrep-local.json`
- `.artifacts/audit/storage_compression/opengrep-local-final.json`
- `.artifacts/audit/storage_compression/opengrep-public.json`
