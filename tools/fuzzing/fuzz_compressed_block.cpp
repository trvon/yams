// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Compressed storage blocks read back from disk or object storage (possibly corrupt).
// Mode 0: parse a stored CompressionHeader and decompress its payload with the header's
// (unchecksummed) uncompressed size; must not crash or exceed -rss_limit_mb, and a success
// must not produce more than that many bytes (the block CRC is checked by the caller). Mode 1:
// every available compressor round-trips the input exactly.

#include <yams/compression/compression_header.h>
#include <yams/compression/compressor_interface.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace {

void fuzzRequire(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr || size < 1 || size > std::size_t{1024} * 1024) {
        return 0;
    }
    using namespace yams::compression;
    const auto body =
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(data + 1), size - 1);
    auto& registry = CompressionRegistry::instance();

    if ((data[0] & 1U) == 0) {
        auto header = CompressionHeader::parse(body);
        if (!header) {
            return 0;
        }
        const auto algorithm = static_cast<CompressionAlgorithm>(header.value().algorithm);
        if (algorithm == CompressionAlgorithm::None) {
            return 0; // raw payload; the block CRC, not a decoder, guards it
        }
        auto compressor = registry.createCompressor(algorithm);
        if (!compressor || body.size() < CompressionHeader::SIZE) {
            return 0;
        }
        const auto payload = body.subspan(CompressionHeader::SIZE);
        const std::size_t expected = header.value().uncompressedSize;
        auto decompressed = compressor->decompress(payload, expected);
        if (decompressed) {
            fuzzRequire(decompressed.value().size() <= expected);
        }
        return 0;
    }

    for (auto algorithm : {CompressionAlgorithm::Zstandard, CompressionAlgorithm::LZMA}) {
        auto compressor = registry.createCompressor(algorithm);
        if (!compressor || body.empty()) {
            continue;
        }
        auto compressed = compressor->compress(body);
        fuzzRequire(compressed.has_value());
        if (compressed.value().algorithm == CompressionAlgorithm::None) {
            // Incompressible input is stored raw and tagged None; callers dispatch on the tag.
            fuzzRequire(std::equal(compressed.value().data.begin(), compressed.value().data.end(),
                                   body.begin(), body.end()));
            continue;
        }
        auto restored = compressor->decompress(compressed.value().data, body.size());
        fuzzRequire(restored.has_value());
        fuzzRequire(restored.value().size() == body.size());
        fuzzRequire(std::equal(restored.value().begin(), restored.value().end(), body.begin()));
    }
    return 0;
}
