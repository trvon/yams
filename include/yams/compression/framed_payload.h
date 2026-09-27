#pragma once

#include <cstddef>
#include <span>
#include <vector>

#include <yams/core/types.h>

namespace yams::compression {

/// Frame data for transfer: a CompressionHeader followed by the Zstandard-compressed body.
/// This is what the daemon sends to clients that set acceptCompressed.
[[nodiscard]] Result<std::vector<std::byte>> encodeFramedPayload(std::span<const std::byte> data);

/// Recover the original bytes from a framed payload. Checks the header, the body size and CRC,
/// and the decompressed size and CRC. A payload that fails any check is an error; callers must
/// never fall back to treating the framed bytes as content.
[[nodiscard]] Result<std::vector<std::byte>> decodeFramedPayload(std::span<const std::byte> framed);

} // namespace yams::compression
