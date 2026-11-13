#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace yams::chunking {

/**
 * Content-defined cut-point detector shared by RabinChunker and StreamingChunker, so the two
 * cannot drift apart.
 *
 * Uses the gear rolling hash from FastCDC (Xia et al., USENIX ATC 2016):
 *
 *     h = (h << 1) + kGearTable[byte]
 *
 * Each byte is shifted out after 64 steps, so bit k of h depends only on the last k + 1 bytes.
 * A cut is declared after a byte when the top `bits` bits of h are all zero, so the decision
 * depends only on the last 64 bytes and every byte value contributes (unlike the previous
 * Rabin variant, whose masked low bits could only be satisfied by byte 0xC5 and therefore never
 * cut ASCII text).
 *
 * Sizes are measured from the start of the current chunk:
 *  - below `minSize` no cut is tested; only the 64 bytes before `minSize` are hashed, so the
 *    first tested hash already covers a full window and is independent of where the chunk began;
 *  - in [minSize, targetSize) a stricter mask (bits + 2) is used, in [targetSize, maxSize) a
 *    looser one (bits - 2), with bits = round(log2(targetSize - minSize)). This is FastCDC
 *    normalized chunking (level 2): it concentrates sizes around the target and keeps the
 *    expected average close to targetSize instead of drifting toward maxSize;
 *  - a chunk that reaches `maxSize` is cut unconditionally.
 *
 * Changing the table, the mask derivation or the warm-up changes every boundary and therefore
 * every block hash for content larger than one chunk; treat it as a format change.
 */
class GearCutter {
public:
    /// Rolling state of the chunk currently being cut. Reset after every cut.
    struct State {
        std::uint64_t hash = 0;
        std::size_t length = 0; ///< bytes of the current chunk consumed so far
    };

    struct Scan {
        std::size_t consumed = 0; ///< bytes of the input that belong to the current chunk
        bool cut = false;         ///< true when the chunk ends after the last consumed byte
    };

    GearCutter(std::size_t minSize, std::size_t targetSize, std::size_t maxSize) noexcept;

    /// Feeds `data` into the chunk tracked by `state`, stopping right after the first cut point.
    /// On a cut, `state` is reset for the next chunk. Input may be split at arbitrary points
    /// across calls; the cut points do not depend on the split.
    Scan scan(State& state, std::span<const std::byte> data) const noexcept;

    /// Length of the first chunk of `data` (all of `data` if no cut point occurs in it).
    std::size_t nextCut(std::span<const std::byte> data) const noexcept;

    std::size_t minSize() const noexcept { return min_; }
    std::size_t targetSize() const noexcept { return normal_; }
    std::size_t maxSize() const noexcept { return max_; }
    /// Number of hash bits tested in [targetSize, maxSize) and [minSize, targetSize).
    unsigned largeMaskBits() const noexcept { return largeBits_; }
    unsigned smallMaskBits() const noexcept { return smallBits_; }

private:
    std::size_t min_;
    std::size_t normal_;
    std::size_t max_;
    std::size_t warmStart_;
    unsigned smallBits_;
    unsigned largeBits_;
    std::uint64_t maskSmall_;
    std::uint64_t maskLarge_;
};

} // namespace yams::chunking
