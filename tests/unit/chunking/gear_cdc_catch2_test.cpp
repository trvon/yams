/**
 * @file gear_cdc_catch2_test.cpp
 * @brief Content-defined chunking behavior of the gear-hash cutter (GearCutter) through
 *        RabinChunker and StreamingChunker.
 *
 * The previous Rabin variant only produced content-defined boundaries on bytes 0xC5, so ASCII
 * text, logs and source code were cut at maxChunkSize and a one-byte insert near the start
 * changed every chunk. These tests pin the behavior that matters for deduplication on text:
 * boundary-shift resilience, size distribution, determinism across buffer splits, agreement of
 * the two chunkers, and a golden boundary vector so future boundary changes are deliberate.
 *
 * All corpora are generated in-process from a fixed-seed splitmix64 (no std distributions,
 * whose output differs between standard libraries).
 */

#include <catch2/catch_test_macros.hpp>
#include <yams/chunking/chunker.h>
#include <yams/chunking/gear_cutter.h>
#include <yams/chunking/streaming_chunker.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <istream>
#include <map>
#include <span>
#include <streambuf>
#include <string>
#include <string_view>
#include <vector>

using namespace yams::chunking;

namespace {

struct SplitMix64 {
    std::uint64_t state;
    std::uint64_t next() {
        state += 0x9E3779B97F4A7C15ULL;
        std::uint64_t z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }
    std::size_t below(std::size_t n) { return static_cast<std::size_t>(next() % n); }
};

constexpr std::array<std::string_view, 48> kWords = {
    "the",      "content",  "store",     "chunk",  "boundary",  "search",   "index",   "daemon",
    "vector",   "document", "query",     "result", "metadata",  "graph",    "path",    "version",
    "snapshot", "repair",   "embedding", "token",  "budget",    "latency",  "quality", "topology",
    "engine",   "fusion",   "rerank",    "shadow", "of",        "and",      "to",      "in",
    "is",       "for",      "with",      "a",      "that",      "on",       "by",      "from",
    "every",    "when",     "local",     "corpus", "retrieval", "manifest", "block",   "hash"};

std::string makeProse(std::size_t size, std::uint64_t seed) {
    SplitMix64 rng{seed};
    std::string out;
    out.reserve(size + 64);
    while (out.size() < size) {
        if (rng.below(12) == 0) {
            out += "## Section ";
            out += std::to_string(rng.below(1000));
            out += "\n\n";
        }
        const std::size_t sentences = 2 + rng.below(5);
        for (std::size_t s = 0; s < sentences; ++s) {
            const std::size_t words = 5 + rng.below(14);
            for (std::size_t w = 0; w < words; ++w) {
                std::string word(kWords[rng.below(kWords.size())]);
                if (w == 0) {
                    word[0] = static_cast<char>(word[0] - 'a' + 'A');
                }
                out += word;
                out += (w + 1 == words) ? ". " : (rng.below(9) == 0 ? ", " : " ");
            }
        }
        out += "\n\n";
    }
    out.resize(size);
    return out;
}

std::string makeIdentifier(SplitMix64& rng) {
    static constexpr std::array<std::string_view, 16> kParts = {
        "chunk", "store", "path", "hash", "index", "read", "write", "query",
        "cache", "node",  "edge", "doc",  "size",  "pos",  "next",  "state"};
    std::string id(kParts[rng.below(kParts.size())]);
    std::string tail(kParts[rng.below(kParts.size())]);
    tail[0] = static_cast<char>(tail[0] - 'a' + 'A');
    return id + tail;
}

// Appends `tmpl` with each '@' replaced by a fresh identifier and each '#' by a number, drawn
// strictly left to right (function-argument and operator+ evaluation order is unspecified, so
// the generators never draw two values within one expression).
void appendFilled(std::string& out, SplitMix64& rng, std::string_view tmpl) {
    for (const char c : tmpl) {
        if (c == '@') {
            out += makeIdentifier(rng);
        } else if (c == '#') {
            out += std::to_string(rng.below(4096));
        } else {
            out += c;
        }
    }
}

std::string makeSource(std::size_t size, std::uint64_t seed) {
    static constexpr std::array<std::string_view, 4> kStatements = {
        "    auto @ = @(@, #);\n",
        "    if (@ > @) {\n        return Error{ErrorCode::InvalidArgument, \"@\"};\n    }\n",
        "    for (std::size_t i = 0; i < @.size(); ++i) {\n        @ += data[i];\n    }\n",
        "    @_ = std::move(@);\n"};
    SplitMix64 rng{seed};
    std::string out = "#include <cstdint>\n#include <vector>\n\nnamespace yams::sample {\n\n";
    while (out.size() < size) {
        appendFilled(out, rng, "// Computes @ for the current @.\n");
        appendFilled(out, rng,
                     "Result<std::size_t> @(std::span<const std::byte> data, std::size_t @) {\n");
        const std::size_t lines = 3 + rng.below(10);
        for (std::size_t l = 0; l < lines; ++l) {
            appendFilled(out, rng, kStatements[rng.below(kStatements.size())]);
        }
        appendFilled(out, rng, "    return @;\n}\n\n");
    }
    out.resize(size);
    return out;
}

std::string makeLog(std::size_t size, std::uint64_t seed) {
    static constexpr std::array<std::string_view, 4> kLevels = {"INFO", "DEBUG", "WARN", "INFO"};
    SplitMix64 rng{seed};
    std::string out;
    char line[256];
    while (out.size() < size) {
        const auto minute = static_cast<unsigned>(rng.below(60));
        const auto second = static_cast<unsigned>(rng.below(60));
        const auto millis = static_cast<unsigned>(rng.below(1000));
        const std::string_view level = kLevels[rng.below(kLevels.size())];
        const auto pid = static_cast<unsigned>(rng.below(9000));
        const auto requestId = static_cast<unsigned>(rng.next() & 0xFFFFFFFFU);
        const auto latency = static_cast<unsigned>(rng.below(500));
        std::snprintf(line, sizeof(line),
                      "2026-11-08T12:%02u:%02u.%03uZ %s daemon[%u] request id=%u served "
                      "path=/api/v1/search latency_ms=%u\n",
                      minute, second, millis, level.data(), pid, requestId, latency);
        out += line;
    }
    out.resize(size);
    return out;
}

std::string makeRandom(std::size_t size, std::uint64_t seed) {
    SplitMix64 rng{seed};
    std::string out(size, '\0');
    for (auto& c : out) {
        c = static_cast<char>(rng.next() & 0xFF);
    }
    return out;
}

std::span<const std::byte> bytesOf(const std::string& s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

ChunkingConfig smallConfig() {
    ChunkingConfig cfg;
    cfg.minChunkSize = 2 * 1024;
    cfg.targetChunkSize = 8 * 1024;
    cfg.maxChunkSize = 64 * 1024;
    return cfg;
}

std::vector<Chunk> chunkLazy(const ChunkingConfig& cfg, const std::string& data) {
    RabinChunker chunker(cfg);
    return chunker.chunkDataLazy(bytesOf(data));
}

// Fraction of `after`'s bytes that live in chunks already produced for `before`.
double retainedFraction(const std::vector<Chunk>& before, const std::vector<Chunk>& after,
                        std::size_t afterSize) {
    std::map<std::string, std::size_t> available;
    for (const auto& c : before) {
        ++available[c.hash];
    }
    std::size_t kept = 0;
    for (const auto& c : after) {
        auto it = available.find(c.hash);
        if (it != available.end() && it->second > 0) {
            --it->second;
            kept += c.size;
        }
    }
    return static_cast<double>(kept) / static_cast<double>(afterSize);
}

void requireTiles(const std::vector<Chunk>& chunks, std::size_t total) {
    std::size_t offset = 0;
    for (const auto& c : chunks) {
        REQUIRE(c.offset == offset);
        offset += c.size;
    }
    REQUIRE(offset == total);
}

class SegmentingStringBuf : public std::streambuf {
public:
    SegmentingStringBuf(const std::string& data, std::size_t segment)
        : data_(data), segment_(segment) {}

protected:
    std::streamsize xsgetn(char* s, std::streamsize count) override {
        if (pos_ >= data_.size()) {
            return 0;
        }
        std::size_t n = std::min(segment_, static_cast<std::size_t>(count));
        n = std::min(n, data_.size() - pos_);
        std::memcpy(s, data_.data() + pos_, n);
        pos_ += n;
        return static_cast<std::streamsize>(n);
    }
    int underflow() override { return traits_type::eof(); }

private:
    const std::string& data_;
    const std::size_t segment_;
    std::size_t pos_ = 0;
};

std::vector<ChunkRef> streamChunks(const ChunkingConfig& cfg, const std::string& data,
                                   std::size_t segment) {
    StreamingChunker chunker(cfg);
    SegmentingStringBuf sb(data, segment);
    std::istream is(&sb);
    std::vector<ChunkRef> refs;
    auto r = chunker.processStream(is, data.size(),
                                   [&](const ChunkRef& ref, std::span<const std::byte> bytes) {
                                       REQUIRE(ref.size == bytes.size());
                                       refs.push_back(ref);
                                   });
    REQUIRE(r.has_value());
    return refs;
}

} // namespace

TEST_CASE("GearCutter - one-byte edits near the start keep almost every chunk",
          "[chunking][gear][cdc]") {
    const auto cfg = smallConfig();
    struct Corpus {
        const char* name;
        std::string data;
    };
    const Corpus corpora[] = {
        {"prose", makeProse(1024 * 1024, 11)},
        {"source", makeSource(1024 * 1024, 12)},
    };

    for (const auto& corpus : corpora) {
        INFO("corpus=" << corpus.name);
        const auto& data = corpus.data;
        const auto base = chunkLazy(cfg, data);
        // A content-defined chunker must cut text well before maxChunkSize.
        REQUIRE(base.size() > 50);

        const std::string inserted = data.substr(0, 100) + "X" + data.substr(100);
        const std::string deleted = data.substr(0, 100) + data.substr(101);

        const auto afterInsert = chunkLazy(cfg, inserted);
        const auto afterDelete = chunkLazy(cfg, deleted);
        requireTiles(afterInsert, inserted.size());
        requireTiles(afterDelete, deleted.size());

        const double insertKept = retainedFraction(base, afterInsert, inserted.size());
        const double deleteKept = retainedFraction(base, afterDelete, deleted.size());
        CAPTURE(insertKept, deleteKept, base.size());
        CHECK(insertKept >= 0.90);
        CHECK(deleteKept >= 0.90);
    }
}

TEST_CASE("GearCutter - chunk sizes follow the configured target", "[chunking][gear][cdc]") {
    const auto cfg = smallConfig();
    struct Corpus {
        const char* name;
        std::string data;
    };
    const Corpus corpora[] = {
        {"random", makeRandom(1024 * 1024, 21)},
        {"prose", makeProse(1024 * 1024, 22)},
        {"log", makeLog(1024 * 1024, 23)},
    };

    for (const auto& corpus : corpora) {
        INFO("corpus=" << corpus.name);
        const auto chunks = chunkLazy(cfg, corpus.data);
        requireTiles(chunks, corpus.data.size());
        REQUIRE(chunks.size() > 1);

        std::size_t atMax = 0;
        for (std::size_t i = 0; i + 1 < chunks.size(); ++i) {
            REQUIRE(chunks[i].size >= cfg.minChunkSize);
            REQUIRE(chunks[i].size <= cfg.maxChunkSize);
            if (chunks[i].size == cfg.maxChunkSize) {
                ++atMax;
            }
        }
        REQUIRE(chunks.back().size <= cfg.maxChunkSize);

        const double average =
            static_cast<double>(corpus.data.size()) / static_cast<double>(chunks.size());
        CAPTURE(average, atMax, chunks.size());
        CHECK(average >= 0.5 * static_cast<double>(cfg.targetChunkSize));
        CHECK(average <= 2.0 * static_cast<double>(cfg.targetChunkSize));
        CHECK(atMax * 50 <= chunks.size()); // at most 2% forced cuts
    }
}

TEST_CASE("GearCutter - default config cuts text before maxChunkSize", "[chunking][gear][cdc]") {
    // Production defaults (4 KiB / 64 KiB / 256 KiB). The old Rabin hash found no boundaries in
    // text and cut it at its maximum.
    const ChunkingConfig defaults{};
    const std::string prose = makeProse(2 * 1024 * 1024, 24);
    const auto chunks = chunkLazy(defaults, prose);
    requireTiles(chunks, prose.size());
    CAPTURE(chunks.size());
    CHECK(chunks.size() >= 4);
    for (std::size_t i = 0; i + 1 < chunks.size(); ++i) {
        CHECK(chunks[i].size >= defaults.minChunkSize);
        CHECK(chunks[i].size < defaults.maxChunkSize);
    }
}

TEST_CASE("GearCutter - boundaries are independent of how input is split",
          "[chunking][gear][cdc][streaming]") {
    const auto cfg = smallConfig();
    const std::string prose = makeProse(256 * 1024 + 333, 31);

    const auto reference = chunkLazy(cfg, prose);
    REQUIRE(reference.size() > 10);

    SECTION("repeated runs are identical") {
        const auto again = chunkLazy(cfg, prose);
        REQUIRE(again == reference);
    }

    SECTION("streaming reads of any size produce the same chunks") {
        for (const std::size_t segment : {std::size_t{3}, std::size_t{7}, std::size_t{4097},
                                          std::size_t{65536 + 13}, prose.size()}) {
            INFO("segment=" << segment);
            const auto refs = streamChunks(cfg, prose, segment);
            REQUIRE(refs.size() == reference.size());
            for (std::size_t i = 0; i < refs.size(); ++i) {
                REQUIRE(refs[i].offset == reference[i].offset);
                REQUIRE(refs[i].size == reference[i].size);
                REQUIRE(refs[i].hash == reference[i].hash);
            }
        }
    }

    SECTION("GearCutter::scan resumes across arbitrary splits") {
        const GearCutter cutter(cfg.minChunkSize, cfg.targetChunkSize, cfg.maxChunkSize);
        const auto bytes = bytesOf(prose);
        std::vector<std::size_t> cuts;
        GearCutter::State state;
        std::size_t consumed = 0;
        std::size_t step = 1;
        while (consumed < bytes.size()) {
            const std::size_t take = std::min(step, bytes.size() - consumed);
            auto piece = bytes.subspan(consumed, take);
            while (!piece.empty()) {
                const auto scan = cutter.scan(state, piece);
                consumed += scan.consumed;
                if (scan.cut) {
                    cuts.push_back(consumed);
                }
                piece = piece.subspan(scan.consumed);
            }
            step = step * 3 + 1; // 1, 4, 13, 40, ... mixes tiny and large pieces
            if (step > 100000) {
                step = 1;
            }
        }
        std::vector<std::size_t> expected;
        for (std::size_t i = 0; i + 1 < reference.size(); ++i) {
            expected.push_back(reference[i].offset + reference[i].size);
        }
        // The final chunk ends at end-of-input; it is a cut only if a boundary falls there.
        if (!cuts.empty() && cuts.back() == bytes.size()) {
            cuts.pop_back();
        }
        REQUIRE(cuts == expected);
    }
}

TEST_CASE("GearCutter - RabinChunker and StreamingChunker agree on text",
          "[chunking][gear][cdc][streaming]") {
    const auto cfg = smallConfig();
    const std::string source = makeSource(1024 * 1024, 41);

    RabinChunker rabin(cfg);
    StreamingChunker streaming(cfg);
    const auto a = rabin.chunkData(bytesOf(source));
    const auto b = streaming.chunkData(bytesOf(source));

    REQUIRE(a.size() > 50);
    REQUIRE(a.size() == b.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        REQUIRE(a[i].offset == b[i].offset);
        REQUIRE(a[i].size == b[i].size);
        REQUIRE(a[i].hash == b[i].hash);
    }
}

TEST_CASE("GearCutter - degenerate configs", "[chunking][gear]") {
    SECTION("target at or below min cuts every minChunkSize bytes") {
        const GearCutter cutter(1000, 500, 4000);
        const std::string prose = makeProse(10'500, 51);
        RabinChunker chunker(
            ChunkingConfig{.minChunkSize = 1000, .targetChunkSize = 500, .maxChunkSize = 4000});
        const auto chunks = chunker.chunkDataLazy(bytesOf(prose));
        REQUIRE(chunks.size() == 11);
        for (std::size_t i = 0; i + 1 < chunks.size(); ++i) {
            REQUIRE(chunks[i].size == 1000);
        }
        REQUIRE(cutter.smallMaskBits() == 0);
        REQUIRE(cutter.largeMaskBits() == 0);
    }

    SECTION("mask width follows target - min") {
        // 240 KiB spacing rounds to 2^18: 20 bits before the target, 16 after (FastCDC level 2).
        const GearCutter wide(16 * 1024, 256 * 1024, 1024 * 1024);
        REQUIRE(wide.smallMaskBits() == 20);
        REQUIRE(wide.largeMaskBits() == 16);
        // The production defaults: 60 KiB spacing rounds to 2^16, so 18 and 14 bits.
        const GearCutter defaults(4 * 1024, 64 * 1024, 256 * 1024);
        REQUIRE(defaults.smallMaskBits() == 18);
        REQUIRE(defaults.largeMaskBits() == 14);
        const GearCutter small(2 * 1024, 8 * 1024, 64 * 1024);
        REQUIRE(small.smallMaskBits() == 15);
        REQUIRE(small.largeMaskBits() == 11);
    }
}

TEST_CASE("GearCutter - golden boundary offsets", "[chunking][gear][golden]") {
    // Changing these offsets changes every block hash YAMS writes for content larger than one
    // chunk (and so drops cross-version deduplication). Update them only deliberately.
    ChunkingConfig cfg;
    cfg.minChunkSize = 512;
    cfg.targetChunkSize = 2048;
    cfg.maxChunkSize = 8192;
    const std::string prose = makeProse(32 * 1024, 2026);

    const auto chunks = chunkLazy(cfg, prose);
    std::vector<std::size_t> ends;
    for (const auto& c : chunks) {
        ends.push_back(c.offset + c.size);
    }
    const std::vector<std::size_t> expected = {3045,  5176,  7988,  8877,  12392,
                                               14927, 17749, 19823, 20778, 23284,
                                               25446, 26763, 29466, 31538, 32768};
    CAPTURE(ends);
    REQUIRE(ends == expected);
}
