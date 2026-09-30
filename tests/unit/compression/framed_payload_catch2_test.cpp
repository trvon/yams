// Framed payloads: CompressionHeader followed by the (possibly compressed) body.
//
// The daemon sends these to clients that set acceptCompressed. Only the MCP server decoded
// them; `yams get` printed the header and the zstd frame as the document from v0.7.3 on.

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include <yams/compression/compression_header.h>
#include <yams/compression/framed_payload.h>

using namespace yams::compression;

namespace {

std::vector<std::byte> bytesOf(const std::string& text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

std::string textOf(const std::vector<std::byte>& bytes) {
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

std::string sampleDocument() {
    std::string text;
    for (int i = 0; i < 2000; ++i) {
        text += "line " + std::to_string(i) + ": the quick brown fox jumps over the lazy dog\n";
    }
    return text;
}

} // namespace

TEST_CASE("A framed payload round-trips to the original bytes", "[compression][framed]") {
    const auto original = bytesOf(sampleDocument());
    auto framed = encodeFramedPayload(original);
    REQUIRE(framed.has_value());
    REQUIRE((framed.value().size() >= CompressionHeader::SIZE));
    CHECK((std::memcmp(framed.value().data(), "CNRK", 4) == 0)); // what `yams get` printed

    auto decoded = decodeFramedPayload(framed.value());
    REQUIRE(decoded.has_value());
    CHECK((textOf(decoded.value()) == sampleDocument()));
}

TEST_CASE("Tiny and empty framed payloads round-trip", "[compression][framed]") {
    const auto original = bytesOf("hi");
    auto framed = encodeFramedPayload(original);
    REQUIRE(framed.has_value());
    auto decoded = decodeFramedPayload(framed.value());
    REQUIRE(decoded.has_value());
    CHECK((textOf(decoded.value()) == "hi"));

    auto empty = encodeFramedPayload({});
    REQUIRE(empty.has_value());
    auto decodedEmpty = decodeFramedPayload(empty.value());
    REQUIRE(decodedEmpty.has_value());
    CHECK(decodedEmpty.value().empty());
}

TEST_CASE("A damaged framed payload is an error, never raw bytes", "[compression][framed]") {
    auto framed = encodeFramedPayload(bytesOf(sampleDocument()));
    REQUIRE(framed.has_value());
    const auto& good = framed.value();

    SECTION("corrupted body") {
        auto bad = good;
        bad[CompressionHeader::SIZE + 8] ^= std::byte{0x5a};
        CHECK_FALSE(decodeFramedPayload(bad).has_value());
    }
    SECTION("truncated body") {
        std::vector<std::byte> bad(good.begin(), good.end() - 16);
        CHECK_FALSE(decodeFramedPayload(bad).has_value());
    }
    SECTION("shorter than a header") {
        std::vector<std::byte> bad(good.begin(), good.begin() + 10);
        CHECK_FALSE(decodeFramedPayload(bad).has_value());
    }
    SECTION("not framed at all") {
        CHECK_FALSE(decodeFramedPayload(bytesOf(sampleDocument())).has_value());
    }
}
