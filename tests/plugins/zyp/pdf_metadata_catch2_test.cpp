#include "../../../plugins/zyp/pdf_metadata.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace {

std::vector<uint8_t> bytes(std::string_view text) {
    return {text.begin(), text.end()};
}

std::ptrdiff_t offsetOf(const std::vector<uint8_t>& data, const uint8_t* match) {
    REQUIRE(match != nullptr);
    return match - data.data();
}

} // namespace

TEST_CASE("PDF metadata reverse byte search handles every boundary",
          "[plugin][zyp][pdf-metadata]") {
    SECTION("marker at offset zero") {
        const auto data = bytes("trailer suffix");
        CHECK(offsetOf(data, yams::zyp::detail::findBackwards(data, "trailer")) == 0);
    }

    SECTION("marker in the middle") {
        const auto data = bytes("prefix trailer suffix");
        CHECK(offsetOf(data, yams::zyp::detail::findBackwards(data, "trailer")) == 7);
    }

    SECTION("marker at the end") {
        const auto data = bytes("prefix trailer");
        CHECK(offsetOf(data, yams::zyp::detail::findBackwards(data, "trailer")) == 7);
    }

    SECTION("last marker wins") {
        const auto data = bytes("trailer prefix trailer");
        CHECK(offsetOf(data, yams::zyp::detail::findBackwards(data, "trailer")) == 15);
    }

    SECTION("absent marker") {
        const auto data = bytes("prefix without marker");
        CHECK(yams::zyp::detail::findBackwards(data, "trailer") == nullptr);
    }

    SECTION("empty input") {
        const std::vector<uint8_t> data;
        CHECK(yams::zyp::detail::findBackwards(data, "trailer") == nullptr);
    }

    SECTION("empty marker") {
        const auto data = bytes("trailer");
        CHECK(yams::zyp::detail::findBackwards(data, "") == nullptr);
    }
}
