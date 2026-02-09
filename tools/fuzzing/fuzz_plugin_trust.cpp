#include <yams/daemon/resource/plugin_trust.h>

#include <cstdint>
#include <filesystem>
#include <string_view>

namespace {

static void splitNul3(std::string_view input, std::string_view& a, std::string_view& b,
                      std::string_view& c) {
    size_t p1 = input.find('\0');
    if (p1 == std::string_view::npos) {
        a = input;
        b = {};
        c = {};
        return;
    }
    a = input.substr(0, p1);
    std::string_view rest = input.substr(p1 + 1);
    size_t p2 = rest.find('\0');
    if (p2 == std::string_view::npos) {
        b = rest;
        c = {};
        return;
    }
    b = rest.substr(0, p2);
    c = rest.substr(p2 + 1);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (!data || size == 0) {
        return 0;
    }

    std::string_view input(reinterpret_cast<const char*>(data), size);
    std::string_view baseS, candidateS, trustBody;
    splitNul3(input, baseS, candidateS, trustBody);

    std::filesystem::path base(baseS);
    std::filesystem::path candidate(candidateS);

    // Fuzz the trust list parsing (should never throw).
    auto roots = yams::daemon::plugin_trust::parseTrustList(trustBody);

    // Exercise component-wise containment checks.
    (void)yams::daemon::plugin_trust::isPathWithin(base, candidate);
    for (const auto& r : roots) {
        (void)yams::daemon::plugin_trust::isPathWithin(r, candidate);
        (void)yams::daemon::plugin_trust::isPathWithin(r, base);
    }

    return 0;
}
