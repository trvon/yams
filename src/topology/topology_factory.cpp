#include <yams/topology/topology_factory.h>

#include <yams/topology/topology_baseline.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>

namespace yams::topology {

namespace {

constexpr std::string_view kConnectedKey = "connected";

// Central registry. Kept as a plain array so the factory has no global state
// and no initialization-order concerns.
constexpr std::array<std::string_view, 1> kKnownAlgorithms{kConnectedKey};

} // namespace

std::shared_ptr<ITopologyEngine> makeEngine(std::string_view algorithm) {
    const auto key = resolveFactoryKey(algorithm);
    if (key == kConnectedKey) {
        return std::make_shared<ConnectedComponentTopologyEngine>();
    }
    // resolveFactoryKey only returns keys we claim to support; this branch
    // exists as a forward-compat safety net for future registrations.
    spdlog::warn("[topology] unknown algorithm '{}'; falling back to connected", algorithm);
    return std::make_shared<ConnectedComponentTopologyEngine>();
}

std::string_view resolveFactoryKey(std::string_view requested) noexcept {
    if (requested.empty()) {
        return kConnectedKey;
    }
    for (const auto& known : kKnownAlgorithms) {
        if (known == requested) {
            return known;
        }
    }
    return kConnectedKey;
}

std::vector<std::string> listAlgorithms() {
    std::vector<std::string> out;
    out.reserve(kKnownAlgorithms.size());
    for (const auto& k : kKnownAlgorithms) {
        out.emplace_back(k);
    }
    std::sort(out.begin(), out.end());
    return out;
}

} // namespace yams::topology
