#include <yams/topology/topology_factory.h>

#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_baseline.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>

namespace yams::topology {

namespace {

constexpr std::string_view kConnectedKey = "connected";
constexpr std::string_view kHdbscanKey = "hdbscan";

constexpr std::array<std::string_view, 2> kKnownAlgorithms{kConnectedKey, kHdbscanKey};

} // namespace

std::shared_ptr<ITopologyEngine> makeEngine(std::string_view algorithm) {
    const auto key = resolveFactoryKey(algorithm);
    if (key == kConnectedKey) {
        return std::make_shared<ConnectedComponentTopologyEngine>();
    }
    if (key == kHdbscanKey) {
        return std::make_shared<HDBSCANTopologyEngine>();
    }
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
