#pragma once

#include <yams/topology/topology_engine.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace yams::topology {

// P3.1: Pluggable topology engines.
//
// Factory returns a fresh ITopologyEngine for the requested algorithm key.
// Unknown or empty keys resolve to the default ("connected"), which maps to
// the existing connected-components engine and preserves pre-P3 behavior.
//
// Known keys (see factoryAlgorithmName for the canonical label each engine
// actually stamps into TopologyArtifactBatch::algorithm):
//   - "connected" (default) -> ConnectedComponentTopologyEngine
//                               stamps "connected_components_v1"
//   - additional algorithms register here as they land (P3.2 Louvain, etc.)
std::shared_ptr<ITopologyEngine> makeEngine(std::string_view algorithm);

// Returns the canonical factory key for a given engine label. Used so the
// dispatch site can echo the selected key into debugStats without re-parsing
// the batch's algorithm field.
[[nodiscard]] std::string_view resolveFactoryKey(std::string_view requested) noexcept;

// All algorithm keys known to the factory, sorted. Useful for diagnostics
// and fallback messages.
[[nodiscard]] std::vector<std::string> listAlgorithms();

} // namespace yams::topology
