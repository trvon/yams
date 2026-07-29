#pragma once

#include <optional>
#include <string_view>

namespace yams::metadata {

/**
 * Return the source path encoded in a symbol node key.
 *
 * Canonical symbol keys use `<kind>:<qualified-name>@<path>`. Versioned keys append
 * `@snap:<snapshot-id>`; the snapshot suffix is metadata and is not part of the path.
 */
[[nodiscard]] inline std::optional<std::string_view>
sourcePathFromNodeKey(std::string_view nodeKey) {
    const auto pathBegin = nodeKey.find('@');
    if (pathBegin == std::string_view::npos || pathBegin + 1 >= nodeKey.size()) {
        return std::nullopt;
    }

    auto pathEnd = nodeKey.rfind("@snap:");
    if (pathEnd == std::string_view::npos || pathEnd <= pathBegin) {
        pathEnd = nodeKey.size();
    }
    if (pathBegin + 1 >= pathEnd) {
        return std::nullopt;
    }
    return nodeKey.substr(pathBegin + 1, pathEnd - pathBegin - 1);
}

} // namespace yams::metadata
