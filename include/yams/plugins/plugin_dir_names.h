// yams/plugins/plugin_dir_names.h - names in a plugin directory that are never plugins
#pragma once

#include <string_view>

namespace yams::plugins {

/// True for entry names in a plugin directory that are never plugins: anything dot-prefixed.
/// Plugin names may not start with '.', and the plugin installer keeps its bookkeeping there
/// under such names (.yams-staging holds in-progress installs and the previous version an
/// upgrade is replacing; .yams-install.lock serializes installs). Plugin scanners skip them.
[[nodiscard]] constexpr bool isReservedPluginDirName(std::string_view name) noexcept {
    return !name.empty() && name.front() == '.';
}

} // namespace yams::plugins
