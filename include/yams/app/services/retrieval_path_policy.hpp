#pragma once

#include <string_view>

namespace yams::app::services::utils {

/// Return true when a path is generated workspace state rather than authored corpus content.
///
/// This policy is applied only to explicit workspace-scoped retrieval. Unscoped retrieval can
/// still address intentionally indexed build, audit, and agent-runtime artifacts.
bool isGeneratedWorkspacePath(std::string_view path);

/// Return true when path is the scope root or one of its descendants.
bool isPathWithinScope(std::string_view path, std::string_view scopeRoot);

} // namespace yams::app::services::utils
