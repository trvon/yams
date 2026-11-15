#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace yams::app::services::utils {

/// Return true when a path is generated workspace state rather than authored corpus content.
///
/// This policy is applied only to explicit workspace-scoped retrieval. Unscoped retrieval can
/// still address intentionally indexed build, audit, and agent-runtime artifacts.
bool isGeneratedWorkspacePath(std::string_view path);

/// Return true when path is the scope root or one of its descendants.
bool isPathWithinScope(std::string_view path, std::string_view scopeRoot);

/// Scope containment for checking many paths against one root, as grep and search do per
/// candidate document. The root is resolved once, and each path's parent directory is resolved
/// once and cached, so symlinked directories match as they do with isPathWithinScope at a cost
/// per distinct directory rather than per document. Not thread-safe.
class PathScope {
public:
    explicit PathScope(std::string_view scopeRoot);

    bool contains(std::string_view path) const;

    /// Number of distinct parent directories resolved so far; exposed for tests.
    std::size_t resolvedDirectoryCount() const { return resolvedParents_.size(); }

private:
    bool underRoot(std::string_view normalizedPath) const;

    std::vector<std::string> roots_; // resolved and lexical root, each ending in '/'
    mutable std::unordered_map<std::string, std::string> resolvedParents_;
};

} // namespace yams::app::services::utils
