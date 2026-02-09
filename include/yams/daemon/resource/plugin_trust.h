#pragma once

#include <filesystem>
#include <set>
#include <string_view>

namespace yams::daemon::plugin_trust {

// Parses a trust file body (one path per line).
// - Trims ASCII whitespace around each line.
// - Ignores empty lines.
// - Ignores comment lines whose first non-whitespace character is '#'.
// Returns raw paths as written; callers may canonicalize if desired.
std::set<std::filesystem::path> parseTrustList(std::string_view content);

// Returns true iff `base` is a prefix of `candidate` in terms of filesystem path
// *components* (not a string prefix).
// This avoids bypasses like base='/trusted' trusting '/trusted_evil/...'.
// Note: Callers should normalize/canonicalize paths as appropriate for their trust model.
bool isPathWithin(const std::filesystem::path& base, const std::filesystem::path& candidate);

} // namespace yams::daemon::plugin_trust
