#pragma once

#include <optional>
#include <string>

namespace yams::app::services {

struct ResolvedListInput {
    bool isLocalFile{false};
    std::string pattern;                // pattern to use for listing
    std::optional<std::string> absPath; // absolute path if local file detected
};

// Detect if user-supplied name is a local file path. If so, produce a concrete
// pattern suitable for list operations (exact path first), otherwise return
// a no-op resolution with pattern untouched.
ResolvedListInput resolveNameToPatternIfLocalFile(const std::string& name);

} // namespace yams::app::services
