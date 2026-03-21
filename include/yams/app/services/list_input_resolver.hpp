#pragma once

#include <optional>
#include <string>

namespace yams::app::services {

enum class ResolvedListInputKind {
    Pattern,
    LocalFile,
    LocalDirectory,
    HistoricalPath,
};

struct ResolvedListInput {
    ResolvedListInputKind kind{ResolvedListInputKind::Pattern};
    bool isLocalFile{false};
    bool isLocalDirectory{false};
    std::string pattern;                // pattern to use for listing
    std::optional<std::string> absPath; // absolute path if local file detected
};

// Classify user-supplied list input so callers can distinguish list patterns,
// concrete files, directories, and historical path lookups.
ResolvedListInput resolveNameToPatternIfLocalFile(const std::string& name);

} // namespace yams::app::services
