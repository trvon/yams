#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace yams::cli::session_store {

// Return XDG-friendly sessions directory: ~/.local/state/yams/sessions
std::filesystem::path sessions_dir();

// Return index file path: sessions_dir()/index.json
std::filesystem::path index_path();

// Current session name: env YAMS_SESSION_CURRENT > index.json current > std::nullopt
std::optional<std::string> current_session();

// Current session UUID (reads from session JSON file)
std::optional<std::string> current_session_uuid();

// Load active include patterns for scoping (paths and possibly resolved doc paths)
// If name is provided, use it instead of current.
std::vector<std::string>
active_include_patterns(const std::optional<std::string>& name = std::nullopt);

} // namespace yams::cli::session_store
