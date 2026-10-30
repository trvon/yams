#pragma once

#include <filesystem>
#include <optional>

namespace yams::config {

// Locates a Simeon static-embedding table (SMSTAT01, *.smstat). Lookup order:
//   1. `configured` (embeddings.simeon.static_table), when it names an existing file
//   2. <dataDir>/models/static/*.smstat
//   3. <executable>/../share/yams/models/static/*.smstat
//   4. YAMS_INSTALL_PREFIX/share/yams/models/static/*.smstat
// Within a directory the lexicographically first *.smstat wins.
std::optional<std::filesystem::path>
findStaticEmbeddingTable(const std::optional<std::filesystem::path>& configured,
                         const std::filesystem::path& dataDir);

} // namespace yams::config
