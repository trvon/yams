#pragma once

#include <filesystem>
#include <optional>

namespace yams::config {

// Resolves an explicitly selected Simeon static-embedding table (SMSTAT01).
// `configured` is embeddings.simeon.static_table, recorded by an explicit model
// download or config choice. Unselected user/package files do not opt in, and a
// missing selection never falls back to another table. The dataDir argument is
// retained for caller compatibility; it does not authorize model discovery.
std::optional<std::filesystem::path>
findStaticEmbeddingTable(const std::optional<std::filesystem::path>& configured,
                         const std::filesystem::path& dataDir);

} // namespace yams::config
