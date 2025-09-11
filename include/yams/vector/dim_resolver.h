#pragma once

#include <cstddef>
#include <filesystem>
#include <optional>

namespace yams::vector::dimres {

// Resolve config path: XDG_CONFIG_HOME/yams/config.toml or HOME/.config/yams/config.toml
std::filesystem::path resolve_config_path();

// Read embedding dim from config.toml (embeddings.embedding_dim, then
// vector_database.embedding_dim, then vector_index.dimension). Returns nullopt if none found.
std::optional<std::size_t> read_dim_from_config();

// Read embedding dim from vectors_sentinel.json in the given dataDir. Returns nullopt if missing.
std::optional<std::size_t> read_dim_from_sentinel(const std::filesystem::path& dataDir);

// Resolve dimension with precedence: config -> sentinel -> generatorDim -> defaultDim.
std::size_t resolve_dim(const std::filesystem::path& dataDir, std::size_t generatorDim,
                        std::size_t defaultDim = 384);

} // namespace yams::vector::dimres
