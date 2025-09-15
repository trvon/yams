#pragma once

#include <cstddef>
#include <filesystem>
#include <optional>

namespace yams::vector::dimres {

// Read embedding dim from vectors_sentinel.json in the given dataDir. Returns nullopt if missing.
std::optional<std::size_t> read_dim_from_sentinel(const std::filesystem::path& dataDir);

// Resolve dimension with precedence: sentinel -> generatorDim -> defaultDim.
std::size_t resolve_dim(const std::filesystem::path& dataDir, std::size_t generatorDim,
                        std::size_t defaultDim = 384);

} // namespace yams::vector::dimres
