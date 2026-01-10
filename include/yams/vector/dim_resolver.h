#pragma once

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>

namespace yams::vector::dimres {

// Lookup dimension by model name pattern (e.g., "jina" -> 768, "minilm" -> 384).
// Returns nullopt for unknown models - caller should query actual model or fail.
std::optional<std::size_t> dim_from_model_name(const std::string& modelName);

// Read dimension from model config.json or sentence_bert_config.json.
// Returns nullopt if config not found or doesn't contain dimension.
std::optional<std::size_t> dim_from_model_config(const std::filesystem::path& modelDir);

// Read embedding dim from vectors_sentinel.json in the given dataDir. Returns nullopt if missing.
std::optional<std::size_t> read_dim_from_sentinel(const std::filesystem::path& dataDir);

// Resolve dimension with precedence: sentinel -> generatorDim -> defaultDim.
// NOTE: Prefer using dim_from_model_name() or dim_from_model_config() with explicit
// error handling rather than relying on defaultDim fallback.
std::size_t resolve_dim(const std::filesystem::path& dataDir, std::size_t generatorDim,
                        std::size_t defaultDim = 0);

} // namespace yams::vector::dimres
