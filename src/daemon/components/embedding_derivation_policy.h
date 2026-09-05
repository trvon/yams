#pragma once

#include <nlohmann/json.hpp>
#include <span>
#include <string>
#include <string_view>
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/InternalEventBus.h>

namespace yams::daemon::embed {

// Missing identity is not a cache hit. This validates preparation at gather time;
// it is not a conditional publication or a fence against later extraction writes.
inline bool preparedEmbeddingMatches(const InternalEventBus::EmbedPreparedDoc& prepared,
                                     std::string_view text, std::string_view recipe) {
    return !prepared.chunks.empty() && !prepared.sourceTextHash.empty() && !recipe.empty() &&
           prepared.preparationRecipe == recipe &&
           prepared.sourceTextHash == crypto::SHA256Hasher::hash(std::as_bytes(std::span(text)));
}

inline std::string
embeddingPreparationRecipe(const ConfigResolver::EmbeddingChunkingPolicy& chunking,
                           const ConfigResolver::EmbeddingSelectionPolicy& selection) {
    const auto& c = chunking.config;
    return nlohmann::json{{"schema", "yams.embedding-preparation/v1"},
                          {"strategy", static_cast<int>(chunking.strategy)},
                          {"target", c.target_chunk_size},
                          {"min", c.min_chunk_size},
                          {"max", c.max_chunk_size},
                          {"overlap", c.overlap_size},
                          {"overlap_percentage", c.overlap_percentage},
                          {"preserve_sentences", c.preserve_sentences},
                          {"preserve_paragraphs", c.preserve_paragraphs},
                          {"preserve_words", c.preserve_words},
                          {"chunk_separator", c.chunk_separator},
                          {"use_tokens", c.use_token_count},
                          {"semantic_threshold", c.semantic_threshold},
                          {"separators", c.separators},
                          {"selection_strategy", static_cast<int>(selection.strategy)},
                          {"selection_mode", static_cast<int>(selection.mode)},
                          {"max_chunks", selection.maxChunksPerDoc},
                          {"max_chars", selection.maxCharsPerDoc},
                          {"heading_boost", selection.headingBoost},
                          {"intro_boost", selection.introBoost}}
        .dump();
}

inline std::string embeddingDerivationRecipe(const std::string& preparation,
                                             const std::string& space,
                                             const std::string& providerVersion,
                                             std::size_t dimension) {
    return nlohmann::json{{"schema", "yams.embedding-derivation/v1"},
                          {"preparation", preparation},
                          {"space", space},
                          {"provider_version", providerVersion},
                          {"dimension", dimension}}
        .dump();
}

} // namespace yams::daemon::embed
