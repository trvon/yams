#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace yams::cli::init_assets {

struct EmbeddingModel {
    std::string name;
    std::string url;
    std::string description;
    size_t size_mb;
    int dimensions;
};

struct GlinerModel {
    std::string name;
    std::string repo;
    std::string model_file;
    std::string description;
    size_t size_mb;
};

struct RerankerModel {
    std::string name;
    std::string repo;
    std::string model_file;
    std::string description;
    size_t size_mb;
    int max_tokens;
};

struct GrammarInfo {
    std::string_view language;
    std::string_view repo;
    std::string_view description;
    bool recommended;
};

const std::vector<EmbeddingModel>& embeddingModels();
const std::vector<GlinerModel>& glinerModels();
const std::vector<std::string>& glinerTokenizerFiles();
const std::vector<RerankerModel>& rerankerModels();
const std::vector<std::string>& rerankerTokenizerFiles();
const std::vector<GrammarInfo>& supportedGrammars();
std::string_view yamsSkillContent();

} // namespace yams::cli::init_assets
