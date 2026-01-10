#include <yams/vector/dim_resolver.h>

#include <nlohmann/json.hpp>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>

namespace yams::vector::dimres {

std::optional<std::size_t> dim_from_model_name(const std::string& modelName) {
    if (modelName.empty())
        return std::nullopt;

    // Lowercase for comparison
    std::string name = modelName;
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // Known model dimensions (add new models here)
    if (name.find("minilm") != std::string::npos)
        return 384;
    if (name.find("mpnet") != std::string::npos)
        return 768;
    if (name.find("jina") != std::string::npos)
        return 768;
    if (name.find("nomic") != std::string::npos)
        return 768;
    if (name.find("bge-small") != std::string::npos)
        return 384;
    if (name.find("bge-base") != std::string::npos)
        return 768;
    if (name.find("bge-large") != std::string::npos)
        return 1024;
    if (name.find("e5-small") != std::string::npos)
        return 384;
    if (name.find("e5-base") != std::string::npos)
        return 768;
    if (name.find("e5-large") != std::string::npos)
        return 1024;

    return std::nullopt; // Unknown model - caller must query actual model
}

std::optional<std::size_t> dim_from_model_config(const std::filesystem::path& modelDir) {
    namespace fs = std::filesystem;
    try {
        for (const char* configName : {"config.json", "sentence_bert_config.json"}) {
            fs::path configPath = modelDir / configName;
            if (!fs::exists(configPath))
                continue;
            std::ifstream in(configPath);
            if (!in)
                continue;
            nlohmann::json j;
            in >> j;
            // Try common dimension field names
            if (j.contains("hidden_size") && j["hidden_size"].is_number_integer())
                return static_cast<std::size_t>(j["hidden_size"].get<int>());
            if (j.contains("output_embedding_size") &&
                j["output_embedding_size"].is_number_integer())
                return static_cast<std::size_t>(j["output_embedding_size"].get<int>());
            if (j.contains("dim") && j["dim"].is_number_integer())
                return static_cast<std::size_t>(j["dim"].get<int>());
        }
    } catch (...) {
    }
    return std::nullopt;
}

std::optional<std::size_t> read_dim_from_sentinel(const std::filesystem::path& dataDir) {
    try {
        namespace fs = std::filesystem;
        fs::path p = dataDir / "vectors_sentinel.json";
        if (!fs::exists(p))
            return std::nullopt;
        std::ifstream in(p);
        if (!in)
            return std::nullopt;
        nlohmann::json j;
        in >> j;
        if (j.contains("embedding_dim"))
            return j["embedding_dim"].get<std::size_t>();
    } catch (...) {
    }
    return std::nullopt;
}

std::size_t resolve_dim(const std::filesystem::path& dataDir, std::size_t generatorDim,
                        std::size_t defaultDim) {
    if (auto s = read_dim_from_sentinel(dataDir))
        return *s;
    if (generatorDim > 0)
        return generatorDim;
    return defaultDim;
}

} // namespace yams::vector::dimres
