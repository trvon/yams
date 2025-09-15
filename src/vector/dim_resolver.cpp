#include <yams/vector/dim_resolver.h>

#include <nlohmann/json.hpp>
#include <cstdlib>
#include <fstream>

namespace yams::vector::dimres {

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
