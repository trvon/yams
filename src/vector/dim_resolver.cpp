#include <yams/vector/dim_resolver.h>

#include <nlohmann/json.hpp>
#include <cstdlib>
#include <fstream>
#include <regex>

namespace yams::vector::dimres {

std::filesystem::path resolve_config_path() {
    namespace fs = std::filesystem;
    fs::path cfgHome;
    if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
        cfgHome = fs::path(xdg);
    } else if (const char* home = std::getenv("HOME")) {
        cfgHome = fs::path(home) / ".config";
    } else {
        cfgHome = fs::path("~/.config");
    }
    return cfgHome / "yams" / "config.toml";
}

std::optional<std::size_t> read_dim_from_config() {
    try {
        auto cfg = resolve_config_path();
        std::ifstream in(cfg);
        if (!in)
            return std::nullopt;
        std::string line;
        std::regex kvRe(R"(^\s*([A-Za-z0-9_\.]+)\s*=\s*([0-9]+))");
        std::optional<std::size_t> emb{}, vdb{}, vidx{};
        while (std::getline(in, line)) {
            std::smatch m;
            if (!std::regex_search(line, m, kvRe))
                continue;
            std::string key = m[1];
            size_t val = 0;
            try {
                val = static_cast<size_t>(std::stoul(m[2]));
            } catch (...) {
                continue;
            }
            if (!emb && key == "embeddings.embedding_dim")
                emb = val;
            if (!vdb && key == "vector_database.embedding_dim")
                vdb = val;
            if (!vidx && (key == "vector_index.dimension" || key == "vector_index.dimenions"))
                vidx = val;
        }
        if (emb)
            return emb;
        if (vdb)
            return vdb;
        if (vidx)
            return vidx;
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
    if (auto c = read_dim_from_config())
        return *c;
    if (auto s = read_dim_from_sentinel(dataDir))
        return *s;
    if (generatorDim > 0)
        return generatorDim;
    return defaultDim;
}

} // namespace yams::vector::dimres
