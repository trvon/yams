#define _CRT_SECURE_NO_WARNINGS
#include <yams/app/services/enhanced_search_executor.h>

#include <filesystem>
#include <fstream>
#include <regex>

namespace yams::app::services {

namespace fs = std::filesystem;

static std::string trim(std::string s) {
    auto l = s.find_first_not_of(" \t");
    if (l == std::string::npos)
        return "";
    auto r = s.find_last_not_of(" \t");
    return s.substr(l, r - l + 1);
}

EnhancedConfig EnhancedSearchExecutor::loadConfigFromToml() {
    EnhancedConfig out{};
    // Resolve config path
    fs::path cfgPath;
    if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
        cfgPath = fs::path(xdg) / "yams" / "config.toml";
    else if (const char* home = std::getenv("HOME"))
        cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
    if (cfgPath.empty() || !fs::exists(cfgPath))
        return out; // defaults (disabled)

    std::ifstream in(cfgPath);
    if (!in)
        return out;

    std::string line;
    std::string section;
    auto is_bool = [](const std::string& v) {
        auto t = v;
        for (auto& c : t)
            c = static_cast<char>(::tolower(c));
        return t == "true" || t == "false" || t == "1" || t == "0";
    };
    auto to_bool = [&](const std::string& v) {
        auto t = v;
        for (auto& c : t)
            c = static_cast<char>(::tolower(c));
        return t == "true" || t == "1";
    };

    while (std::getline(in, line)) {
        auto l = trim(line);
        if (l.empty() || l[0] == '#')
            continue;
        if (l.front() == '[') {
            auto end = l.find(']');
            if (end != std::string::npos)
                section = l.substr(1, end - 1);
            continue;
        }
        auto eq = l.find('=');
        if (eq == std::string::npos)
            continue;
        auto key = trim(l.substr(0, eq));
        auto val = trim(l.substr(eq + 1));
        if (!val.empty() && val.front() == '"' && val.back() == '"')
            val = val.substr(1, val.size() - 2);

        if (section == "search.enhanced" || section == "experimental.enhanced_search") {
            if (key == "enable" && is_bool(val))
                out.enable = to_bool(val);
            else if (key == "classification_weight")
                out.classification_weight = std::stod(val);
            else if (key == "kg_expansion_weight")
                out.kg_expansion_weight = std::stod(val);
            else if (key == "hotzone_weight")
                out.hotzone_weight = std::stod(val);
            else if (key == "enhanced_search_timeout_ms")
                out.enhanced_search_timeout_ms = std::stoi(val);
        } else if (section == "search.hotzones" || section == "experimental.hotzones") {
            if (key == "decay_interval_hours")
                out.hotzones.half_life_hours = std::stod(val);
            else if (key == "max_boost_factor")
                out.hotzones.max_boost_factor = std::stod(val);
            else if (key == "enable_persistence" && is_bool(val))
                out.hotzones.enable_persistence = to_bool(val);
            else if (key == "data_file")
                out.hotzones.data_file = val;
            // Other experimental keys (e.g., decay_factor, min_frequency) are ignored in Phase A.
        }
    }
    return out;
}

void EnhancedSearchExecutor::apply(const AppContext& /*ctx*/, const EnhancedConfig& cfg,
                                   const std::string& /*query*/,
                                   std::vector<SearchItem>& items) const {
    if (!cfg.enable || items.empty())
        return;

    // Phase A: apply hotzone multiplier if we have a manager, otherwise leave scores as-is.
    if (hotzones_ && cfg.hotzone_weight > 0.0) {
        for (auto& it : items) {
            // Prefer stable key: path, then hash, then id.
            const std::string& key =
                !it.path.empty() ? it.path : (!it.hash.empty() ? it.hash : std::to_string(it.id));
            const double boost = hotzones_->getBoost(key);
            // Blend: score' = score * (1 + hotzone_weight * (boost - 1))
            const double blended = it.score * (1.0 + cfg.hotzone_weight * (boost - 1.0));
            it.score = blended;
        }
    }
}

} // namespace yams::app::services
