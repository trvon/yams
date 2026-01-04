#include <yams/cli/session_store.h>

#include <nlohmann/json.hpp>
#include <cstdlib>
#include <fstream>

namespace yams::cli::session_store {

using json = nlohmann::json;

static std::filesystem::path state_root() {
    const char* xdg = std::getenv("XDG_STATE_HOME");
    const char* home = std::getenv("HOME");
    if (xdg && *xdg)
        return std::filesystem::path(xdg) / "yams";
    if (home && *home)
        return std::filesystem::path(home) / ".local" / "state" / "yams";
    return std::filesystem::current_path() / ".yams_state";
}

std::filesystem::path sessions_dir() {
    auto dir = state_root() / "sessions";
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    (void)ec;
    return dir;
}

std::filesystem::path index_path() {
    return sessions_dir() / "index.json";
}

std::optional<std::string> current_session() {
    if (const char* env = std::getenv("YAMS_SESSION_CURRENT")) {
        if (*env)
            return std::string(env);
    }
    auto path = index_path();
    if (!std::filesystem::exists(path))
        return std::nullopt;
    try {
        std::ifstream in(path);
        if (!in.good())
            return std::nullopt;
        json j;
        in >> j;
        if (j.contains("current") && j["current"].is_string()) {
            auto s = j["current"].get<std::string>();
            if (!s.empty())
                return s;
        }
    } catch (...) {
    }
    return std::nullopt;
}

static std::vector<std::string> load_pinned_legacy() {
    // Legacy pinned.json fallback
    auto legacy = state_root() / "pinned.json";
    std::vector<std::string> out;
    if (!std::filesystem::exists(legacy))
        return out;
    try {
        std::ifstream in(legacy);
        if (!in.good())
            return out;
        json j;
        in >> j;
        if (j.is_array()) {
            for (auto& e : j) {
                if (e.contains("path") && e["path"].is_string()) {
                    out.push_back(e["path"].get<std::string>());
                }
            }
        }
    } catch (...) {
    }
    return out;
}

static bool has_wildcards(const std::string& value) {
    return value.find('*') != std::string::npos || value.find('?') != std::string::npos ||
           value.find('[') != std::string::npos || value.find(']') != std::string::npos;
}

static bool looks_like_path(const std::string& value) {
    return value.find('/') != std::string::npos || value.find('\\') != std::string::npos ||
           value.find(':') != std::string::npos;
}

static std::string normalize_selector_path(const std::string& value) {
    if (!looks_like_path(value))
        return value;

    std::string normalized = value;
    std::replace(normalized.begin(), normalized.end(), '\\', '/');

    if (!has_wildcards(normalized)) {
        std::error_code ec;
        std::filesystem::path candidate(value);
        if (std::filesystem::exists(candidate, ec) && std::filesystem::is_directory(candidate, ec)) {
            if (!normalized.empty() && normalized.back() != '/')
                normalized += '/';
            normalized += "**/*";
        }
    }

    return normalized;
}

std::vector<std::string> active_include_patterns(const std::optional<std::string>& name) {
    std::vector<std::string> out;
    std::string session = name.value_or(current_session().value_or(""));
    if (!session.empty()) {
        auto path = sessions_dir() / (session + ".json");
        if (std::filesystem::exists(path)) {
            try {
                std::ifstream in(path);
                if (in.good()) {
                    json j;
                    in >> j;
                    if (j.contains("selectors") && j["selectors"].is_array()) {
                        for (auto& sel : j["selectors"]) {
                            if (sel.contains("path") && sel["path"].is_string()) {
                                out.push_back(
                                    normalize_selector_path(sel["path"].get<std::string>()));
                            }
                        }
                    }
                    if (j.contains("materialized") && j["materialized"].is_array()) {
                        for (auto& m : j["materialized"]) {
                            if (m.contains("path") && m["path"].is_string())
                                out.push_back(
                                    normalize_selector_path(m["path"].get<std::string>()));
                            else if (m.contains("name") && m["name"].is_string())
                                out.push_back(m["name"].get<std::string>());
                        }
                    }
                }
            } catch (...) {
            }
        }
    }
    if (out.empty()) {
        // Fallback to legacy pins
        out = load_pinned_legacy();
    }
    return out;
}

} // namespace yams::cli::session_store
