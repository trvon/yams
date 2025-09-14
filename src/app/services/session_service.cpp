#include <nlohmann/json.hpp>
#include <cstdlib>
#include <fstream>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>

namespace yams::app::services {

using json = nlohmann::json;

namespace {
static std::filesystem::path state_root() {
    if (const char* xdg = std::getenv("XDG_STATE_HOME"); xdg && *xdg)
        return std::filesystem::path(xdg) / "yams";
    if (const char* home = std::getenv("HOME"); home && *home)
        return std::filesystem::path(home) / ".local" / "state" / "yams";
    return std::filesystem::current_path() / ".yams_state";
}
static std::filesystem::path sessions_dir() {
    auto d = state_root() / "sessions";
    std::error_code ec;
    std::filesystem::create_directories(d, ec);
    (void)ec;
    return d;
}
static std::filesystem::path index_path() {
    return sessions_dir() / "index.json";
}

static json load_json(const std::filesystem::path& p) {
    json j;
    if (!std::filesystem::exists(p))
        return j;
    try {
        std::ifstream in(p);
        if (in.good())
            in >> j;
    } catch (...) {
    }
    return j;
}
static void save_json(const std::filesystem::path& p, const json& j) {
    std::ofstream out(p);
    out << j.dump(2) << std::endl;
}
} // namespace

class SessionService : public ISessionService {
public:
    explicit SessionService(const AppContext* ctx) : ctx_(ctx) {}

    std::optional<std::string> current() const override {
        auto j = load_json(index_path());
        if (j.contains("current") && j["current"].is_string())
            return j["current"].get<std::string>();
        if (const char* env = std::getenv("YAMS_SESSION_CURRENT"); env && *env)
            return std::string(env);
        return std::nullopt;
    }

    std::vector<std::string> listSessions() const override {
        std::vector<std::string> out;
        for (auto& e : std::filesystem::directory_iterator(sessions_dir())) {
            if (e.is_regular_file() && e.path().extension() == ".json")
                out.push_back(e.path().stem().string());
        }
        return out;
    }

    bool exists(const std::string& name) const override {
        return std::filesystem::exists(sessions_dir() / (name + ".json"));
    }

    void init(const std::string& name, const std::string& desc) override {
        json j;
        j["name"] = name;
        if (!desc.empty())
            j["desc"] = desc;
        j["selectors"] = json::array();
        j["materialized"] = json::array();
        save_json(sessions_dir() / (name + ".json"), j);
        use(name);
    }

    void use(const std::string& name) override {
        json idx = load_json(index_path());
        idx["current"] = name;
        save_json(index_path(), idx);
    }

    void remove(const std::string& name) override {
        std::filesystem::remove(sessions_dir() / (name + ".json"));
    }

    std::vector<std::string> listPathSelectors(const std::string& name) const override {
        std::vector<std::string> out;
        auto j = load_json(sessions_dir() / (name + ".json"));
        if (j.contains("selectors")) {
            for (auto& s : j["selectors"]) {
                if (s.contains("path"))
                    out.push_back(s["path"].get<std::string>());
            }
        }
        return out;
    }

    void addPathSelector(const std::string& path, const std::vector<std::string>& tags,
                         const std::vector<std::pair<std::string, std::string>>& meta) override {
        auto cur = current();
        if (!cur)
            return;
        auto p = sessions_dir() / (*cur + ".json");
        auto j = load_json(p);
        json sel;
        sel["path"] = path;
        if (!tags.empty())
            sel["tags"] = tags;
        if (!meta.empty()) {
            json m = json::object();
            for (auto& kv : meta)
                m[kv.first] = kv.second;
            sel["metadata"] = m;
        }
        j["selectors"].push_back(sel);
        save_json(p, j);
    }

    void removePathSelector(const std::string& path) override {
        auto cur = current();
        if (!cur)
            return;
        auto p = sessions_dir() / (*cur + ".json");
        auto j = load_json(p);
        json out = json::array();
        if (j.contains("selectors")) {
            for (auto& s : j["selectors"]) {
                if (!(s.contains("path") && s["path"].is_string() && s["path"] == path))
                    out.push_back(s);
            }
        }
        j["selectors"] = std::move(out);
        save_json(p, j);
    }

    std::vector<std::string>
    activeIncludePatterns(const std::optional<std::string>& name) const override {
        std::vector<std::string> out;
        auto useName = name ? *name : current().value_or("");
        if (!useName.empty()) {
            auto j = load_json(sessions_dir() / (useName + ".json"));
            if (j.contains("selectors")) {
                for (auto& s : j["selectors"]) {
                    if (s.contains("path"))
                        out.push_back(s["path"].get<std::string>());
                }
            }
            if (j.contains("materialized")) {
                for (auto& m : j["materialized"]) {
                    if (m.contains("path"))
                        out.push_back(m["path"].get<std::string>());
                    else if (m.contains("name"))
                        out.push_back(m["name"].get<std::string>());
                }
            }
        }
        // Legacy fallback not included here (kept in CLI session_store helper if needed)
        return out;
    }

    std::size_t warm(std::size_t limit, std::size_t snippetLen) override {
        // Use DocumentService to materialize
        auto cur = current();
        if (!cur || !ctx_ || !ctx_->metadataRepo || !ctx_->store)
            return 0;
        auto patterns = listPathSelectors(*cur);
        std::size_t warmed = 0;
        json materialized = json::array();
        auto j = load_json(sessions_dir() / (*cur + ".json"));
        try {
            auto doc = makeDocumentService(*ctx_);
            if (!doc)
                return 0;
            for (const auto& pat : patterns) {
                app::services::ListDocumentsRequest lreq;
                lreq.pattern = pat;
                lreq.limit = limit;
                lreq.showSnippets = true;
                lreq.snippetLength = snippetLen;
                auto lres = doc->list(lreq);
                if (!lres)
                    continue;
                for (const auto& d : lres.value().documents) {
                    json m;
                    m["name"] = d.name;
                    m["path"] = d.path;
                    m["hash"] = d.hash;
                    m["mime"] = d.mimeType;
                    m["size"] = d.size;
                    m["snippet"] = d.snippet ? *d.snippet : std::string();
                    materialized.push_back(std::move(m));
                    ++warmed;
                }
            }
            // TODO: enforce TTL/limits here (Phase 2)
            j["materialized"] = std::move(materialized);
            save_json(sessions_dir() / (*cur + ".json"), j);
        } catch (...) {
        }
        return warmed;
    }

    std::size_t prepare(const PrepareBudget& budget, std::size_t limit,
                        std::size_t snippetLen) override {
        (void)budget; // Phase 2: feed budgets into daemon orchestration
        return warm(limit, snippetLen);
    }

    void clearMaterialized() override {
        auto cur = current();
        if (!cur)
            return;
        auto p = sessions_dir() / (*cur + ".json");
        auto j = load_json(p);
        j["materialized"] = json::array();
        save_json(p, j);
    }

    std::vector<MaterializedItem>
    listMaterialized(const std::optional<std::string>& name) const override {
        std::vector<MaterializedItem> out;
        auto use = name ? *name : current().value_or("");
        if (use.empty())
            return out;
        auto j = load_json(sessions_dir() / (use + ".json"));
        if (!j.contains("materialized") || !j["materialized"].is_array())
            return out;
        for (const auto& m : j["materialized"]) {
            MaterializedItem it;
            if (m.contains("name"))
                it.name = m.at("name").get<std::string>();
            if (m.contains("path"))
                it.path = m.at("path").get<std::string>();
            if (m.contains("hash"))
                it.hash = m.at("hash").get<std::string>();
            if (m.contains("mime"))
                it.mime = m.at("mime").get<std::string>();
            if (m.contains("size"))
                it.size = m.at("size").get<std::uint64_t>();
            if (m.contains("snippet") && m.at("snippet").is_string())
                it.snippet = m.at("snippet").get<std::string>();
            out.push_back(std::move(it));
        }
        return out;
    }

    void save(const std::optional<std::filesystem::path>& outFile) const override {
        auto cur = current();
        if (!cur)
            return;
        auto src = sessions_dir() / (*cur + ".json");
        auto dst = outFile ? *outFile : src;
        std::error_code ec;
        std::filesystem::copy_file(src, dst, std::filesystem::copy_options::overwrite_existing, ec);
    }

    void load(const std::filesystem::path& inFile,
              const std::optional<std::string>& name) override {
        auto j = load_json(inFile);
        std::string target = name ? *name : j.value("name", std::string("imported"));
        if (target.empty())
            target = "imported";
        j["name"] = target;
        save_json(sessions_dir() / (target + ".json"), j);
        use(target);
    }

    // ---- Watch config (Phase 1) ----
    void enableWatch(bool on, const std::optional<std::string>& name) override {
        auto use = name ? *name : current().value_or("");
        if (use.empty())
            return;
        auto p = sessions_dir() / (use + ".json");
        auto j = load_json(p);
        j["watch"]["enabled"] = on;
        if (!j["watch"].contains("interval_ms"))
            j["watch"]["interval_ms"] = 2000; // default 2s
        save_json(p, j);
    }

    bool watchEnabled(const std::optional<std::string>& name) const override {
        auto use = name ? *name : current().value_or("");
        if (use.empty())
            return false;
        auto j = load_json(sessions_dir() / (use + ".json"));
        if (j.contains("watch") && j["watch"].contains("enabled"))
            return j["watch"]["enabled"].get<bool>();
        return false;
    }

    void setWatchIntervalMs(uint32_t intervalMs, const std::optional<std::string>& name) override {
        auto use = name ? *name : current().value_or("");
        if (use.empty())
            return;
        auto p = sessions_dir() / (use + ".json");
        auto j = load_json(p);
        j["watch"]["enabled"] = j.contains("watch") && j["watch"].contains("enabled")
                                    ? j["watch"]["enabled"].get<bool>()
                                    : false;
        j["watch"]["interval_ms"] = static_cast<uint32_t>(intervalMs);
        save_json(p, j);
    }

    uint32_t watchIntervalMs(const std::optional<std::string>& name) const override {
        auto use = name ? *name : current().value_or("");
        if (use.empty())
            return 2000;
        auto j = load_json(sessions_dir() / (use + ".json"));
        if (j.contains("watch") && j["watch"].contains("interval_ms"))
            return j["watch"]["interval_ms"].get<uint32_t>();
        return 2000;
    }

    std::vector<std::string>
    getPinnedPatterns(const std::optional<std::string>& name) const override {
        auto use = name ? *name : current().value_or("");
        if (use.empty())
            return {};
        return listPathSelectors(use);
    }

private:
    const AppContext* ctx_{nullptr};
};

std::shared_ptr<ISessionService> makeSessionService(const AppContext* ctx) {
    return std::make_shared<SessionService>(ctx);
}

} // namespace yams::app::services
