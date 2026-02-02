#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/core/uuid.h>

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
    } catch (const std::exception& e) {
        spdlog::warn("SessionService: Failed to load JSON from {}: {}", p.string(), e.what());
    } catch (...) {
        spdlog::warn("SessionService: Failed to load JSON from {}: unknown error", p.string());
    }
    return j;
}
static bool save_json(const std::filesystem::path& p, const json& j) {
    std::ofstream out(p);
    if (!out) {
        spdlog::warn("SessionService: Failed to open {} for writing", p.string());
        return false;
    }
    out << j.dump(2) << std::endl;
    if (!out) {
        spdlog::warn("SessionService: Failed to write JSON to {}", p.string());
        return false;
    }
    return true;
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
        j["uuid"] = yams::core::generateUUID();
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
        auto cur = current();
        if (!cur || !ctx_ || !ctx_->metadataRepo || !ctx_->store)
            return 0;

        auto patterns = listPathSelectors(*cur);
        std::size_t warmed = 0;
        json materialized = json::array();
        auto j = load_json(sessions_dir() / (*cur + ".json"));
        std::size_t remaining = limit;

        try {
            // Use tree-based queries for efficient document retrieval
            for (const auto& pat : patterns) {
                if (remaining == 0)
                    break;

                auto docsResult = ctx_->metadataRepo->findDocumentsByPathTreePrefix(
                    pat, true, static_cast<int>(remaining));

                if (!docsResult)
                    continue;

                for (const auto& doc : docsResult.value()) {
                    if (remaining == 0)
                        break;

                    json m;
                    m["name"] = doc.fileName;
                    m["path"] = doc.filePath;
                    m["hash"] = doc.sha256Hash;
                    m["mime"] = doc.mimeType;
                    m["size"] = doc.fileSize;

                    // Optionally fetch snippet if requested
                    if (snippetLen > 0 && ctx_->store) {
                        auto contentResult = ctx_->store->retrieveBytes(doc.sha256Hash);
                        if (contentResult && !contentResult.value().empty()) {
                            std::string content(
                                reinterpret_cast<const char*>(contentResult.value().data()),
                                std::min(contentResult.value().size(), snippetLen));
                            m["snippet"] = content;
                        } else {
                            m["snippet"] = "";
                        }
                    } else {
                        m["snippet"] = "";
                    }

                    materialized.push_back(std::move(m));
                    ++warmed;
                    --remaining;
                }
            }

            j["materialized"] = std::move(materialized);
            save_json(sessions_dir() / (*cur + ".json"), j);
        } catch (const std::exception& e) {
            spdlog::warn("SessionService: Error during warm: {}", e.what());
        } catch (...) {
            spdlog::warn("SessionService: Unknown error during warm");
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

    std::optional<TreeBranchInfo>
    getTreeBranch(const std::string& pathPrefix,
                  const std::optional<std::string>& name) const override {
        auto use = name ? *name : current().value_or("");
        if (use.empty() || !ctx_ || !ctx_->metadataRepo)
            return std::nullopt;

        auto nodeResult = ctx_->metadataRepo->findPathTreeNodeByFullPath(pathPrefix);
        if (!nodeResult || !nodeResult.value().has_value())
            return std::nullopt;

        const auto& node = nodeResult.value().value();
        TreeBranchInfo info;
        info.path = node.fullPath;
        info.docCount = node.docCount;

        auto childrenResult = ctx_->metadataRepo->listPathTreeChildren(pathPrefix, 100);
        if (childrenResult) {
            for (const auto& child : childrenResult.value()) {
                info.childSegments.push_back(child.pathSegment);
            }
        }

        return info;
    }

    std::vector<MaterializedItem>
    getDocumentsFromTree(std::size_t limit, const std::optional<std::string>& name) const override {
        std::vector<MaterializedItem> out;
        auto use = name ? *name : current().value_or("");
        if (use.empty() || !ctx_ || !ctx_->metadataRepo)
            return out;

        auto patterns = listPathSelectors(use);
        std::size_t remaining = limit;

        for (const auto& pat : patterns) {
            if (remaining == 0)
                break;

            auto docsResult = ctx_->metadataRepo->findDocumentsByPathTreePrefix(
                pat, true, static_cast<int>(remaining));

            if (!docsResult)
                continue;

            for (const auto& doc : docsResult.value()) {
                if (remaining == 0)
                    break;

                MaterializedItem item;
                item.name = doc.fileName;
                item.path = doc.filePath;
                item.hash = doc.sha256Hash;
                item.mime = doc.mimeType;
                item.size = doc.fileSize;
                out.push_back(std::move(item));
                --remaining;
            }
        }

        return out;
    }

    // Session-isolated memory (PBI-082)
    void create(const std::string& name, const std::string& desc) override {
        if (exists(name))
            throw std::runtime_error("Session already exists: " + name);

        auto now = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

        json j;
        j["name"] = name;
        j["uuid"] = yams::core::generateUUID();
        j["description"] = desc;
        j["state"] = "closed";
        j["createdTime"] = now;
        j["lastOpenedTime"] = 0;
        j["lastClosedTime"] = 0;
        j["selectors"] = json::array();
        j["materialized"] = json::array();
        save_json(sessions_dir() / (name + ".json"), j);
    }

    void open(const std::string& name) override {
        if (!exists(name))
            throw std::runtime_error("Session does not exist: " + name);

        auto cur = current();
        if (cur && *cur != name) {
            auto oldPath = sessions_dir() / (*cur + ".json");
            auto oldJson = load_json(oldPath);
            if (oldJson.contains("state") && oldJson["state"] == "active") {
                oldJson["state"] = "closed";
                oldJson["lastClosedTime"] = std::chrono::duration_cast<std::chrono::seconds>(
                                                std::chrono::system_clock::now().time_since_epoch())
                                                .count();
                save_json(oldPath, oldJson);
            }
        }

        auto p = sessions_dir() / (name + ".json");
        auto j = load_json(p);
        j["state"] = "active";
        j["lastOpenedTime"] = std::chrono::duration_cast<std::chrono::seconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
        // Record instance that opened this session (set by caller or env)
        if (const char* instEnv = std::getenv("YAMS_INSTANCE_ID"); instEnv && *instEnv) {
            j["lastInstanceId"] = std::string(instEnv);
        }
        save_json(p, j);
        use(name);
    }

    void close() override {
        auto cur = current();
        if (!cur)
            return;

        auto p = sessions_dir() / (*cur + ".json");
        auto j = load_json(p);
        j["state"] = "closed";
        j["lastClosedTime"] = std::chrono::duration_cast<std::chrono::seconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
        save_json(p, j);

        json idx = load_json(index_path());
        idx.erase("current");
        save_json(index_path(), idx);
    }

    SessionState getState(const std::string& name) const override {
        if (!exists(name))
            return SessionState::NotExists;

        auto j = load_json(sessions_dir() / (name + ".json"));
        if (j.contains("state") && j["state"] == "active")
            return SessionState::Active;
        return SessionState::Closed;
    }

    std::optional<SessionInfo> getSessionInfo(const std::string& name) const override {
        if (!exists(name))
            return std::nullopt;

        auto j = load_json(sessions_dir() / (name + ".json"));
        SessionInfo info;
        info.name = name;
        info.uuid = j.value("uuid", "");
        info.instanceId = j.value("lastInstanceId", "");
        info.description = j.value("description", j.value("desc", ""));
        info.state = (j.contains("state") && j["state"] == "active") ? SessionState::Active
                                                                     : SessionState::Closed;
        info.createdTime = j.value("createdTime", std::int64_t{0});
        info.lastOpenedTime = j.value("lastOpenedTime", std::int64_t{0});
        info.lastClosedTime = j.value("lastClosedTime", std::int64_t{0});

        if (ctx_ && ctx_->metadataRepo) {
            auto countResult = ctx_->metadataRepo->countDocumentsBySessionId(name);
            if (countResult)
                info.documentCount = static_cast<std::size_t>(countResult.value());
        }

        return info;
    }

    std::vector<SessionInfo> listAllSessions() const override {
        std::vector<SessionInfo> out;
        for (const auto& name : listSessions()) {
            auto info = getSessionInfo(name);
            if (info)
                out.push_back(*info);
        }
        return out;
    }

    MergeResult merge(const std::string& name, const MergeOptions& opts) override {
        if (!exists(name))
            throw std::runtime_error("Session does not exist: " + name);

        MergeResult result;

        if (!ctx_ || !ctx_->metadataRepo)
            return result;

        auto docsResult = ctx_->metadataRepo->findDocumentsBySessionId(name);
        if (!docsResult)
            return result;

        for (const auto& doc : docsResult.value()) {
            bool excluded = false;
            for (const auto& pattern : opts.excludePatterns) {
                if (doc.filePath.find(pattern) != std::string::npos) {
                    excluded = true;
                    break;
                }
            }

            if (excluded) {
                result.documentsExcluded++;
            } else {
                result.mergedPaths.push_back(doc.filePath);
                result.documentsMerged++;
            }
        }

        if (!opts.dryRun) {
            ctx_->metadataRepo->removeSessionIdFromDocuments(name);
            remove(name);

            json idx = load_json(index_path());
            if (idx.contains("current") && idx["current"] == name) {
                idx.erase("current");
                save_json(index_path(), idx);
            }
        }

        return result;
    }

    std::size_t discard(const std::string& name, bool confirm) override {
        if (!exists(name))
            throw std::runtime_error("Session does not exist: " + name);

        if (!confirm)
            throw std::runtime_error(
                "Discard requires confirmation. Pass confirm=true or use --confirm flag.");

        if (!ctx_ || !ctx_->metadataRepo)
            return 0;

        auto countResult = ctx_->metadataRepo->deleteDocumentsBySessionId(name);
        std::size_t deleted = countResult ? static_cast<std::size_t>(countResult.value()) : 0;

        remove(name);

        json idx = load_json(index_path());
        if (idx.contains("current") && idx["current"] == name) {
            idx.erase("current");
            save_json(index_path(), idx);
        }

        return deleted;
    }

    std::size_t getDocumentCount(const std::string& name) const override {
        if (!ctx_ || !ctx_->metadataRepo)
            return 0;

        auto countResult = ctx_->metadataRepo->countDocumentsBySessionId(name);
        return countResult ? static_cast<std::size_t>(countResult.value()) : 0;
    }

private:
    const AppContext* ctx_{nullptr};
};

std::shared_ptr<ISessionService> makeSessionService(const AppContext* ctx) {
    return std::make_shared<SessionService>(ctx);
}

} // namespace yams::app::services
