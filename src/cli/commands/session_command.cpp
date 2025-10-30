#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/session_store.h>
#include <yams/cli/yams_cli.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

using json = nlohmann::json;

namespace yams::cli {

namespace {
struct PinItem {
    std::string path;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;
};

static std::mutex
    sessionMutex_; // Global mutex for JSON access in multi-threaded contexts (e.g., MCP)
static std::filesystem::path resolveStateFile() {
    const char* xdgState = std::getenv("XDG_STATE_HOME");
    const char* home = std::getenv("HOME");
    std::filesystem::path base;
    if (xdgState && *xdgState) {
        base = std::filesystem::path(xdgState) / "yams";
    } else if (home && *home) {
        base = std::filesystem::path(home) / ".local" / "state" / "yams";
    } else {
        base = std::filesystem::current_path() / ".yams_state";
    }
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    (void)ec;
    return base / "pinned.json";
}

static std::vector<PinItem> loadPins() {
    std::lock_guard<std::mutex> lock(sessionMutex_);
    std::vector<PinItem> out;
    auto f = resolveStateFile();
    if (!std::filesystem::exists(f))
        return out;
    std::ifstream in(f);
    if (!in.good())
        return out;
    try {
        json j;
        in >> j;
        if (j.is_array()) {
            for (const auto& e : j) {
                PinItem it;
                it.path = e.value("path", "");
                if (e.contains("tags"))
                    it.tags = e.at("tags").get<std::vector<std::string>>();
                if (e.contains("metadata"))
                    it.metadata = e.at("metadata").get<std::map<std::string, std::string>>();
                if (!it.path.empty())
                    out.push_back(std::move(it));
            }
        }
    } catch (...) {
        // ignore parse errors
    }
    return out;
}

static void savePins(const std::vector<PinItem>& pins) {
    std::lock_guard<std::mutex> lock(sessionMutex_);
    json j = json::array();
    for (const auto& p : pins) {
        j.push_back({{"path", p.path}, {"tags", p.tags}, {"metadata", p.metadata}});
    }
    std::ofstream out(resolveStateFile());
    out << j.dump(2) << std::endl;
}

class SessionCommand final : public ICommand {
public:
    std::string getName() const override { return "session"; }
    std::string getDescription() const override {
        return "Manage interactive session pins and warming";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand(getName(), getDescription());
        cmd->require_subcommand();

        // Quick, pipe-friendly help examples for Unix ergonomics
        cmd->footer(R"(Examples:
  # Create and use a session
  yams session start mywork --desc "PBI-008-11"
  yams session use mywork

  # Add selectors and warm a small cache
  yams session add --path "src/**/*.cpp"
  yams session warm --limit 200

  # Scope search/list to current session by default
  yams search -q "allocator leak"
  yams list --format json

  # Pipe-friendly emitters: compose tags/metadata explicitly
  yams session emit --kind names | xargs -n1 yams update --add-tag pinned
  yams session emit --kind paths --materialized | while read p; do \
    yams update --name "$p" --meta session=mywork; \
  done

  # Save and load sessions
  yams session save /tmp/mywork.json
  yams session load /tmp/mywork.json --name mywork-restored

Env:
  YAMS_SESSION_CURRENT=<name> selects the default session for this process.)");

        // session lifecycle
        auto* initCmd = cmd->add_subcommand("start", "Start/initialize a named session");
        initCmd->add_option("name", sessionName_, "Session name")->required();
        initCmd->add_option("--desc", sessionDesc_, "Session description");
        initCmd->callback([this]() { this->mode_ = Mode::Init; });

        auto* useCmd = cmd->add_subcommand("use", "Set current session");
        useCmd->add_option("name", sessionName_, "Session name")->required();
        useCmd->callback([this]() { this->mode_ = Mode::Use; });

        auto* lsCmd = cmd->add_subcommand("ls", "List sessions");
        lsCmd->callback([this]() { this->mode_ = Mode::Ls; });

        auto* showCmd = cmd->add_subcommand("show", "Show current session details");
        showCmd->add_flag("--json", jsonOutput_, "JSON output");
        showCmd->callback([this]() { this->mode_ = Mode::Show; });

        auto* rmCmd = cmd->add_subcommand("rm", "Remove a session");
        rmCmd->add_option("name", sessionName_, "Session name")->required();
        rmCmd->callback([this]() { this->mode_ = Mode::Rm; });

        // Working set ops
        auto* addCmd = cmd->add_subcommand("add", "Add selector to current session");
        addCmd->add_option("--path", pinPath_, "Path or glob selector");
        addCmd->add_option("--tag", pinTags_, "Tags")->take_all();
        addCmd->add_option("--meta", pinMetaPairs_, "k=v metadata")->take_all();
        addCmd->callback([this]() { this->mode_ = Mode::Add; });

        auto* rmPathCmd = cmd->add_subcommand("rm-path", "Remove selector from current session");
        rmPathCmd->add_option("--path", pinPath_, "Path or glob selector")->required();
        rmPathCmd->callback([this]() { this->mode_ = Mode::RmPath; });

        auto* listSelCmd = cmd->add_subcommand("list", "List selectors/materialized docs");
        listSelCmd->add_flag("--json", jsonOutput_, "JSON output");
        listSelCmd->callback([this]() { this->mode_ = Mode::List; });

        // session warm
        auto* warmCmd =
            cmd->add_subcommand("warm", "Warm session (budgets supported) for fast retrieval");
        warmCmd->add_option("--limit", warmLimit_, "Max docs to materialize per selector")
            ->default_val(200);
        warmCmd->add_option("--snippet-len", snippetLen_, "Snippet length")->default_val(160);
        warmCmd->add_option("--cores", budgetCores_, "Max CPU cores");
        warmCmd->add_option("--memory-gb", budgetMemGb_, "Max memory in GB");
        warmCmd->add_option("--time-ms", budgetTimeMs_, "Max time in ms");
        warmCmd->add_flag("--aggressive", budgetAggressive_, "Aggressive warming");
        warmCmd->callback([this]() { this->mode_ = Mode::Warm; });

        // Sugar wrappers
        auto* tagsCmd = cmd->add_subcommand("tags", "Add/remove tags on materialized items");
        tagsCmd->add_option("--add", tagAdds_, "Tags to add")->take_all();
        tagsCmd->add_option("--remove", tagRemoves_, "Tags to remove")->take_all();
        tagsCmd->callback([this]() { this->mode_ = Mode::Tags; });

        auto* annCmd = cmd->add_subcommand("annotate", "Add metadata to materialized items (k=v)");
        annCmd->add_option("--meta", annotateMetaPairs_, "k=v pairs")->take_all();
        annCmd->callback([this]() { this->mode_ = Mode::Annotate; });

        // Clear materialized cache only
        auto* clearCmd =
            cmd->add_subcommand("clear", "Clear materialized cache for current session");
        clearCmd->callback([this]() { this->mode_ = Mode::Clear; });

        // Save/load/export/import
        auto* saveCmd = cmd->add_subcommand("save", "Save current session to file");
        saveCmd->add_option("file", ioFile_, "Output file");
        saveCmd->callback([this]() { this->mode_ = Mode::Save; });

        auto* loadCmd = cmd->add_subcommand("load", "Load session from file");
        loadCmd->add_option("file", ioFile_, "Input file")->required();
        loadCmd->add_option("--name", sessionName_, "Target session name (optional)");
        loadCmd->callback([this]() { this->mode_ = Mode::Load; });

        // Emitters (pipe-friendly)
        auto* emitCmd = cmd->add_subcommand("emit", "Emit current session targets for piping");
        emitCmd->add_flag("--json", jsonOutput_, "JSON array output");
        emitCmd->add_flag("--materialized", emitMaterialized_, "Emit from materialized cache");
        emitCmd->add_option("--kind", emitKind_, "names|paths|hashes")
            ->default_val("names")
            ->check(CLI::IsMember({"names", "paths", "hashes"}));
        emitCmd->callback([this]() { this->mode_ = Mode::Emit; });

        // Watch controls (Phase 1: config only; daemon monitor to consume settings)
        auto* watchCmd = cmd->add_subcommand("watch", "Configure session auto-ingest watch");
        bool start = false, stop = false;
        std::string whichSession;
        uint32_t intervalMs = 0;
        watchCmd->add_flag("--start", start, "Enable watch for the session");
        watchCmd->add_flag("--stop", stop, "Disable watch for the session");
        watchCmd->add_option("--interval", intervalMs, "Polling interval in ms (set)");
        watchCmd->add_option("--session", whichSession, "Target session (default: current)");
        watchCmd->callback([this, &start, &stop, &whichSession, &intervalMs]() {
            auto svc = sessionSvc();
            if (!svc) {
                std::cout << "Session service unavailable\n";
                return;
            }
            std::optional<std::string> name =
                whichSession.empty() ? std::nullopt : std::optional<std::string>(whichSession);
            if (intervalMs > 0)
                svc->setWatchIntervalMs(intervalMs, name);
            if (start && stop) {
                std::cout << "Specify only one of --start/--stop\n";
                return;
            }
            if (start)
                svc->enableWatch(true, name);
            else if (stop)
                svc->enableWatch(false, name);
            // Status output
            bool enabled = svc->watchEnabled(name);
            uint32_t curMs = svc->watchIntervalMs(name);
            auto sess = name ? *name : (svc->current().value_or(std::string{"(none)"}));
            std::cout << "Watch: " << (enabled ? "enabled" : "disabled") << ", interval=" << curMs
                      << " ms, session='" << sess << "'\n";
        });

        // Diff: expose tree diff for latest snapshots of a pinned directory
        auto* diffCmd =
            cmd->add_subcommand("diff", "Show tree diff for session (latest snapshots)");
        std::string baseSnap, targetSnap, dirPrefix, typeFilter;
        bool jsonOut = false;
        diffCmd->add_option("--base", baseSnap, "Base snapshot id");
        diffCmd->add_option("--target", targetSnap, "Target snapshot id");
        diffCmd->add_option("--dir", dirPrefix,
                            "Pinned directory to use (defaults to first pinned)");
        diffCmd->add_option("--type", typeFilter, "Filter: added|modified|deleted|renamed");
        diffCmd->add_flag("--json", jsonOut, "JSON output");
        diffCmd->callback([this, &baseSnap, &targetSnap, &dirPrefix, &typeFilter, &jsonOut]() {
            auto appContext = cli_->getAppContext();
            if (!appContext || !appContext->metadataRepo) {
                std::cout << "Metadata repository unavailable\n";
                return;
            }
            auto svc = sessionSvc();
            if (!svc) {
                std::cout << "Session service unavailable\n";
                return;
            }
            auto cur = svc->current();
            if (!cur) {
                std::cout << "No current session\n";
                return;
            }
            // Determine directory to scope by: first pinned selector that is a directory path
            if (dirPrefix.empty()) {
                for (const auto& sel : svc->listPathSelectors(*cur)) {
                    std::error_code ec;
                    std::filesystem::path p(sel);
                    if (std::filesystem::is_directory(p, ec)) {
                        dirPrefix = p.string();
                        break;
                    }
                }
            }
            // Resolve snapshots if not specified: pick latest two for this directory
            if (baseSnap.empty() || targetSnap.empty()) {
                auto snaps = appContext->metadataRepo->listTreeSnapshots(100);
                if (!snaps || snaps.value().empty()) {
                    std::cout << "No snapshots found\n";
                    return;
                }
                // Filter by directory_path match when provided
                std::vector<yams::metadata::TreeSnapshotRecord> filtered;
                for (const auto& s : snaps.value()) {
                    auto it = s.metadata.find("directory_path");
                    if (!dirPrefix.empty()) {
                        if (it != s.metadata.end() && it->second == dirPrefix)
                            filtered.push_back(s);
                    } else {
                        filtered.push_back(s);
                    }
                }
                if (filtered.size() < 2) {
                    std::cout << "Need at least two snapshots\n";
                    return;
                }
                // Sort by createdTime descending
                std::sort(filtered.begin(), filtered.end(), [](const auto& a, const auto& b) {
                    return a.createdTime > b.createdTime;
                });
                if (targetSnap.empty())
                    targetSnap = filtered[0].snapshotId;
                if (baseSnap.empty())
                    baseSnap = filtered[1].snapshotId;
            }
            yams::metadata::TreeDiffQuery q;
            q.baseSnapshotId = baseSnap;
            q.targetSnapshotId = targetSnap;
            if (!dirPrefix.empty())
                q.pathPrefix = dirPrefix;
            if (!typeFilter.empty()) {
                auto toLower = [](std::string s) {
                    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
                    return s;
                };
                auto tf = toLower(typeFilter);
                if (tf == "added")
                    q.typeFilter = yams::metadata::TreeChangeType::Added;
                else if (tf == "modified")
                    q.typeFilter = yams::metadata::TreeChangeType::Modified;
                else if (tf == "deleted")
                    q.typeFilter = yams::metadata::TreeChangeType::Deleted;
                else if (tf == "renamed")
                    q.typeFilter = yams::metadata::TreeChangeType::Renamed;
            }
            q.limit = 1000;
            q.offset = 0;
            auto res = appContext->metadataRepo->listTreeChanges(q);
            if (!res) {
                std::cout << "Tree diff error: " << res.error().message << "\n";
                return;
            }
            if (jsonOut) {
                nlohmann::json arr = nlohmann::json::array();
                for (const auto& c : res.value()) {
                    std::string ct;
                    switch (c.type) {
                        case yams::metadata::TreeChangeType::Added:
                            ct = "added";
                            break;
                        case yams::metadata::TreeChangeType::Deleted:
                            ct = "deleted";
                            break;
                        case yams::metadata::TreeChangeType::Modified:
                            ct = "modified";
                            break;
                        case yams::metadata::TreeChangeType::Renamed:
                            ct = "renamed";
                            break;
                        default:
                            ct = "unknown";
                            break;
                    }
                    nlohmann::json j{{"type", ct},
                                     {"path", c.newPath},
                                     {"old_path", c.oldPath},
                                     {"hash", c.newHash},
                                     {"old_hash", c.oldHash}};
                    arr.push_back(std::move(j));
                }
                std::cout << arr.dump(2) << std::endl;
            } else {
                for (const auto& c : res.value()) {
                    std::string ct;
                    switch (c.type) {
                        case yams::metadata::TreeChangeType::Added:
                            ct = "+";
                            break;
                        case yams::metadata::TreeChangeType::Deleted:
                            ct = "-";
                            break;
                        case yams::metadata::TreeChangeType::Modified:
                            ct = "~";
                            break;
                        case yams::metadata::TreeChangeType::Renamed:
                            ct = ">";
                            break;
                        default:
                            ct = "?";
                            break;
                    }
                    std::cout << ct << " " << (!c.newPath.empty() ? c.newPath : c.oldPath) << "\n";
                }
            }
        });
    }

    Result<void> execute() override {
        switch (mode_) {
            case Mode::Init:
                return doInit();
            case Mode::Use:
                return doUse();
            case Mode::Ls:
                return doLs();
            case Mode::Show:
                return doShow();
            case Mode::Rm:
                return doRm();
            case Mode::Add:
                return doAdd();
            case Mode::RmPath:
                return doRmPath();
            case Mode::List:
                return doList();
            case Mode::Warm:
                return doWarm();
            case Mode::Clear:
                return doClear();
            case Mode::Save:
                return doSave();
            case Mode::Load:
                return doLoad();
            case Mode::Emit:
                return doEmit();
            case Mode::Tags:
                return doTags();
            case Mode::Annotate:
                return doAnnotate();
            default:
                return Result<void>(Error{ErrorCode::InvalidArgument, "No session subcommand"});
        }
    }

private:
    enum class Mode {
        None,
        Init,
        Use,
        Ls,
        Show,
        Rm,
        Add,
        RmPath,
        List,
        Warm,
        Clear,
        Save,
        Load,
        Emit,
        Tags,
        Annotate
    };

    std::shared_ptr<app::services::ISessionService> sessionSvc() const {
        auto appContext = cli_->getAppContext();
        return app::services::makeSessionService(appContext.get());
    }

    // Session file helpers
    static std::filesystem::path sessionsDir() { return yams::cli::session_store::sessions_dir(); }
    static std::filesystem::path indexPath() { return yams::cli::session_store::index_path(); }
    static std::optional<std::string> currentSession() {
        if (const char* env = std::getenv("YAMS_SESSION_CURRENT"); env && *env) {
            return std::string(env);
        }
        return yams::cli::session_store::current_session();
    }
    static json loadSessionJson(const std::string& name) {
        json j;
        auto p = sessionsDir() / (name + ".json");
        if (std::filesystem::exists(p)) {
            try {
                std::ifstream in(p);
                if (in.good())
                    in >> j;
            } catch (...) {
            }
        }
        return j;
    }
    static void saveSessionJson(const std::string& name, const json& j) {
        auto p = sessionsDir() / (name + ".json");
        std::ofstream out(p);
        out << j.dump(2) << std::endl;
    }
    static void setCurrent(const std::string& name) {
        std::lock_guard<std::mutex> lock(sessionMutex_);
        json idx;
        auto ip = indexPath();
        if (std::filesystem::exists(ip)) {
            try {
                std::ifstream in(ip);
                if (in.good())
                    in >> idx;
            } catch (...) {
            }
        }
        idx["current"] = name;
        std::ofstream out(ip);
        out << idx.dump(2) << std::endl;
    }

    Result<void> doInit() {
        if (sessionName_.empty())
            return Error{ErrorCode::InvalidArgument, "Session name required"};
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        svc->init(sessionName_, sessionDesc_);
        std::cout << "Initialized session: " << sessionName_ << std::endl;
        return {};
    }

    Result<void> doUse() {
        if (sessionName_.empty())
            return Error{ErrorCode::InvalidArgument, "Name required"};
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        if (!svc->exists(sessionName_))
            return Error{ErrorCode::NotFound, "Session not found"};
        svc->use(sessionName_);
        std::cout << "Using session: " << sessionName_ << std::endl;
        return {};
    }

    Result<void> doLs() {
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        auto cur = svc->current().value_or("");
        auto names = svc->listSessions();
        if (names.empty()) {
            std::cout << "(no sessions)\n";
        } else {
            for (const auto& n : names) {
                std::cout << (n == cur ? "* " : "  ") << n << std::endl;
            }
        }
        return {};
    }

    Result<void> doShow() {
        auto svc = sessionSvc();
        auto cur = svc ? svc->current() : std::optional<std::string>{};
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        auto j = loadSessionJson(*cur);
        if (jsonOutput_) {
            std::cout << j.dump(2) << std::endl;
        } else {
            std::cout << "Session: " << *cur << std::endl;
            size_t selCount = svc ? svc->listPathSelectors(*cur).size()
                                  : (j.contains("selectors") ? j["selectors"].size() : 0);
            auto mat =
                svc ? svc->listMaterialized(*cur) : std::vector<app::services::MaterializedItem>{};
            std::cout << "Selectors: " << selCount << std::endl;
            std::cout << "Materialized: " << mat.size() << std::endl;
        }
        return {};
    }

    Result<void> doRm() {
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        if (!svc->exists(sessionName_))
            return Error{ErrorCode::NotFound, "Not found"};
        svc->remove(sessionName_);
        std::cout << "Removed session: " << sessionName_ << std::endl;
        return {};
    }

    Result<void> doAdd() {
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        auto cur = svc->current();
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        std::vector<std::pair<std::string, std::string>> metaPairs;
        for (const auto& p : pinMetaPairs_) {
            auto pos = p.find('=');
            if (pos != std::string::npos)
                metaPairs.emplace_back(p.substr(0, pos), p.substr(pos + 1));
        }
        if (pinPath_.empty())
            return Error{ErrorCode::InvalidArgument, "--path required"};
        svc->addPathSelector(pinPath_, pinTags_, metaPairs);
        std::cout << "Added selector to session '" << *cur << "'\n";
        return {};
    }

    Result<void> doRmPath() {
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        auto cur = svc->current();
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        if (pinPath_.empty())
            return Error{ErrorCode::InvalidArgument, "--path required"};
        svc->removePathSelector(pinPath_);
        std::cout << "Removed selector from session '" << *cur << "'\n";
        return {};
    }

    Result<void> doList() {
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        auto cur = svc->current();
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        if (jsonOutput_) {
            auto j = loadSessionJson(*cur);
            std::cout << j.dump(2) << std::endl;
            return {};
        }
        std::cout << "Session '" << *cur << "'\n";
        auto selectors = svc->listPathSelectors(*cur);
        if (selectors.empty()) {
            std::cout << "(no selectors)\n";
        } else {
            for (const auto& s : selectors) {
                std::cout << "- selector: " << s << "\n";
            }
        }
        auto j = loadSessionJson(*cur);
        if (j.contains("materialized")) {
            std::cout << "Materialized: " << j["materialized"].size() << "\n";
        }
        return {};
    }

    Result<void> doPin(bool add) {
        if (pinPath_.empty()) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "--path is required"});
        }

        // Normalize path to absolute to satisfy daemon (expand ~, make absolute, normalize slashes)
        auto normalizePathPattern = [](const std::string& input) -> std::string {
            std::string s = input;
            if (!s.empty() && s[0] == '~') {
                const char* home = std::getenv("HOME");
                if (home && *home) {
                    if (s.size() == 1) {
                        s = std::string(home);
                    } else if (s.size() > 1 && (s[1] == '/' || s[1] == '\\')) {
                        s = std::string(home) + s.substr(1);
                    }
                }
            }
#ifdef _WIN32
            std::replace(s.begin(), s.end(), '\\', '/');
#endif
            std::filesystem::path p(s);
            if (p.is_relative()) {
                p = std::filesystem::absolute(p);
            }
            return p.generic_string();
        };
        std::string normPath = normalizePathPattern(pinPath_);

        // Load existing pins and update local list first
        auto pins = loadPins();
        if (add) {
            PinItem it{normPath, pinTags_, {}};
            for (const auto& p : pinMetaPairs_) {
                auto pos = p.find('=');
                if (pos != std::string::npos)
                    it.metadata[p.substr(0, pos)] = p.substr(pos + 1);
            }
            // ensure 'pinned' tag is present
            if (std::find(it.tags.begin(), it.tags.end(), "pinned") == it.tags.end())
                it.tags.push_back("pinned");
            // de-dup by path
            pins.erase(std::remove_if(pins.begin(), pins.end(),
                                      [&](const PinItem& e) { return e.path == it.path; }),
                       pins.end());
            pins.push_back(std::move(it));
        } else {
            pins.erase(std::remove_if(pins.begin(), pins.end(),
                                      [&](const PinItem& e) { return e.path == normPath; }),
                       pins.end());
        }
        savePins(pins);

        // Reflect pins into repository metadata via DocumentService
        auto appContext = cli_->getAppContext();
        if (!appContext) {
            return Result<void>(Error{ErrorCode::NotInitialized, "CLI context not initialized"});
        }
        auto doc = app::services::makeDocumentService(*appContext);
        if (!doc) {
            return Result<void>(Error{ErrorCode::NotInitialized, "Document service not available"});
        }
        // List matching documents by glob-like pattern
        app::services::ListDocumentsRequest lreq;
        lreq.pattern = normPath;
        lreq.limit = 10000; // reasonable cap
        auto lres = doc->list(lreq);
        if (!lres) {
            spdlog::warn("session {}: list failed: {}", add ? "pin" : "unpin",
                         lres.error().message);
        } else {
            std::size_t updated = 0;
            for (const auto& d : lres.value().documents) {
                app::services::UpdateMetadataRequest u;
                u.name = d.name; // update by name
                if (add) {
                    u.addTags.push_back("pinned");
                    for (const auto& p : pinMetaPairs_) {
                        auto pos = p.find('=');
                        if (pos != std::string::npos)
                            u.keyValues[p.substr(0, pos)] = p.substr(pos + 1);
                    }
                    // also set pinned=true metadata
                    u.keyValues["pinned"] = "true";
                } else {
                    u.removeTags.push_back("pinned");
                    u.keyValues["pinned"] = "false";
                }
                auto ur = doc->updateMetadata(u);
                if (ur && ur.value().success)
                    ++updated;
                else if (!ur) {
                    spdlog::debug("updateMetadata failed for {}: {}", d.name, ur.error().message);
                }
            }
            std::cout << (add ? "Pinned " : "Unpinned ") << updated << " item(s)." << std::endl;
        }
        return Result<void>();
    }

    Result<void> doWarm() {
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured)
            return ensured;
        // Attempt daemon offload first; fallback to local
        try {
            yams::daemon::ClientConfig cfg;
            if (cli_ && cli_->hasExplicitDataDir()) {
                cfg.dataDir = cli_->getDataPath();
            }
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg, 1, 1);
            if (leaseRes) {
                auto leaseHandle = std::move(leaseRes.value());
                yams::daemon::PrepareSessionRequest dreq;
                dreq.sessionName = ""; // use current
                dreq.cores = budgetCores_;
                dreq.memoryGb = budgetMemGb_;
                dreq.timeMs = budgetTimeMs_;
                dreq.aggressive = budgetAggressive_;
                dreq.limit = static_cast<std::size_t>(warmLimit_);
                dreq.snippetLen = static_cast<std::size_t>(snippetLen_ > 0 ? snippetLen_ : 160);
                boost::asio::co_spawn(
                    getExecutor(),
                    [leaseHandle, dreq]() mutable -> boost::asio::awaitable<void> {
                        auto& client = **leaseHandle;
                        (void)co_await client.call<yams::daemon::PrepareSessionRequest>(dreq);
                        co_return;
                    },
                    boost::asio::detached);
                std::cout << "Warming (daemon) requested." << std::endl;
                return Result<void>();
            }
        } catch (...) {
            // ignore and fallback
        }
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        app::services::PrepareBudget b;
        b.maxCores = budgetCores_;
        b.maxMemoryGb = budgetMemGb_;
        b.maxTimeMs = budgetTimeMs_;
        b.aggressive = budgetAggressive_;
        std::size_t count =
            svc->prepare(b, static_cast<std::size_t>(warmLimit_),
                         static_cast<std::size_t>(snippetLen_ > 0 ? snippetLen_ : 160));
        std::cout << "Warmed (local) " << count << " document(s)." << std::endl;
        return Result<void>();
    }

    Result<void> doTags() {
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured)
            return ensured;
        auto appContext = cli_->getAppContext();
        auto doc = app::services::makeDocumentService(*appContext);
        if (!doc)
            return Error{ErrorCode::NotInitialized, "Document service not available"};
        auto cur = currentSession();
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        auto j = loadSessionJson(*cur);
        if (!j.contains("materialized") || !j["materialized"].is_array()) {
            std::cout << "No materialized items to tag." << std::endl;
            return Result<void>();
        }
        std::size_t updated = 0;
        for (const auto& m : j["materialized"]) {
            app::services::UpdateMetadataRequest u;
            if (m.contains("name"))
                u.name = m.at("name").get<std::string>();
            if (m.contains("hash") && u.name.empty())
                u.hash = m.at("hash").get<std::string>();
            u.addTags = tagAdds_;
            u.removeTags = tagRemoves_;
            auto ur = doc->updateMetadata(u);
            if (ur && ur.value().success)
                ++updated;
        }
        std::cout << "Tagged " << updated << " item(s)." << std::endl;
        return Result<void>();
    }

    Result<void> doAnnotate() {
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured)
            return ensured;
        auto appContext = cli_->getAppContext();
        auto doc = app::services::makeDocumentService(*appContext);
        if (!doc)
            return Error{ErrorCode::NotInitialized, "Document service not available"};
        auto cur = currentSession();
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        auto j = loadSessionJson(*cur);
        if (!j.contains("materialized") || !j["materialized"].is_array()) {
            std::cout << "No materialized items to annotate." << std::endl;
            return Result<void>();
        }
        std::size_t updated = 0;
        for (const auto& m : j["materialized"]) {
            app::services::UpdateMetadataRequest u;
            if (m.contains("name"))
                u.name = m.at("name").get<std::string>();
            if (m.contains("hash") && u.name.empty())
                u.hash = m.at("hash").get<std::string>();
            for (const auto& p : annotateMetaPairs_) {
                auto pos = p.find('=');
                if (pos != std::string::npos)
                    u.keyValues[p.substr(0, pos)] = p.substr(pos + 1);
            }
            auto ur = doc->updateMetadata(u);
            if (ur && ur.value().success)
                ++updated;
        }
        std::cout << "Annotated " << updated << " item(s)." << std::endl;
        return Result<void>();
    }

    Result<void> doClear() {
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        auto cur = svc->current();
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        svc->clearMaterialized();
        std::cout << "Cleared materialized cache for '" << *cur << "'\n";
        return {};
    }

    Result<void> doSave() {
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        auto cur = svc->current();
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        std::optional<std::filesystem::path> out;
        if (!ioFile_.empty())
            out = std::filesystem::path(ioFile_);
        svc->save(out);
        std::cout << "Saved session to "
                  << (out ? out->string() : (sessionsDir() / (*cur + ".json")).string())
                  << std::endl;
        return {};
    }

    Result<void> doLoad() {
        if (ioFile_.empty())
            return Error{ErrorCode::InvalidArgument, "File required"};
        auto svc = sessionSvc();
        if (!svc)
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        std::string name = !sessionName_.empty() ? sessionName_ : std::string();
        svc->load(std::filesystem::path(ioFile_),
                  name.empty() ? std::nullopt : std::optional<std::string>(name));
        std::cout << "Loaded session as '"
                  << (name.empty() ? svc->current().value_or("current") : name) << "'\n";
        return {};
    }

    Result<void> doEmit() {
        auto svc = sessionSvc();
        auto cur = svc ? svc->current() : std::optional<std::string>{};
        if (!cur)
            return Error{ErrorCode::InvalidArgument, "No current session"};
        std::vector<std::string> out;

        if (emitMaterialized_) {
            auto items = svc->listMaterialized(*cur);
            for (const auto& m : items) {
                if (emitKind_ == "paths")
                    out.push_back(m.path);
                else if (emitKind_ == "hashes")
                    out.push_back(m.hash);
                else {
                    if (!m.name.empty())
                        out.push_back(m.name);
                    else if (!m.path.empty())
                        out.push_back(m.path);
                }
            }
        } else {
            auto selectors = svc->listPathSelectors(*cur);
            if (emitKind_ == "paths") {
                out = std::move(selectors);
            }
        }

        if (jsonOutput_) {
            json arr = json::array();
            for (const auto& v : out)
                arr.push_back(v);
            std::cout << arr.dump(2) << std::endl;
        } else {
            for (const auto& v : out)
                std::cout << v << "\n";
        }
        return {};
    }

    YamsCLI* cli_ = nullptr;
    Mode mode_{Mode::None};
    // lifecycle fields
    std::string sessionName_;
    std::string sessionDesc_;
    std::string ioFile_;
    bool jsonOutput_{false};
    int warmLimit_{200};
    int snippetLen_{160};
    // prepare budgets
    int budgetCores_{-1};
    int budgetMemGb_{-1};
    long budgetTimeMs_{-1};
    bool budgetAggressive_{false};
    std::string pinPath_;
    std::vector<std::string> pinTags_;
    std::vector<std::string> pinMetaPairs_;
    // sugar and emit options
    std::vector<std::string> tagAdds_;
    std::vector<std::string> tagRemoves_;
    std::vector<std::string> annotateMetaPairs_;
    // emit options
    bool emitMaterialized_{false};
    std::string emitKind_{"names"};
};
} // namespace

// Factory
std::unique_ptr<ICommand> createSessionCommand() {
    return std::make_unique<SessionCommand>();
}

} // namespace yams::cli
