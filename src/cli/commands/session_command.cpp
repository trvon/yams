#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <string>
#include <vector>

using json = nlohmann::json;

namespace yams::cli {

namespace {
struct PinItem {
    std::string path;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;
};

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

        // session list
        auto* listCmd = cmd->add_subcommand("list", "List pinned items (client-side)");
        listCmd->callback([this]() { this->mode_ = Mode::List; });

        // session pin --path PATTERN [--tag t]* [--meta k=v]*
        auto* pinCmd =
            cmd->add_subcommand("pin", "Pin items by path pattern; add 'pinned' tag in repository");
        pinCmd->add_option("--path", pinPath_, "Path or glob pattern to pin")->required();
        pinCmd->add_option("--tag", pinTags_, "Tags to attach when pinning")->take_all();
        pinCmd->add_option("--meta", pinMetaPairs_, "Metadata pairs key=value")->take_all();
        pinCmd->callback([this]() { this->mode_ = Mode::Pin; });

        // session unpin --path PATTERN
        auto* unpinCmd =
            cmd->add_subcommand("unpin", "Unpin items by path pattern; remove 'pinned' tag");
        unpinCmd->add_option("--path", pinPath_, "Path or glob pattern to unpin")->required();
        unpinCmd->callback([this]() { this->mode_ = Mode::Unpin; });

        // session warm: retrieve pinned docs to warm caches
        auto* warmCmd =
            cmd->add_subcommand("warm", "Warm pinned items (metadata/snippets) for fast retrieval");
        warmCmd->callback([this]() { this->mode_ = Mode::Warm; });
    }

    Result<void> execute() override {
        switch (mode_) {
            case Mode::List:
                return doList();
            case Mode::Pin:
                return doPin(true);
            case Mode::Unpin:
                return doPin(false);
            case Mode::Warm:
                return doWarm();
            default:
                return Result<void>(Error{ErrorCode::InvalidArgument, "No session subcommand"});
        }
    }

private:
    enum class Mode { None, List, Pin, Unpin, Warm };

    Result<void> doList() {
        auto pins = loadPins();
        if (pins.empty()) {
            std::cout << "No pinned items." << std::endl;
            return Result<void>();
        }
        for (const auto& p : pins) {
            std::cout << p.path;
            if (!p.tags.empty()) {
                std::cout << " [tags:";
                for (size_t i = 0; i < p.tags.size(); ++i) {
                    std::cout << (i ? "," : "") << p.tags[i];
                }
                std::cout << "]";
            }
            if (!p.metadata.empty()) {
                std::cout << " [meta:";
                bool first = true;
                for (const auto& kv : p.metadata) {
                    std::cout << (first ? "" : ";") << kv.first << "=" << kv.second;
                    first = false;
                }
                std::cout << "]";
            }
            std::cout << "\n";
        }
        return Result<void>();
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
        auto pins = loadPins();
        if (pins.empty()) {
            std::cout << "No pinned items to warm." << std::endl;
            return Result<void>();
        }
        auto appContext = cli_->getAppContext();
        if (!appContext) {
            return Result<void>(Error{ErrorCode::NotInitialized, "CLI context not initialized"});
        }
        auto doc = app::services::makeDocumentService(*appContext);
        if (!doc) {
            return Result<void>(Error{ErrorCode::NotInitialized, "Document service not available"});
        }
        std::size_t warmed = 0;
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
        for (const auto& p : pins) {
            app::services::ListDocumentsRequest lreq;
            std::string norm = normalizePathPattern(p.path);
            lreq.pattern = norm;
            lreq.limit = 1000;
            lreq.showSnippets = true;
            lreq.snippetLength = 120;
            auto lres = doc->list(lreq);
            if (!lres)
                continue;
            for (const auto& d : lres.value().documents) {
                // Read a small cat to ensure extraction path is exercised
                app::services::CatDocumentRequest creq;
                creq.name = d.name; // resolve by name
                auto catRes = doc->cat(creq);
                (void)catRes; // warming side-effect
                ++warmed;
            }
        }
        std::cout << "Warmed " << warmed << " document(s)." << std::endl;
        return Result<void>();
    }

    YamsCLI* cli_ = nullptr;
    Mode mode_{Mode::None};
    std::string pinPath_;
    std::vector<std::string> pinTags_;
    std::vector<std::string> pinMetaPairs_;
};
} // namespace

// Factory
std::unique_ptr<ICommand> createSessionCommand() {
    return std::make_unique<SessionCommand>();
}

} // namespace yams::cli
