#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/core/uuid.h>
#include <yams/daemon/client/daemon_client.h>

#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <optional>
#include <sstream>
#include <string>
#include <thread>

namespace yams::cli {

namespace fs = std::filesystem;

namespace {
static bool envTruthy(const char* val) {
    if (!val || !*val)
        return false;
    std::string v(val);
    std::transform(v.begin(), v.end(), v.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return v == "1" || v == "true" || v == "yes" || v == "on";
}

static fs::path findGitRoot(const fs::path& start) {
    std::error_code ec;
    fs::path cur = fs::absolute(start, ec);
    if (ec)
        cur = start;
    while (!cur.empty()) {
        auto candidate = cur / ".git";
        if (fs::exists(candidate, ec)) {
            return cur;
        }
        auto parent = cur.parent_path();
        if (parent == cur)
            break;
        cur = parent;
    }
    return {};
}

static std::string sanitizeName(std::string s) {
    if (s.empty())
        return "project";
    for (auto& c : s) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_')) {
            c = '-';
        } else {
            c = static_cast<char>(std::tolower(c));
        }
    }
    return s;
}

} // namespace

class WatchCommand final : public ICommand {
public:
    std::string getName() const override { return "watch"; }
    std::string getDescription() const override {
        return "Enable session-based auto-ingest for a project";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand("watch", getDescription());
        cmd->add_flag("--start", start_, "Enable watch (default)");
        cmd->add_flag("--stop", stop_, "Disable watch");
        cmd->add_option("action", action_, "Action: start|stop")
            ->check(CLI::IsMember({"start", "stop"}));
        cmd->add_option("--interval", intervalMs_, "Polling interval in ms (set)");
        cmd->add_option("--root", rootPath_, "Project root to watch (default: git root or cwd)");
        cmd->add_option("--session", sessionName_, "Session name (default: auto)");
        cmd->add_flag("--no-use", noUse_, "Do not set the session as current");
        cmd->add_flag("--no-selector", noSelector_, "Do not add root selector to the session");
        cmd->add_option("--daemon-ready-timeout-ms", daemonReadyTimeoutMs_,
                        "Max time to wait for daemon readiness (ms, 0 to skip)")
            ->default_val(10000);

        cmd->callback([this]() { cli_->setPendingCommand(this); });
    }

    Result<void> execute() override {
        if (!action_.empty()) {
            if (action_ == "start") {
                start_ = true;
            } else if (action_ == "stop") {
                stop_ = true;
            }
        }

        if (stop_ && start_) {
            return Error{ErrorCode::InvalidArgument, "Specify only one of --start/--stop"};
        }

        auto svc = app::services::makeSessionService(nullptr);
        if (!svc) {
            return Error{ErrorCode::NotInitialized, "Session service not available"};
        }

        if (envTruthy(std::getenv("YAMS_DISABLE_PROJECT_SESSION"))) {
            return Error{ErrorCode::InvalidState,
                         "Project sessions are disabled (YAMS_DISABLE_PROJECT_SESSION=1)"};
        }

        std::error_code ec;
        fs::path cwd = fs::current_path(ec);
        if (ec)
            return Error{ErrorCode::InvalidArgument, "Failed to resolve current working directory"};

        fs::path root;
        if (!rootPath_.empty()) {
            root = fs::path(rootPath_);
        } else {
            root = findGitRoot(cwd);
            if (root.empty())
                root = cwd;
        }
        auto absRoot = fs::absolute(root, ec);
        if (!ec)
            root = absRoot;
        const std::string rootStr = root.string();

        std::string targetSession;
        if (!sessionName_.empty()) {
            targetSession = sessionName_;
        } else if (const char* envSession = std::getenv("YAMS_SESSION_CURRENT");
                   envSession && *envSession) {
            targetSession = envSession;
        } else {
            std::string base = root.filename().string();
            if (base.empty())
                base = "project";
            targetSession = "proj-" + sanitizeName(base) + "-" + yams::core::shortHash(rootStr);
        }

        if (!svc->exists(targetSession)) {
            if (stop_) {
                return Error{ErrorCode::NotFound, "Session not found: " + targetSession};
            }
            svc->init(targetSession, "auto: " + rootStr);
        }

        if (!stop_) {
            yams::daemon::ClientConfig cfg;
            if (cli_->hasExplicitDataDir()) {
                cfg.dataDir = cli_->getDataPath();
            }

            const auto timeout = std::chrono::milliseconds(std::max(0, daemonReadyTimeoutMs_));
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_policy(
                cfg, yams::cli::CliDaemonAccessPolicy::RequireSocket, 1, 1, timeout);
            if (!leaseRes) {
                return Error{ErrorCode::InternalError,
                             std::string("Failed to initialize daemon watch backend: ") +
                                 leaseRes.error().message};
            }

            auto daemonLease = std::move(leaseRes.value());

            if (daemonReadyTimeoutMs_ > 0) {
                auto& client = **daemonLease.lease;
                const auto deadline = std::chrono::steady_clock::now() + timeout;
                auto sleepFor = std::chrono::milliseconds(100);
                while (std::chrono::steady_clock::now() < deadline) {
                    auto statusRes = yams::cli::run_result<yams::daemon::StatusResponse>(
                        client.status(), std::chrono::milliseconds(1500));
                    if (statusRes && statusRes.value().ready) {
                        break;
                    }
                    auto now = std::chrono::steady_clock::now();
                    if (now >= deadline) {
                        return Error{ErrorCode::Timeout, "Daemon did not become ready in time"};
                    }
                    auto remaining =
                        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
                    std::this_thread::sleep_for(std::min(sleepFor, remaining));
                    sleepFor = std::min(sleepFor * 2, std::chrono::milliseconds(1000));
                }
            }
        }

        std::optional<std::string> previousSession;
        bool changedCurrent = false;
        auto ensureCurrent = [&]() {
            if (!noUse_) {
                svc->use(targetSession);
                changedCurrent = true;
                return;
            }
            previousSession = svc->current();
            if (!previousSession || *previousSession != targetSession) {
                svc->use(targetSession);
                changedCurrent = true;
            }
        };

        if (intervalMs_ > 0)
            svc->setWatchIntervalMs(intervalMs_, targetSession);

        if (!stop_) {
            if (!noSelector_) {
                ensureCurrent();
                auto selectors = svc->listPathSelectors(targetSession);
                if (std::find(selectors.begin(), selectors.end(), rootStr) == selectors.end()) {
                    svc->addPathSelector(rootStr, {}, {});
                }
            } else if (!noUse_) {
                ensureCurrent();
            }
            svc->enableWatch(true, targetSession);
        } else {
            if (!noUse_) {
                ensureCurrent();
            }
            svc->enableWatch(false, targetSession);
        }

        if (noUse_ && changedCurrent) {
            if (previousSession && *previousSession != targetSession) {
                svc->use(*previousSession);
            } else if (!previousSession) {
                svc->close();
            }
        }

        bool enabled = svc->watchEnabled(targetSession);
        uint32_t curMs = svc->watchIntervalMs(targetSession);
        std::cout << "Watch: " << (enabled ? "enabled" : "disabled") << ", interval=" << curMs
                  << " ms, session='" << targetSession << "', root='" << rootStr << "'\n";
        return {};
    }

private:
    YamsCLI* cli_{nullptr};
    bool start_{false};
    bool stop_{false};
    bool noUse_{false};
    bool noSelector_{false};
    uint32_t intervalMs_{0};
    int daemonReadyTimeoutMs_{10000};
    std::string rootPath_;
    std::string sessionName_;
    std::string action_;
};

std::unique_ptr<ICommand> createWatchCommand() {
    return std::make_unique<WatchCommand>();
}

} // namespace yams::cli
