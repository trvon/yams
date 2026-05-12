#include <yams/cli/doctor/checks/daemon_check.h>

#include <yams/cli/daemon_helpers.h>
#include <yams/cli/doctor/plugin_trust.h>
#include <yams/cli/doctor/rendering/render.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>

#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace yams::cli::doctor {

void DaemonCheck::execute(std::ostream& os, YamsCLI* cli,
                          std::optional<daemon::StatusResponse>& cachedStatus) {
    using namespace yams::daemon;
    try {
        std::string effectiveSocket = YamsCLI::resolveConfiguredDaemonSocketPath().string();

        auto printTrustDiagnostics = [&os, cli]() {
            try {
                auto trustRootsFromDaemon =
                    yams::cli::doctor::PluginTrust::fetchTrustedRootsFromDaemon(cli);
                bool usedDaemonTrust = trustRootsFromDaemon.has_value();

                std::vector<std::filesystem::path> trustedRoots;
                if (trustRootsFromDaemon) {
                    trustedRoots =
                        yams::cli::doctor::PluginTrust::dedupeRoots(*trustRootsFromDaemon);
                } else {
                    auto localTrusted = yams::cli::doctor::PluginTrust::readTrusted();
                    trustedRoots.assign(localTrusted.begin(), localTrusted.end());
                }

                bool strictMode = yams::cli::doctor::PluginTrust::resolveStrictPluginDirMode();
                auto defaultRoots =
                    yams::cli::doctor::PluginTrust::getDefaultPluginRoots(strictMode);
                auto checks = yams::cli::doctor::PluginTrust::assessTrustedRoots(
                    trustedRoots, strictMode, defaultRoots);

                std::size_t problematic = 0;
                for (const auto& check : checks) {
                    bool hasProblem = std::any_of(
                        check.issues.begin(), check.issues.end(), [](const std::string& issue) {
                            return issue == "missing" || issue == "temporary-path" ||
                                   issue == "build-artifact-path";
                        });
                    if (hasProblem) {
                        ++problematic;
                    }
                }

                DoctorRender::printHeader(os, "Plugin Trust");
                std::vector<yams::cli::ui::Row> trustRows;
                trustRows.push_back(
                    {"Trust Source", usedDaemonTrust ? "daemon" : "local trust file fallback", ""});
                trustRows.push_back(
                    {"Trust File", yams::config::get_daemon_plugin_trust_file().string(), ""});
                trustRows.push_back({"Strict Mode", strictMode ? "on" : "off", ""});

                std::string trustSummary = std::to_string(trustedRoots.size()) + " root(s)";
                if (problematic > 0) {
                    trustSummary += " (" + std::to_string(problematic) + " problematic)";
                }
                trustRows.push_back({"Trusted Roots", trustSummary, ""});
                yams::cli::ui::render_rows(os, trustRows);

                if (!checks.empty()) {
                    os << "\n";
                    for (const auto& check : checks) {
                        const bool hasIssues = !check.issues.empty();
                        const bool hasProblem = std::any_of(
                            check.issues.begin(), check.issues.end(), [](const std::string& issue) {
                                return issue == "missing" || issue == "temporary-path" ||
                                       issue == "build-artifact-path";
                            });
                        const std::string mark =
                            hasProblem
                                ? yams::cli::ui::colorize("⚠", yams::cli::ui::Ansi::YELLOW)
                                : (hasIssues
                                       ? yams::cli::ui::colorize("i", yams::cli::ui::Ansi::CYAN)
                                       : yams::cli::ui::colorize("✓", yams::cli::ui::Ansi::GREEN));
                        os << "  " << mark << " " << check.path.string();
                        if (hasIssues) {
                            os << " [";
                            for (size_t i = 0; i < check.issues.size(); ++i) {
                                if (i) {
                                    os << ", ";
                                }
                                os << check.issues[i];
                            }
                            os << "]";
                        }
                        os << "\n";
                    }
                }

                if (problematic > 0) {
                    os << "\nCleanup commands:\n";
                    for (const auto& check : checks) {
                        bool hasProblem = std::any_of(
                            check.issues.begin(), check.issues.end(), [](const std::string& issue) {
                                return issue == "missing" || issue == "temporary-path" ||
                                       issue == "build-artifact-path";
                            });
                        if (!hasProblem) {
                            continue;
                        }
                        os << "  yams plugin trust remove \"" << check.path.string() << "\"\n";
                    }
                    os << "  yams plugin trust reset\n";
                    os << "  yams plugin trust status\n";
                }
            } catch (...) {
            }
        };

        if (!cachedStatus) {
            if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                DoctorRender::printHeader(os, "Daemon Health");
                os << yams::cli::ui::colorize("✗ UNAVAILABLE", yams::cli::ui::Ansi::RED)
                   << " - Daemon not running on socket: " << effectiveSocket << "\n";
                os << "\n"
                   << yams::cli::ui::colorize("Hint: Start the daemon with 'yams daemon start'",
                                              yams::cli::ui::Ansi::DIM)
                   << "\n";
                printTrustDiagnostics();
                return;
            }
        }

        if (!cachedStatus) {
            yams::daemon::ClientConfig cfg;
            if (cli && cli->hasExplicitDataDir()) {
                cfg.dataDir = cli->getDataPath();
            }
            cfg.requestTimeout = std::chrono::milliseconds(10000);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes) {
                DoctorRender::printHeader(os, "Daemon Health");
                os << yams::cli::ui::colorize("✗ UNAVAILABLE", yams::cli::ui::Ansi::RED) << " - "
                   << leaseRes.error().message << "\n";
                printTrustDiagnostics();
                return;
            }
            auto leaseHandle = std::move(leaseRes.value());
            auto& clientRef = **leaseHandle;

            auto sres =
                yams::cli::run_result<StatusResponse>(clientRef.status(), std::chrono::seconds(10));
            if (!sres) {
                DoctorRender::printHeader(os, "Daemon Health");
                os << yams::cli::ui::colorize("✗ Failed to get status", yams::cli::ui::Ansi::RED)
                   << " - " << sres.error().message << "\n";
                printTrustDiagnostics();
                return;
            }
            cachedStatus = std::move(sres.value());
        }

        try {
            namespace fs = std::filesystem;
            fs::path dbPath = cli ? (cli->getDataPath() / "yams.db") : fs::path();
            if (!dbPath.empty()) {
                sqlite3* db = nullptr;
                if (sqlite3_open(dbPath.string().c_str(), &db) == SQLITE_OK) {
                    auto queryScalar = [&](const char* sql) -> std::optional<std::string> {
                        sqlite3_stmt* stmt = nullptr;
                        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
                            return std::nullopt;
                        }
                        std::optional<std::string> out{};
                        if (sqlite3_step(stmt) == SQLITE_ROW) {
                            const unsigned char* txt = sqlite3_column_text(stmt, 0);
                            if (txt) {
                                out = std::string(reinterpret_cast<const char*>(txt));
                            }
                        }
                        sqlite3_finalize(stmt);
                        return out;
                    };

                    sqlite3_stmt* st = nullptr;
                    bool haveFail = false;
                    if (sqlite3_prepare_v2(db,
                                           "SELECT version,name,error FROM migration_history "
                                           "WHERE success=0 ORDER BY applied_at DESC LIMIT 3",
                                           -1, &st, nullptr) == SQLITE_OK) {
                        while (sqlite3_step(st) == SQLITE_ROW) {
                            if (!haveFail) {
                                DoctorRender::printHeader(os, "Database Migrations");
                                haveFail = true;
                            }
                            int ver = sqlite3_column_int(st, 0);
                            const unsigned char* n = sqlite3_column_text(st, 1);
                            const unsigned char* e = sqlite3_column_text(st, 2);
                            os << yams::cli::ui::status_error("version=" + std::to_string(ver) +
                                                              ", name='" +
                                                              (n ? (const char*)n : "?") + "'");
                            if (e && *e) {
                                os << ", error=\"" << (const char*)e << "\"";
                            }
                            os << "\n";
                        }
                        sqlite3_finalize(st);
                    }
                    if (auto ftsNew =
                            queryScalar("SELECT name FROM sqlite_master WHERE type='table' AND "
                                        "name='documents_fts_new'")) {
                        (void)ftsNew;
                        if (haveFail == false) {
                            DoctorRender::printHeader(os, "Database Migrations");
                            haveFail = true;
                        }
                        os << yams::cli::ui::status_warning(
                                  "Found leftover 'documents_fts_new' — an FTS migration likely "
                                  "failed mid-way.")
                           << "\n";
                        os << "  Run: yams repair --fts5\n";
                    }
                    if (haveFail) {
                        os << "Hint: try 'yams repair --fts5' to rebuild FTS, or rerun your "
                              "command with '--verbose' for details.\n";
                    }
                    sqlite3_close(db);
                }
            }
        } catch (...) {
        }

        DoctorRender::printHeader(os, "Daemon Health");

        if (!cachedStatus) {
            os << yams::cli::ui::colorize("✗ Failed to get status", yams::cli::ui::Ansi::RED)
               << "\n";
            printTrustDiagnostics();
            return;
        }
        const auto& st = cachedStatus.value();

        std::vector<yams::cli::ui::Row> daemonRows;
        std::string statusDisplay;
        if (st.ready) {
            statusDisplay = yams::cli::ui::colorize("✓ READY", yams::cli::ui::Ansi::GREEN);
        } else {
            statusDisplay = yams::cli::ui::colorize("◷ NOT READY", yams::cli::ui::Ansi::YELLOW);
        }
        daemonRows.push_back({"Status", statusDisplay, ""});

        std::string state = st.lifecycleState.empty() ? st.overallStatus : st.lifecycleState;
        daemonRows.push_back({"State", state, ""});
        daemonRows.push_back({"Version", st.version.empty() ? "unknown" : st.version, ""});
        daemonRows.push_back({"Connections", std::to_string(st.activeConnections), ""});
        yams::cli::ui::render_rows(os, daemonRows);

        os << "\n";
        std::vector<yams::cli::ui::Row> resourceRows;

        try {
            bool vecAvail = false, vecEnabled = false;
            if (auto it = st.readinessStates.find("vector_embeddings_available");
                it != st.readinessStates.end()) {
                vecAvail = it->second;
            }
            if (auto it = st.readinessStates.find("vector_scoring_enabled");
                it != st.readinessStates.end()) {
                vecEnabled = it->second;
            }

            std::string vecStatus;
            if (vecEnabled) {
                vecStatus = yams::cli::ui::colorize("✓ enabled", yams::cli::ui::Ansi::GREEN);
            } else {
                vecStatus = yams::cli::ui::colorize("✗ disabled", yams::cli::ui::Ansi::YELLOW);
                vecStatus += " (" +
                             std::string(vecAvail ? "config weight=0" : "embeddings unavailable") +
                             ")";
            }
            resourceRows.push_back({"Vector Scoring", vecStatus, ""});
        } catch (...) {
        }

        try {
            const auto& phase = (st.lifecycleState.empty() ? st.overallStatus : st.lifecycleState);
            double ram = st.memoryUsageMb;
            double cpu = st.cpuUsagePercent;
            auto wt = st.requestCounts.find("worker_threads");
            auto wa = st.requestCounts.find("worker_active");
            auto wq = st.requestCounts.find("worker_queued");
            bool haveWorkers = (wt != st.requestCounts.end()) || (wa != st.requestCounts.end()) ||
                               (wq != st.requestCounts.end());
            bool haveResources = (ram > 0.0 || cpu > 0.0);

            if (haveResources) {
                std::ostringstream ramStr;
                ramStr << std::fixed << std::setprecision(1) << ram << " MB";
                resourceRows.push_back({"Memory", ramStr.str(), ""});
                resourceRows.push_back({"CPU", std::to_string((int)cpu) + "%", ""});
            } else if (!phase.empty()) {
                std::string pending = "pending (" + phase + ")";
                if (st.retryAfterMs > 0) {
                    pending += " - retry after " + std::to_string(st.retryAfterMs) + "ms";
                }
                resourceRows.push_back({"Resources", pending, ""});
            }

            if (haveWorkers) {
                size_t threads = wt != st.requestCounts.end() ? wt->second : 0;
                size_t active = wa != st.requestCounts.end() ? wa->second : 0;
                size_t queued = wq != st.requestCounts.end() ? wq->second : 0;
                std::ostringstream workerStr;
                workerStr << threads << " threads, " << active << " active, " << queued
                          << " queued";
                resourceRows.push_back({"Worker Pool", workerStr.str(), ""});
            } else if (!st.ready) {
                resourceRows.push_back({"Worker Pool", "pending", ""});
            }

            if (st.muxQueuedBytes > 0) {
                resourceRows.push_back(
                    {"Mux Queued", std::to_string(st.muxQueuedBytes) + " bytes", ""});
            }
        } catch (...) {
        }

        if (!resourceRows.empty()) {
            yams::cli::ui::render_rows(os, resourceRows);
        }

        printTrustDiagnostics();
    } catch (const std::exception& e) {
        os << "Daemon: ERROR - " << e.what() << "\n";
    }
}

} // namespace yams::cli::doctor
