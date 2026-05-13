#include <yams/cli/doctor/repairs/db_repair.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/query_helpers.h>
#include <spdlog/spdlog.h>

namespace yams::cli::doctor {

void DbRepairCommand::execute(std::ostream& os, YamsCLI* cli, const Config& cfg) {
    using namespace yams::cli::ui;
    using namespace yams::daemon;
    doctor::DoctorSignalGuard signalGuard;

    std::shared_ptr<app::services::AppContext> appCtx;
    auto ensureAppCtx = [&]() -> bool {
        if (!appCtx)
            appCtx = cli->getAppContext();
        if (!appCtx) {
            os << "AppContext unavailable\n";
            return false;
        }
        return true;
    };

    if (!cfg.repairEmbeddings && !cfg.repairFts5 && !cfg.repairGraph) {
        os << "Nothing to repair. Use --embeddings, --fts5, --graph or --all.\n";
        return;
    }

    try {
        // Embeddings repair

        // Embeddings repair
        if (cfg.repairEmbeddings) {
            os << status_pending("Repairing missing embeddings") << "\n";
            yams::repair::EmbeddingRepairConfig rcfg;
            rcfg.batchSize = 8;
            rcfg.skipExisting = true;
            bool daemonRepairAlreadyAttempted = false;

            if (!cfg.noDaemonRepair) {
                const bool tty = stdout_is_tty();
                auto progressStarted = std::chrono::steady_clock::now();
                auto lastPrint = progressStarted;
                auto renderEmbeddingProgress = [&](size_t current, size_t total,
                                                   const std::string& details) {
                    auto now = std::chrono::steady_clock::now();
                    if (!tty && (current % 200 != 0)) {
                        return;
                    }
                    if (tty && (now - lastPrint) < std::chrono::milliseconds(200)) {
                        return;
                    }
                    uint64_t elapsed_s = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::seconds>(now - progressStarted)
                            .count());
                    std::string detail = details;
                    if (!detail.empty()) {
                        detail = truncate_to_width(detail, 60);
                    }
                    std::ostringstream oss;
                    oss << status_pending("Embeddings") << " ";
                    if (total > 0) {
                        double frac = (static_cast<double>(current) / static_cast<double>(total));
                        oss << progress_with_stats(
                            frac, 18,
                            std::make_optional(std::make_pair(static_cast<uint64_t>(current),
                                                              static_cast<uint64_t>(total))),
                            "docs");
                    } else {
                        oss << colorize("working", Ansi::DIM);
                    }
                    oss << " " << colorize("elapsed " + format_duration(elapsed_s), Ansi::DIM);
                    if (!detail.empty()) {
                        oss << " " << colorize(detail, Ansi::DIM);
                    }

                    if (tty) {
                        os << "\r\033[K" << oss.str() << std::flush;
                    } else {
                        os << "  " << oss.str() << "\n" << std::flush;
                    }
                    lastPrint = now;
                };
                auto finishEmbeddingProgress = [&]() {
                    if (tty) {
                        os << "\n";
                    }
                };

                try {
                    daemonRepairAlreadyAttempted = true;
                    yams::daemon::ClientConfig cfg;
                    cfg.dataDir = cli->getDataPath();
                    int rpc_ms = 60000;
                    if (const char* env = std::getenv("YAMS_DOCTOR_RPC_TIMEOUT_MS")) {
                        try {
                            rpc_ms = std::max(1000, std::stoi(env));
                        } catch (...) {
                        }
                    }
                    cfg.requestTimeout = std::chrono::milliseconds(rpc_ms);
                    auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                    if (!leaseRes) {
                        throw std::runtime_error(leaseRes.error().message);
                    }
                    auto leaseHandle = std::move(leaseRes.value());
                    auto& client = **leaseHandle;
                    auto daemonStatus = yams::cli::run_result<yams::daemon::StatusResponse>(
                        client.status(true), std::chrono::milliseconds(rpc_ms));
                    if (daemonStatus) {
                        const auto expectedDir =
                            std::filesystem::weakly_canonical(cli->getDataPath()).string();
                        const auto expectedMetadataDb =
                            std::filesystem::weakly_canonical(cli->getDataPath() / "yams.db")
                                .string();
                        const auto expectedVectorDb =
                            std::filesystem::weakly_canonical(cli->getDataPath() / "vectors.db")
                                .string();
                        const auto normalizePath = [](const std::string& raw) -> std::string {
                            if (raw.empty()) {
                                return {};
                            }
                            return std::filesystem::weakly_canonical(raw).string();
                        };
                        if (!daemonStatus.value().dataDir.empty()) {
                            const auto daemonDir = normalizePath(daemonStatus.value().dataDir);
                            if (daemonDir != expectedDir) {
                                throw std::runtime_error(
                                    "Connected daemon is serving a different data directory (" +
                                    daemonDir + ") than the CLI corpus (" + expectedDir + ")");
                            }
                        }
                        if (!daemonStatus.value().metadataDbPath.empty()) {
                            const auto daemonMetadataDb =
                                normalizePath(daemonStatus.value().metadataDbPath);
                            if (daemonMetadataDb != expectedMetadataDb) {
                                throw std::runtime_error(
                                    "Connected daemon has metadata DB open at " + daemonMetadataDb +
                                    " but CLI expects " + expectedMetadataDb);
                            }
                        }
                        if (!daemonStatus.value().vectorDbPath.empty()) {
                            const auto daemonVectorDb =
                                normalizePath(daemonStatus.value().vectorDbPath);
                            if (daemonVectorDb != expectedVectorDb) {
                                throw std::runtime_error("Connected daemon has vector DB open at " +
                                                         daemonVectorDb + " but CLI expects " +
                                                         expectedVectorDb);
                            }
                        }
                    }
                    yams::daemon::RepairRequest repairReq;
                    repairReq.repairEmbeddings = true;
                    repairReq.foreground = true;
                    if (const char* preferred = std::getenv("YAMS_PREFERRED_MODEL")) {
                        repairReq.embeddingModel = preferred;
                    }
                    os << "  " << ui::status_pending("Daemon: generating missing embeddings")
                       << "\n"
                       << std::flush;
                    auto er = yams::cli::run_result<yams::daemon::RepairResponse>(
                        client.callRepair(repairReq,
                                          [&](const yams::daemon::RepairEvent& ev) {
                                              if (ev.operation == "embeddings" &&
                                                  ev.phase == "repairing") {
                                                  renderEmbeddingProgress(
                                                      static_cast<size_t>(ev.processed),
                                                      static_cast<size_t>(ev.total), ev.message);
                                              }
                                          }),
                        std::chrono::milliseconds{0});
                    finishEmbeddingProgress();
                    if (er) {
                        const auto& daemonResp = er.value();
                        const yams::daemon::RepairOperationResult* embeddingOp = nullptr;
                        for (const auto& op : daemonResp.operationResults) {
                            if (op.operation == "embeddings") {
                                embeddingOp = &op;
                                break;
                            }
                        }
                        if (!daemonResp.success || embeddingOp == nullptr) {
                            os << "  "
                               << ui::status_warning("Daemon embeddings returned an invalid repair "
                                                     "result; falling back to local mode.")
                               << "\n";
                        } else {
                            std::string summary;
                            if (!embeddingOp->message.empty() && embeddingOp->succeeded == 0 &&
                                embeddingOp->failed == 0) {
                                summary = embeddingOp->message;
                            } else {
                                summary = "Daemon embeddings: processed=" +
                                          std::to_string(embeddingOp->processed) +
                                          ", embedded=" + std::to_string(embeddingOp->succeeded) +
                                          ", skipped=" + std::to_string(embeddingOp->skipped) +
                                          ", failed=" + std::to_string(embeddingOp->failed);
                            }
                            os << "  " << ui::status_ok(summary) << "\n";
                            return;
                        }
                    } else {
                        os << "  "
                           << ui::status_warning(
                                  "Daemon embeddings failed (" + er.error().message +
                                  ") — falling back to local mode. Use '--no-daemon' to "
                                  "skip RPC.")
                           << "\n";
                    }
                } catch (const std::exception& ex) {
                    finishEmbeddingProgress();
                    os << "  "
                       << ui::status_warning(std::string("Daemon embeddings exception (") +
                                             ex.what() +
                                             ") — falling back to local mode. Use '--no-daemon' to "
                                             "skip RPC.")
                       << "\n";
                }
            }
            rcfg.dataPath = cli->getDataPath();
            rcfg.cancelRequested = &doctor::DoctorSignalGuard::cancelFlag();
            auto emb = cli->getEmbeddingGenerator();
            std::shared_ptr<yams::daemon::IModelProvider> localProvider;
            auto getLocalProvider = [&]() -> std::shared_ptr<yams::daemon::IModelProvider> {
                if (!localProvider) {
                    localProvider = cli->getLocalModelProvider();
                }
                return localProvider;
            };
            if (!emb) {
                localProvider = getLocalProvider();
            }
            if (!emb && !localProvider) {
                os << "  Embedding generation unavailable -- ensure model provider is "
                      "configured.\n";
            } else {
                // Guard: abort repair if DB schema dim mismatches configured target
                try {
                    namespace fs = std::filesystem;
                    fs::path dbPath = cli->getDataPath() / "vectors.db";
                    auto readDbDim = [&](const fs::path& p) -> std::optional<size_t> {
                        sqlite3* db = nullptr;
                        if (sqlite3_open(p.string().c_str(), &db) != SQLITE_OK) {
                            if (db)
                                sqlite3_close(db);
                            return std::nullopt;
                        }
                        const char* sql =
                            "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
                        sqlite3_stmt* stmt = nullptr;
                        std::optional<size_t> out{};
                        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
                            if (sqlite3_step(stmt) == SQLITE_ROW) {
                                const unsigned char* txt = sqlite3_column_text(stmt, 0);
                                if (txt) {
                                    std::string ddl(reinterpret_cast<const char*>(txt));
                                    auto pos = ddl.find("float[");
                                    if (pos != std::string::npos) {
                                        auto end = ddl.find(']', pos);
                                        if (end != std::string::npos && end > pos + 6) {
                                            std::string num = ddl.substr(pos + 6, end - (pos + 6));
                                            try {
                                                out = static_cast<size_t>(std::stoul(num));
                                            } catch (...) {
                                            }
                                        }
                                    }
                                }
                            }
                            sqlite3_finalize(stmt);
                        }
                        sqlite3_close(db);
                        return out;
                    };
                    auto dbDim = readDbDim(dbPath);
                    auto dimResolved = vecutil::resolveEmbeddingDimension(cli, cli->getDataPath());
                    size_t targetDim = dimResolved.dimension;
                    if (targetDim == 0 && !emb) {
                        localProvider = getLocalProvider();
                    }
                    if (targetDim == 0 && localProvider) {
                        targetDim = localProvider->getEmbeddingDim(cli->getEmbeddingModelName());
                    }
                    if (targetDim == 0 && emb) {
                        targetDim = emb->getEmbeddingDimension();
                    }
                    if (dbDim && *dbDim != targetDim) {
                        os << "  "
                           << ui::status_error(
                                  "Schema dimension mismatch (db=" + std::to_string(*dbDim) +
                                  ", target=" + std::to_string(targetDim) + ") — aborting repair.")
                           << "\n";
                        os << "    Run: yams doctor (recreate vector tables), then yams "
                              "repair --embeddings\n";
                        return;
                    }
                } catch (...) {
                }
                if (!stdout_is_tty()) {
                    os << "  " << ui::status_pending("Scanning for documents missing embeddings")
                       << "\n"
                       << std::flush;
                }
                SpinnerRunner spin;
                spin.start("Scanning for documents missing embeddings");
                auto missing = yams::repair::getDocumentsMissingEmbeddings(appCtx->metadataRepo,
                                                                           cli->getDataPath(), 0);
                spin.stop();
                if (!missing) {
                    os << "  Could not query missing embeddings: " << missing.error().message
                       << "\n";
                } else if (missing.value().empty()) {
                    os << "  " << ui::status_ok("No documents missing embeddings") << "\n";
                } else {
                    const bool tty = stdout_is_tty();
                    auto progressStarted = std::chrono::steady_clock::now();
                    auto lastPrint = progressStarted;
                    auto renderEmbeddingProgress = [&](size_t current, size_t total,
                                                       const std::string& details) {
                        auto now = std::chrono::steady_clock::now();
                        if (!tty && (current % 200 != 0)) {
                            return;
                        }
                        if (tty && (now - lastPrint) < std::chrono::milliseconds(200)) {
                            return;
                        }
                        uint64_t elapsed_s = static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::seconds>(now - progressStarted)
                                .count());
                        std::string detail = details;
                        if (!detail.empty()) {
                            detail = truncate_to_width(detail, 60);
                        }
                        std::ostringstream oss;
                        oss << status_pending("Embeddings") << " ";
                        if (total > 0) {
                            double frac =
                                (static_cast<double>(current) / static_cast<double>(total));
                            oss << progress_with_stats(
                                frac, 18,
                                std::make_optional(std::make_pair(static_cast<uint64_t>(current),
                                                                  static_cast<uint64_t>(total))),
                                "docs");
                        } else {
                            oss << colorize("working", Ansi::DIM);
                        }
                        oss << " " << colorize("elapsed " + format_duration(elapsed_s), Ansi::DIM);
                        if (!detail.empty()) {
                            oss << " " << colorize(detail, Ansi::DIM);
                        }

                        if (tty) {
                            os << "\r\033[K" << oss.str() << std::flush;
                        } else {
                            os << "  " << oss.str() << "\n" << std::flush;
                        }
                        lastPrint = now;
                    };
                    auto finishEmbeddingProgress = [&]() {
                        if (tty) {
                            os << "\n";
                        }
                    };

                    bool attemptedDaemon = false;
                    bool skipDaemon = cfg.noDaemonRepair || daemonRepairAlreadyAttempted;
                    if (skipDaemon) {
                        const std::string reason = daemonRepairAlreadyAttempted
                                                       ? "Skipping second daemon embedding "
                                                         "attempt after fallback"
                                                       : "Skipping daemon embedding RPC "
                                                         "(--no-daemon)";
                        os << "  " << ui::status_info(reason) << "\n";
                    }
                    {
                        try {
                            if (skipDaemon) {
                                throw std::runtime_error("skipped");
                            }
                            yams::daemon::ClientConfig cfg;
                            cfg.dataDir = cli->getDataPath();
                            // Configurable RPC timeout (default 60s)
                            int rpc_ms = 60000;
                            if (const char* env = std::getenv("YAMS_DOCTOR_RPC_TIMEOUT_MS")) {
                                try {
                                    rpc_ms = std::max(1000, std::stoi(env));
                                } catch (...) {
                                }
                            }
                            cfg.requestTimeout = std::chrono::milliseconds(rpc_ms);
                            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                            if (!leaseRes) {
                                throw std::runtime_error(leaseRes.error().message);
                            }
                            auto leaseHandle = std::move(leaseRes.value());
                            auto& client = **leaseHandle;
                            auto daemonStatus = yams::cli::run_result<yams::daemon::StatusResponse>(
                                client.status(true), std::chrono::milliseconds(rpc_ms));
                            if (daemonStatus) {
                                const auto expectedDir =
                                    std::filesystem::weakly_canonical(cli->getDataPath()).string();
                                const auto expectedMetadataDb = std::filesystem::weakly_canonical(
                                                                    cli->getDataPath() / "yams.db")
                                                                    .string();
                                const auto expectedVectorDb = std::filesystem::weakly_canonical(
                                                                  cli->getDataPath() / "vectors.db")
                                                                  .string();
                                const auto normalizePath =
                                    [](const std::string& raw) -> std::string {
                                    if (raw.empty()) {
                                        return {};
                                    }
                                    return std::filesystem::weakly_canonical(raw).string();
                                };
                                if (!daemonStatus.value().dataDir.empty()) {
                                    const auto daemonDir =
                                        normalizePath(daemonStatus.value().dataDir);
                                    if (daemonDir != expectedDir) {
                                        throw std::runtime_error(
                                            "Connected daemon is serving a different data "
                                            "directory (" +
                                            daemonDir + ") than the CLI corpus (" + expectedDir +
                                            ")");
                                    }
                                }
                                if (!daemonStatus.value().metadataDbPath.empty()) {
                                    const auto daemonMetadataDb =
                                        normalizePath(daemonStatus.value().metadataDbPath);
                                    if (daemonMetadataDb != expectedMetadataDb) {
                                        throw std::runtime_error(
                                            "Connected daemon has metadata DB open at " +
                                            daemonMetadataDb + " but CLI expects " +
                                            expectedMetadataDb);
                                    }
                                }
                                if (!daemonStatus.value().vectorDbPath.empty()) {
                                    const auto daemonVectorDb =
                                        normalizePath(daemonStatus.value().vectorDbPath);
                                    if (daemonVectorDb != expectedVectorDb) {
                                        throw std::runtime_error(
                                            "Connected daemon has vector DB open at " +
                                            daemonVectorDb + " but CLI expects " +
                                            expectedVectorDb);
                                    }
                                }
                            }
                            yams::daemon::RepairRequest repairReq;
                            repairReq.repairEmbeddings = true;
                            repairReq.foreground = true;
                            if (const char* preferred = std::getenv("YAMS_PREFERRED_MODEL")) {
                                repairReq.embeddingModel = preferred;
                            }
                            os << "  "
                               << ui::status_pending("Daemon: generating missing embeddings")
                               << "\n"
                               << std::flush;
                            auto er = yams::cli::run_result<yams::daemon::RepairResponse>(
                                client.callRepair(repairReq,
                                                  [&](const yams::daemon::RepairEvent& ev) {
                                                      if (ev.operation == "embeddings" &&
                                                          ev.phase == "repairing") {
                                                          renderEmbeddingProgress(
                                                              static_cast<size_t>(ev.processed),
                                                              static_cast<size_t>(ev.total),
                                                              ev.message);
                                                      }
                                                  }),
                                std::chrono::milliseconds{0});
                            finishEmbeddingProgress();
                            if (er) {
                                const auto& daemonResp = er.value();
                                const yams::daemon::RepairOperationResult* embeddingOp = nullptr;
                                for (const auto& op : daemonResp.operationResults) {
                                    if (op.operation == "embeddings") {
                                        embeddingOp = &op;
                                        break;
                                    }
                                }
                                if (!daemonResp.success || embeddingOp == nullptr) {
                                    os << "  "
                                       << ui::status_warning(
                                              "Daemon embeddings returned an invalid "
                                              "repair result; falling back to local "
                                              "mode.")
                                       << "\n";
                                } else {
                                    std::string summary;
                                    if (!embeddingOp->message.empty() &&
                                        embeddingOp->succeeded == 0 && embeddingOp->failed == 0) {
                                        summary = embeddingOp->message;
                                    } else {
                                        summary =
                                            "Daemon embeddings: processed=" +
                                            std::to_string(embeddingOp->processed) +
                                            ", embedded=" + std::to_string(embeddingOp->succeeded) +
                                            ", skipped=" + std::to_string(embeddingOp->skipped) +
                                            ", failed=" + std::to_string(embeddingOp->failed);
                                    }
                                    os << "  " << ui::status_ok(summary) << "\n";
                                    attemptedDaemon = true;
                                }
                            } else {
                                os << "  "
                                   << ui::status_warning("Daemon embeddings failed (" +
                                                         er.error().message +
                                                         ") — falling back to local mode. Use "
                                                         "'--no-daemon' to skip RPC.")
                                   << "\n";
                            }
                        } catch (const std::exception& ex) {
                            finishEmbeddingProgress();
                            if (skipDaemon && std::string(ex.what()) == "skipped") {
                                // Intentional local-mode path; no warning needed.
                            } else {
                                os << "  "
                                   << ui::status_warning(
                                          std::string("Daemon embeddings exception (") + ex.what() +
                                          ") — falling back to local mode. Use "
                                          "'--no-daemon' to skip RPC.")
                                   << "\n";
                            }
                        }
                    }
                    if (!attemptedDaemon) {
                        if (!ensureAppCtx()) {
                            return;
                        }
                        progressStarted = std::chrono::steady_clock::now();
                        lastPrint = progressStarted;
                        auto onProgress = [&](size_t current, size_t total,
                                              const std::string& details) {
                            renderEmbeddingProgress(current, total, details);
                        };

                        Result<yams::repair::EmbeddingRepairStats> stats = Error{
                            ErrorCode::NotInitialized, "No embedding generation path available"};
                        localProvider = getLocalProvider();
                        if (localProvider) {
                            std::string modelName = cli->getEmbeddingModelName();
                            if (modelName.empty()) {
                                if (const char* preferred = std::getenv("YAMS_PREFERRED_MODEL")) {
                                    modelName = preferred;
                                }
                            }
                            if (!modelName.empty()) {
                                rcfg.preferredModel = modelName;
                            }
                            stats = yams::repair::repairMissingEmbeddings(
                                appCtx->store, appCtx->metadataRepo, localProvider, modelName, rcfg,
                                missing.value(), onProgress, appCtx->contentExtractors);
                        } else if (emb) {
                            stats = yams::repair::repairMissingEmbeddings(
                                appCtx->store, appCtx->metadataRepo, emb, rcfg, missing.value(),
                                onProgress, appCtx->contentExtractors);
                        }

                        finishEmbeddingProgress();
                        if (!stats) {
                            if (stats.error().code == ErrorCode::OperationCancelled) {
                                os << "  "
                                   << ui::status_warning(
                                          "Cancelled by user (partial results may exist)")
                                   << "\n";
                                return;
                            }
                            os << "  "
                               << ui::status_error("Embedding repair failed: " +
                                                   stats.error().message)
                               << "\n";
                        } else {
                            os << "  "
                               << ui::status_ok(
                                      "Embeddings generated=" +
                                      std::to_string(stats.value().embeddingsGenerated) +
                                      ", skipped=" +
                                      std::to_string(stats.value().embeddingsSkipped) +
                                      ", failed=" + std::to_string(stats.value().failedOperations))
                               << "\n";
                        }
                    }
                }
            }
        }

        // Knowledge graph repair (build from tags/metadata)
        if (cfg.repairGraph) {
            auto kg = cli->getKnowledgeGraphStore();
            if (!kg) {
                os << "Repair: knowledge graph store unavailable — skipped.\n";
            } else {
                doctor::DoctorContext::repairGraph(os, cli);
                os << "Repair: knowledge graph completed.\n";
            }
        }

        // FTS5 rebuild
        if (cfg.repairFts5) {
            if (!ensureAppCtx()) {
                return;
            }
            os << status_pending("Repairing FTS5 index") << "\n";
            if (!appCtx->store || !appCtx->metadataRepo) {
                os << "  Store/Metadata unavailable\n";
                return;
            }

            // Prefer daemon repair when available (streaming events). Fall back to local loop.
            if (!cfg.noDaemonRepair) {
                try {
                    yams::daemon::ClientConfig cfg;
                    if (cli->hasExplicitDataDir()) {
                        cfg.dataDir = cli->getDataPath();
                    }
                    cfg.requestTimeout = std::chrono::milliseconds(600000);
                    auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                    if (leaseRes) {
                        auto leaseHandle = std::move(leaseRes.value());
                        auto& client = **leaseHandle;

                        yams::daemon::RepairRequest req;
                        req.repairFts5 = true;
                        req.force = true;
                        req.verbose = false;
                        req.dryRun = false;
                        req.foreground = true;

                        const bool tty = stdout_is_tty();
                        std::string lastOperation;
                        uint64_t lastProcessed = 0;
                        auto lastPrint = std::chrono::steady_clock::now();

                        auto onEvent = [&](const yams::daemon::RepairEvent& ev) {
                            if (ev.operation != lastOperation) {
                                if (!lastOperation.empty()) {
                                    os << "\n";
                                }
                                lastOperation = ev.operation;
                                os << "  " << ui::status_pending("Daemon: " + ev.operation) << "\n";
                                lastProcessed = 0;
                            }

                            if (ev.phase == "repairing" && ev.total > 0) {
                                auto now = std::chrono::steady_clock::now();
                                if (tty && (now - lastPrint) < std::chrono::milliseconds(200) &&
                                    ev.processed == lastProcessed) {
                                    return;
                                }
                                lastProcessed = ev.processed;
                                lastPrint = now;
                                double frac = static_cast<double>(ev.processed) /
                                              static_cast<double>(ev.total);
                                std::ostringstream oss;
                                oss << ui::status_pending("FTS5") << " "
                                    << ui::progress_with_stats(frac, 18,
                                                               std::make_optional(std::make_pair(
                                                                   ev.processed, ev.total)),
                                                               "docs");
                                if (tty) {
                                    os << "\r\033[K" << oss.str() << std::flush;
                                }
                            } else if (ev.phase == "error") {
                                os << "  " << ui::status_warning(ev.message) << "\n";
                            } else if (ev.phase == "completed") {
                                if (tty) {
                                    os << "\n";
                                }
                                os << "  " << ui::status_ok("Daemon FTS5 repair done")
                                   << "  processed=" << ev.processed << " ok=" << ev.succeeded
                                   << " failed=" << ev.failed << "\n";
                            }
                        };

                        auto rr = yams::cli::run_result<yams::daemon::RepairResponse>(
                            client.callRepair(req, onEvent), std::chrono::minutes(10));
                        if (rr) {
                            return;
                        }
                        os << "  "
                           << ui::status_warning("Daemon FTS5 repair failed (" +
                                                 rr.error().message +
                                                 ") — falling back to local mode")
                           << "\n";
                    }
                } catch (const std::exception& ex) {
                    os << "  "
                       << ui::status_warning(std::string("Daemon FTS5 repair exception (") +
                                             ex.what() + ") — falling back to local mode")
                       << "\n";
                }
            }

            SpinnerRunner spin;
            spin.start("Loading document list for FTS5 rebuild");
            auto docs = metadata::queryDocumentsByPattern(*appCtx->metadataRepo, "%");
            spin.stop();
            if (!docs) {
                os << "  Query failed: " << docs.error().message << "\n";
                return;
            }
            size_t ok = 0, fail = 0, total = docs.value().size(), cur = 0;

            if (total == 0) {
                os << "  " << status_ok("No documents to reindex") << "\n";
                return;
            }

            os << "  "
               << status_info("Reindexing " + format_number(static_cast<uint64_t>(total)) +
                              " documents (extract text + index into FTS5)")
               << "\n";

            const bool tty = stdout_is_tty();
            auto started = std::chrono::steady_clock::now();
            auto lastPrint = started;
            auto printProgress = [&](bool forceNewline) {
                auto now = std::chrono::steady_clock::now();
                double elapsed =
                    std::chrono::duration_cast<std::chrono::duration<double>>(now - started)
                        .count();
                double rate = (elapsed > 0.0) ? (static_cast<double>(cur) / elapsed) : 0.0;
                uint64_t elapsed_s = static_cast<uint64_t>(elapsed);
                uint64_t eta_s = 0;
                if (rate > 0.0 && cur < total) {
                    eta_s = static_cast<uint64_t>(static_cast<double>(total - cur) / rate);
                }
                double frac =
                    (total > 0) ? (static_cast<double>(cur) / static_cast<double>(total)) : 1.0;
                std::ostringstream oss;
                oss << status_pending("FTS5") << " "
                    << progress_with_stats(
                           frac, 18,
                           std::make_optional(std::make_pair(static_cast<uint64_t>(cur),
                                                             static_cast<uint64_t>(total))),
                           "docs")
                    << " " << colorize("elapsed " + format_duration(elapsed_s), Ansi::DIM);
                if (rate > 0.0) {
                    std::ostringstream r;
                    r << std::fixed << std::setprecision(rate < 10.0 ? 1 : 0) << rate;
                    oss << " " << colorize(r.str() + " docs/s", Ansi::DIM);
                }
                if (eta_s > 0) {
                    oss << " " << colorize("eta " + format_duration(eta_s), Ansi::DIM);
                }

                const std::string line = oss.str();
                if (tty && !forceNewline) {
                    os << "\r\033[K" << line << std::flush;
                } else {
                    os << "  " << line << "\n" << std::flush;
                }
                lastPrint = now;
            };

            bool cancelled = false;
            uint64_t truncated = 0;

            auto isTooBigError = [](const std::string& msg) -> bool {
                // sqlite3_errstr(SQLITE_TOOBIG) => "string or blob too big"
                // Our bind diagnostics also include "limit=<n>" and "len=<n>".
                return msg.find("too big") != std::string::npos ||
                       msg.find("TOOBIG") != std::string::npos;
            };

            auto parseSqliteLimitLen = [](const std::string& msg) -> std::optional<size_t> {
                const std::string key = "limit=";
                auto pos = msg.find(key);
                if (pos == std::string::npos)
                    return std::nullopt;
                pos += key.size();
                size_t end = pos;
                while (end < msg.size() && msg[end] >= '0' && msg[end] <= '9') {
                    ++end;
                }
                if (end == pos)
                    return std::nullopt;
                try {
                    return static_cast<size_t>(std::stoull(msg.substr(pos, end - pos)));
                } catch (...) {
                    return std::nullopt;
                }
            };

            for (const auto& d : docs.value()) {
                if (doctor::DoctorSignalGuard::cancelFlag().load(std::memory_order_relaxed)) {
                    cancelled = true;
                    break;
                }
                ++cur;
                std::string ext = d.fileExtension;
                if (!ext.empty() && ext[0] == '.')
                    ext.erase(0, 1);
                try {
                    auto extracted = yams::extraction::util::extractDocumentText(
                        appCtx->store, d.sha256Hash, d.mimeType, ext, appCtx->contentExtractors);
                    if (extracted && !extracted->empty()) {
                        auto ir = appCtx->metadataRepo->indexDocumentContent(
                            d.id, d.fileName, *extracted, d.mimeType);
                        if (!ir && isTooBigError(ir.error().message)) {
                            // Best-effort retry: index a truncated prefix when SQLite
                            // length limits would otherwise stall the entire repair.
                            constexpr size_t kDefaultTruncateBytes = 8 * 1024 * 1024; // 8 MiB
                            size_t cap = kDefaultTruncateBytes;
                            if (auto limit = parseSqliteLimitLen(ir.error().message);
                                limit && *limit > 1024) {
                                cap = std::min(cap, *limit - 1024);
                            }
                            if (cap > 0 && extracted->size() > cap) {
                                std::string truncatedText = extracted->substr(0, cap);
                                auto ir2 = appCtx->metadataRepo->indexDocumentContent(
                                    d.id, d.fileName, truncatedText, d.mimeType);
                                if (ir2) {
                                    ++ok;
                                    ++truncated;
                                } else {
                                    ++fail;
                                }
                            } else {
                                ++fail;
                            }
                        } else if (ir) {
                            ++ok;
                        } else {
                            ++fail;
                        }
                    } else {
                        ++fail;
                    }
                } catch (...) {
                    ++fail;
                }

                // Always show forward progress on long-running batches.
                // - Count-based update keeps output bounded for fast runs.
                // - Time-based heartbeat prevents "hung" perception on slow runs.
                auto now = std::chrono::steady_clock::now();
                bool countTick = (cur % 500 == 0);
                bool timeTick = (now - lastPrint) >= std::chrono::seconds(2);
                if (countTick || timeTick) {
                    printProgress(false);
                }
            }

            if (tty) {
                // Ensure the final progress line is terminated.
                printProgress(true);
            }

            if (cancelled) {
                os << "  "
                   << ui::status_warning("Cancelled by user (FTS5 index may be partially rebuilt)")
                   << "\n";
                return;
            }
            os << "  "
               << ui::status_ok("FTS5 reindex complete: ok=" + std::to_string(ok) + ", fail=" +
                                std::to_string(fail) + ", truncated=" + std::to_string(truncated))
               << "\n";
        }
    } catch (const std::exception& e) {
        os << "Doctor repair error: " << e.what() << "\n";
    }
}

} // namespace yams::cli::doctor
