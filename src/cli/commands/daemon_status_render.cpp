#include <yams/cli/daemon_status_render.h>

#include <yams/cli/pipeline_stage_render.h>
#include <yams/cli/status_metrics.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/metric_keys.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <map>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yams::cli {

namespace {

// Use shared Severity enum from ui_helpers
using Severity = yams::cli::ui::Severity;

struct ReadinessDisplay {
    std::string key;
    std::string label;
    Severity severity{Severity::Good};
    std::string text;
    bool issue{false};
};

std::string humanizeToken(const std::string& token) {
    // Special case mappings for known acronyms and technical terms
    static const std::map<std::string, std::string> specialCases = {
        {"cas", "CAS"}, {"dedup", "Dedup"}, {"ema", "EMA"}, {"ipc", "IPC"},
        {"fsm", "FSM"}, {"cpu", "CPU"},     {"db", "DB"},   {"sec", "sec"},
        {"ms", "ms"},   {"us", "µs"},       {"kb", "KB"},   {"mb", "MB"},
        {"gb", "GB"},   {"io", "I/O"},      {"api", "API"}, {"id", "ID"},
    };

    std::string out;
    out.reserve(token.size() + 10);
    std::string word;

    auto flushWord = [&]() {
        if (word.empty())
            return;
        // Check if this word is a special case
        std::string lower = word;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        auto it = specialCases.find(lower);
        if (it != specialCases.end()) {
            if (!out.empty() && out.back() != ' ')
                out.push_back(' ');
            out += it->second;
        } else {
            // Normal word: capitalize first letter, lowercase rest
            if (!out.empty() && out.back() != ' ')
                out.push_back(' ');
            for (size_t i = 0; i < word.size(); ++i) {
                auto uch = static_cast<unsigned char>(word[i]);
                out.push_back(static_cast<char>(i == 0 ? std::toupper(uch) : std::tolower(uch)));
            }
        }
        word.clear();
    };

    for (char ch : token) {
        if (ch == '_' || ch == '-' || ch == '.') {
            flushWord();
            continue;
        }
        word.push_back(ch);
    }
    flushWord();

    return out;
}

ReadinessDisplay classifyReadiness(const std::string& key, bool value) {
    ReadinessDisplay display;
    display.key = key;
    display.label = humanizeToken(key);

    const std::string lowerKey = [&]() {
        std::string s;
        s.reserve(key.size());
        for (char ch : key)
            s.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        return s;
    }();

    const bool isDegradedFlag = lowerKey.find("degraded") != std::string::npos ||
                                lowerKey.find("error") != std::string::npos ||
                                lowerKey.find("failed") != std::string::npos;
    const bool isAvailabilityFlag = lowerKey.find("ready") != std::string::npos ||
                                    lowerKey.find("available") != std::string::npos ||
                                    lowerKey.find("enabled") != std::string::npos ||
                                    lowerKey.find("initialized") != std::string::npos;

    if (isDegradedFlag) {
        if (value) {
            display.severity = Severity::Warn;
            display.text = "Degraded";
            display.issue = true;
        } else {
            display.severity = Severity::Good;
            display.text = "Healthy";
        }
        return display;
    }

    if (value) {
        display.severity = Severity::Good;
        display.text = isAvailabilityFlag ? "Ready" : "Active";
    } else {
        display.severity = Severity::Warn;
        display.text = isAvailabilityFlag ? "Waiting" : "Unavailable";
        display.issue = true;
    }

    return display;
}

std::filesystem::path normalizePath(std::filesystem::path path) {
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(path, ec);
    return ec ? path.lexically_normal() : canonical;
}

bool isEphemeralDataDir(const std::filesystem::path& path) {
    if (path.empty())
        return false;

    const auto normalized = normalizePath(path);
    std::error_code ec;
    const auto tmpRoot = std::filesystem::temp_directory_path(ec);
    if (!ec) {
        const auto normalizedTmp = normalizePath(tmpRoot);
        const auto rel = normalized.lexically_relative(normalizedTmp);
        if (rel.empty() || rel == "." || (!rel.empty() && *rel.begin() != "..")) {
            return true;
        }
    }

    const std::string generic = normalized.generic_string();
    return generic == "/tmp" || generic.rfind("/tmp/", 0) == 0 || generic == "/private/tmp" ||
           generic.rfind("/private/tmp/", 0) == 0;
}

std::optional<std::filesystem::path>
daemonDataDirFromStatus(const yams::daemon::StatusResponse& status) {
    if (status.contentStoreRoot.empty())
        return std::nullopt;

    std::filesystem::path contentRoot(status.contentStoreRoot);
    if (!contentRoot.empty() && contentRoot.filename() == "storage")
        return contentRoot.parent_path();
    return contentRoot;
}

// Shared helper: paint a status value with severity icon and color
// Delegates to ui::severity_text for consistency
std::string paintStatus(Severity sev, std::string text) {
    return yams::cli::ui::severity_text(sev, text, true);
}

// Shared helper: neutral text (no severity icon)
std::string neutralText(const std::string& text) {
    using namespace yams::cli::ui;
    return colorize(text, Ansi::WHITE);
}

// Human label for the wire code published under kRepairCurrentOperationCode.
std::string repairOperationLabel(uint64_t code) {
    std::string label(yams::daemon::metrics::repairOperationNameForCode(code));
    std::replace(label.begin(), label.end(), '_', ' ');
    return label;
}

} // namespace

void renderDaemonStatusWithOptionalSection(
    const yams::daemon::StatusResponse& status, const DaemonStatusRenderContext& ctx, bool detailed,
    std::ostream& os, const std::function<void(std::ostream&)>& optionalSection) {
    auto primaryContext = ctx;
    primaryContext.memorySync = nullptr;
    if (detailed)
        renderDaemonStatusDetailed(status, primaryContext, os);
    else
        renderDaemonStatusBrief(status, primaryContext, os);
    os.flush(); // Make useful status visible before a best-effort RPC can wait on its deadline.
    if (optionalSection)
        optionalSection(os);
}

void renderMemorySyncSection(const yams::daemon::MemorySyncResponse* sync, std::ostream& os) {
    if (sync == nullptr || !sync->started) {
        return;
    }
    const auto& m = *sync;
    using yams::cli::ui::format_duration;
    using yams::cli::ui::render_rows;
    using yams::cli::ui::Row;
    using yams::cli::ui::section_header;
    using yams::cli::ui::Severity;
    using yams::cli::ui::severity_text;
    auto health = [](bool bad) { return bad ? Severity::Warn : Severity::Good; };
    std::vector<Row> rows;
    rows.push_back({"Backend", m.backend, ""});
    rows.push_back({"Mode", m.mode, ""});
    rows.push_back({"Corpus", m.corpusId + " (epoch " + std::to_string(m.corpusEpoch) + ")", ""});
    rows.push_back({"Node", m.nodeId, ""});
    const bool strongTrust =
        m.trustMode == "authenticated-writers" || m.trustMode == "mutual-tls-operator-pinned";
    rows.push_back(
        {"Trust", severity_text(strongTrust ? Severity::Good : Severity::Warn, m.trustMode), ""});
    if (m.backend == "direct") {
        rows.push_back({"Peers", std::to_string(m.peerCount), "yams p2p peers for details"});
    }
    rows.push_back({"Records", std::to_string(m.records), ""});
    rows.push_back(
        {"Quarantined",
         severity_text(health(m.quarantinedRecords > 0), std::to_string(m.quarantinedRecords)),
         m.quarantinedRecords > 0 ? "check daemon log for reasons" : ""});
    rows.push_back({"Auth failures",
                    severity_text(health(m.authFailures > 0), std::to_string(m.authFailures)), ""});
    rows.push_back(
        {"Sync cycles",
         std::to_string(m.successfulCycles) + " ok · " + std::to_string(m.failedCycles) + " failed",
         severity_text(health(m.failedCycles > 0), "")});
    rows.push_back({"Last sync",
                    severity_text(health(m.lastSuccessAgeMs > 60000),
                                  format_duration(m.lastSuccessAgeMs / 1000) + " ago"),
                    ""});
    os << "\n" << section_header("Memory Sync") << "\n\n";
    render_rows(os, rows);
}

void renderDaemonStatusBrief(const yams::daemon::StatusResponse& s,
                             const DaemonStatusRenderContext& ctx, std::ostream& os) {
    using namespace yams::cli::ui;

    // Determine lifecycle state and severity
    std::string lifecycle =
        !s.lifecycleState.empty()
            ? humanizeToken(s.lifecycleState)
            : (!s.overallStatus.empty() ? humanizeToken(s.overallStatus)
                                        : (s.ready ? std::string("Ready")
                                                   : (s.running ? std::string("Initializing")
                                                                : std::string("Stopped"))));

    Severity stateSeverity = Severity::Good;
    if (!s.running) {
        stateSeverity = Severity::Bad;
    } else if (!s.ready) {
        stateSeverity = Severity::Warn;
    }
    if (!s.lastError.empty())
        stateSeverity = Severity::Bad;

    // Collect issues for quick summary (skip informational flags)
    std::vector<std::string> issues;
    namespace readiness = yams::daemon::readiness;
    auto suppressDerivedIssue = [&](std::string_view key) {
        if (key == readiness::kTopologyArtifactsFresh ||
            key == readiness::kTopologyRebuildRunning || key == readiness::kVectorIndex) {
            return true;
        }
        const bool emptyCorpusReady =
            s.vectorDbInitAttempted && !s.vectorDbReady && s.embeddingAvailable;
        if (emptyCorpusReady && (key == readiness::kSearchEngineVectorUsable ||
                                 key == readiness::kSearchEngineHybridUsable)) {
            return true;
        }
        // Simeon lexical enhancement is opt-in; when not configured or
        // when the corpus exceeded the size budget, the daemon stays on
        // FTS5 only — these keys reflect runtime state, not a blocker
        // on readiness. Only surface during an active build.
        // Additionally, once the main search engine is Ready, the Simeon
        // sub-features (fragment geometry, concept mining) are best-effort
        // enhancements that should not appear as blockers.
        auto find = [&](std::string_view k) {
            auto it = s.readinessStates.find(std::string(k));
            return it != s.readinessStates.end() && it->second;
        };
        const bool simeonConfigured = find(readiness::kSearchEngineLexicalEnhancementConfigured);
        const bool simeonBuilding = find(readiness::kSearchEngineLexicalEnhancementBuilding);
        const bool searchEngineReady = find(readiness::kSearchEngine);
        if ((key == readiness::kSearchEngineLexicalEnhancementConfigured ||
             key == readiness::kSearchEngineLexicalEnhancementReady ||
             key == readiness::kSearchEngineLexicalEnhancementBuilding ||
             key == readiness::kSearchEngineFragmentGeometryReady) &&
            (!(simeonConfigured && simeonBuilding) || searchEngineReady)) {
            return true;
        }
        return false;
    };
    for (const auto& [key, ready] : s.readinessStates) {
        if (!ready) {
            const std::string_view k{key};
            if (k == readiness::kSearchEngineBuildReasonInitial ||
                k == readiness::kSearchEngineBuildReasonRebuild ||
                k == readiness::kSearchEngineBuildReasonDegraded || k.ends_with("_degraded")) {
                continue;
            }
            const bool vectorDisabled = !s.vectorDbInitAttempted;
            if (k == readiness::kVectorDb && s.vectorDbInitAttempted) {
                continue;
            }
            if (k == readiness::kVectorDbReady || k == readiness::kVectorDbInitAttempted) {
                continue;
            }
            if (vectorDisabled &&
                (k == readiness::kVectorDb || k == readiness::kVectorDbDim ||
                 k == readiness::kVectorIndex || k == readiness::kVectorEmbeddingsAvailable ||
                 k == readiness::kVectorScoringEnabled ||
                 k == readiness::kSearchEngineVectorUsable ||
                 k == readiness::kSearchEngineHybridUsable)) {
                continue;
            }
            if (suppressDerivedIssue(k)) {
                continue;
            }
            auto rd = classifyReadiness(key, ready);
            auto pit = s.initProgress.find(key);
            if (pit != s.initProgress.end() && pit->second < 100) {
                rd.label += " (" + std::to_string(static_cast<int>(pit->second)) + "%)";
            }
            issues.push_back(rd.label);
        }
    }
    std::sort(issues.begin(), issues.end());
    issues.erase(std::unique(issues.begin(), issues.end()), issues.end());

    const auto daemonDataDir = daemonDataDirFromStatus(s);
    const auto configuredDataDir = ctx.configuredDataDir;
    const bool hasDaemonDataDir = daemonDataDir.has_value() && !daemonDataDir->empty();
    const bool dataDirMismatch = hasDaemonDataDir && !configuredDataDir.empty() &&
                                 normalizePath(*daemonDataDir) != normalizePath(configuredDataDir);
    const bool daemonUsesEphemeralData = hasDaemonDataDir && isEphemeralDataDir(*daemonDataDir);
    auto findCompactCount = [&](const char* key) -> uint64_t {
        auto it = s.requestCounts.find(key);
        return it != s.requestCounts.end() ? it->second : 0ULL;
    };
    const auto searchMetrics = effectiveSearchMetrics(s);
    const auto snapshotAge = findCompactCount("status_snapshot_age_ms");
    const auto snapshotStale = findCompactCount("status_snapshot_stale") > 0;

    os << title_banner("YAMS Daemon") << "\n\n";

    if (!s.storageWarning.empty()) {
        os << colorize("⚠ Slow storage: ", Ansi::YELLOW) << s.storageWarning << "\n\n";
    }

    // Single overview table with essential info
    std::vector<Row> overview;
    overview.push_back(
        {"State", paintStatus(stateSeverity, lifecycle), s.version.empty() ? "" : "v" + s.version});
    overview.push_back({"Uptime", format_duration(s.uptimeSeconds),
                        std::to_string(s.requestsProcessed) + " requests"});
    {
        std::ostringstream freshness;
        freshness << snapshotAge << " ms";
        overview.push_back(
            {"Freshness",
             paintStatus(snapshotStale ? Severity::Warn : Severity::Good, freshness.str()),
             snapshotStale ? "stale" : "fresh"});
    }

    // Memory: show governor if available, else basic RSS
    if (s.governorBudgetBytes > 0) {
        const char* levelNames[] = {"Normal", "Warning", "Critical", "Emergency"};
        uint8_t lvl = std::min(s.governorPressureLevel, static_cast<uint8_t>(3));
        Severity pressSev = (lvl == 0)   ? Severity::Good
                            : (lvl == 1) ? Severity::Warn
                                         : Severity::Bad;
        std::ostringstream memInfo;
        memInfo << std::fixed << std::setprecision(0)
                << (static_cast<double>(s.governorRssBytes) / (1024 * 1024)) << " / "
                << (static_cast<double>(s.governorBudgetBytes) / (1024 * 1024)) << " MB";
        overview.push_back({"Memory", paintStatus(pressSev, levelNames[lvl]), memInfo.str()});
    } else {
        Severity memSev = s.memoryUsageMb > 4096 ? Severity::Warn : Severity::Good;
        overview.push_back(
            {"Memory",
             paintStatus(memSev, std::to_string(static_cast<int>(s.memoryUsageMb)) + " MB"), ""});
    }

    // Database phase visibility: lets the user tell a slow open / repair from a hang.
    {
        namespace dbphase = yams::daemon::dbphase;
        const std::string& phase = s.databasePhase;
        const uint64_t elapsedMs = s.databasePhaseElapsedMs;
        const std::string elapsedSec =
            elapsedMs > 0 ? format_duration(elapsedMs / 1000) + " elapsed" : "";
        std::string label;
        std::string extra;
        Severity dbSev = Severity::Good;

        if (phase == dbphase::kOpening) {
            label = "Opening";
            extra = elapsedSec;
            dbSev = (elapsedMs > 30000) ? Severity::Warn : Severity::Good;
            if (!s.metadataDbPath.empty()) {
                if (!extra.empty())
                    extra += " · ";
                extra += s.metadataDbPath;
            }
        } else if (phase == dbphase::kRecovering) {
            label = "Repairing";
            extra = elapsedSec.empty() ? std::string("quarantining corrupt DB")
                                       : elapsedSec + " · quarantining corrupt DB";
            dbSev = Severity::Warn;
        } else if (phase == dbphase::kSalvaging) {
            label = "Salvaging";
            extra = elapsedSec.empty() ? std::string("recovering documents")
                                       : elapsedSec + " · recovering documents";
            dbSev = Severity::Warn;
        } else if (phase == dbphase::kMigrating) {
            label = "Migrating";
            extra = elapsedSec;
            dbSev = Severity::Warn;
        } else if (phase == dbphase::kReady ||
                   s.readinessStates.count(std::string(readiness::kDatabase))) {
            auto it = s.readinessStates.find(std::string(readiness::kDatabase));
            const bool ready = (it != s.readinessStates.end()) ? it->second : true;
            if (!ready) {
                label = "Initializing";
                extra = elapsedSec;
                dbSev = Severity::Warn;
            } else {
                label = "Ready";
                if (!s.databaseRecoveredFrom.empty()) {
                    extra = "recovered from " + s.databaseRecoveredFrom +
                            " · run 'yams repair --orphans'";
                    dbSev = Severity::Warn;
                }
            }
        }
        if (!label.empty()) {
            overview.push_back({"Database", paintStatus(dbSev, label), extra});
        }

        if (!s.maintenancePhase.empty() && s.maintenancePhase != "idle") {
            const std::string maintenanceElapsed =
                s.maintenancePhaseElapsedMs > 0
                    ? format_duration(s.maintenancePhaseElapsedMs / 1000) + " elapsed"
                    : "";
            std::string maintenanceLabel = s.maintenancePhase;
            std::replace(maintenanceLabel.begin(), maintenanceLabel.end(), '_', ' ');
            if (!maintenanceLabel.empty()) {
                maintenanceLabel.front() = static_cast<char>(
                    std::toupper(static_cast<unsigned char>(maintenanceLabel.front())));
            }
            overview.push_back({"DB Maintenance", paintStatus(Severity::Warn, maintenanceLabel),
                                maintenanceElapsed});
        }
    }

    // Search summary
    Severity searchSev = searchMetrics.queued > 50   ? Severity::Bad
                         : searchMetrics.queued > 10 ? Severity::Warn
                                                     : Severity::Good;
    std::ostringstream searchInfo;
    searchInfo << searchMetrics.active << " active · " << searchMetrics.queued << " queued";
    overview.push_back({"Search", paintStatus(searchSev, searchInfo.str()), ""});

    // Embeddings summary
    Severity embSev = s.embeddingAvailable ? Severity::Good : Severity::Warn;
    std::string embText = s.embeddingAvailable ? "Available" : "Unavailable";
    std::string embExtra = s.embeddingModel.empty() ? "" : s.embeddingModel;
    overview.push_back({"Embeddings", paintStatus(embSev, embText), embExtra});

    // Repair summary
    const bool repairRunning = findCompactCount("repair_running") > 0;
    const bool repairInProgress = findCompactCount("repair_in_progress") > 0;
    const uint64_t repairQueue = findCompactCount("repair_queue_depth");
    const uint64_t repairFailed = findCompactCount("repair_failed_operations");
    const uint64_t repairCurrentOp = findCompactCount("repair_current_operation_code");
    const uint64_t repairCurrentElapsedMs = findCompactCount("repair_current_operation_elapsed_ms");
    Severity repairSev = !repairRunning          ? Severity::Warn
                         : (repairFailed > 0)    ? Severity::Warn
                         : repairInProgress      ? Severity::Warn
                         : (repairCurrentOp > 0) ? Severity::Warn
                         : (repairQueue > 0)     ? Severity::Warn
                                                 : Severity::Good;
    std::ostringstream repairText;
    repairText << (repairRunning ? "Running" : "Stopped");
    if (repairInProgress) {
        repairText << " · RPC active";
    }
    if (repairCurrentOp > 0) {
        repairText << " · " << repairOperationLabel(repairCurrentOp);
        if (repairCurrentElapsedMs > 0) {
            repairText << " " << format_duration(repairCurrentElapsedMs / 1000);
        }
    }
    std::ostringstream repairExtra;
    repairExtra << repairQueue << " pending";
    if (repairFailed > 0) {
        repairExtra << " · " << repairFailed << " failed";
    }
    overview.push_back({"Repair", paintStatus(repairSev, repairText.str()), repairExtra.str()});

    // Maintenance / rebuild summary
    const bool topologyRebuildRunning =
        findCompactCount(std::string(yams::daemon::metrics::kTopologyRebuildRunning).c_str()) > 0;
    const uint64_t topologyDirtyDocs =
        findCompactCount(std::string(yams::daemon::metrics::kTopologyDirtyDocuments).c_str());
    const uint64_t topologyRunAgeMs =
        findCompactCount(std::string(yams::daemon::metrics::kTopologyRebuildRunningAgeMs).c_str());
    const bool vectorIndexReady = [&]() {
        auto it = s.readinessStates.find(std::string(readiness::kVectorIndex));
        return it == s.readinessStates.end() ? true : it->second;
    }();
    const bool vectorDisabled = !s.vectorDbInitAttempted;
    const std::string& vectorIndexEngine = s.vectorIndexEngine;
    const uint64_t vectorIndexProgress = [&]() -> uint64_t {
        auto it = s.initProgress.find(std::string(readiness::kVectorIndex));
        return it == s.initProgress.end() ? 0 : it->second;
    }();

    if (vectorDisabled) {
        // Vectors are disabled by configuration (e.g. YAMS_DISABLE_VECTORS=1 or an
        // empty vector config); this is intentional, not a rebuild failure.
        overview.push_back(
            {"Vector Index", paintStatus(Severity::Good, "disabled"), "by configuration"});
    }

    if (topologyRebuildRunning || (!vectorDisabled && !vectorIndexReady)) {
        std::vector<std::string> activity;
        if (!vectorIndexReady) {
            std::ostringstream label;
            if (vectorIndexEngine == "simeon_pq_adc") {
                label << "PQ rebuild";
            } else if (vectorIndexEngine == "vec0_l2") {
                label << "vec0 rebuild";
            } else if (vectorIndexEngine == "hnsw_cosine") {
                label << "HNSW rebuild";
            } else {
                label << "vector index rebuild";
            }
            if (vectorIndexProgress > 0 && vectorIndexProgress < 100) {
                label << " " << vectorIndexProgress << "%";
            }
            activity.push_back(label.str());
        }
        if (topologyRebuildRunning) {
            activity.push_back("topology rebuild");
        }

        std::ostringstream maintText;
        for (std::size_t i = 0; i < activity.size(); ++i) {
            if (i)
                maintText << " · ";
            maintText << activity[i];
        }

        std::ostringstream maintExtra;
        if (topologyDirtyDocs > 0) {
            maintExtra << topologyDirtyDocs << " dirty";
        }
        if (topologyRebuildRunning && topologyRunAgeMs > 0) {
            if (maintExtra.tellp() > 0)
                maintExtra << " · ";
            maintExtra << format_duration(topologyRunAgeMs / 1000);
        }
        overview.push_back(
            {"Maintenance", paintStatus(Severity::Warn, maintText.str()), maintExtra.str()});
    }

    render_rows(os, overview);

    // Memory sync (P2P) status, best-effort so a slow cycle never stalls the view.
    renderMemorySyncSection(ctx.memorySync, os);

    std::vector<std::string> dataDirWarnings;
    if (daemonUsesEphemeralData) {
        dataDirWarnings.push_back("Daemon is serving a temporary data directory");
    }
    if (dataDirMismatch && !ctx.explicitCustomSocket) {
        dataDirWarnings.push_back("Daemon data dir differs from current CLI/config");
    }
    if (!dataDirWarnings.empty()) {
        std::string joined;
        for (std::size_t i = 0; i < dataDirWarnings.size(); ++i) {
            if (i) {
                joined += " · ";
            }
            joined += dataDirWarnings[i];
        }
        os << "\n" << colorize("• Data dir warning: " + joined, Ansi::YELLOW) << "\n";
    }

    // Show issues if any
    if (!issues.empty()) {
        std::string joined;
        const std::size_t limit = std::min<std::size_t>(issues.size(), 4);
        for (std::size_t i = 0; i < limit; ++i) {
            if (i)
                joined += ", ";
            joined += issues[i];
        }
        if (issues.size() > limit)
            joined += ", …";
        os << "\n" << colorize("• Waiting on: " + joined, Ansi::YELLOW) << "\n";
    }

    // Show last error if any
    if (!s.lastError.empty()) {
        os << "\n" << colorize("✗ Error: " + s.lastError, Ansi::RED) << "\n";
    }

    os << "\n"
       << colorize("→ Use 'yams daemon status -d' for detailed diagnostics", Ansi::DIM) << "\n";
}

void renderDaemonStatusDetailed(const yams::daemon::StatusResponse& status,
                                const DaemonStatusRenderContext& ctx, std::ostream& os) {
    using namespace yams::cli::ui;

    std::vector<ReadinessDisplay> readinessList;
    readinessList.reserve(status.readinessStates.size());
    for (const auto& [key, ready] : status.readinessStates) {
        readinessList.push_back(classifyReadiness(key, ready));
    }

    std::vector<std::string> waiting;
    waiting.reserve(readinessList.size());
    namespace readiness = yams::daemon::readiness;
    auto suppressDerivedWaiting = [&](std::string_view key) {
        if (key == readiness::kTopologyArtifactsFresh ||
            key == readiness::kTopologyRebuildRunning || key == readiness::kVectorIndex) {
            return true;
        }
        const bool emptyCorpusReady =
            status.vectorDbInitAttempted && !status.vectorDbReady && status.embeddingAvailable;
        if (emptyCorpusReady && (key == readiness::kSearchEngineVectorUsable ||
                                 key == readiness::kSearchEngineHybridUsable)) {
            return true;
        }
        // Simeon lexical enhancement is opt-in: when not configured (or
        // skipped because the corpus exceeded the size budget), the
        // daemon stays on FTS5 only — these keys reflect runtime state,
        // not a blocker on readiness. Only surface them when an active
        // build is in progress (configured + building).
        auto find = [&](std::string_view k) {
            auto it = status.readinessStates.find(std::string(k));
            return it != status.readinessStates.end() && it->second;
        };
        const bool simeonConfigured = find(readiness::kSearchEngineLexicalEnhancementConfigured);
        const bool simeonBuilding = find(readiness::kSearchEngineLexicalEnhancementBuilding);
        if ((key == readiness::kSearchEngineLexicalEnhancementConfigured ||
             key == readiness::kSearchEngineLexicalEnhancementReady ||
             key == readiness::kSearchEngineLexicalEnhancementBuilding ||
             key == readiness::kSearchEngineFragmentGeometryReady) &&
            !(simeonConfigured && simeonBuilding)) {
            return true;
        }
        return false;
    };
    for (const auto& rd : readinessList) {
        if (rd.issue) {
            const std::string_view k{rd.key};
            const bool vectorDisabled = !status.vectorDbInitAttempted;
            const bool skipReadinessLabel =
                k == readiness::kSearchEngineBuildReasonInitial ||
                k == readiness::kSearchEngineBuildReasonRebuild ||
                k == readiness::kSearchEngineBuildReasonDegraded ||
                k == readiness::kVectorDbReady || k == readiness::kVectorDbInitAttempted ||
                (k == readiness::kVectorDb && status.vectorDbInitAttempted) ||
                (vectorDisabled &&
                 (k == readiness::kVectorDb || k == readiness::kVectorDbDim ||
                  k == readiness::kVectorIndex || k == readiness::kVectorEmbeddingsAvailable ||
                  k == readiness::kVectorScoringEnabled ||
                  k == readiness::kSearchEngineVectorUsable ||
                  k == readiness::kSearchEngineHybridUsable)) ||
                suppressDerivedWaiting(k);
            if (!skipReadinessLabel)
                waiting.push_back(rd.label);
        }
    }
    std::sort(waiting.begin(), waiting.end());
    waiting.erase(std::unique(waiting.begin(), waiting.end()), waiting.end());

    const auto daemonDataDir = daemonDataDirFromStatus(status);
    const auto configuredDataDir = ctx.configuredDataDir;
    const bool hasDaemonDataDir = daemonDataDir.has_value() && !daemonDataDir->empty();
    const bool dataDirMismatch = hasDaemonDataDir && !configuredDataDir.empty() &&
                                 normalizePath(*daemonDataDir) != normalizePath(configuredDataDir);
    const bool daemonUsesEphemeralData = hasDaemonDataDir && isEphemeralDataDir(*daemonDataDir);

    std::string lifecycle =
        !status.lifecycleState.empty()
            ? humanizeToken(status.lifecycleState)
            : (!status.overallStatus.empty()
                   ? humanizeToken(status.overallStatus)
                   : (status.ready ? std::string{"Ready"} : std::string{"Initializing"}));

    Severity stateSeverity =
        status.running ? (status.ready ? Severity::Good : Severity::Warn) : Severity::Bad;
    if (!status.lastError.empty())
        stateSeverity = Severity::Bad;

    os << title_banner("YAMS Daemon — Detailed") << "\n\n";

    std::vector<Row> overview;
    overview.push_back({"State", paintStatus(stateSeverity, lifecycle),
                        status.running ? (status.ready ? "ready" : "starting") : "stopped"});
    overview.push_back({"Version", status.version.empty() ? "unknown" : status.version, ""});
    overview.push_back({"Uptime", format_duration(status.uptimeSeconds), ""});
    overview.push_back({"Requests", std::to_string(status.requestsProcessed),
                        std::string{"connections: "} + std::to_string(status.activeConnections)});
    if (status.retryAfterMs > 0) {
        overview.push_back(
            {"Backpressure",
             paintStatus(Severity::Warn, std::to_string(status.retryAfterMs) + " ms cooldown"),
             ""});
    }
    if (!status.lastError.empty()) {
        overview.push_back({"Last error", paintStatus(Severity::Bad, status.lastError), ""});
    }
    render_rows(os, overview);

    os << "\n" << section_header("Resources") << "\n\n";
    std::vector<Row> resourceRows;
    {
        std::ostringstream cpu;
        cpu << std::fixed << std::setprecision(1) << status.cpuUsagePercent << "%";
        Severity cpuSeverity =
            status.cpuUsagePercent >= 95.0
                ? Severity::Bad
                : (status.cpuUsagePercent >= 80.0 ? Severity::Warn : Severity::Good);
        resourceRows.push_back({"CPU", paintStatus(cpuSeverity, cpu.str()), ""});
    }
    resourceRows.push_back(
        {"Memory",
         paintStatus(status.memoryUsageMb > 4096 ? Severity::Warn : Severity::Good,
                     std::to_string(static_cast<int>(status.memoryUsageMb)) + " MB"),
         ""});
    {
        auto count = [&](std::string_view key) -> uint64_t {
            auto it = status.requestCounts.find(std::string(key));
            return it != status.requestCounts.end() ? it->second : 0ULL;
        };
        const uint64_t mslEnabled = count("status_diag_msl_enabled");
        const uint64_t sampleUs = count("status_diag_memory_sample_us");
        const uint64_t allocUs = count("status_diag_allocator_sample_us");
        const uint64_t logBytes = count("status_diag_msl_stack_log_bytes");
        const uint64_t logFiles = count("status_diag_msl_stack_log_files");
        uint64_t logWarnBytes = count("status_diag_msl_stack_log_warn_bytes");
        if (logWarnBytes == 0) {
            logWarnBytes = 2ULL * 1024ULL * 1024ULL * 1024ULL;
        }
        if (mslEnabled || sampleUs || allocUs || logBytes || logFiles) {
            std::ostringstream value;
            bool first = true;
            auto append = [&](std::string_view part) {
                if (!first)
                    value << " · ";
                first = false;
                value << part;
            };
            if (mslEnabled)
                append("MSL on");
            if (sampleUs)
                append("mem_probe=" + std::to_string(sampleUs) + "µs");
            if (allocUs)
                append("alloc_probe=" + std::to_string(allocUs) + "µs");
            if (logBytes || logFiles) {
                append("stack_logs=" + std::to_string(logBytes / (1024ULL * 1024ULL)) + " MB/" +
                       std::to_string(logFiles) + " files");
            }
            const bool logPressure = logBytes >= logWarnBytes && logWarnBytes > 0;
            resourceRows.push_back(
                {"Memory Diagnostics",
                 paintStatus(logPressure ? Severity::Warn : Severity::Good, value.str()),
                 logPressure ? "restart after profiling; use memory profile" : ""});
        }
    }
    resourceRows.push_back(
        {"Pools",
         std::to_string(status.ipcPoolSize) + " ipc · " + std::to_string(status.ioPoolSize) + " io",
         ""});

    std::size_t threads = 0, activeJobs = 0, queuedJobs = 0;
    if (auto it = status.requestCounts.find("worker_threads"); it != status.requestCounts.end())
        threads = it->second;
    if (auto it = status.requestCounts.find("worker_active"); it != status.requestCounts.end())
        activeJobs = it->second;
    if (auto it = status.requestCounts.find("worker_queued"); it != status.requestCounts.end())
        queuedJobs = it->second;

    std::size_t workingThreads = activeJobs;
    std::size_t sleepingThreads = (threads > workingThreads) ? (threads - workingThreads) : 0;
    std::size_t util = threads ? static_cast<std::size_t>((100.0 * workingThreads) / threads) : 0;
    double workerFraction = threads > 0 ? static_cast<double>(workingThreads) / threads : 0.0;

    std::ostringstream workerVal;
    workerVal << progress_bar(workerFraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW, Ansi::RED,
                              true)
              << " " << threads << " threads · " << workingThreads << " working"
              << " · " << sleepingThreads << " sleeping · " << activeJobs << " jobs active";
    if (queuedJobs > 0)
        workerVal << " · " << queuedJobs << " queued";

    Severity workerSeverity =
        util >= 95 ? Severity::Bad : (util >= 85 ? Severity::Warn : Severity::Good);
    resourceRows.push_back({"Workers", paintStatus(workerSeverity, workerVal.str()), ""});
    render_rows(os, resourceRows);

    // Resource Governor section (memory pressure management)
    if (status.governorBudgetBytes > 0) {
        os << "\n" << section_header("Resource Governor") << "\n\n";
        std::vector<Row> governor;

        // Memory pressure level with progress bar
        const char* levelNames[] = {"Normal", "Warning", "Critical", "Emergency"};
        uint8_t lvl = std::min(status.governorPressureLevel, static_cast<uint8_t>(3));
        Severity pressSev = (lvl == 0)   ? Severity::Good
                            : (lvl == 1) ? Severity::Warn
                                         : Severity::Bad;
        double memFraction =
            status.governorBudgetBytes > 0
                ? static_cast<double>(status.governorRssBytes) / status.governorBudgetBytes
                : 0.0;
        uint64_t memMb = status.governorRssBytes / (1024ULL * 1024ULL);
        uint64_t budgetMb = status.governorBudgetBytes / (1024ULL * 1024ULL);
        std::ostringstream memBar;
        memBar << progress_bar(memFraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW, Ansi::RED,
                               true)
               << " " << static_cast<int>(memFraction * 100) << "% (" << memMb << "/" << budgetMb
               << " MB)";
        std::string statusIndicator = (lvl == 0) ? " ✓ " : (lvl == 1) ? " ⚠ " : " ✗ ";
        governor.push_back({"Memory",
                            paintStatus(pressSev, memBar.str() + statusIndicator + levelNames[lvl]),
                            ""});

        // Scaling headroom
        Severity headroomSev = (status.governorHeadroomPct >= 50)   ? Severity::Good
                               : (status.governorHeadroomPct >= 20) ? Severity::Warn
                                                                    : Severity::Bad;
        governor.push_back(
            {"Scaling Headroom",
             paintStatus(headroomSev, std::to_string(status.governorHeadroomPct) + "%"), ""});

        // ONNX concurrency
        if (status.onnxTotalSlots > 0) {
            std::ostringstream onnxInfo;
            onnxInfo << status.onnxUsedSlots << " / " << status.onnxTotalSlots << " slots";
            std::ostringstream onnxBreak;
            onnxBreak << "gliner " << status.onnxGlinerUsed << " · embed " << status.onnxEmbedUsed
                      << " · rerank " << status.onnxRerankerUsed;
            Severity onnxSev =
                status.onnxUsedSlots >= status.onnxTotalSlots ? Severity::Warn : Severity::Good;
            governor.push_back(
                {"ONNX Concurrency", paintStatus(onnxSev, onnxInfo.str()), onnxBreak.str()});
        }

        render_rows(os, governor);
    }

    os << "\n" << section_header("Transport") << "\n\n";
    std::vector<Row> transport;
    if (status.muxActiveHandlers || status.muxQueuedBytes || status.muxWriterBudgetBytes) {
        double pressure = 0.0;
        if (status.muxWriterBudgetBytes > 0) {
            pressure = (100.0 * static_cast<double>(status.muxQueuedBytes)) /
                       static_cast<double>(status.muxWriterBudgetBytes);
            if (pressure < 0)
                pressure = 0;
        }
        Severity muxSeverity =
            pressure >= 75.0 ? Severity::Bad : (pressure >= 40.0 ? Severity::Warn : Severity::Good);
        std::ostringstream muxVal;
        muxVal << status.muxActiveHandlers << " handlers";
        std::ostringstream muxExtra;
        muxExtra << "queued " << status.muxQueuedBytes << " B";
        if (status.muxWriterBudgetBytes > 0)
            muxExtra << " · budget " << status.muxWriterBudgetBytes << " B";
        transport.push_back({"Mux", paintStatus(muxSeverity, muxVal.str()), muxExtra.str()});
        if (status.muxWriterBudgetBytes > 0) {
            std::ostringstream pressureStr;
            pressureStr << std::fixed << std::setprecision(1) << pressure << "%";
            transport.push_back({"Mux pressure", paintStatus(muxSeverity, pressureStr.str()), ""});
        }
    }
    if (status.fsmTransitions || status.fsmHeaderReads || status.fsmPayloadWrites ||
        status.fsmPayloadReads || status.fsmBytesSent || status.fsmBytesReceived) {
        std::ostringstream fsm;
        fsm << status.fsmTransitions << " transitions";
        std::ostringstream fsmExtra;
        fsmExtra << "hdr " << status.fsmHeaderReads << " · read " << status.fsmPayloadReads
                 << " · write " << status.fsmPayloadWrites;
        transport.push_back({"IPC FSM", fsm.str(), fsmExtra.str()});
        std::ostringstream bytes;
        bytes << "sent " << status.fsmBytesSent << " · recv " << status.fsmBytesReceived;
        transport.push_back({"IPC bytes", bytes.str(), ""});
    }
    // Proxy socket info
    if (!status.proxySocketPath.empty()) {
        std::ostringstream proxyVal;
        proxyVal << status.proxyActiveConnections << " active";
        transport.push_back({"Proxy", proxyVal.str(), status.proxySocketPath});
    }
    if (!transport.empty())
        render_rows(os, transport);

    os << "\n" << section_header("Search") << "\n\n";
    std::vector<Row> searchRows;
    const auto searchMetrics = effectiveSearchMetrics(status);
    std::ostringstream base;
    base << searchMetrics.active << " active · " << searchMetrics.queued << " queued";
    std::ostringstream extra;
    extra << "executed " << searchMetrics.executed << " · cache " << std::fixed
          << std::setprecision(1) << (searchMetrics.cacheHitRate * 100.0) << "% · latency "
          << searchMetrics.avgLatencyUs << "µs";
    Severity searchSeverity = searchMetrics.queued > 50
                                  ? Severity::Bad
                                  : (searchMetrics.queued > 10 ? Severity::Warn : Severity::Good);
    searchRows.push_back({"Queries", paintStatus(searchSeverity, base.str()), extra.str()});
    if (searchMetrics.concurrencyLimit > 0) {
        searchRows.push_back({"Concurrency", std::to_string(searchMetrics.concurrencyLimit), ""});
    }
    render_rows(os, searchRows);

    // Post-Ingest Pipeline section
    os << "\n" << section_header("Post-Ingest Pipeline") << "\n\n";
    std::vector<Row> postIngestRows;
    auto findPostIngestCount = [&](const char* key) -> uint64_t {
        auto it = status.requestCounts.find(key);
        return it != status.requestCounts.end() ? it->second : 0ULL;
    };
    {
        uint64_t queued = findPostIngestCount("post_ingest_queued");
        uint64_t inflight = findPostIngestCount("post_ingest_inflight");
        uint64_t cap = findPostIngestCount("post_ingest_capacity");
        uint64_t rpcQueued = findPostIngestCount("post_ingest_rpc_queued");
        uint64_t rpcCap = findPostIngestCount("post_ingest_rpc_capacity");
        uint64_t rpcMaxPerBatch = findPostIngestCount("post_ingest_rpc_max_per_batch");
        uint64_t processed = findPostIngestCount("post_ingest_processed");
        uint64_t failed = findPostIngestCount("post_ingest_failed");
        uint64_t latency = findPostIngestCount("post_ingest_latency_ms_ema");
        uint64_t rate = findPostIngestCount("post_ingest_rate_sec_ema");

        // Queue progress bar
        double queueFraction = cap > 0 ? static_cast<double>(queued) / cap : 0.0;
        std::ostringstream queueBar;
        queueBar << progress_bar(queueFraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW, Ansi::RED,
                                 true)
                 << " " << static_cast<int>(queueFraction * 100) << "% (" << queued << "/" << cap
                 << ")";
        Severity qSev = queued > cap * 0.8 ? Severity::Bad
                                           : (queued > cap * 0.5 ? Severity::Warn : Severity::Good);
        postIngestRows.push_back({"Queue", paintStatus(qSev, queueBar.str()), ""});
        if (inflight > 0) {
            postIngestRows.push_back({"  Inflight", std::to_string(inflight) + " active", ""});
        }

        std::ostringstream throughput;
        throughput << rate << "/s · " << latency << "ms latency";
        postIngestRows.push_back({"Throughput", throughput.str(), ""});

        if (rpcCap > 0) {
            std::ostringstream rpc;
            rpc << rpcQueued << "/" << rpcCap;
            if (rpcMaxPerBatch > 0)
                rpc << " · max/batch " << rpcMaxPerBatch;
            postIngestRows.push_back({"RPC Queue", rpc.str(), ""});
        }

        std::ostringstream stats;
        stats << processed << " processed";
        if (failed > 0)
            stats << " · " << failed << " failed";
        Severity statSev = failed > 0 ? Severity::Warn : Severity::Good;
        postIngestRows.push_back({"Stats", paintStatus(statSev, stats.str()), ""});

        uint64_t watchEnabled = findPostIngestCount("watch_enabled");
        uint64_t watchInterval = findPostIngestCount("watch_interval_ms");
        if (watchEnabled > 0 || watchInterval > 0) {
            std::ostringstream watchVal;
            watchVal << (watchEnabled > 0 ? "enabled" : "disabled");
            if (watchInterval > 0)
                watchVal << " · " << watchInterval << "ms";
            postIngestRows.push_back({"Watch", watchVal.str(), ""});
        }

        // Per-stage breakdown with progress bars
        uint64_t extractInFlight = findPostIngestCount("extraction_inflight");
        uint64_t kgInFlight = findPostIngestCount("kg_inflight");
        uint64_t kgQueueDepth = findPostIngestCount("kg_queue_depth");
        uint64_t enrichInFlight = findPostIngestCount("enrich_inflight");
        uint64_t enrichQueueDepth = findPostIngestCount("enrich_queue_depth");
        uint64_t enrichLimit = std::max<uint64_t>(1, findPostIngestCount("post_enrich_limit"));
        // Get dynamic concurrency limits (floor of 1 to prevent div-by-zero)
        uint64_t extractLimit = std::max<uint64_t>(1, findPostIngestCount("post_extraction_limit"));
        uint64_t kgLimit = std::max<uint64_t>(1, findPostIngestCount("post_kg_limit"));

        // Fetch audit metrics for all stages
        uint64_t kgAuditQueued = findPostIngestCount("kg_queued");
        uint64_t kgAuditConsumed = findPostIngestCount("kg_consumed");
        uint64_t kgAuditDropped = findPostIngestCount("kg_dropped");
        uint64_t enrichAuditQueued = findPostIngestCount("symbol_queued") +
                                     findPostIngestCount("entity_queued") +
                                     findPostIngestCount("title_queued");
        uint64_t enrichAuditConsumed = findPostIngestCount("symbol_consumed") +
                                       findPostIngestCount("entity_consumed") +
                                       findPostIngestCount("title_consumed");
        uint64_t enrichAuditDropped = findPostIngestCount("symbol_dropped") +
                                      findPostIngestCount("entity_dropped") +
                                      findPostIngestCount("title_dropped");

        // Unified Pipeline Stages block
        using yams::cli::detail::StageInfo;
        StageInfo stages[] = {
            {"Extraction", extractInFlight, 0, extractLimit, 0, 0, 0},
            {"Knowledge Graph", kgInFlight, kgQueueDepth, kgLimit, kgAuditQueued, kgAuditConsumed,
             kgAuditDropped},
            {"Enrich", enrichInFlight, enrichQueueDepth, enrichLimit, enrichAuditQueued,
             enrichAuditConsumed, enrichAuditDropped},
        };

        auto stageRows = yams::cli::detail::renderPipelineStages(stages, 3);
        if (!stageRows.empty()) {
            postIngestRows.push_back({"", "", ""}); // Separator
            postIngestRows.push_back({subsection_header("Pipeline Stages"), "", ""});
            postIngestRows.insert(postIngestRows.end(), stageRows.begin(), stageRows.end());
        }
    }
    render_rows(os, postIngestRows);

    // Internal Processing Metrics section (Stream, WorkCoordinator, InternalEventBus)
    os << "\n" << section_header("Internal Processing") << "\n\n";
    std::vector<Row> internalRows;

    // WorkCoordinator metrics
    uint64_t workCoordThreads = findPostIngestCount("worker_threads");
    uint64_t workCoordActive = findPostIngestCount("worker_active");
    uint64_t workCoordRunning = findPostIngestCount("work_coordinator_running");
    uint64_t workCoordQueued = findPostIngestCount("worker_queued");

    if (workCoordRunning > 0 || workCoordThreads > 0) {
        std::ostringstream workVal;
        workVal << workCoordActive << "/" << workCoordThreads << " threads active";
        if (workCoordQueued > 0)
            workVal << " · " << workCoordQueued << " jobs queued";
        internalRows.push_back({"WorkCoordinator", workVal.str(), ""});
    }

    // Stream metrics
    uint64_t streamTotal = findPostIngestCount("stream_total");
    uint64_t streamBatches = findPostIngestCount("stream_batches");
    uint64_t streamKeepalives = findPostIngestCount("stream_keepalives");
    if (streamTotal > 0 || streamBatches > 0) {
        std::ostringstream streamVal;
        streamVal << streamTotal << " streams";
        if (streamBatches > 0) {
            streamVal << " · " << streamBatches << " batches";
        }
        if (streamKeepalives > 0) {
            streamVal << " · " << streamKeepalives << " keepalives";
        }
        internalRows.push_back({"Streams", streamVal.str(), ""});
    }

    if (!internalRows.empty()) {
        render_rows(os, internalRows);
    }

    os << "\n" << section_header("Repair Service") << "\n\n";
    std::vector<Row> repairRows;
    const bool repairRunning = findPostIngestCount("repair_running") > 0;
    const bool repairInProgress = findPostIngestCount("repair_in_progress") > 0;
    const uint64_t repairQueue = findPostIngestCount("repair_queue_depth");
    const uint64_t repairBatches = findPostIngestCount("repair_batches_attempted");
    const uint64_t repairEmbeddings = findPostIngestCount("repair_embeddings_generated");
    const uint64_t repairFailed = findPostIngestCount("repair_failed_operations");
    const uint64_t repairBacklog = findPostIngestCount("repair_total_backlog");
    const uint64_t repairProcessed = findPostIngestCount("repair_processed");
    const uint64_t repairCurrentOp = findPostIngestCount("repair_current_operation_code");
    const uint64_t repairCurrentElapsedMs =
        findPostIngestCount("repair_current_operation_elapsed_ms");

    std::string repairStatus = repairRunning ? "running" : "stopped";
    Severity repairStatusSev = repairRunning ? Severity::Good : Severity::Warn;
    if (repairInProgress) {
        repairStatus += " · RPC active";
        repairStatusSev = Severity::Warn;
    }
    if (repairCurrentOp > 0) {
        repairStatus += " · " + repairOperationLabel(repairCurrentOp);
        repairStatusSev = Severity::Warn;
    }
    repairRows.push_back({"Status", paintStatus(repairStatusSev, repairStatus), ""});

    if (repairCurrentOp > 0) {
        std::ostringstream current;
        current << repairOperationLabel(repairCurrentOp);
        if (repairCurrentElapsedMs > 0) {
            current << " · elapsed " << format_duration(repairCurrentElapsedMs / 1000);
        }
        repairRows.push_back({"Current", paintStatus(Severity::Warn, current.str()), ""});
    }

    if (repairBacklog > 0) {
        const double fraction = std::min(1.0, static_cast<double>(repairProcessed) / repairBacklog);
        std::ostringstream progress;
        progress << progress_bar(fraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW, Ansi::RED, true)
                 << " " << repairProcessed << "/" << repairBacklog;
        repairRows.push_back({"Progress", progress.str(), ""});
    }

    std::ostringstream queue;
    queue << repairQueue << " pending";
    repairRows.push_back({"Queue", queue.str(), ""});

    std::ostringstream repairStats;
    repairStats << repairBatches << " batches · " << repairEmbeddings << " embeddings";
    Severity repairStatsSev = repairFailed > 0 ? Severity::Warn : Severity::Good;
    if (repairFailed > 0) {
        repairStats << " · " << repairFailed << " failed";
    }
    repairRows.push_back({"Stats", paintStatus(repairStatsSev, repairStats.str()), ""});

    render_rows(os, repairRows);

    const bool topologyRebuildRunning = findPostIngestCount("topology_rebuild_running") > 0;
    const uint64_t topologyDirtyDocs = findPostIngestCount("topology_dirty_documents");
    const uint64_t topologyRunAgeMs = findPostIngestCount("topology_rebuild_running_age_ms");
    const uint64_t topologyLastDurationMs = findPostIngestCount("topology_last_duration_ms");
    const bool vectorIndexReady = [&]() {
        auto it = status.readinessStates.find(std::string(readiness::kVectorIndex));
        return it == status.readinessStates.end() ? true : it->second;
    }();
    const std::string& vectorIndexEngine = status.vectorIndexEngine;
    const uint64_t vectorIndexProgress = [&]() -> uint64_t {
        auto it = status.initProgress.find(std::string(readiness::kVectorIndex));
        return it == status.initProgress.end() ? 0 : it->second;
    }();

    const bool vectorDisabled = !status.vectorDbInitAttempted;

    if (topologyRebuildRunning || (!vectorDisabled && !vectorIndexReady)) {
        os << "\n" << section_header("Maintenance") << "\n\n";
        std::vector<Row> maintenanceRows;

        std::ostringstream indexState;
        std::string indexLabel = "Vector Index";
        if (vectorIndexEngine == "simeon_pq_adc") {
            indexLabel = "PQ Index";
        } else if (vectorIndexEngine == "vec0_l2") {
            indexLabel = "vec0 Index";
        } else if (vectorIndexEngine == "hnsw_cosine") {
            indexLabel = "HNSW Index";
        }
        if (vectorDisabled) {
            indexState << "disabled (by configuration)";
        } else if (vectorIndexReady) {
            indexState << "ready";
        } else {
            indexState << "rebuilding";
            if (vectorIndexProgress > 0 && vectorIndexProgress < 100) {
                indexState << " · " << vectorIndexProgress << "%";
            }
        }
        maintenanceRows.push_back(
            {indexLabel,
             paintStatus(vectorDisabled ? Severity::Good
                                        : (vectorIndexReady ? Severity::Good : Severity::Warn),
                         indexState.str()),
             ""});

        if (topologyRebuildRunning) {
            std::ostringstream topoState;
            topoState << "running";
            std::ostringstream topoExtra;
            if (topologyDirtyDocs > 0) {
                topoExtra << topologyDirtyDocs << " dirty";
            }
            if (topologyRunAgeMs > 0) {
                if (topoExtra.tellp() > 0)
                    topoExtra << " · ";
                topoExtra << "age " << format_duration(topologyRunAgeMs / 1000);
            }
            if (topologyLastDurationMs > 0) {
                if (topoExtra.tellp() > 0)
                    topoExtra << " · ";
                topoExtra << "last " << format_duration(topologyLastDurationMs / 1000);
            }
            maintenanceRows.push_back(
                {"Topology", paintStatus(Severity::Warn, topoState.str()), topoExtra.str()});
        }

        render_rows(os, maintenanceRows);
    }

    os << "\n" << section_header("Storage & Embeddings") << "\n\n";
    std::vector<Row> storageRows;
    auto findCount = [&](const char* key) -> uint64_t {
        auto it = status.requestCounts.find(key);
        return it != status.requestCounts.end() ? it->second : 0ULL;
    };
    const uint64_t docs = findCount("documents_total");
    const uint64_t indexed = findCount("documents_indexed");
    if (docs > 0 || indexed > 0) {
        std::ostringstream docsVal;
        docsVal << docs << " docs";
        if (indexed > 0)
            docsVal << " · indexed " << indexed;
        storageRows.push_back({"Documents", docsVal.str(), ""});
    }
    const uint64_t casObjects = findCount("storage_documents");
    if (casObjects > 0) {
        std::ostringstream casVal;
        casVal << casObjects << " objects";
        storageRows.push_back({"CAS Objects", casVal.str(), ""});
    }
    const uint64_t logical = findCount("storage_logical_bytes");
    const uint64_t physical = findCount("physical_total_bytes");
    const uint64_t dedup = findCount("cas_dedup_saved_bytes");
    const uint64_t comp = findCount("cas_compress_saved_bytes");
    auto humanBytes = [](uint64_t b) {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        auto val = static_cast<double>(b);
        int idx = 0;
        while (val >= 1024.0 && idx < 4) {
            val /= 1024.0;
            ++idx;
        }
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(val < 10 ? 1 : 0) << val << units[idx];
        return oss.str();
    };
    if (logical > 0 || physical > 0) {
        std::ostringstream size;
        size << "logical " << humanBytes(logical);
        if (physical > 0)
            size << " · physical " << humanBytes(physical);
        storageRows.push_back({"Storage", size.str(), ""});
    }
    if (dedup > 0 || comp > 0) {
        std::ostringstream savings;
        savings << "dedup " << humanBytes(dedup);
        if (comp > 0)
            savings << " · compress " << humanBytes(comp);
        storageRows.push_back({"Savings", savings.str(), ""});
    }

    // Storage breakdown with overhead details
    const uint64_t storageObjects = findCount("storage_objects_bytes");
    const uint64_t storageRefsDb = findCount("storage_refs_db_bytes");
    const uint64_t dbBytes = findCount("db_bytes");
    const uint64_t vectorsDbBytes = findCount("vectors_db_bytes");
    const uint64_t vectorIndexBytes = findCount("vector_index_bytes");

    if (storageObjects > 0 || storageRefsDb > 0 || dbBytes > 0 || vectorsDbBytes > 0 ||
        vectorIndexBytes > 0) {
        storageRows.push_back({"", "", ""}); // Separator
        storageRows.push_back({subsection_header("Disk Usage Breakdown"), "", ""});

        if (storageObjects > 0) {
            std::ostringstream detail;
            const uint64_t objFiles = findCount("storage_objects_files");
            if (objFiles > 0) {
                detail << objFiles << " files";
            }
            storageRows.push_back({"  CAS Blocks", humanBytes(storageObjects), detail.str()});
        }

        if (storageRefsDb > 0) {
            storageRows.push_back({"  Ref Counter DB", humanBytes(storageRefsDb), ""});
        }

        if (dbBytes > 0) {
            storageRows.push_back({"  Metadata DB", humanBytes(dbBytes), ""});
        }

        if (vectorsDbBytes > 0) {
            storageRows.push_back({"  Vector DB", humanBytes(vectorsDbBytes), ""});
        }

        if (vectorIndexBytes > 0) {
            storageRows.push_back({"  Vector Index", humanBytes(vectorIndexBytes), ""});
        }

        // Calculate total overhead (everything except CAS blocks)
        const uint64_t totalOverhead = storageRefsDb + dbBytes + vectorsDbBytes + vectorIndexBytes;
        if (totalOverhead > 0 && storageObjects > 0) {
            double overheadPct = (static_cast<double>(totalOverhead) / storageObjects) * 100.0;
            std::ostringstream pct;
            pct << std::fixed << std::setprecision(1) << overheadPct << "% of CAS";
            storageRows.push_back({"  Total Overhead", humanBytes(totalOverhead), pct.str()});
        }
    }

    auto getReadiness = [&](const char* key) -> bool {
        auto it = status.readinessStates.find(key);
        return it != status.readinessStates.end() && it->second;
    };

    // Vector DB
    {
        const bool ready = getReadiness("vector_db");
        const bool initialized = status.vectorDbInitAttempted;
        Severity sev = ready ? Severity::Good : Severity::Warn;
        std::string text =
            ready ? "Ready" : (initialized ? "Initialized (empty)" : "Not initialized");
        std::string extra;
        if (status.vectorDbDim > 0) {
            extra = "dim=" + std::to_string(status.vectorDbDim);
        }
        storageRows.push_back({"Vector DB", paintStatus(sev, text), extra});
    }

    // Embeddings
    {
        bool available = status.embeddingAvailable;
        Severity sev = available ? Severity::Good : Severity::Warn;
        std::string text = available ? "Available" : "Unavailable";
        std::string extra;
        if (!status.embeddingModel.empty())
            extra += status.embeddingModel;
        if (!status.embeddingBackend.empty()) {
            if (!extra.empty())
                extra += " · ";
            extra += status.embeddingBackend;
        }
        if (status.embeddingDim > 0) {
            if (!extra.empty())
                extra += " · ";
            extra += "dim " + std::to_string(status.embeddingDim);
        }
        storageRows.push_back({"Embeddings", paintStatus(sev, text), extra});
    }

    if (!status.contentStoreRoot.empty()) {
        storageRows.push_back({"Content root", status.contentStoreRoot, status.contentStoreError});
    }
    if (hasDaemonDataDir) {
        storageRows.push_back({"Daemon data dir", daemonDataDir->string(), ""});
    }
    if (!configuredDataDir.empty()) {
        std::string extra;
        if (dataDirMismatch) {
            extra = paintStatus(Severity::Warn, "daemon differs from CLI/config");
        }
        storageRows.push_back({"Expected data dir", configuredDataDir.string(), extra});
    }
    render_rows(os, storageRows);

    std::vector<Row> dataDirWarningRows;
    if (daemonUsesEphemeralData) {
        dataDirWarningRows.push_back(
            {"Ephemeral data dir",
             paintStatus(Severity::Warn, "Daemon is serving a temporary data directory"),
             daemonDataDir->string()});
    }
    if (dataDirMismatch && !ctx.explicitCustomSocket) {
        dataDirWarningRows.push_back(
            {"Data dir mismatch",
             paintStatus(Severity::Warn,
                         "Daemon is not using the current CLI/config data directory"),
             "Restart daemon if this was accidental"});
    }
    if (!dataDirWarningRows.empty()) {
        os << "\n" << section_header("Data Directory Warnings") << "\n\n";
        render_rows(os, dataDirWarningRows);
    }

    // Memory sync (P2P) status, best-effort so a slow cycle never stalls the view.
    renderMemorySyncSection(ctx.memorySync, os);

    // Only show Readiness section if there are issues or non-ready components
    std::vector<Row> issueRows;
    auto suppressDetailedIssue = [&](const std::string& lowerLabel) {
        if ((lowerLabel == "embedding ready" || lowerLabel == "embedding_ready") &&
            (status.embeddingAvailable || (status.readinessStates.contains("model_provider") &&
                                           status.readinessStates.at("model_provider")))) {
            return true;
        }
        if ((lowerLabel == "plugins ready" || lowerLabel == "plugins_ready") &&
            (status.readinessStates.contains("plugins") && status.readinessStates.at("plugins"))) {
            return true;
        }
        if (lowerLabel == "topology artifacts fresh" || lowerLabel == "topology_artifacts_fresh" ||
            lowerLabel == "topology rebuild running" || lowerLabel == "topology_rebuild_running" ||
            lowerLabel == "vector index" || lowerLabel == "vector_index") {
            return true;
        }
        return false;
    };
    auto matchesReady = [&](std::string_view k) {
        auto it = status.readinessStates.find(std::string(k));
        return it != status.readinessStates.end() && it->second;
    };
    auto isSimeonKey = [](std::string_view k) {
        return k == readiness::kSearchEngineLexicalEnhancementConfigured ||
               k == readiness::kSearchEngineLexicalEnhancementReady ||
               k == readiness::kSearchEngineLexicalEnhancementBuilding ||
               k == readiness::kSearchEngineFragmentGeometryReady;
    };
    const bool simeonActiveBuild =
        matchesReady(readiness::kSearchEngineLexicalEnhancementConfigured) &&
        matchesReady(readiness::kSearchEngineLexicalEnhancementBuilding);
    const bool searchEngineReady = matchesReady(readiness::kSearchEngine);
    for (const auto& rd : readinessList) {
        // Skip "degraded" flags (inverses of ready flags) and items already shown
        // elsewhere
        std::string lowerLabel = rd.label;
        std::transform(lowerLabel.begin(), lowerLabel.end(), lowerLabel.begin(), ::tolower);
        bool isDegraded = lowerLabel.find("degraded") != std::string::npos;
        bool isAlreadyShown = lowerLabel.find("vector db") != std::string::npos ||
                              lowerLabel.find("embedding") != std::string::npos;
        // Skip "build reason" keys - they're informational, not readiness indicators.
        // The search_engine key itself indicates readiness.
        bool isBuildReason = lowerLabel.find("build reason") != std::string::npos;
        bool isSimeonSteady = (isSimeonKey(rd.key) && !simeonActiveBuild) ||
                              (isSimeonKey(rd.key) && searchEngineReady);
        // Vectors disabled by configuration: the vector DB, index, scoring, and
        // vector-dependent search gates are intentionally off, not unhealthy.
        const bool vectorGate =
            vectorDisabled &&
            (rd.key == readiness::kVectorIndex || rd.key == readiness::kVectorDbDim ||
             rd.key == readiness::kVectorEmbeddingsAvailable ||
             rd.key == readiness::kVectorScoringEnabled ||
             rd.key == readiness::kSearchEngineVectorUsable ||
             rd.key == readiness::kSearchEngineHybridUsable);

        if (!isDegraded && !isAlreadyShown && !isBuildReason && !isSimeonSteady && !vectorGate &&
            rd.issue && !suppressDetailedIssue(lowerLabel)) {
            issueRows.push_back({rd.label, paintStatus(rd.severity, rd.text), ""});
        }
    }
    if (!issueRows.empty()) {
        os << "\n" << section_header("Components Not Ready") << "\n\n";
        render_rows(os, issueRows);
    }

    if (!status.initProgress.empty()) {
        os << "\n" << section_header("Initialization progress") << "\n\n";
        std::vector<Row> initRows;
        std::vector<std::pair<std::string, uint8_t>> init(status.initProgress.begin(),
                                                          status.initProgress.end());
        std::sort(init.begin(), init.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });
        const std::size_t limit = std::min<std::size_t>(init.size(), 10);
        for (std::size_t i = 0; i < limit; ++i) {
            initRows.push_back({humanizeToken(init[i].first),
                                std::to_string(static_cast<int>(init[i].second)) + "%", ""});
        }
        render_rows(os, initRows);
    }

    if (!status.requestCounts.empty()) {
        auto formatCountValue = [](const std::string& key, uint64_t value) -> std::string {
            std::string lowerKey = key;
            std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
            const bool isBytes = lowerKey.find("bytes") != std::string::npos ||
                                 lowerKey.find("_mb") != std::string::npos ||
                                 lowerKey.find("_kb") != std::string::npos ||
                                 lowerKey.find("_gb") != std::string::npos;

            if (isBytes && value > 1024) {
                const char* units[] = {"B", "KB", "MB", "GB", "TB"};
                auto val = static_cast<double>(value);
                int idx = 0;
                while (val >= 1024.0 && idx < 4) {
                    val /= 1024.0;
                    ++idx;
                }
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(val < 10 ? 1 : 0) << val << " "
                    << units[idx];
                return oss.str();
            }

            if (value >= 100000) {
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(value < 1000000 ? 0 : 1)
                    << (static_cast<double>(value) / 1000.0) << "k";
                return oss.str();
            } else if (value >= 10000) {
                std::string num = std::to_string(value);
                std::string formatted;
                int count = 0;
                for (auto it = num.rbegin(); it != num.rend(); ++it) {
                    if (count > 0 && count % 3 == 0)
                        formatted.insert(0, 1, ',');
                    formatted.insert(0, 1, *it);
                    ++count;
                }
                return formatted;
            }

            return std::to_string(value);
        };

        // Filter out internal/tuning counters - show user-facing metrics only
        std::vector<std::pair<std::string, size_t>> counts;
        for (const auto& [key, value] : status.requestCounts) {
            std::string lowerKey = key;
            std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);

            // Skip internal state/tuning counters
            bool isInternal = lowerKey.find("tuning_") == 0 ||
                              lowerKey.find("_fsm_state") != std::string::npos ||
                              lowerKey.find("service_fsm") != std::string::npos ||
                              lowerKey.find("embedding_state") != std::string::npos ||
                              lowerKey.find("plugin_host_state") != std::string::npos;

            if (!isInternal) {
                counts.emplace_back(key, value);
            }
        }

        if (!counts.empty()) {
            std::sort(counts.begin(), counts.end(),
                      [](const auto& a, const auto& b) { return a.second > b.second; });
            os << "\n" << section_header("Top Metrics") << "\n\n";
            std::vector<Row> countRows;
            const std::size_t limit = std::min<std::size_t>(counts.size(), 10);
            for (std::size_t i = 0; i < limit; ++i) {
                countRows.push_back({humanizeToken(counts[i].first),
                                     formatCountValue(counts[i].first, counts[i].second), ""});
            }
            render_rows(os, countRows);
        }
    }

    if (!status.providers.empty()) {
        os << "\n" << section_header("Providers") << "\n\n";
        std::vector<Row> providerRows;
        const std::size_t limit = std::min<std::size_t>(status.providers.size(), 8);
        for (std::size_t i = 0; i < limit; ++i) {
            const auto& p = status.providers[i];
            Severity provSeverity = p.ready ? Severity::Good : Severity::Warn;
            std::string extra;
            if (p.modelsLoaded > 0)
                extra += std::to_string(p.modelsLoaded) + " models";
            if (!p.error.empty()) {
                if (!extra.empty())
                    extra += " · ";
                extra += p.error;
                provSeverity = Severity::Warn;
            }
            if (p.isProvider) {
                if (!extra.empty())
                    extra += " · ";
                extra += "active";
            }
            providerRows.push_back(
                {p.name.empty() ? "(unnamed)" : p.name,
                 paintStatus(provSeverity,
                             p.ready ? "Ready" : (p.degraded ? "Degraded" : "Starting")),
                 extra});
        }
        render_rows(os, providerRows);
    }

    {
        auto getCount = [&](std::string_view key) -> uint64_t {
            auto it = status.requestCounts.find(std::string(key));
            return it == status.requestCounts.end() ? 0ULL : it->second;
        };
        const auto contentExtractorCount = getCount("content_extractors_loaded");
        const auto symbolExtractorCount = getCount("symbol_extractors_loaded");
        const auto entityExtractorCount = getCount("entity_extractors_loaded");
        const bool titleExtractorEnabled = getCount("title_extractor_enabled") != 0;
        const auto skippedPluginCount = getCount("plugin_skipped_count");
        if (contentExtractorCount > 0 || symbolExtractorCount > 0 || entityExtractorCount > 0 ||
            titleExtractorEnabled || skippedPluginCount > 0) {
            os << "\n" << section_header("Plugin Capabilities") << "\n\n";
            std::vector<Row> capabilityRows;
            capabilityRows.push_back(
                {"Content Extractors",
                 paintStatus(contentExtractorCount > 0 ? Severity::Good : Severity::Warn,
                             contentExtractorCount > 0 ? "Ready" : "Unavailable"),
                 std::to_string(contentExtractorCount) + " loaded"});
            capabilityRows.push_back(
                {"Symbol Extractors",
                 paintStatus(symbolExtractorCount > 0 ? Severity::Good : Severity::Warn,
                             symbolExtractorCount > 0 ? "Ready" : "Unavailable"),
                 std::to_string(symbolExtractorCount) + " loaded"});
            capabilityRows.push_back(
                {"Entity Extractors",
                 paintStatus(entityExtractorCount > 0 ? Severity::Good : Severity::Warn,
                             entityExtractorCount > 0 ? "Ready" : "Unavailable"),
                 std::to_string(entityExtractorCount) + " loaded"});
            capabilityRows.push_back(
                {"Title Extractor",
                 paintStatus(titleExtractorEnabled ? Severity::Good : Severity::Warn,
                             titleExtractorEnabled ? "Enabled" : "Disabled"),
                 titleExtractorEnabled ? "entity-backed title enrichment"
                                       : "requires entity extractors"});
            if (skippedPluginCount > 0) {
                capabilityRows.push_back({"Plugin Warnings",
                                          paintStatus(Severity::Warn, "Skipped during load"),
                                          std::to_string(skippedPluginCount) + " plugin(s)"});
            }
            render_rows(os, capabilityRows);
        }
    }

    if (!status.skippedPlugins.empty()) {
        os << "\n" << section_header("Skipped Plugins") << "\n\n";
        std::vector<Row> skippedRows;
        const std::size_t limit = std::min<std::size_t>(status.skippedPlugins.size(), 8);
        for (std::size_t i = 0; i < limit; ++i) {
            const auto& sp = status.skippedPlugins[i];
            skippedRows.push_back({sp.path.empty() ? "(unknown)" : sp.path,
                                   paintStatus(Severity::Warn, "Skipped"), sp.reason});
        }
        render_rows(os, skippedRows);
    }

    if (!status.models.empty()) {
        os << "\n" << section_header("Models") << "\n\n";
        std::vector<Row> modelRows;
        const std::size_t limit = std::min<std::size_t>(status.models.size(), 10);
        for (std::size_t i = 0; i < limit; ++i) {
            const auto& m = status.models[i];
            if (m.name == "(provider)")
                continue;
            std::ostringstream detail;
            if (m.memoryMb > 0)
                detail << m.memoryMb << " MB";
            if (m.requestCount > 0) {
                if (detail.tellp() > 0)
                    detail << " · ";
                detail << m.requestCount << " req";
            }
            modelRows.push_back({m.name, m.type, detail.str()});
        }
        render_rows(os, modelRows);
    }

    if (!waiting.empty()) {
        std::string joined;
        for (std::size_t i = 0; i < waiting.size(); ++i) {
            if (i)
                joined += ", ";
            joined += waiting[i];
        }
        os << "\n" << colorize("• Waiting on: " + joined, Ansi::YELLOW) << "\n";
    }
}

} // namespace yams::cli
