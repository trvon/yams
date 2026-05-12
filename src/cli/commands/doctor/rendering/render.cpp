#include <yams/cli/doctor/rendering/render.h>
#include <yams/cli/doctor/plugin_trust.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/recommendation_util.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <filesystem>
#include <set>
#include <vector>

namespace yams::cli::doctor {

void DoctorRender::printHeader(std::ostream& os, const std::string& title) {
    os << "\n" << title << "\n";
    for (size_t i = 0; i < title.size(); ++i)
        os << '-';
    os << "\n";
}

void DoctorRender::printStatusLine(std::ostream& os, const std::string& label,
                                   const std::string& value) {
    os << "- " << label << ": " << value << "\n";
}

void DoctorRender::printSummary(std::ostream& os, const std::string& title,
                                const std::vector<StepResult>& steps) {
    printHeader(os, title);
    for (const auto& s : steps) {
        os << "  " << (s.ok ? ui::status_ok(s.name) : ui::status_error(s.name));
        if (!s.message.empty())
            os << " — " << s.message;
        os << "\n";
    }
}

nlohmann::json DoctorRender::buildDoctorJson(YamsCLI* cli, const CachedDaemonState& cachedState,
                                             const RecommendationBuilder* recs) {
    nlohmann::json jsonResult;
    jsonResult["daemon"]["socket"] = cachedState.effectiveSocket;
    jsonResult["daemon"]["running"] = cachedState.daemonUp;

    if (cachedState.status) {
        const auto& s = *cachedState.status;
        jsonResult["daemon"]["ready"] = s.ready;
        jsonResult["daemon"]["running"] = s.running;
        jsonResult["daemon"]["version"] = s.version;
        jsonResult["daemon"]["lifecycle_state"] = s.lifecycleState;
        jsonResult["daemon"]["active_connections"] = s.activeConnections;
        jsonResult["daemon"]["memory_mb"] = s.memoryUsageMb;
        jsonResult["daemon"]["cpu_percent"] = s.cpuUsagePercent;

        jsonResult["embedding"]["available"] = s.embeddingAvailable;
        jsonResult["embedding"]["backend"] = s.embeddingBackend;
        jsonResult["embedding"]["model"] = s.embeddingModel;
        jsonResult["embedding"]["path"] = s.embeddingModelPath;
        jsonResult["embedding"]["dimension"] = s.embeddingDim;
        jsonResult["embedding"]["threads_intra"] = s.embeddingThreadsIntra;
        jsonResult["embedding"]["threads_inter"] = s.embeddingThreadsInter;

        for (const auto& [k, v] : s.readinessStates) {
            jsonResult["readiness"][k] = v;
        }

        nlohmann::json pluginsJson = nlohmann::json::array();
        for (const auto& p : s.providers) {
            nlohmann::json pj;
            pj["name"] = p.name;
            pj["ready"] = p.ready;
            pj["degraded"] = p.degraded;
            pj["is_provider"] = p.isProvider;
            pj["models_loaded"] = p.modelsLoaded;
            if (!p.error.empty())
                pj["error"] = p.error;
            pluginsJson.push_back(std::move(pj));
        }
        jsonResult["plugins"] = std::move(pluginsJson);
    }

    try {
        auto trustRootsFromDaemon = PluginTrust::fetchTrustedRootsFromDaemon(cli);
        bool usedDaemonTrust = trustRootsFromDaemon.has_value();

        std::vector<std::filesystem::path> trustedRoots;
        if (trustRootsFromDaemon) {
            trustedRoots = PluginTrust::dedupeRoots(*trustRootsFromDaemon);
        } else {
            auto localTrusted = PluginTrust::readTrusted();
            trustedRoots.assign(localTrusted.begin(), localTrusted.end());
        }

        bool strictMode = PluginTrust::resolveStrictPluginDirMode();
        auto defaultRoots = PluginTrust::getDefaultPluginRoots(strictMode);
        auto checks = PluginTrust::assessTrustedRoots(trustedRoots, strictMode, defaultRoots);

        nlohmann::json trustJson;
        trustJson["source"] = usedDaemonTrust ? "daemon" : "local";
        trustJson["trust_file"] = yams::config::get_daemon_plugin_trust_file().string();
        trustJson["legacy_trust_file"] = yams::config::get_legacy_plugin_trust_file().string();
        trustJson["strict_mode"] = strictMode;

        nlohmann::json rootsJson = nlohmann::json::array();
        for (const auto& check : checks) {
            nlohmann::json root;
            root["path"] = check.path.string();
            root["issues"] = check.issues;
            root["problematic"] =
                std::any_of(check.issues.begin(), check.issues.end(), [](const std::string& issue) {
                    return issue == "missing" || issue == "temporary-path" ||
                           issue == "build-artifact-path";
                });
            rootsJson.push_back(std::move(root));
        }
        trustJson["roots"] = std::move(rootsJson);
        jsonResult["plugin_trust"] = std::move(trustJson);
    } catch (...) {
    }

    namespace fs = std::filesystem;
    fs::path modelsPath = cli ? cli->getDataPath() / "models" : fs::path();
    nlohmann::json modelsJson = nlohmann::json::array();
    std::error_code ec;
    if (!modelsPath.empty() && fs::exists(modelsPath, ec) && fs::is_directory(modelsPath, ec)) {
        for (const auto& entry : fs::directory_iterator(modelsPath, ec)) {
            if (!entry.is_directory())
                continue;
            fs::path modelOnnx = entry.path() / "model.onnx";
            if (fs::exists(modelOnnx, ec)) {
                nlohmann::json mj;
                mj["name"] = entry.path().filename().string();
                mj["has_config"] = fs::exists(entry.path() / "config.json", ec) ||
                                   fs::exists(entry.path() / "sentence_bert_config.json", ec);
                mj["has_tokenizer"] = fs::exists(entry.path() / "tokenizer.json", ec);
                modelsJson.push_back(std::move(mj));
            }
        }
    }
    jsonResult["models"] = std::move(modelsJson);

    fs::path vecDbPath = cli ? cli->getDataPath() / "vectors.db" : fs::path();
    jsonResult["vector_db"]["path"] = vecDbPath.string();
    jsonResult["vector_db"]["exists"] = !vecDbPath.empty() && fs::exists(vecDbPath, ec);

    try {
        auto r2Status = DoctorContext::evaluateR2Config();
        jsonResult["storage"]["r2"]["enabled"] = r2Status.enabled;
        jsonResult["storage"]["r2"]["auth_mode"] = r2Status.authMode;
        jsonResult["storage"]["r2"]["account_id"] = r2Status.accountId;
        jsonResult["storage"]["r2"]["keychain_supported"] = r2Status.keychainSupported;
        jsonResult["storage"]["r2"]["keychain_token_present"] = r2Status.tokenPresent;
        if (!r2Status.detail.empty()) {
            jsonResult["storage"]["r2"]["detail"] = r2Status.detail;
        }
    } catch (...) {
    }

    try {
        auto db = cli->getDatabase();
        if (db && db->isOpen()) {
            auto countTable = [&](const char* sql) -> long long {
                auto stR = db->prepare(sql);
                if (!stR)
                    return -1;
                auto st = std::move(stR).value();
                auto step = st.step();
                if (step && step.value())
                    return st.getInt64(0);
                return -1;
            };
            jsonResult["knowledge_graph"]["nodes"] = countTable("SELECT COUNT(1) FROM kg_nodes");
            jsonResult["knowledge_graph"]["edges"] = countTable("SELECT COUNT(1) FROM kg_edges");
            jsonResult["knowledge_graph"]["aliases"] =
                countTable("SELECT COUNT(1) FROM kg_aliases");
            jsonResult["knowledge_graph"]["embeddings"] =
                countTable("SELECT COUNT(1) FROM kg_node_embeddings");
            jsonResult["knowledge_graph"]["doc_entities"] =
                countTable("SELECT COUNT(1) FROM doc_entities");
        }
    } catch (...) {
    }

    if (recs && !recs->empty()) {
        jsonResult["recommendations"] = yams::cli::recommendationsToJson(*recs);
    }

    return jsonResult;
}

} // namespace yams::cli::doctor
