#include <yams/cli/doctor/rendering/display.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/recommendation_util.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <sstream>
#include <thread>

namespace yams::cli::doctor {

void DoctorDisplay::renderEmbeddingRuntime(std::ostream& os, const daemon::StatusResponse& status) {
    os << "\n" << ui::section_header("Embedding Runtime") << "\n\n";

    std::vector<ui::Row> embRows;
    std::string availStatus = status.embeddingAvailable ? ui::colorize("yes", ui::Ansi::GREEN)
                                                        : ui::colorize("no", ui::Ansi::YELLOW);
    embRows.push_back({"Available", availStatus, ""});

    if (!status.embeddingBackend.empty())
        embRows.push_back({"Backend", status.embeddingBackend, ""});
    if (!status.embeddingModel.empty())
        embRows.push_back({"Model", status.embeddingModel, ""});
    if (!status.embeddingModelPath.empty())
        embRows.push_back({"Path", status.embeddingModelPath, ""});
    if (status.embeddingDim > 0)
        embRows.push_back({"Dimension", std::to_string(status.embeddingDim), ""});
    if (status.embeddingThreadsIntra > 0 || status.embeddingThreadsInter > 0) {
        std::ostringstream thrStr;
        thrStr << status.embeddingThreadsIntra << " intra / " << status.embeddingThreadsInter
               << " inter";
        embRows.push_back({"Threads", thrStr.str(), ""});
    }

    ui::render_rows(os, embRows);
}

void DoctorDisplay::renderKnowledgeGraph(std::ostream& os, YamsCLI* cli,
                                         RecommendationBuilder& recs) {
    if (!cli)
        return;
    auto db = cli->getDatabase();
    if (!(db && db->isOpen()))
        return;

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
    long long nodes = countTable("SELECT COUNT(1) FROM kg_nodes");
    long long edges = countTable("SELECT COUNT(1) FROM kg_edges");
    long long aliases = countTable("SELECT COUNT(1) FROM kg_aliases");
    long long embeddings = countTable("SELECT COUNT(1) FROM kg_node_embeddings");
    long long entities = countTable("SELECT COUNT(1) FROM doc_entities");

    os << "\n" << ui::section_header("Knowledge Graph") << "\n\n";
    if (entities <= 0 && nodes <= 0) {
        std::string msg = "Knowledge graph empty — run 'yams doctor repair --graph' to build from "
                          "tags/metadata";
        os << ui::colorize(msg, ui::Ansi::YELLOW) << "\n";
        recs.warning("DOCTOR_KG_EMPTY", msg);
        return;
    }

    std::vector<ui::Row> kgRows;
    if (nodes >= 0)
        kgRows.push_back({"Nodes", std::to_string(nodes), ""});
    if (edges >= 0)
        kgRows.push_back({"Edges", std::to_string(edges), ""});
    if (aliases >= 0)
        kgRows.push_back({"Aliases", std::to_string(aliases), ""});
    if (embeddings >= 0)
        kgRows.push_back({"Embeddings", std::to_string(embeddings), ""});
    if (entities >= 0)
        kgRows.push_back({"Doc Entities", std::to_string(entities), ""});
    ui::render_rows(os, kgRows);
}

void DoctorDisplay::renderR2Credentials(std::ostream&, YamsCLI*, RecommendationBuilder&) {
    // Stub — depends on evaluateR2KeychainStatus which is still in the monolith
}

void DoctorDisplay::renderLiveRepairProgress(std::ostream& os, YamsCLI*) {
    try {
        using namespace yams::daemon;
        ClientConfig cfg;
        cfg.requestTimeout = std::chrono::milliseconds(1200);
        auto leaseRes = acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes)
            return;
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        uint64_t lastGen = 0, lastFail = 0, lastQ = 0, lastBatches = 0;
        bool printedHeader = false;
        for (int i = 0; i < 8; ++i) {
            GetStatsRequest req;
            req.detailed = false;
            req.showFileTypes = false;
            auto r =
                run_result<GetStatsResponse>(client.getStats(req), std::chrono::milliseconds(1300));
            if (!r)
                break;

            const auto& st = r.value();
            auto getU64 = [&](const char* k) -> uint64_t {
                auto it = st.additionalStats.find(k);
                if (it == st.additionalStats.end())
                    return 0;
                try {
                    return static_cast<uint64_t>(std::stoull(it->second));
                } catch (...) {
                    return 0;
                }
            };

            uint64_t gen = getU64("repair_embeddings_generated");
            uint64_t fail = getU64("repair_failed_operations");
            uint64_t q = getU64("repair_queue_depth");
            if (q == 0 && gen == 0 && fail == 0)
                break;

            uint64_t batches = getU64("repair_batches_attempted");
            if (!printedHeader) {
                os << "\nEmbeddings Repair (live):\n";
                printedHeader = true;
            }

            os << "  generated=" << gen << " failed=" << fail << " pending=" << q
               << " batches=" << batches << "\r" << std::flush;

            if (gen == lastGen && fail == lastFail && q == lastQ && batches == lastBatches)
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
            else {
                lastGen = gen;
                lastFail = fail;
                lastQ = q;
                lastBatches = batches;
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
            }
        }

        if (printedHeader)
            os << "\n";
    } catch (...) {
    }
}

} // namespace yams::cli::doctor
