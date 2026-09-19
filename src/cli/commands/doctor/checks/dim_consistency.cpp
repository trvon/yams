#include <yams/cli/doctor/checks/dim_consistency.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <filesystem>

namespace yams::cli::doctor {

DimConsistencyCheck::Result
DimConsistencyCheck::execute(const DoctorContext& ctx, const daemon::StatusResponse* daemonStatus) {
    Result r;

    // Get DB dimension from daemon status
    if (daemonStatus) {
        r.dbDim = daemonStatus->vectorDbDim;
    }

    // Resolve target dimension from provider/config/model (not from DB)
    size_t targetDim = 0;
    std::string targetSrc;

    // Priority 1: Daemon model provider
    if (daemonStatus && daemonStatus->embeddingAvailable && daemonStatus->embeddingDim > 0) {
        targetDim = daemonStatus->embeddingDim;
        targetSrc = "daemon_provider";
    }

    // Priority 2: one command-local effective policy snapshot. Resolve without a data directory
    // so an existing DB cannot become the target and mask a mismatch.
    yams::daemon::DaemonConfig embeddingConfig;
    embeddingConfig.configFilePath = ctx.configPath();
    const auto policy = yams::daemon::ConfigResolver::resolveEmbeddingConfig(embeddingConfig, {});
    if (targetDim == 0 && policy.dimension) {
        targetDim = *policy.dimension;
        switch (policy.dimensionSource) {
            case yams::daemon::EmbeddingDimensionSource::Config:
                targetSrc = "config(" + policy.provenance.at("dimension") + ")";
                break;
            case yams::daemon::EmbeddingDimensionSource::Environment:
                targetSrc = "env(YAMS_EMBED_DIM)";
                break;
            default:
                targetSrc = policy.provenance.at("dimension");
                break;
        }
    }

    // Priority 3: model metadata/heuristic selected by the same effective policy.
    if (targetDim == 0 && !policy.preferredModel.empty()) {
        const auto dataPath = ctx.dataDir();
        if (auto cfgDim = vecutil::getModelDimensionFromMetadata(dataPath, policy.preferredModel)) {
            targetDim = *cfgDim;
            targetSrc = "model_metadata(" + policy.preferredModel + ")";
        } else if (auto heurDim = vecutil::getModelDimensionHeuristic(policy.preferredModel)) {
            targetDim = *heurDim;
            targetSrc = "model_heuristic(" + policy.preferredModel + ")";
        }
    }

    r.targetDim = targetDim;
    r.targetSource = targetSrc;
    r.mismatch = (targetDim != 0 && r.dbDim != 0 && r.dbDim != targetDim);

    // Config consistency check
    auto cfgPath = ctx.configPath();
    if (!cfgPath.empty() && std::filesystem::exists(cfgPath)) {
        auto dimConfig = ctx.readConfigDims();
        if ((dimConfig.embeddings && dimConfig.vectorDb &&
             *dimConfig.embeddings != *dimConfig.vectorDb) ||
            (dimConfig.embeddings && dimConfig.index &&
             *dimConfig.embeddings != *dimConfig.index) ||
            (dimConfig.vectorDb && dimConfig.index && *dimConfig.vectorDb != *dimConfig.index)) {
            r.configInconsistent = true;
        }
    }

    return r;
}

void DimConsistencyCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (r.dbDim == 0) {
        os << "Vector database is not ready according to the daemon.\n";
        return;
    }

    if (r.targetDim == 0) {
        os << "DB dimension: " << r.dbDim
           << ". Unable to resolve target embedding dimension from config or models."
           << "\n";
        return;
    }

    os << "DB dimension: " << r.dbDim << ", Target ('" << r.targetSource
       << "') dimension: " << r.targetDim << "\n";

    if (r.mismatch) {
        os << "\n⚠ Embedding dimension mismatch detected.\n";
        os << "- Stored vectors are " << r.dbDim << "-d but target requires " << r.targetDim
           << "-d.\n";
        os << "- This will cause vector search to fail or return incorrect results.\n";
        os << "\nTo fix this, you can either:\n";
        os << "  1. Change your model/config to match the database (" << r.dbDim << "-d).\n";
        os << "  2. Recreate the vector database with the new dimension (" << r.targetDim
           << "-d).\n";
        os << "     (This will require re-running 'yams repair --embeddings')\n";
    } else {
        os << ui::status_ok("Embedding dimension matches model.") << "\n";
    }
}

} // namespace yams::cli::doctor
