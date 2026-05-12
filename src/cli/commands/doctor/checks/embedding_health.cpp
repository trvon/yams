#include <yams/cli/doctor/checks/embedding_health.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli::doctor {

EmbeddingHealthCheck::Result EmbeddingHealthCheck::execute(const DoctorContext& ctx,
                                                           const daemon::StatusResponse* status) {
    Result r;

    if (!status)
        return r;

    r.embeddingAvailable = status->embeddingAvailable;
    r.modelId = status->embeddingModel;
    if (status->embeddingDim > 0 && status->vectorDbDim > 0 &&
        status->embeddingDim != status->vectorDbDim)
        r.dimMismatch = 1;

    // Stale count: check repair metrics from cached state
    const auto& state = ctx.cachedState();
    if (state.stats) {
        auto getU64 = [&](const char* k) -> uint64_t {
            auto it = state.stats->additionalStats.find(k);
            if (it == state.stats->additionalStats.end())
                return 0;
            try {
                return static_cast<uint64_t>(std::stoull(it->second));
            } catch (...) {
                return 0;
            }
        };
        r.staleCount = getU64("repair_stale_embeddings");
    }

    if (r.dimMismatch > 0 || r.staleCount > 0)
        r.ok = false;
    return r;
}

void EmbeddingHealthCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (!r.embeddingAvailable) {
        os << "  " << status_warning("Embedding subsystem not available") << "\n";
        return;
    }

    os << "  " << status_ok("Embedding: available");
    if (!r.modelId.empty())
        os << " (" << r.modelId << ")";
    os << "\n";

    if (r.dimMismatch > 0)
        os << "  " << status_error("Embedding dimension mismatch — DB and model dims differ")
           << "\n";

    if (r.staleCount > 0)
        os << "  "
           << status_warning(std::to_string(r.staleCount) + " stale embedding(s) need regeneration")
           << "\n";

    if (r.ok)
        os << "  " << status_ok("Embedding health: OK") << "\n";
}

} // namespace yams::cli::doctor
