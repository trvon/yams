#pragma once

#include <yams/cli/recommendation_util.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <optional>
#include <ostream>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

/// Displays daemon status information: R2 credentials, embedding runtime,
/// knowledge graph counts, live repair progress, and loaded plugins.
struct DoctorDisplay {
    static void renderR2Credentials(std::ostream& os, YamsCLI* cli, RecommendationBuilder& recs);
    static void renderEmbeddingRuntime(std::ostream& os, const daemon::StatusResponse& status);
    static void renderKnowledgeGraph(std::ostream& os, YamsCLI* cli, RecommendationBuilder& recs);
    static void renderLiveRepairProgress(std::ostream& os, YamsCLI* cli);
};

} // namespace yams::cli::doctor
