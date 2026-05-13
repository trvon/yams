#pragma once

#include <cstddef>
#include <ostream>
#include <string>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::daemon {
struct StatusResponse;
} // namespace yams::daemon

namespace yams::cli::doctor {

class DoctorContext;

/// Checks embedding health: stale embeddings, model version consistency,
/// and embedding dimension alignment.
class EmbeddingHealthCheck {
public:
    struct Result {
        size_t staleCount{0};
        size_t dimMismatch{0};
        bool embeddingAvailable{false};
        std::string modelId;
        std::string modelVersion;
        bool ok{true};
    };

    Result execute(const DoctorContext& ctx, const daemon::StatusResponse* daemonStatus);

    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
