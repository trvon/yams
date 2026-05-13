#pragma once

#include <cstddef>
#include <ostream>
#include <string>

namespace yams::daemon {
struct StatusResponse;
} // namespace yams::daemon

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

class DoctorContext;

/// Checks for embedding dimension mismatches between the DB, config, and model provider.
class DimConsistencyCheck {
public:
    struct Result {
        size_t dbDim{0};
        size_t targetDim{0};
        std::string targetSource;
        bool mismatch{false};
        bool configInconsistent{false};
        std::string message;
    };

    /// Run the diagnostic using daemon status and config files.
    Result execute(const DoctorContext& ctx, const daemon::StatusResponse* daemonStatus);

    /// Render the result to the given output stream using UI helpers.
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
