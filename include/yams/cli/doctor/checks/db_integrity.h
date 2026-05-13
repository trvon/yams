#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

class DoctorContext;

/// Checks WAL file sizes, runs PRAGMA integrity_check on DB files,
/// and looks for corrupt database artifacts.
class DbIntegrityCheck {
public:
    struct Result {
        uint64_t metadataWalBytes{0};
        uint64_t vectorWalBytes{0};
        std::vector<std::string> corruptArtifacts;
        bool integrityCheckRan{false};
        bool metadataIntegrityOk{true};
        bool vectorIntegrityOk{true};
        std::string metadataIntegrityDetail;
        std::string vectorIntegrityDetail;
        bool ok{true};
    };

    explicit DbIntegrityCheck();

    /// Run the diagnostic (read-only, no mutations).
    Result execute(const DoctorContext& ctx);

    /// Render the result to the given output stream using UI helpers.
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
