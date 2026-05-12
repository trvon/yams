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

/// Checks WAL file sizes and looks for corrupt database artifacts.
/// WAL sizes are read from daemon status when available or from the filesystem.
/// Corrupt artifacts (*.corrupt-*) are detected from the data directory.
class DbIntegrityCheck {
public:
    struct Result {
        uint64_t metadataWalBytes{0};
        uint64_t vectorWalBytes{0};
        std::vector<std::string> corruptArtifacts;
        bool ok{true};
    };

    explicit DbIntegrityCheck();

    /// Run the diagnostic (read-only, no mutations).
    Result execute(const DoctorContext& ctx);

    /// Render the result to the given output stream using UI helpers.
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
