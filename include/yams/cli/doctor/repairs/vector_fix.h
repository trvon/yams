#pragma once

#include <cstddef>
#include <optional>
#include <ostream>
#include <string>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

class DoctorContext;

/// Detects embedding dimension mismatches between DB and installed models,
/// then fixes the config to use a matching model. Implements `yams doctor --vectors`.
class VectorFixRepair {
public:
    struct Result {
        std::optional<size_t> dbDim;
        std::optional<size_t> modelDim;
        std::string modelName;
        std::string matchingModel;
        bool mismatch{false};
        bool fixed{false};
        std::string error;
    };

    explicit VectorFixRepair(YamsCLI* cli);

    /// Run the detection + fix (mutates config when mismatch is found).
    Result execute(const DoctorContext& ctx);

    /// Render the result.
    static void render(std::ostream& os, const Result& r, bool jsonOutput);

private:
    YamsCLI* cli_{nullptr};
};

} // namespace yams::cli::doctor
