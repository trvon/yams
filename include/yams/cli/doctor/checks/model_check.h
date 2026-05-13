#pragma once

#include <ostream>
#include <string>
#include <vector>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

class DoctorContext;

/// Checks installed model directories, detects missing companion files,
/// and warns about old model locations that need migration.
class ModelCheck {
public:
    struct ModelInfo {
        std::string name;
        int dim{-1}; // heuristic dimension, or -1 if unknown
        bool missingConfig{false};
        bool missingTokenizer{false};
    };

    struct Result {
        std::vector<ModelInfo> models;
        size_t oldLocationCount{0};
        std::string oldLocationPath;
        std::string newLocationPath;
    };

    /// Run the diagnostic (filesystem-only, no SQLite).
    Result execute(const DoctorContext& ctx);

    /// Render the result to the given output stream using UI helpers.
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
