#pragma once

#include <ostream>
#include <string>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

class DoctorContext;

/// Validates that the vector database uses the vec0 module and has a valid schema.
class Vec0Check {
public:
    struct Result {
        bool vec0Available{false};
        bool schemaValid{false};
        bool usesVec0{false};
        std::string ddl;
        std::string dimension;
        std::string error;
    };

    /// Run the diagnostic (read-only, may open vectors.db).
    Result execute(const DoctorContext& ctx);

    /// Render the result to the given output stream using UI helpers.
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
