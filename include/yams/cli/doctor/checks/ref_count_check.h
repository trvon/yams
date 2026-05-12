#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

namespace yams::cli::doctor {

class DoctorContext;

/// Validates reference-count consistency across `block_references`
/// and `ref_statistics` tables in the storage refs database.
class RefCountCheck {
public:
    struct AdjustedStat {
        std::string name;
        int64_t cachedValue;
        int64_t authoritativeValue;
    };

    struct Result {
        uint64_t totalBlocks{0};
        uint64_t totalReferences{0};
        uint64_t totalBytes{0};
        uint64_t unreferencedBlocks{0};
        uint64_t unreferencedBytes{0};
        uint64_t negativeRefCounts{0};
        bool statsDrift{false};
        std::vector<AdjustedStat> driftDetails; // capped at 20
        bool ok{true};
    };

    Result execute(const DoctorContext& ctx);
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
