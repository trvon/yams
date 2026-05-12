#include <yams/cli/doctor/checks/ref_count_check.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>

#include <sqlite3.h>
#include <filesystem>
#include <iomanip>
#include <sstream>

namespace yams::cli::doctor {

namespace {

struct RefDbGuard {
    sqlite3* db_{nullptr};
    explicit RefDbGuard(const std::filesystem::path& dbPath) {
        sqlite3_open_v2(dbPath.string().c_str(), &db_, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                        nullptr);
    }
    ~RefDbGuard() {
        if (db_)
            sqlite3_close(db_);
    }
    RefDbGuard(const RefDbGuard&) = delete;
    RefDbGuard& operator=(const RefDbGuard&) = delete;
    sqlite3* get() { return db_; }
};

struct TableCounts {
    uint64_t totalBlocks{0};
    uint64_t totalReferences{0};
    uint64_t totalBytes{0};
    uint64_t totalUncompressedBytes{0};
    uint64_t unreferencedBlocks{0};
    uint64_t unreferencedBytes{0};
    uint64_t negativeRefCounts{0};
};

TableCounts queryBlockRefs(sqlite3* db) {
    TableCounts tc;
    if (!db)
        return tc;

    auto query = [&](const char* sql, auto& dest) {
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
            return;
        if (sqlite3_step(stmt) == SQLITE_ROW)
            dest = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
        sqlite3_finalize(stmt);
    };

    query("SELECT COUNT(*) FROM block_references", tc.totalBlocks);
    query("SELECT COALESCE(SUM(ref_count), 0) FROM block_references", tc.totalReferences);
    query("SELECT COALESCE(SUM(block_size), 0) FROM block_references", tc.totalBytes);
    query("SELECT COALESCE(SUM(uncompressed_size), 0) FROM block_references",
          tc.totalUncompressedBytes);
    query("SELECT COUNT(*) FROM block_references WHERE ref_count = 0", tc.unreferencedBlocks);
    query("SELECT COALESCE(SUM(block_size), 0) FROM block_references WHERE ref_count = 0",
          tc.unreferencedBytes);
    query("SELECT COUNT(*) FROM block_references WHERE ref_count < 0", tc.negativeRefCounts);

    return tc;
}

struct StatRow {
    std::string name;
    int64_t value{0};
};

std::vector<StatRow> queryRefStatistics(sqlite3* db) {
    std::vector<StatRow> stats;
    if (!db)
        return stats;

    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT stat_name, stat_value FROM ref_statistics";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
        return stats;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        StatRow sr;
        sr.name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        sr.value = sqlite3_column_int64(stmt, 1);
        stats.push_back(std::move(sr));
    }
    sqlite3_finalize(stmt);
    return stats;
}

int64_t findStatValue(const std::vector<StatRow>& stats, const std::string& name) {
    for (const auto& sr : stats) {
        if (sr.name == name)
            return sr.value;
    }
    return -1;
}

} // anonymous namespace

RefCountCheck::Result RefCountCheck::execute(const DoctorContext& ctx) {
    Result r;
    namespace fs = std::filesystem;

    auto refsPath = ctx.dataDir() / "storage" / "refs.db";
    std::error_code ec;
    if (!fs::exists(refsPath, ec)) {
        r.ok = true;
        return r; // no refs DB yet (empty corpus)
    }

    RefDbGuard refDb(refsPath);
    if (!refDb.get()) {
        r.ok = false;
        return r;
    }

    // ── Authoritative stats from block_references ──
    auto tc = queryBlockRefs(refDb.get());
    r.totalBlocks = tc.totalBlocks;
    r.totalReferences = tc.totalReferences;
    r.totalBytes = tc.totalBytes;
    r.unreferencedBlocks = tc.unreferencedBlocks;
    r.unreferencedBytes = tc.unreferencedBytes;
    r.negativeRefCounts = tc.negativeRefCounts;

    // ── Stats drift detection: compare ref_statistics vs block_references ──
    auto cachedStats = queryRefStatistics(refDb.get());

    auto checkDrift = [&](const std::string& statName, int64_t authoritative,
                          std::vector<RefCountCheck::AdjustedStat>* details) {
        int64_t cached = findStatValue(cachedStats, statName);
        if (cached < 0)
            return; // stat not found — skip
        if (cached != authoritative) {
            r.statsDrift = true;
            if (details->size() < 20) {
                details->push_back({statName, cached, authoritative});
            }
        }
    };

    checkDrift("total_blocks", static_cast<int64_t>(tc.totalBlocks), &r.driftDetails);
    checkDrift("total_references", static_cast<int64_t>(tc.totalReferences), &r.driftDetails);
    checkDrift("total_bytes", static_cast<int64_t>(tc.totalBytes), &r.driftDetails);
    checkDrift("total_uncompressed_bytes", static_cast<int64_t>(tc.totalUncompressedBytes),
               &r.driftDetails);

    r.ok = !r.statsDrift && r.negativeRefCounts == 0;
    return r;
}

void RefCountCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (r.totalBlocks == 0) {
        os << "  " << status_ok("No reference-count data — corpus is empty or no CAS blocks.")
           << "\n";
        return;
    }

    os << "  " << status_ok("Total blocks: ") << format_number(r.totalBlocks) << "\n";
    os << "  " << status_ok("Total references: ") << format_number(r.totalReferences) << "\n";

    if (r.totalBytes > 0) {
        double mb = r.totalBytes / (1024.0 * 1024.0);
        std::ostringstream s;
        s << std::fixed << std::setprecision(1) << mb << " MB";
        os << "  " << status_ok("Total block bytes: ") << s.str() << "\n";
    }

    if (r.unreferencedBlocks > 0) {
        os << "  " << status_warning("Unreferenced blocks: ")
           << format_number(r.unreferencedBlocks);
        if (r.unreferencedBytes > 0) {
            double mb = r.unreferencedBytes / (1024.0 * 1024.0);
            std::ostringstream s;
            s << std::fixed << std::setprecision(1) << mb << " MB";
            os << " (" << s.str() << " reclaimable)";
        }
        os << "\n";
    }

    if (r.negativeRefCounts > 0) {
        os << "  " << status_error("Blocks with negative ref counts: ")
           << format_number(r.negativeRefCounts) << "\n";
    }

    if (r.statsDrift) {
        os << "  " << status_warning("Stats drift detected (ref_statistics != block_references):")
           << "\n";
        for (const auto& d : r.driftDetails) {
            os << "    " << bullet(d.name) << " cached=" << d.cachedValue
               << " authoritative=" << d.authoritativeValue << "\n";
        }
    }

    if (r.ok)
        os << "  " << status_ok("Reference count integrity: OK") << "\n";
    else
        os << "  " << status_error("Reference count integrity: ISSUES DETECTED") << "\n";
}

} // namespace yams::cli::doctor
