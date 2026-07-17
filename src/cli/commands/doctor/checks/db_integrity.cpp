#include <yams/cli/doctor/checks/db_integrity.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>

#include <sqlite3.h>
#include <filesystem>
#include <sstream>
#include <vector>

extern "C" int sqlite3_vec_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi);

namespace yams::cli::doctor {

namespace {
bool isReadOnlyFtsValidationLine(std::string_view line) {
    return line.find("unable to validate the inverted index for FTS5 table") !=
               std::string_view::npos &&
           line.find("attempt to write a readonly database") != std::string_view::npos;
}

std::vector<std::string> runIntegrityCheck(sqlite3* db, const std::string& dbName) {
    std::vector<std::string> issues;
    if (!db)
        return issues;
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        issues.push_back(dbName + ": integrity_check prepare failed — " +
                         std::string(sqlite3_errmsg(db)));
        return issues;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char* txt = sqlite3_column_text(stmt, 0);
        if (txt) {
            std::string line(reinterpret_cast<const char*>(txt));
            if (line != "ok" && !isReadOnlyFtsValidationLine(line))
                issues.push_back(dbName + ": " + line);
        }
    }
    sqlite3_finalize(stmt);
    return issues;
}
} // namespace

bool testing_isReadOnlyFtsValidationLine(std::string_view line) {
    return isReadOnlyFtsValidationLine(line);
}

DbIntegrityCheck::DbIntegrityCheck() = default;

DbIntegrityCheck::Result DbIntegrityCheck::execute(const DoctorContext& ctx) {
    Result r;

    auto dataDir = ctx.dataDir();
    const auto& state = ctx.cachedState();

    if (state.status && (state.status->metadataWalBytes > 0 || state.status->vectorWalBytes > 0)) {
        r.metadataWalBytes = state.status->metadataWalBytes;
        r.vectorWalBytes = state.status->vectorWalBytes;
    } else {
        r.metadataWalBytes = DoctorContext::walFileSize(dataDir, "yams.db-wal");
        r.vectorWalBytes = DoctorContext::walFileSize(dataDir, "vectors.db-wal");
    }

    constexpr uint64_t kWarnBytes = 128ULL * 1024ULL * 1024ULL;
    if (r.metadataWalBytes >= kWarnBytes || r.vectorWalBytes >= kWarnBytes)
        r.ok = false;

    // ── Corrupt artifacts ──
    std::error_code ec;
    namespace fs = std::filesystem;
    if (fs::exists(dataDir, ec)) {
        for (const auto& entry : fs::directory_iterator(dataDir, ec)) {
            if (ec)
                break;
            auto name = entry.path().filename().string();
            if (name.find(".corrupt-") != std::string::npos) {
                std::ostringstream oss;
                oss << name;
                if (entry.is_regular_file() && entry.file_size() > 0)
                    oss << " (" << (entry.file_size() / (1024ULL * 1024ULL)) << " MB)";
                r.corruptArtifacts.push_back(oss.str());
            }
        }
    }
    if (!r.corruptArtifacts.empty())
        r.ok = false;

    // ── PRAGMA integrity_check (only when daemon is not holding locks) ──
    if (!state.daemonUp) {
        auto checkDb = [&](const std::string& dbName, bool& okFlag, std::string& detail) {
            auto dbPath = dataDir / dbName;
            if (!fs::exists(dbPath)) {
                okFlag = true;
                return;
            } // no DB to check
            sqlite3* db = nullptr;
            if (sqlite3_open_v2(dbPath.string().c_str(), &db,
                                SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
                okFlag = false;
                r.ok = false;
                detail = "cannot open";
                if (db)
                    sqlite3_close(db);
                return;
            }
            r.integrityCheckRan = true;
            auto issues = runIntegrityCheck(db, dbName);
            sqlite3_close(db);
            if (issues.empty()) {
                okFlag = true;
            } else {
                okFlag = false;
                detail = issues[0];
                if (detail.size() > 120)
                    detail = detail.substr(0, 120) + "...";
            }
        };
        checkDb("yams.db", r.metadataIntegrityOk, r.metadataIntegrityDetail);
        checkDb("vectors.db", r.vectorIntegrityOk, r.vectorIntegrityDetail);
    }

    if (!r.metadataIntegrityOk || !r.vectorIntegrityOk)
        r.ok = false;
    return r;
}

void DbIntegrityCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    constexpr uint64_t kWarnBytes = 128ULL * 1024ULL * 1024ULL;

    auto walStatus = [](uint64_t bytes, uint64_t warnBytes) -> std::string {
        if (bytes == 0)
            return status_ok("0 B");
        if (bytes >= warnBytes)
            return status_error(std::to_string(bytes / (1024ULL * 1024ULL)) + " MB");
        return status_ok(std::to_string(bytes / (1024ULL * 1024ULL)) + " MB");
    };

    os << "  yams.db-wal:   " << walStatus(r.metadataWalBytes, kWarnBytes);
    if (r.metadataWalBytes > 0 && r.metadataWalBytes < 1024ULL * 1024ULL)
        os << " (" << r.metadataWalBytes << " B)";
    os << "\n";
    os << "  vectors.db-wal: " << walStatus(r.vectorWalBytes, kWarnBytes);
    if (r.vectorWalBytes > 0 && r.vectorWalBytes < 1024ULL * 1024ULL)
        os << " (" << r.vectorWalBytes << " B)";
    os << "\n";

    for (const auto& artifact : r.corruptArtifacts)
        os << "\n  " << status_warning(artifact) << "\n";

    // ── Integrity check results ──
    if (r.integrityCheckRan) {
        os << "\n  " << status_ok("Integrity check: yams.db — ")
           << (r.metadataIntegrityOk ? colorize("OK", Ansi::GREEN) : colorize("FAILED", Ansi::RED));
        if (!r.metadataIntegrityDetail.empty())
            os << " (" << r.metadataIntegrityDetail << ")";
        os << "\n  " << status_ok("Integrity check: vectors.db — ")
           << (r.vectorIntegrityOk ? colorize("OK", Ansi::GREEN) : colorize("FAILED", Ansi::RED));
        if (!r.vectorIntegrityDetail.empty())
            os << " (" << r.vectorIntegrityDetail << ")";
        os << "\n";
    }

    if (r.ok)
        os << "\n  " << status_ok("No DB integrity issues detected.") << "\n";
}

} // namespace yams::cli::doctor
