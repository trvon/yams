#include <yams/cli/doctor/checks/vec0_check.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>

#include <sqlite3.h>
#include <filesystem>
#include <sstream>

extern "C" int sqlite3_vec_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi);

namespace yams::cli::doctor {

Vec0Check::Result Vec0Check::execute(const DoctorContext& ctx) {
    Result r;
    namespace fs = std::filesystem;

    auto dbPath = ctx.dataDir() / "vectors.db";

    if (!fs::exists(dbPath)) {
        r.error = "Vector database does not exist yet: " + dbPath.string();
        return r;
    }

    sqlite3* db = nullptr;
    int rc = sqlite3_open(dbPath.string().c_str(), &db);
    if (rc != SQLITE_OK) {
        r.error = "Failed to open vector database: " + std::string(sqlite3_errmsg(db));
        if (db)
            sqlite3_close(db);
        return r;
    }

    char* error_msg = nullptr;
    rc = sqlite3_vec_init(db, &error_msg, nullptr);
    if (rc != SQLITE_OK) {
        r.error =
            "Failed to initialize vec0 module: " + std::string(error_msg ? error_msg : "unknown");
        if (error_msg)
            sqlite3_free(error_msg);
        sqlite3_close(db);
        return r;
    }

    // Check vec0 module availability
    const char* vec0Check = "SELECT 1 FROM pragma_module_list WHERE name='vec0'";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, vec0Check, -1, &stmt, nullptr) == SQLITE_OK) {
        r.vec0Available = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);
    }

    if (!r.vec0Available) {
        r.error = "vec0 module failed to load";
        sqlite3_close(db);
        return r;
    }

    // Check doc_embeddings schema
    const char* schemaCheck =
        "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' AND type='table'";
    if (sqlite3_prepare_v2(db, schemaCheck, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char* txt = sqlite3_column_text(stmt, 0);
            if (txt)
                r.ddl = reinterpret_cast<const char*>(txt);
        }
        sqlite3_finalize(stmt);
    }

    r.schemaValid = !r.ddl.empty();
    r.usesVec0 = r.ddl.find("USING vec0") != std::string::npos;

    // Extract dimension from schema
    auto pos = r.ddl.find("float[");
    if (pos != std::string::npos) {
        auto end = r.ddl.find(']', pos);
        if (end != std::string::npos && end > pos + 6) {
            r.dimension = r.ddl.substr(pos + 6, end - (pos + 6));
        }
    }

    sqlite3_close(db);
    return r;
}

void Vec0Check::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (!r.error.empty()) {
        if (r.error.find("does not exist") != std::string::npos) {
            os << "  " << status_warning(r.error) << "\n";
            os << "  → Database will be created automatically when daemon starts.\n";
            return;
        }
        os << "  " << status_error(r.error) << "\n";
        return;
    }

    os << "  " << status_ok("vec0 module is available") << "\n";

    if (r.ddl.empty()) {
        os << "  " << status_warning("doc_embeddings table does not exist yet") << "\n";
        os << "  → Table will be created automatically when daemon starts.\n";
    } else if (!r.usesVec0) {
        os << "  " << status_error("doc_embeddings table not using vec0 virtual table") << "\n";
        os << "\nSchema: " << r.ddl.substr(0, 100) << "...\n\n";
        os << "This table was created without the vec0 module.\n";
        os << "Vector search will not work correctly.\n\n";
        os << "Fix options:\n";
        os << "  1. Recreate the vector tables:\n";
        os << "     yams doctor --recreate-vectors --stop-daemon\n\n";
    } else {
        os << "  " << status_ok("doc_embeddings table correctly uses vec0 virtual table") << "\n";
        if (!r.dimension.empty())
            os << "  Schema dimension: " << r.dimension << "\n";
    }
}

} // namespace yams::cli::doctor
