#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

#include <yams/cli/vector_db_util.h>

#include "../../common/test_helpers_catch2.h"

extern "C" int sqlite3_vec_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi);

namespace yams::cli::vecutil {
namespace {

namespace fs = std::filesystem;

struct TempDb {
    fs::path dir;
    fs::path dbPath;

    TempDb() : dir(yams::test::make_temp_dir("yams_vecutil_catch2_")), dbPath(dir / "vectors.db") {
        fs::create_directories(dir);
    }

    ~TempDb() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
};

void exec(sqlite3* db, const char* sql) {
    char* err = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    INFO(std::string(sql));
    INFO(std::string(err ? err : ""));
    REQUIRE(rc == SQLITE_OK);
    if (err) {
        sqlite3_free(err);
    }
}

} // namespace

TEST_CASE("vecutil validateVecSchema reports missing database", "[cli][vecutil][catch2]") {
    TempDb temp;

    const auto result = validateVecSchema(temp.dbPath);
    CHECK_FALSE(result.exists);
    CHECK_FALSE(result.vec0Available);
    CHECK_FALSE(result.schemaValid);
    CHECK(result.error == "Database file does not exist");
}

TEST_CASE("vecutil initVecModule validates null handle", "[cli][vecutil][catch2]") {
    auto result = initVecModule(nullptr);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().message.find("Null database handle") != std::string::npos);
}

TEST_CASE("vecutil validateVecSchema recognizes vec0 virtual table schema",
          "[cli][vecutil][vec0][catch2]") {
    TempDb temp;

    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(temp.dbPath.string().c_str(), &db) == SQLITE_OK);
    REQUIRE(sqlite3_vec_init(db, nullptr, nullptr) == SQLITE_OK);
    exec(db, "CREATE VIRTUAL TABLE doc_embeddings USING vec0(embedding float[8])");
    sqlite3_close(db);

    const auto result = validateVecSchema(temp.dbPath);
    CHECK(result.exists);
    CHECK(result.vec0Available);
    CHECK(result.schemaValid);
    CHECK(result.usesVec0);
    REQUIRE(result.dimension.has_value());
    CHECK(result.dimension.value() == 8);
    CHECK(result.ddl.find("USING vec0") != std::string::npos);
}

TEST_CASE("vecutil validateVecSchema flags non-vec0 doc_embeddings schema",
          "[cli][vecutil][vec0][catch2]") {
    TempDb temp;

    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(temp.dbPath.string().c_str(), &db) == SQLITE_OK);
    exec(db, "CREATE TABLE doc_embeddings(rowid INTEGER PRIMARY KEY, embedding BLOB)");
    sqlite3_close(db);

    const auto result = validateVecSchema(temp.dbPath);
    CHECK(result.exists);
    CHECK(result.vec0Available);
    CHECK(result.schemaValid);
    CHECK_FALSE(result.usesVec0);
    CHECK(result.ddl.find("CREATE TABLE doc_embeddings") != std::string::npos);
}

} // namespace yams::cli::vecutil
