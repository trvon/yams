#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/components/ConfigResolver.h>

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
    REQUIRE((rc == SQLITE_OK));
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
    CHECK((result.error == "Database file does not exist"));
}

TEST_CASE("vecutil initVecModule validates null handle", "[cli][vecutil][catch2]") {
    auto result = initVecModule(nullptr);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().message.find("Null database handle") != std::string::npos));
}

TEST_CASE("vecutil validateVecSchema recognizes vec0 virtual table schema",
          "[cli][vecutil][vec0][catch2]") {
    TempDb temp;

    sqlite3* db = nullptr;
    REQUIRE((sqlite3_open(temp.dbPath.string().c_str(), &db) == SQLITE_OK));
    REQUIRE((sqlite3_vec_init(db, nullptr, nullptr) == SQLITE_OK));
    exec(db, "CREATE VIRTUAL TABLE doc_embeddings USING vec0(embedding float[8])");
    sqlite3_close(db);

    const auto result = validateVecSchema(temp.dbPath);
    CHECK(result.exists);
    CHECK(result.vec0Available);
    CHECK(result.schemaValid);
    CHECK(result.usesVec0);
    REQUIRE(result.dimension.has_value());
    CHECK((result.dimension.value() == 8));
    CHECK((result.ddl.find("USING vec0") != std::string::npos));
}

TEST_CASE("vecutil resolves configured dimension through effective embedding policy",
          "[cli][vecutil][config][snapshot][catch2]") {
    TempDb temp;
    const auto configPath = yams::test::write_file(temp.dir / "config.toml", R"toml(
[embeddings]
backend = "onnxruntime"
preferred_model = "all-MiniLM-L6-v2"
embedding_dim = 16

[vector_database]
embedding_dim = 32
)toml");
    yams::test::ScopedEnvVar configPathEnvironment{"YAMS_CONFIG_PATH", configPath.string()};
    yams::test::ScopedEnvVar configEnvironment{"YAMS_CONFIG", std::nullopt};
    yams::test::ScopedEnvVar dimensionEnvironment{"YAMS_EMBED_DIM", "64"};
    yams::test::ScopedEnvVar backendEnvironment{"YAMS_EMBED_BACKEND", std::nullopt};
    yams::test::ScopedEnvVar modelEnvironment{"YAMS_PREFERRED_MODEL", std::nullopt};

    const auto result = resolveEmbeddingDimension(nullptr, temp.dir);

    CHECK((result.dimension == 16U));
    CHECK((result.source == "config"));
}

TEST_CASE("vecutil uses effective preferred model for dimension fallback",
          "[cli][vecutil][config][snapshot][catch2]") {
    TempDb temp;
    const auto configPath = yams::test::write_file(temp.dir / "config.toml", R"toml(
[embeddings]
backend = "onnxruntime"
preferred_model = "all-MiniLM-L6-v2"
)toml");
    yams::test::ScopedEnvVar configPathEnvironment{"YAMS_CONFIG_PATH", configPath.string()};
    yams::test::ScopedEnvVar configEnvironment{"YAMS_CONFIG", std::nullopt};
    yams::test::ScopedEnvVar dimensionEnvironment{"YAMS_EMBED_DIM", std::nullopt};
    yams::test::ScopedEnvVar backendEnvironment{"YAMS_EMBED_BACKEND", std::nullopt};
    yams::test::ScopedEnvVar modelEnvironment{"YAMS_PREFERRED_MODEL", std::nullopt};

    const auto result = resolveEmbeddingDimension(nullptr, temp.dir);

    CHECK((result.dimension == 384U));
    CHECK((result.source == "model"));
}

TEST_CASE("vecutil preserves the CLI embedding snapshot after ambient config changes",
          "[cli][vecutil][config][snapshot][catch2]") {
    TempDb temp;
    const auto initialConfig = yams::test::write_file(temp.dir / "initial.toml", R"toml(
[embeddings]
backend = "simeon"
preferred_model = "simeon-default"
embedding_dim = 16
)toml");
    const auto changedConfig = yams::test::write_file(temp.dir / "changed.toml", R"toml(
[embeddings]
backend = "simeon"
preferred_model = "simeon-default"
embedding_dim = 64
)toml");
    yams::test::ScopedEnvVar configPathEnvironment{"YAMS_CONFIG_PATH", initialConfig.string()};
    yams::test::ScopedEnvVar configEnvironment{"YAMS_CONFIG", std::nullopt};
    yams::test::ScopedEnvVar dimensionEnvironment{"YAMS_EMBED_DIM", std::nullopt};
    yams::test::ScopedEnvVar backendEnvironment{"YAMS_EMBED_BACKEND", std::nullopt};
    yams::test::ScopedEnvVar modelEnvironment{"YAMS_PREFERRED_MODEL", std::nullopt};

    YamsCLI cli;
    cli.setDataPath(temp.dir);
    REQUIRE((cli.getResolvedEmbeddingConfig().dimension == 16U));

    {
        yams::test::ScopedEnvVar changedConfigEnvironment{"YAMS_CONFIG_PATH",
                                                          changedConfig.string()};
        const auto lifecycleResult = resolveEmbeddingDimension(&cli, temp.dir);
        const auto standaloneResult = resolveEmbeddingDimension(nullptr, temp.dir);

        CHECK((lifecycleResult.dimension == 16U));
        CHECK((lifecycleResult.source == "config"));
        CHECK((standaloneResult.dimension == 64U));
    }
}

TEST_CASE("vecutil validateVecSchema flags non-vec0 doc_embeddings schema",
          "[cli][vecutil][vec0][catch2]") {
    TempDb temp;

    sqlite3* db = nullptr;
    REQUIRE((sqlite3_open(temp.dbPath.string().c_str(), &db) == SQLITE_OK));
    exec(db, "CREATE TABLE doc_embeddings(rowid INTEGER PRIMARY KEY, embedding BLOB)");
    sqlite3_close(db);

    const auto result = validateVecSchema(temp.dbPath);
    CHECK(result.exists);
    CHECK(result.vec0Available);
    CHECK(result.schemaValid);
    CHECK_FALSE(result.usesVec0);
    CHECK((result.ddl.find("CREATE TABLE doc_embeddings") != std::string::npos));
}

} // namespace yams::cli::vecutil
