// Unit tests for extracted doctor check modules
// Catch2 tests for DbIntegrityCheck

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/doctor/checks/db_integrity.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>

#include "../../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using namespace yams::cli::doctor;

// Catch2's expression decomposition intentionally overloads comparison operators.
// NOLINTBEGIN(bugprone-chained-comparison)

namespace {

struct DoctorTestEnv {
    fs::path dataDir;
    yams::test::ScopedEnvVar dataEnv;
    yams::test::ScopedEnvVar homeEnv;

    explicit DoctorTestEnv()
        : dataDir(yams::test::make_temp_dir("yams_doctor_test_")),
          dataEnv("YAMS_DATA_DIR", dataDir.string()), homeEnv("HOME", dataDir.string()) {
        fs::create_directories(dataDir);
    }

    std::unique_ptr<yams::cli::YamsCLI> makeCli() { return std::make_unique<yams::cli::YamsCLI>(); }
};

} // namespace

TEST_CASE("DbIntegrityCheck - empty data dir has no issues", "[doctor][db_integrity]") {
    DoctorTestEnv env;

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    DbIntegrityCheck check;
    auto result = check.execute(ctx);

    CHECK(result.metadataWalBytes == 0);
    CHECK(result.vectorWalBytes == 0);
    CHECK(result.corruptArtifacts.empty());
    CHECK(result.ok == true);
}

TEST_CASE("DbIntegrityCheck - detects WAL file presence", "[doctor][db_integrity]") {
    DoctorTestEnv env;

    // Create a small WAL file (under the 128 MB warn threshold)
    fs::path walPath = env.dataDir / "yams.db-wal";
    {
        std::ofstream w(walPath, std::ios::binary);
        for (int i = 0; i < 1024; ++i)
            w.put(static_cast<char>(i % 256));
        w.close();
    }

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    DbIntegrityCheck check;
    auto result = check.execute(ctx);

    CHECK(result.metadataWalBytes == 1024);
    CHECK(result.ok == true);
}

TEST_CASE("DbIntegrityCheck - warns on large WAL", "[doctor][db_integrity]") {
    DoctorTestEnv env;

    // Create a sparse WAL that reports > 128 MB. Use seek + put for sparse file.
    fs::path walPath = env.dataDir / "yams.db-wal";
    {
        std::ofstream w(walPath, std::ios::binary);
        w.seekp(200ULL * 1024ULL * 1024ULL - 1);
        w.put('\0');
        w.close();
    }

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    DbIntegrityCheck check;
    auto result = check.execute(ctx);

    CHECK(result.metadataWalBytes >= 128ULL * 1024ULL * 1024ULL);
    CHECK(result.ok == false);
}

TEST_CASE("DbIntegrityCheck - readonly FTS validation row is not corruption",
          "[doctor][db_integrity][readonly][fts5]") {
    const std::string readonlyFtsRow =
        "unable to validate the inverted index for FTS5 table main.kg_aliases_fts: attempt to "
        "write a readonly database";

    CHECK(testing_isReadOnlyFtsValidationLine(readonlyFtsRow));
    CHECK_FALSE(testing_isReadOnlyFtsValidationLine(
        "unable to validate the inverted index for FTS5 table main.kg_aliases_fts: database is "
        "malformed"));
    CHECK_FALSE(testing_isReadOnlyFtsValidationLine("row 3 missing from index idx_documents_path"));
}

TEST_CASE("DbIntegrityCheck - detects corrupt artifacts", "[doctor][db_integrity]") {
    DoctorTestEnv env;

    // Create a corrupt artifact file
    fs::path corruptPath = env.dataDir / "yams.db.corrupt-20260501T120000Z";
    {
        std::ofstream c(corruptPath, std::ios::binary);
        c << "corrupt data";
        c.close();
    }

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    DbIntegrityCheck check;
    auto result = check.execute(ctx);

    CHECK_FALSE(result.corruptArtifacts.empty());
    CHECK(result.ok == false);
}

TEST_CASE("DbIntegrityCheck - render produces output", "[doctor][db_integrity]") {
    DoctorTestEnv env;

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    DbIntegrityCheck check;
    auto result = check.execute(ctx);

    std::ostringstream os;
    DbIntegrityCheck::render(os, result);
    auto output = os.str();

    CHECK_FALSE(output.empty());
    CHECK(output.find("yams.db-wal") != std::string::npos);
    CHECK(output.find("vectors.db-wal") != std::string::npos);
}

// NOLINTEND(bugprone-chained-comparison)
