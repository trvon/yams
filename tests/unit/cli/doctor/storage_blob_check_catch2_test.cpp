// Unit tests for StorageBlobCheck — blob integrity verification
#include <catch2/catch_test_macros.hpp>

#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/doctor/checks/storage_blob_check.h>
#include <yams/cli/yams_cli.h>

#include <fstream>
#include <filesystem>
#include <sstream>
#include <memory>

#include "../../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using namespace yams::cli::doctor;

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

TEST_CASE("StorageBlobCheck - empty data dir has no issues", "[doctor][storage]") {
    DoctorTestEnv env;
    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    StorageBlobCheck check;
    auto result = check.execute(ctx);

    CHECK(result.storageObjects == 0);
    CHECK(result.metadataDocuments == 0);
    CHECK(result.missingBlobs == 0);
    CHECK(result.orphanedBlobs == 0);
    CHECK(result.ok == true);
}

TEST_CASE("StorageBlobCheck - counts storage objects on disk", "[doctor][storage]") {
    DoctorTestEnv env;

    auto objectsDir = env.dataDir / "storage" / "objects";
    fs::create_directories(objectsDir);
    {
        std::ofstream f(objectsDir / "abc123deadbeef");
        f << "test content";
        f.close();
    }
    {
        std::ofstream f(objectsDir / "def456cafebabe");
        f << "more test content with longer data";
        f.close();
    }

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    StorageBlobCheck check;
    auto result = check.execute(ctx);

    CHECK(result.storageObjects == 2);
    CHECK(result.storageBytes > 0);
    // No metadata DB (empty corpus): orphans can't be detected, ok is true
    CHECK(result.orphanedBlobs == 0);
    CHECK(result.ok == true);
}

TEST_CASE("StorageBlobCheck - render produces output", "[doctor][storage]") {
    DoctorTestEnv env;
    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    StorageBlobCheck check;
    auto result = check.execute(ctx);

    std::ostringstream os;
    StorageBlobCheck::render(os, result);
    auto output = os.str();

    CHECK_FALSE(output.empty());
    CHECK(output.find("empty") != std::string::npos);
}
