// Unit tests for DimConsistencyCheck
#include <catch2/catch_test_macros.hpp>

#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/doctor/checks/dim_consistency.h>
#include <yams/cli/yams_cli.h>

#include <fstream>
#include <filesystem>
#include <memory>
#include <sstream>

#include "../../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using namespace yams::cli::doctor;

namespace {
struct DimTestEnv {
    fs::path dataDir;
    yams::test::ScopedEnvVar dataEnv;
    yams::test::ScopedEnvVar homeEnv;

    DimTestEnv()
        : dataDir(yams::test::make_temp_dir("yams_dim_test_")),
          dataEnv("YAMS_DATA_DIR", dataDir.string()), homeEnv("HOME", dataDir.string()) {
        fs::create_directories(dataDir);
    }

    std::unique_ptr<yams::cli::YamsCLI> makeCli() { return std::make_unique<yams::cli::YamsCLI>(); }
};
} // namespace

TEST_CASE("DimConsistencyCheck - no daemon status returns empty result",
          "[doctor][dim_consistency]") {
    DimTestEnv env;
    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());

    DimConsistencyCheck check;
    auto result = check.execute(ctx, nullptr);

    CHECK(result.dbDim == 0);
    CHECK(result.targetDim == 0);
    CHECK_FALSE(result.mismatch);
    CHECK_FALSE(result.configInconsistent);
}

TEST_CASE("DimConsistencyCheck - renders without daemon status", "[doctor][dim_consistency]") {
    DimConsistencyCheck::Result r;
    r.dbDim = 0;
    r.targetDim = 0;

    std::ostringstream os;
    DimConsistencyCheck::render(os, r);
    auto output = os.str();

    CHECK_FALSE(output.empty());
    CHECK(output.find("not ready") != std::string::npos);
}

TEST_CASE("DimConsistencyCheck - renders mismatch", "[doctor][dim_consistency]") {
    DimConsistencyCheck::Result r;
    r.dbDim = 384;
    r.targetDim = 1024;
    r.targetSource = "daemon_provider";
    r.mismatch = true;

    std::ostringstream os;
    DimConsistencyCheck::render(os, r);
    auto output = os.str();

    CHECK_FALSE(output.empty());
    CHECK(output.find("384") != std::string::npos);
    CHECK(output.find("1024") != std::string::npos);
    CHECK(output.find("mismatch") != std::string::npos);
}

TEST_CASE("DimConsistencyCheck - renders match", "[doctor][dim_consistency]") {
    DimConsistencyCheck::Result r;
    r.dbDim = 384;
    r.targetDim = 384;
    r.targetSource = "config";
    r.mismatch = false;

    std::ostringstream os;
    DimConsistencyCheck::render(os, r);
    auto output = os.str();

    CHECK_FALSE(output.empty());
    CHECK(output.find("matches") != std::string::npos);
}
