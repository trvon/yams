
#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <optional>

#include "env_compat.h"
#include "test_helpers_catch2.h"

#include <yams/common/fs_utils.h>
#include <yams/common/metric_helpers.h>
#include <yams/common/string_utils.h>
#include <yams/common/test_utils.h>
#include <yams/compat/unistd.h>

using yams::test::isModelDownloadAllowed;
using yams::test::isTestDiscoveryMode;
using yams::test::ScopedEnvVar;
using yams::test::shouldSkipModelLoading;

TEST_CASE("common asciiToLowerCopy normalizes ASCII strings", "[common][string][catch2]") {
    CHECK(yams::common::asciiToLowerCopy("TRUE-On_123") == "true-on_123");
    CHECK(yams::common::asciiToLowerCopy(std::string{"MiXeD.Path"}) == "mixed.path");
    CHECK(yams::common::asciiToLowerCopy("") == "");
}

TEST_CASE("common project names use stable session-safe characters",
          "[common][string][project][catch2]") {
    CHECK(yams::common::sanitizeProjectName("") == "project");
    CHECK(yams::common::sanitizeProjectName("My Repo_2") == "my-repo_2");
    CHECK(yams::common::sanitizeProjectName("Already-Safe") == "already-safe");
}

TEST_CASE("common findGitRoot returns the nearest repository marker",
          "[common][filesystem][git][catch2]") {
    yams::test::TempDirGuard tempDir{"yams_git_root_"};
    const auto repository = tempDir.path() / "repository";
    const auto nested = repository / "src" / "nested";
    REQUIRE(std::filesystem::create_directories(repository / ".git"));
    REQUIRE(std::filesystem::create_directories(nested));

    CHECK(yams::common::findGitRoot(nested) == repository);
}

TEST_CASE("common sanitizeForTerminal preserves printable text controls and masks bytes",
          "[common][string][catch2]") {
    CHECK(yams::common::sanitizeForTerminal(std::string_view{"ok\n\t\r"}) == "ok\n\t\r");
    CHECK(yams::common::sanitizeForTerminal(std::string_view{"a\x01\x7f"}) == "a??");
}

TEST_CASE("common metric helpers increment relaxed counters conditionally",
          "[common][metrics][catch2]") {
    std::atomic<std::uint64_t> counter{0};
    yams::common::metrics::incrementRelaxed(counter, 2);
    CHECK(counter.load(std::memory_order_relaxed) == 2);
    yams::common::metrics::incrementIfEnabled(false, [&]() -> auto& { return counter; }, 3);
    CHECK(counter.load(std::memory_order_relaxed) == 2);
    yams::common::metrics::incrementIfEnabled(true, [&]() -> auto& { return counter; }, 3);
    CHECK(counter.load(std::memory_order_relaxed) == 5);
}

TEST_CASE("test::isTestDiscoveryMode checks env var", "[common][test-utils][catch2]") {
    ScopedEnvVar environment{"GTEST_DISCOVERY_MODE", std::nullopt};
    CHECK_FALSE(isTestDiscoveryMode());

    environment.set("1");
    CHECK(isTestDiscoveryMode());
}

TEST_CASE("test::shouldSkipModelLoading honors env toggles", "[common][test-utils][catch2]") {
    ScopedEnvVar discovery{"GTEST_DISCOVERY_MODE", std::nullopt};
    ScopedEnvVar skipLoading{"YAMS_SKIP_MODEL_LOADING", std::nullopt};
    ScopedEnvVar testMode{"YAMS_TEST_MODE", std::nullopt};

    CHECK_FALSE(shouldSkipModelLoading());

    skipLoading.set("1");
    CHECK(shouldSkipModelLoading());
    skipLoading.unset();

    testMode.set("1");
    CHECK(shouldSkipModelLoading());
    testMode.unset();

    discovery.set("1");
    CHECK(shouldSkipModelLoading());
}

TEST_CASE("test::isModelDownloadAllowed checks env var", "[common][test-utils][catch2]") {
    ScopedEnvVar environment{"YAMS_ALLOW_MODEL_DOWNLOAD", std::nullopt};
    CHECK_FALSE(isModelDownloadAllowed());

    environment.set("1");
    CHECK(isModelDownloadAllowed());
}
