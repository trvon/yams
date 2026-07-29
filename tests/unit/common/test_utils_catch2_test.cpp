
#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <filesystem>

#include "env_compat.h"
#include "test_helpers_catch2.h"

#include <yams/common/fs_utils.h>
#include <yams/common/metric_helpers.h>
#include <yams/common/string_utils.h>
#include <yams/common/test_utils.h>
#include <yams/compat/unistd.h>

using yams::test::isModelDownloadAllowed;
using yams::test::isTestDiscoveryMode;
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

namespace {

struct EnvVarGuard {
    const char* key;
    bool hadOld;

    explicit EnvVarGuard(const char* k) : key(k), hadOld(std::getenv(k) != nullptr) {
        // Ensure a stable baseline.
        unsetenv(key);
    }

    ~EnvVarGuard() {
        // We can't restore the exact previous value without storing it, but for these
        // boolean toggles we only need to ensure we don't leak a set value into other tests.
        unsetenv(key);
    }
};

} // namespace

TEST_CASE("test::isTestDiscoveryMode checks env var", "[common][test-utils][catch2]") {
    EnvVarGuard g("GTEST_DISCOVERY_MODE");
    CHECK_FALSE(isTestDiscoveryMode());

    setenv("GTEST_DISCOVERY_MODE", "1", 1);
    CHECK(isTestDiscoveryMode());
}

TEST_CASE("test::shouldSkipModelLoading honors env toggles", "[common][test-utils][catch2]") {
    EnvVarGuard g1("GTEST_DISCOVERY_MODE");
    EnvVarGuard g2("YAMS_SKIP_MODEL_LOADING");
    EnvVarGuard g3("YAMS_TEST_MODE");

    CHECK_FALSE(shouldSkipModelLoading());

    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    CHECK(shouldSkipModelLoading());
    unsetenv("YAMS_SKIP_MODEL_LOADING");

    setenv("YAMS_TEST_MODE", "1", 1);
    CHECK(shouldSkipModelLoading());
    unsetenv("YAMS_TEST_MODE");

    setenv("GTEST_DISCOVERY_MODE", "1", 1);
    CHECK(shouldSkipModelLoading());
}

TEST_CASE("test::isModelDownloadAllowed checks env var", "[common][test-utils][catch2]") {
    EnvVarGuard g("YAMS_ALLOW_MODEL_DOWNLOAD");
    CHECK_FALSE(isModelDownloadAllowed());

    setenv("YAMS_ALLOW_MODEL_DOWNLOAD", "1", 1);
    CHECK(isModelDownloadAllowed());
}
