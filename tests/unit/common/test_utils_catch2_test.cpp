
#include <catch2/catch_test_macros.hpp>

#include <cstdlib>

#include "env_compat.h"

#include <yams/common/test_utils.h>
#include <yams/compat/unistd.h>

using yams::test::isModelDownloadAllowed;
using yams::test::isTestDiscoveryMode;
using yams::test::shouldSkipModelLoading;

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
