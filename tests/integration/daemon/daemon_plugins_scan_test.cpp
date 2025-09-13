#include "test_daemon_harness.h"
#include <gtest/gtest.h>

// Ensure autoloadPluginsNow() is a no-op under mock/disabled ABI plugins.
TEST(DaemonPlugins, ScanReturnsZeroUnderMock) {
    // Force mock and disable ABI plugins to avoid dlopen on CI/macOS
#ifdef __APPLE__
    ::setenv("YAMS_USE_MOCK_PROVIDER", "1", 1);
    ::setenv("YAMS_DISABLE_ABI_PLUGINS", "1", 1);
#else
    setenv("YAMS_USE_MOCK_PROVIDER", "1", 1);
    setenv("YAMS_DISABLE_ABI_PLUGINS", "1", 1);
#endif

    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start(std::chrono::seconds(2)));

    // Autoload should find 0 plugins when ABI plugins are disabled
    auto r = h.daemon()->autoloadPluginsNow();
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value(), 0u) << "Autoload should be a no-op under mock";

    h.stop();
}
