#include <cstdlib>
#include "test_daemon_harness.h"
#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST(DaemonComponents, EmbeddingsAutoOnAddEnvToggle) {
    // Case 1: Explicitly disable via env
#ifdef __APPLE__
    ::setenv("YAMS_EMBED_ON_ADD", "0", 1);
#else
    setenv("YAMS_EMBED_ON_ADD", "0", 1);
#endif
    {
        yams::test::DaemonHarness h;
        ASSERT_TRUE(h.start(2s));
        auto* sm = h.daemon()->getServiceManager();
        ASSERT_NE(sm, nullptr);
        EXPECT_FALSE(sm->isEmbeddingsAutoOnAdd())
            << "Expected embeddingsAutoOnAdd to be false when YAMS_EMBED_ON_ADD=0";
        h.stop();
    }
    // Case 2: Default (no env) should enable in YAMS_TESTING builds
#ifdef __APPLE__
    ::unsetenv("YAMS_EMBED_ON_ADD");
#else
    unsetenv("YAMS_EMBED_ON_ADD");
#endif
    {
        yams::test::DaemonHarness h;
        ASSERT_TRUE(h.start(2s));
        auto* sm = h.daemon()->getServiceManager();
        ASSERT_NE(sm, nullptr);
        EXPECT_TRUE(sm->isEmbeddingsAutoOnAdd())
            << "Expected embeddingsAutoOnAdd to default true under tests";
        h.stop();
    }
}
