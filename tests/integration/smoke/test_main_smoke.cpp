// Custom GTest main for smoke tests that exits without running static destructors.
#include <cstdlib>
#include <gtest/gtest.h>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    int rc = RUN_ALL_TESTS();
    // Avoid potential aborts in static destructors on some environments by exiting immediately.
#if defined(_WIN32)
    _exit(rc == 0 ? 0 : 1);
#else
    _Exit(rc == 0 ? 0 : 1);
#endif
}
