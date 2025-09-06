#pragma once

#include <cstdlib>
#include <string>

namespace yams::test {

/**
 * Check if we're running in test discovery mode
 * (when CMake/gtest is listing tests, not running them)
 */
inline bool isTestDiscoveryMode() {
    // Check if running test discovery
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        return true;
    }

    // Check command line for --gtest_list_tests
    // Note: In a real implementation, we'd need access to argc/argv
    // For now, we'll rely on the environment variable
    return false;
}

/**
 * Check if we should skip model loading
 * (either in discovery mode or explicitly requested)
 */
inline bool shouldSkipModelLoading() {
    // Honor multiple env toggles to avoid heavy model work in tests/CI
    return isTestDiscoveryMode() || std::getenv("YAMS_SKIP_MODEL_LOADING") ||
           std::getenv("YAMS_TEST_MODE");
}

/**
 * Check if automatic model downloads are allowed
 */
inline bool isModelDownloadAllowed() {
    return std::getenv("YAMS_ALLOW_MODEL_DOWNLOAD") != nullptr;
}

} // namespace yams::test
