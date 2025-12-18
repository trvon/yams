// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/EntityGraphService.h>

#include <chrono>
#include <thread>

using yams::daemon::EntityGraphService;

TEST_CASE("EntityGraphService: queue and process without services", "[daemon]") {
    // Service with nullptr ServiceManager should not crash; processing will be counted as failed
    EntityGraphService svc(nullptr, 1);
    svc.start();

    EntityGraphService::Job j;
    j.documentHash = "deadbeef";
    j.filePath = "/tmp/file.cpp";
    j.contentUtf8 = "int main() { return 0; }";
    j.language = "cpp";

    auto r = svc.submitExtraction(j);
    // submitExtraction may fail immediately when ServiceManager is nullptr on some builds.
    // The key thing is it should not crash. If it succeeds, process will be counted as failed.
    if (r.has_value()) {
        // Give worker a short time slice
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        auto stats = svc.getStats();
        CHECK(stats.accepted >= 1u);
        CHECK(stats.processed >= 1u);
        CHECK(stats.failed >= 1u);
    } else {
        // Submission failed immediately - that's also acceptable behavior
        INFO("submitExtraction returned error: " << r.error().message);
        SUCCEED("submitExtraction rejected job with nullptr ServiceManager (expected behavior)");
    }

    svc.stop();
}
