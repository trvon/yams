// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <yams/core/uuid.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

#include <chrono>
#include <thread>

using yams::daemon::EntityGraphService;

TEST_CASE("EntityGraphService: full channel reports failed admission", "[daemon][kg-intent]") {
    using namespace yams::daemon;
    DaemonConfig config;
    config.dataDir =
        std::filesystem::temp_directory_path() / yams::core::generateId("kg-admission-test");
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    } cleanup{config.dataDir};
    StateComponent state;
    DaemonLifecycleFsm lifecycle;
    ServiceManager services(config, state, lifecycle); // Not initialized; no corpus or daemon.
    EntityGraphService svc(&services, 1);              // Do not start the consumer.
    auto& bus = InternalEventBus::instance();
    auto channel =
        bus.get_or_create_channel<InternalEventBus::EntityGraphJob>("entity_graph_jobs", 4096);
    struct Drain {
        decltype(channel) queue;
        ~Drain() {
            InternalEventBus::EntityGraphJob job;
            while (queue->try_pop(job)) {
            }
        }
    } drain{channel};
    while (channel->try_push(InternalEventBus::EntityGraphJob{})) {
    }
    EntityGraphService::Job job;
    job.documentHash = "intent-hash";
    job.documentDbId = 7;
    job.knowledgeGraphToken = "intent-token";
    auto rejected = svc.submitExtraction(job);
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(svc.getStats().accepted == 0);
    InternalEventBus::EntityGraphJob buffered;
    while (channel->try_pop(buffered)) {
    }
    REQUIRE(svc.submitExtraction(job).has_value());
    REQUIRE(channel->try_pop(buffered));
    CHECK(buffered.documentDbId == 7);
    CHECK(buffered.knowledgeGraphToken == "intent-token");
    CHECK(svc.getStats().accepted == 1);
}

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
