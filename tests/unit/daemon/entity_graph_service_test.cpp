#include <gtest/gtest.h>
#include <yams/daemon/components/EntityGraphService.h>

using yams::daemon::EntityGraphService;

TEST(EntityGraphServiceTest, QueueAndProcessWithoutServices) {
    // Service with nullptr ServiceManager should not crash; processing will be counted as failed
    EntityGraphService svc(nullptr, 1);
    svc.start();

    EntityGraphService::Job j;
    j.documentHash = "deadbeef";
    j.filePath = "/tmp/file.cpp";
    j.contentUtf8 = "int main() { return 0; }";
    j.language = "cpp";

    auto r = svc.submitExtraction(j);
    ASSERT_TRUE(r.has_value());

    // Give worker a short time slice
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto stats = svc.getStats();
    EXPECT_GE(stats.accepted, 1u);
    EXPECT_GE(stats.processed, 1u);
    EXPECT_GE(stats.failed, 1u);

    svc.stop();
}
