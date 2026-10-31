// RepairScanCursor: the startup repair scan resumes after hitting the queue cap, so an
// embedding-space migration re-embeds the whole corpus, not just the first maxPendingRepairs.

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/RepairService.h>

using yams::daemon::RepairScanCursor;

TEST_CASE("RepairScanCursor runs once when the scan completes", "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0U);
    CHECK_FALSE(cursor.shouldStart()); // in flight
    cursor.finished();
    CHECK_FALSE(cursor.shouldStart()); // whole corpus scanned
}

TEST_CASE("RepairScanCursor resumes where a capped scan stopped",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0U);
    cursor.stoppedAt(1000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 1000U);
    cursor.stoppedAt(2000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 2000U);
    cursor.finished();
    CHECK_FALSE(cursor.shouldStart());
}

TEST_CASE("RepairScanCursor never starts two scans at once", "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    (void)cursor.begin();
    CHECK_FALSE(cursor.shouldStart());
    cursor.aborted(); // exception or shutdown mid-scan: allow a retry from the same offset
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0U);
}
