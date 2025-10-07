// Validates Statement chrono bind(int, sys_seconds) and getTime(int)
#include <chrono>
#include <gtest/gtest.h>
#include <yams/metadata/database.h>

using namespace std::chrono;
using namespace yams::metadata;

TEST(DatabaseStatementChronoTest, BindAndGetTimeRoundtrip) {
    Database db;
    ASSERT_TRUE(db.open(":memory:", ConnectionMode::Memory).has_value());

    ASSERT_TRUE(db.execute("CREATE TABLE t(ts INTEGER)").has_value());

    // Use a stable epoch value to avoid flakiness
    sys_seconds t = sys_seconds{seconds{1'234'567}};

    // Insert via chrono bind
    auto ins = db.prepare("INSERT INTO t(ts) VALUES(?)");
    ASSERT_TRUE(ins.has_value());
    Statement istmt = std::move(ins).value();
    ASSERT_TRUE(istmt.bind(1, t).has_value());
    ASSERT_TRUE(istmt.execute().has_value());

    // Read back via getTime
    auto sel = db.prepare("SELECT ts FROM t LIMIT 1");
    ASSERT_TRUE(sel.has_value());
    Statement sstmt = std::move(sel).value();
    auto step = sstmt.step();
    ASSERT_TRUE(step.has_value());
    ASSERT_TRUE(step.value());

    sys_seconds out = sstmt.getTime(0);
    EXPECT_EQ(out.time_since_epoch().count(), t.time_since_epoch().count());
}
