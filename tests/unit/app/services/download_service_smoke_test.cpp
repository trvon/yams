#include <gtest/gtest.h>
#include <yams/app/services/services.hpp>

TEST(DownloadService, FactoryConstructsWithMinimalContext) {
    yams::app::services::AppContext ctx; // minimal; store/metadata optional for construction
    auto svc = yams::app::services::makeDownloadService(ctx);
    EXPECT_NE(svc, nullptr);
}
