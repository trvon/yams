#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>

using namespace yams::app::services;

namespace {
std::filesystem::path temp_state_root() {
    auto p = std::filesystem::temp_directory_path() / "yams_test_state";
    std::error_code ec;
    std::filesystem::create_directories(p, ec);
    return p;
}
} // namespace

TEST(SessionService, InitUseAndSelectors) {
    auto root = temp_state_root();
    // Prefer XDG_STATE_HOME to avoid polluting real HOME
    setenv("XDG_STATE_HOME", root.string().c_str(), 1);

    AppContext ctx; // allow service to operate on session files only
    auto svc = makeSessionService(&ctx);
    ASSERT_TRUE(svc);

    // init and use
    svc->init("unittest", "test session");
    ASSERT_TRUE(svc->current().has_value());
    EXPECT_EQ(svc->current().value(), "unittest");

    // add path selector
    svc->addPathSelector("src/**/*.cpp", {"pinned"}, {});
    auto sels = svc->listPathSelectors("unittest");
    EXPECT_FALSE(sels.empty());

    // remove selector
    svc->removePathSelector("src/**/*.cpp");
    sels = svc->listPathSelectors("unittest");
    EXPECT_TRUE(sels.empty());
}

TEST(SessionService, ListMaterializedViewExists) {
    auto root = temp_state_root();
    setenv("XDG_STATE_HOME", root.string().c_str(), 1);
    AppContext ctx;
    auto svc = makeSessionService(&ctx);
    svc->init("mat", "mat test");

    // Should be empty before warming
    auto items = svc->listMaterialized("mat");
    EXPECT_TRUE(items.empty());
}
