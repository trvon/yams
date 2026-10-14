// Copyright (C) 2026 YAMS contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>

// The shared Catch2 main (tests/support/catch2_main.cpp) gives every test process a private
// XDG_STATE_HOME unless the caller exported one. Either way the host user's live session must
// not be visible: SessionService reads <XDG_STATE_HOME>/yams/sessions/index.json, and a session
// with watch enabled makes the session watcher walk its pinned trees during ServiceManager init.
TEST_CASE("test process does not see the host user's yams session", "[common][state-isolation]") {
    const char* value = std::getenv("XDG_STATE_HOME");
    REQUIRE(value != nullptr);
    REQUIRE(value[0] != '\0');

    const std::filesystem::path stateHome{value};
    std::error_code ec;
    REQUIRE(std::filesystem::is_directory(stateHome, ec));

    const bool isolated = stateHome.filename().string().rfind("yams-test-state-", 0) == 0;
    if (isolated) {
        // Fresh per-process directory: nothing but what this binary's own tests created.
        CHECK_FALSE(std::filesystem::exists(stateHome / "yams" / "sessions" / "index.json", ec));
    } else {
        WARN("XDG_STATE_HOME was provided by the caller: " << stateHome.string());
    }
}
