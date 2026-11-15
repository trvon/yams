// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/retrieval_path_policy.hpp>

#include "../../../common/test_helpers_catch2.h"

#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;
using yams::app::services::utils::isPathWithinScope;
using yams::app::services::utils::PathScope;

TEST_CASE("PathScope matches the root and its descendants only", "[services][scope]") {
    const PathScope scope{"/repo/src"};
    CHECK(scope.contains("/repo/src"));
    CHECK(scope.contains("/repo/src/daemon/a.cpp"));
    CHECK(scope.contains("/repo/src/../src/b.cpp"));
    CHECK_FALSE(scope.contains("/repo/srcx/a.cpp"));
    CHECK_FALSE(scope.contains("/repo/src/../other/a.cpp"));
    CHECK_FALSE(scope.contains("/elsewhere/a.cpp"));
    CHECK_FALSE(scope.contains(""));
    CHECK_FALSE(PathScope{""}.contains("/repo/src/a.cpp"));
}

#ifndef _WIN32
TEST_CASE("PathScope resolves symlinked directories like isPathWithinScope",
          "[services][scope][symlink]") {
    yams::test::TempDirGuard temp{"yams_path_scope_"};
    const auto real = fs::canonical(temp.path()) / "real";
    const auto alias = fs::canonical(temp.path()) / "alias";
    fs::create_directories(real / "sub");
    fs::create_directory_symlink(real, alias);
    std::ofstream(real / "sub" / "file.txt") << "x";

    // A scope given through the alias contains documents stored under the real path, and the
    // other way round (the /var -> /private/var case on macOS).
    for (const auto& [root, doc] : {std::pair{alias, real / "sub" / "file.txt"},
                                    std::pair{real, alias / "sub" / "file.txt"}}) {
        INFO("root=" << root << " doc=" << doc);
        CHECK(PathScope{root.string()}.contains(doc.string()));
        CHECK(isPathWithinScope(doc.string(), root.string()));
    }
    CHECK_FALSE(PathScope{(real / "sub").string()}.contains((temp.path() / "x.txt").string()));
}
#endif

TEST_CASE("PathScope resolves each directory once, not each document", "[services][scope]") {
    // Grep checked scope with two weakly_canonical calls per candidate document, root included:
    // 43% of the daemon's CPU for a scoped grep over a real store.
    const PathScope scope{"/repo/src/daemon"};
    for (int i = 0; i < 1000; ++i) {
        (void)scope.contains("/other/project/dir" + std::to_string(i % 3) + "/f" +
                             std::to_string(i) + ".cpp");
        (void)scope.contains("/repo/src/daemon/f" + std::to_string(i) + ".cpp");
    }
    CHECK(scope.resolvedDirectoryCount() == 3);
}
