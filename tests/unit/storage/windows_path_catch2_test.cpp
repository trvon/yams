#include "../../../src/storage/windows_path.h"

#include <string>
#include <catch2/catch_test_macros.hpp>

TEST_CASE("Windows storage path spelling preserves drive and UNC roots",
          "[storage][backend][windows-path]") {
    using yams::storage::detail::extendedWindowsPath;
    const auto check = [](std::wstring_view input, std::wstring_view expected) {
        auto result = extendedWindowsPath(input);
        REQUIRE(result.has_value());
        CHECK(result.value() == expected);
        auto repeated = extendedWindowsPath(result.value());
        REQUIRE(repeated.has_value());
        CHECK(repeated.value() == expected);
    };
    check(LR"(C:\corpus)", LR"(\\?\C:\corpus)");
    check(LR"(c:\)", LR"(\\?\c:\)");
    check(LR"(\\server\share\corpus)", LR"(\\?\UNC\server\share\corpus)");
    check(LR"(\\server\share)", LR"(\\?\UNC\server\share)");
    check(L"C:\\corpus\\\u03b1", L"\\\\?\\C:\\corpus\\\u03b1");
    const auto longPath =
        std::wstring(LR"(C:\corpus\)") + std::wstring(180, L'a') + L"\\" + std::wstring(120, L'b');
    check(longPath, std::wstring(LR"(\\?\)") + longPath);
}

TEST_CASE("Windows storage path spelling rejects unresolved paths",
          "[storage][backend][windows-path]") {
    for (const auto input : {L"", L"relative", L"C:relative", LR"(\rooted)", L"C:/corpus",
                             LR"(\\server)", LR"(\\server\)", LR"(\\.\device)"}) {
        auto result = yams::storage::detail::extendedWindowsPath(input);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::InvalidPath);
    }
}

TEST_CASE("Windows storage path helpers round-trip the extended namespace",
          "[storage][backend][windows-path]") {
    using yams::storage::detail::extendedWindowsPath;
    using yams::storage::detail::isExtendedWindowsPath;
    using yams::storage::detail::kWindowsMaxPath;
    using yams::storage::detail::stripExtendedWindowsPath;

    // The threshold is the documented ordinary Win32 limit; the backend only prefixes at or
    // beyond it, which is what keeps short-path behaviour byte-identical.
    CHECK(kWindowsMaxPath == 260u);

    CHECK(isExtendedWindowsPath(LR"(\\?\C:\corpus)"));
    CHECK(isExtendedWindowsPath(LR"(\\?\UNC\server\share)"));
    CHECK_FALSE(isExtendedWindowsPath(LR"(C:\corpus)"));
    CHECK_FALSE(isExtendedWindowsPath(LR"(\\server\share)"));
    CHECK_FALSE(isExtendedWindowsPath(L""));

    CHECK(stripExtendedWindowsPath(LR"(\\?\C:\corpus)") == LR"(C:\corpus)");
    CHECK(stripExtendedWindowsPath(LR"(\\?\UNC\server\share\x)") == LR"(\\server\share\x)");
    CHECK(stripExtendedWindowsPath(LR"(C:\corpus)") == LR"(C:\corpus)");

    for (const auto input : {LR"(C:\corpus)", LR"(C:\corpus\child)", LR"(\\server\share\corpus)",
                             LR"(\\server\share)"}) {
        auto extended = extendedWindowsPath(input);
        REQUIRE(extended.has_value());
        CHECK((stripExtendedWindowsPath(extended.value()) == input));
    }
}
