#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <catch2/catch_test_macros.hpp>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/compat/unistd.h>


using namespace yams::app::services;

namespace {
std::filesystem::path createTempStateRoot() {
    auto p = std::filesystem::temp_directory_path() / "yams_test_state";
    std::error_code ec;
    std::filesystem::create_directories(p, ec);
    return p;
}

struct ScopedWorkingDirectory {
    std::filesystem::path original;

    explicit ScopedWorkingDirectory(const std::filesystem::path& target) {
        original = std::filesystem::current_path();
        std::filesystem::current_path(target);
    }

    ~ScopedWorkingDirectory() {
        try {
            std::filesystem::current_path(original);
        } catch (...) {
        }
    }
};
} // namespace

TEST_CASE("SessionService - Configuration and State", "[session][service][config]") {
    SECTION("Watch configuration roundtrip") {
        auto root = std::filesystem::temp_directory_path() / "yams_watch_rt";
        setenv("XDG_STATE_HOME", root.string().c_str(), 1);

        AppContext ctx;
        auto svc = makeSessionService(&ctx);
        svc->init("watchme", "");

        CHECK_FALSE(svc->watchEnabled());

        svc->enableWatch(true);
        svc->setWatchIntervalMs(1500);

        CHECK(svc->watchEnabled());
        CHECK(svc->watchIntervalMs() >= 1000u);
    }

    SECTION("Session initialization and selectors") {
        auto root = createTempStateRoot();
        setenv("XDG_STATE_HOME", root.string().c_str(), 1);

        AppContext ctx;
        auto svc = makeSessionService(&ctx);
        REQUIRE(svc);

        svc->init("unittest", "test session");
        REQUIRE(svc->current().has_value());
        CHECK(svc->current().value() == "unittest");

        svc->addPathSelector("src/**/*.cpp", {"pinned"}, {});
        auto sels = svc->listPathSelectors("unittest");
        CHECK_FALSE(sels.empty());

        svc->removePathSelector("src/**/*.cpp");
        sels = svc->listPathSelectors("unittest");
        CHECK(sels.empty());
    }

    SECTION("Materialized view listing") {
        auto root = createTempStateRoot();
        setenv("XDG_STATE_HOME", root.string().c_str(), 1);

        AppContext ctx;
        auto svc = makeSessionService(&ctx);
        svc->init("mat", "mat test");

        auto items = svc->listMaterialized("mat");
        CHECK(items.empty());
    }
}

TEST_CASE("Service Utils - Path Normalization", "[utils][service][paths]") {
    using utils::NormalizedLookupPath;
    using utils::normalizeLookupPath;

    SECTION("Convert relative path to canonical") {
        namespace fs = std::filesystem;
        const auto uniqueSuffix =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        fs::path tempRoot = fs::temp_directory_path() / ("yams_utils_test_" + uniqueSuffix);
        fs::create_directories(tempRoot);
        fs::path nestedDir = tempRoot / "nested";
        fs::create_directories(nestedDir);
        fs::path targetFile = nestedDir / "file.txt";

        std::ofstream{targetFile} << "sample";

        ScopedWorkingDirectory cwdScope(tempRoot);
        NormalizedLookupPath result = normalizeLookupPath("./nested/../nested/file.txt");

        CHECK(result.changed);
        CHECK_FALSE(result.hasWildcards);
        CHECK(fs::weakly_canonical(targetFile).string() == result.normalized);
        CHECK(result.original == "./nested/../nested/file.txt");

        fs::remove_all(tempRoot);
    }

    SECTION("Glob inputs canonicalize directory prefix but preserve pattern") {
        NormalizedLookupPath result = normalizeLookupPath("docs/**/*.md");

        CHECK(result.hasWildcards);
        CHECK(result.normalized.find("**") != std::string::npos);
        CHECK(result.normalized.find("*.md") != std::string::npos);
    }

    SECTION("Leave dash literal untouched") {
        NormalizedLookupPath result = normalizeLookupPath("-");

        CHECK_FALSE(result.changed);
        CHECK_FALSE(result.hasWildcards);
        CHECK(result.normalized == "-");
    }
}

TEST_CASE("DownloadService - Factory Construction", "[download][service][factory]") {
    SECTION("Factory constructs with minimal context") {
        AppContext ctx;
        auto svc = makeDownloadService(ctx);

        CHECK(svc != nullptr);
    }
}
