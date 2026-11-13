#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include "../../../common/env_compat.h"
#include "../../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/list_input_resolver.hpp>
#include <yams/app/services/path_projection.hpp>
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
        std::error_code error;
        std::filesystem::current_path(original, error);
    }
};
} // namespace

TEST_CASE("SessionService - Configuration and State", "[session][service][config]") {
    SECTION("Watch configuration roundtrip") {
        auto root = std::filesystem::temp_directory_path() / "yams_watch_rt";
        yams::test::ScopedEnvVar stateHome{"XDG_STATE_HOME", root.string()};

        AppContext ctx;
        auto svc = makeSessionService(&ctx);
        svc->init("watchme", "");

        CHECK_FALSE(svc->watchEnabled());

        svc->enableWatch(true);
        svc->setWatchIntervalMs(1500);

        CHECK(svc->watchEnabled());
        CHECK((svc->watchIntervalMs() >= 1000u));
    }

    SECTION("Listing sessions leaves out the index file") {
        auto root = createTempStateRoot();
        yams::test::ScopedEnvVar stateHome{"XDG_STATE_HOME", root.string()};

        AppContext ctx;
        auto svc = makeSessionService(&ctx);
        svc->init("alpha", "");
        svc->init("beta", "");

        // sessions/index.json records the current session; it is not a session.
        // The state root may hold sessions from other sections.
        const auto names = svc->listSessions();
        const auto has = [&](const char* name) {
            return std::find(names.begin(), names.end(), name) != names.end();
        };
        CHECK(has("alpha"));
        CHECK(has("beta"));
        CHECK_FALSE(has("index"));
    }

    SECTION("Session initialization and selectors") {
        auto root = createTempStateRoot();
        yams::test::ScopedEnvVar stateHome{"XDG_STATE_HOME", root.string()};

        AppContext ctx;
        auto svc = makeSessionService(&ctx);
        REQUIRE(svc);

        svc->init("unittest", "test session");
        REQUIRE(svc->current().has_value());
        CHECK((svc->current().value() == "unittest"));

        svc->addPathSelector("src/**/*.cpp", {"pinned"}, {});
        auto sels = svc->listPathSelectors("unittest");
        CHECK_FALSE(sels.empty());

        svc->removePathSelector("src/**/*.cpp");
        sels = svc->listPathSelectors("unittest");
        CHECK(sels.empty());
    }

    SECTION("Session create generates UUID") {
        auto uniqueSuffix =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        auto root = std::filesystem::temp_directory_path() / ("yams_uuid_test_" + uniqueSuffix);
        std::filesystem::create_directories(root);
        yams::test::ScopedEnvVar stateHome{"XDG_STATE_HOME", root.string()};

        AppContext ctx;
        auto svc = makeSessionService(&ctx);
        REQUIRE(svc);

        svc->create("uuid-test", "uuid test session");
        auto info = svc->getSessionInfo("uuid-test");
        REQUIRE(info.has_value());
        CHECK_FALSE(info->uuid.empty());
        CHECK((info->uuid.size() == 36));
        CHECK((info->uuid.find('-') != std::string::npos));
    }

    SECTION("Materialized view listing") {
        auto root = createTempStateRoot();
        yams::test::ScopedEnvVar stateHome{"XDG_STATE_HOME", root.string()};

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

        {
            ScopedWorkingDirectory cwdScope(tempRoot);
            NormalizedLookupPath result = normalizeLookupPath("./nested/../nested/file.txt");

            CHECK(result.changed);
            CHECK_FALSE(result.hasWildcards);
            CHECK((fs::weakly_canonical(targetFile).string() == result.normalized));
            CHECK((result.original == "./nested/../nested/file.txt"));
        } // cwdScope destructor restores original CWD before cleanup

        std::error_code ec;
        fs::remove_all(tempRoot, ec); // Ignore errors on cleanup
    }

    SECTION("Glob inputs canonicalize directory prefix but preserve pattern") {
        NormalizedLookupPath result = normalizeLookupPath("docs/**/*.md");

        CHECK(result.hasWildcards);
        CHECK((result.normalized.find("**") != std::string::npos));
        CHECK((result.normalized.find("*.md") != std::string::npos));
    }

    SECTION("Leave dash literal untouched") {
        NormalizedLookupPath result = normalizeLookupPath("-");

        CHECK_FALSE(result.changed);
        CHECK_FALSE(result.hasWildcards);
        CHECK((result.normalized == "-"));
    }
}

TEST_CASE("Service Utils - Query snippets center matched code", "[utils][service][snippet]") {
    const std::string prefix(400, 'x');
    const std::string content =
        prefix + "\nint affectedTests() {\n    return traverseGraph();\n}\n";

    const auto result =
        utils::createQuerySnippet(content, "graph affected tests traversal", 120, true);

    CHECK(result.queryMatched);
    CHECK((result.matchedTerms >= 2));
    CHECK((result.sourceOffset > 0));
    CHECK((result.text.find("affectedTests") != std::string::npos));
    CHECK((result.text.find("traverseGraph") != std::string::npos));
    CHECK((result.text.find(std::string(80, 'x')) == std::string::npos));
}

TEST_CASE("Service Utils - Query snippets retain prefix fallback", "[utils][service][snippet]") {
    const std::string content = "alpha beta gamma delta";

    const auto result = utils::createQuerySnippet(content, "missing terms", 10, true);
    CHECK_FALSE(result.queryMatched);
    CHECK((result.text == utils::createSnippet(content, 10, true)));
}

TEST_CASE("Path projection gives paths-only responses one shared contract",
          "[utils][service][paths-only]") {
    SearchResponse resp;
    std::vector<SearchItem> items;

    SearchItem first;
    first.path = "/tmp/a.cpp";
    first.fileName = "a.cpp";
    items.push_back(first);

    SearchItem duplicate = first;
    items.push_back(duplicate);

    SearchItem fallback;
    fallback.fileName = "b.cpp";
    items.push_back(fallback);

    path_projection::assignPathsOnly(resp, items, 10, [](const SearchItem& item) {
        return path_projection::displayPath(item.path, item.fileName);
    });

    REQUIRE((resp.paths.size() == 2));
    CHECK((resp.paths[0] == "/tmp/a.cpp"));
    CHECK((resp.paths[1] == "b.cpp"));
    CHECK((resp.total == resp.paths.size()));
    CHECK((resp.results.empty()));

    SearchResponse fuzzy;
    fuzzy.paths = {"/tmp/a.cpp", "/tmp/c.cpp"};
    path_projection::mergePathsOnly(resp, fuzzy, 2);
    REQUIRE((resp.paths.size() == 2));
    CHECK((resp.paths[0] == "/tmp/a.cpp"));
    CHECK((resp.paths[1] == "b.cpp"));
    CHECK((resp.total == 2));
}

TEST_CASE("List input resolver classifies wildcards and directories", "[utils][service][list]") {
    using yams::app::services::ResolvedListInputKind;
    using yams::app::services::resolveNameToPatternIfLocalFile;

    SECTION("Absolute wildcard path stays a pattern") {
        auto result = resolveNameToPatternIfLocalFile("/tmp/project/**/*");

        CHECK((result.kind == ResolvedListInputKind::Pattern));
        CHECK_FALSE(result.isLocalFile);
        CHECK_FALSE(result.isLocalDirectory);
        CHECK((result.pattern.find("**") != std::string::npos));
    }

    SECTION("Existing directory resolves as local directory") {
        namespace fs = std::filesystem;
        const auto uniqueSuffix =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        fs::path tempRoot = fs::temp_directory_path() / ("yams_list_resolver_" + uniqueSuffix);
        fs::create_directories(tempRoot / "nested");

        auto result = resolveNameToPatternIfLocalFile((tempRoot / "nested").string());

        CHECK((result.kind == ResolvedListInputKind::LocalDirectory));
        CHECK_FALSE(result.isLocalFile);
        CHECK(result.isLocalDirectory);
        REQUIRE(result.absPath.has_value());
        CHECK((*result.absPath == fs::weakly_canonical(tempRoot / "nested").string()));

        std::error_code ec;
        fs::remove_all(tempRoot, ec);
    }
}

TEST_CASE("DownloadService - Factory Construction", "[download][service][factory]") {
    SECTION("Factory constructs with minimal context") {
        AppContext ctx;
        auto svc = makeDownloadService(ctx);

        CHECK((svc != nullptr));
    }

    SECTION("Explicit storage root wins over the environment default") {
        const auto base =
            std::filesystem::temp_directory_path() /
            ("yams_download_root_" + std::to_string(::getpid()) + "_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        const auto envRoot = base / "from_env";
        const auto optRoot = base / "from_options";
        yams::test::ScopedEnvVar storageEnv{"YAMS_STORAGE", envRoot.string()};

        AppContext ctx;
        DownloadServiceOptions opts;
        opts.storageRoot = optRoot;
        auto svc = makeDownloadService(ctx, opts);

        REQUIRE((svc != nullptr));
        CHECK(std::filesystem::is_directory(optRoot / "storage" / "objects"));
        CHECK(std::filesystem::is_directory(optRoot / "storage" / "staging"));
        CHECK_FALSE(std::filesystem::exists(envRoot));

        std::error_code ec;
        std::filesystem::remove_all(base, ec);
    }

    SECTION("Empty storage root keeps the environment default") {
        const auto envRoot =
            std::filesystem::temp_directory_path() /
            ("yams_download_env_" + std::to_string(::getpid()) + "_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        yams::test::ScopedEnvVar storageEnv{"YAMS_STORAGE", envRoot.string()};

        AppContext ctx;
        auto svc = makeDownloadService(ctx, DownloadServiceOptions{});

        REQUIRE((svc != nullptr));
        CHECK(std::filesystem::is_directory(envRoot / "storage" / "objects"));

        std::error_code ec;
        std::filesystem::remove_all(envRoot, ec);
    }
}

TEST_CASE("Service factory handles incomplete context", "[service][factory]") {
    AppContext ctx;

    CHECK((makeGraphQueryService(ctx) == nullptr));
}
