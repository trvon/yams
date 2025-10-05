#include <gtest/gtest.h>

#include <yams/api/mobile_bindings.h>

#include "tests/common/fixture_manager.h"
#include "tests/common/search_corpus_presets.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>

#include <nlohmann/json.hpp>

namespace {

std::string make_unique_temp_dir() {
    auto base = std::filesystem::temp_directory_path();
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<std::uint64_t> dist;
    std::filesystem::path candidate;
    do {
        candidate = base / ("yams-mobile-abi-" + std::to_string(dist(rng)));
    } while (std::filesystem::exists(candidate));
    std::filesystem::create_directories(candidate);
    return candidate.string();
}

struct TempDirGuard {
    explicit TempDirGuard(std::string path) : path(std::move(path)) {}
    ~TempDirGuard() {
        if (!path.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }
    std::string path;
};

} // namespace

TEST(MobileAbiSmokeTest, VersionMatchesHeaderMacros) {
    const auto version = yams_mobile_get_version();
    EXPECT_EQ(version.major, YAMS_MOBILE_API_VERSION_MAJOR);
    EXPECT_EQ(version.minor, YAMS_MOBILE_API_VERSION_MINOR);
    EXPECT_EQ(version.patch, YAMS_MOBILE_API_VERSION_PATCH);
}

TEST(MobileAbiSmokeTest, DocumentRoundTripWithMobileCorpus) {
    const auto workingDir = make_unique_temp_dir();
    const auto cacheDir = workingDir + "/cache";
    TempDirGuard guard(workingDir);

    yams_mobile_context_config config = yams_mobile_context_config_default();
    config.working_directory = workingDir.c_str();
    config.cache_directory = cacheDir.c_str();

    yams_mobile_context_t* ctx = nullptr;
    ASSERT_EQ(YAMS_MOBILE_STATUS_OK, yams_mobile_context_create(&config, &ctx))
        << yams_mobile_last_error_message();
    ASSERT_NE(nullptr, ctx);

    yams::test::FixtureManager fixtures;
    auto spec = yams::test::mobileSearchCorpusSpec();
    auto corpus = fixtures.createSearchCorpus(spec);
    ASSERT_FALSE(corpus.fixtures.empty());

    const auto documentPath = corpus.fixtures.front().path.string();
    const char* tags[] = {"mobile", "fixture", nullptr};

    yams_mobile_document_store_request store{};
    store.header = yams_mobile_request_header_default();
    store.path = documentPath.c_str();
    store.tags = tags;
    store.tag_count = 2;
    store.sync_now = 1;

    yams_mobile_string_view stored_hash{};
    ASSERT_EQ(YAMS_MOBILE_STATUS_OK, yams_mobile_store_document(ctx, &store, &stored_hash))
        << yams_mobile_last_error_message();
    ASSERT_GT(stored_hash.length, 0U);

    yams_mobile_list_request listRequest{};
    listRequest.header = yams_mobile_request_header_default();
    listRequest.limit = 25;
    yams_mobile_list_result_t* listResult = nullptr;
    ASSERT_EQ(YAMS_MOBILE_STATUS_OK, yams_mobile_list_documents(ctx, &listRequest, &listResult))
        << yams_mobile_last_error_message();
    ASSERT_NE(nullptr, listResult);

    const auto listJson = yams_mobile_list_result_json(listResult);
    ASSERT_NE(nullptr, listJson.data);
    const auto parsed = nlohmann::json::parse(std::string(listJson.data, listJson.length));
    ASSERT_TRUE(parsed.contains("documents"));
    const auto& documents = parsed.at("documents");
    ASSERT_FALSE(documents.empty());

    const auto hashString = std::string(stored_hash.data, stored_hash.length);
    const bool found = std::any_of(documents.begin(), documents.end(), [&](const auto& entry) {
        return entry.contains("hash") && entry.at("hash").template get<std::string>() == hashString;
    });
    EXPECT_TRUE(found);

    yams_mobile_list_result_destroy(listResult);

    yams_mobile_search_request searchRequest{};
    searchRequest.header = yams_mobile_request_header_default();
    searchRequest.query = "offline";
    searchRequest.limit = 10;
    yams_mobile_search_result_t* searchResult = nullptr;
    EXPECT_EQ(YAMS_MOBILE_STATUS_OK, yams_mobile_search_execute(ctx, &searchRequest, &searchResult))
        << yams_mobile_last_error_message();
    if (searchResult) {
        const auto payload = yams_mobile_search_result_json(searchResult);
        EXPECT_NE(nullptr, payload.data);
        yams_mobile_search_result_destroy(searchResult);
    }

    yams_mobile_context_destroy(ctx);
}
