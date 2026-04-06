#include <catch2/catch_test_macros.hpp>

#include <yams/api/mobile_bindings.h>

#include "tests/common/fixture_manager.h"
#include "tests/common/search_corpus_presets.h"
#include "tests/integration/daemon/test_daemon_harness.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <thread>

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

bool wait_for_document(yams_mobile_context_t* ctx, const std::string& hash,
                       std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        yams_mobile_document_get_request getReq{};
        getReq.header = yams_mobile_request_header_default();
        getReq.document_hash = hash.c_str();
        getReq.metadata_only = 1;

        yams_mobile_document_get_result_t* result = nullptr;
        const auto status = yams_mobile_get_document(ctx, &getReq, &result);
        if (status == YAMS_MOBILE_STATUS_OK) {
            yams_mobile_document_get_result_destroy(result);
            return true;
        }
        if (result != nullptr)
            yams_mobile_document_get_result_destroy(result);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

} // namespace

TEST_CASE("Mobile ABI version matches header macros", "[mobile][abi]") {
    const auto version = yams_mobile_get_version();
    CHECK(version.major == YAMS_MOBILE_API_VERSION_MAJOR);
    CHECK(version.minor == YAMS_MOBILE_API_VERSION_MINOR);
    CHECK(version.patch == YAMS_MOBILE_API_VERSION_PATCH);
}

TEST_CASE("Mobile corpus document round trip works in embedded mode", "[mobile][abi]") {
    const auto workingDir = make_unique_temp_dir();
    const auto cacheDir = workingDir + "/cache";
    TempDirGuard guard(workingDir);

    yams_mobile_context_config config = yams_mobile_context_config_default();
    config.working_directory = workingDir.c_str();
    config.cache_directory = cacheDir.c_str();

    yams_mobile_context_t* ctx = nullptr;
    REQUIRE(yams_mobile_context_create(&config, &ctx) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(ctx != nullptr);

    yams::test::FixtureManager fixtures;
    auto spec = yams::test::mobileSearchCorpusSpec();
    auto corpus = fixtures.createSearchCorpus(spec);
    REQUIRE_FALSE(corpus.fixtures.empty());

    const auto documentPath = corpus.fixtures.front().path.string();
    const char* tags[] = {"mobile", "fixture", nullptr};

    yams_mobile_document_store_request store{};
    store.header = yams_mobile_request_header_default();
    store.path = documentPath.c_str();
    store.tags = tags;
    store.tag_count = 2;
    store.sync_now = 1;

    yams_mobile_string_view stored_hash{};
    REQUIRE(yams_mobile_store_document(ctx, &store, &stored_hash) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(stored_hash.length > 0U);

    yams_mobile_list_request listRequest{};
    listRequest.header = yams_mobile_request_header_default();
    listRequest.limit = 25;
    yams_mobile_list_result_t* listResult = nullptr;
    REQUIRE(yams_mobile_list_documents(ctx, &listRequest, &listResult) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(listResult != nullptr);

    const auto listJson = yams_mobile_list_result_json(listResult);
    REQUIRE(listJson.data != nullptr);
    const auto parsed = nlohmann::json::parse(std::string(listJson.data, listJson.length));
    REQUIRE(parsed.contains("documents"));
    const auto& documents = parsed.at("documents");
    REQUIRE_FALSE(documents.empty());

    const auto hashString = std::string(stored_hash.data, stored_hash.length);
    const bool found = std::any_of(documents.begin(), documents.end(), [&](const auto& entry) {
        return entry.contains("hash") && entry.at("hash").template get<std::string>() == hashString;
    });
    CHECK(found);

    yams_mobile_list_result_destroy(listResult);

    const auto documentName = std::filesystem::path(documentPath).filename().string();

    const char* addTags[] = {"smoke-updated", nullptr};
    const char* removeTags[] = {"fixture", nullptr};
    const char* metadataKeys[] = {"source", nullptr};
    const char* metadataValues[] = {"mobile-smoke", nullptr};

    yams_mobile_update_request updateReq{};
    updateReq.header = yams_mobile_request_header_default();
    updateReq.hash = hashString.c_str();
    updateReq.add_tags = addTags;
    updateReq.add_tag_count = 1;
    updateReq.remove_tags = removeTags;
    updateReq.remove_tag_count = 1;
    updateReq.metadata_keys = metadataKeys;
    updateReq.metadata_values = metadataValues;
    updateReq.metadata_count = 1;

    yams_mobile_update_result_t* updateResult = nullptr;
    REQUIRE(yams_mobile_update_document(ctx, &updateReq, &updateResult) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(updateResult != nullptr);
    const auto updateJson = yams_mobile_update_result_json(updateResult);
    REQUIRE(updateJson.data != nullptr);
    const auto parsedUpdate =
        nlohmann::json::parse(std::string(updateJson.data, updateJson.length));
    CHECK(parsedUpdate.value("success", false));
    yams_mobile_update_result_destroy(updateResult);

    yams_mobile_search_request searchRequest{};
    searchRequest.header = yams_mobile_request_header_default();
    searchRequest.query = "offline";
    searchRequest.limit = 10;
    yams_mobile_search_result_t* searchResult = nullptr;
    CHECK(yams_mobile_search_execute(ctx, &searchRequest, &searchResult) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    if (searchResult) {
        const auto payload = yams_mobile_search_result_json(searchResult);
        CHECK(payload.data != nullptr);
        yams_mobile_search_result_destroy(searchResult);
    }

    yams_mobile_delete_request deleteReq{};
    deleteReq.header = yams_mobile_request_header_default();
    deleteReq.name = documentName.c_str();
    deleteReq.dry_run = 0;

    yams_mobile_delete_result_t* deleteResult = nullptr;
    REQUIRE(yams_mobile_delete_by_name(ctx, &deleteReq, &deleteResult) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(deleteResult != nullptr);
    const auto deleteJson = yams_mobile_delete_result_json(deleteResult);
    REQUIRE(deleteJson.data != nullptr);
    const auto parsedDelete =
        nlohmann::json::parse(std::string(deleteJson.data, deleteJson.length));
    REQUIRE(parsedDelete.contains("count"));
    CHECK(parsedDelete.at("count").template get<std::size_t>() >= 1U);
    yams_mobile_delete_result_destroy(deleteResult);

    yams_mobile_document_get_request getAfterDelete{};
    getAfterDelete.header = yams_mobile_request_header_default();
    getAfterDelete.document_hash = hashString.c_str();
    getAfterDelete.metadata_only = 1;
    yams_mobile_document_get_result_t* getAfterDeleteResult = nullptr;
    CHECK(yams_mobile_get_document(ctx, &getAfterDelete, &getAfterDeleteResult) ==
          YAMS_MOBILE_STATUS_NOT_FOUND);

    yams_mobile_graph_query_request graphReq{};
    graphReq.header = yams_mobile_request_header_default();
    graphReq.document_hash = hashString.c_str();
    graphReq.max_depth = 1;
    graphReq.max_results = 10;
    yams_mobile_graph_query_result_t* graphResult = nullptr;
    CHECK(yams_mobile_graph_query(ctx, &graphReq, &graphResult) ==
          YAMS_MOBILE_STATUS_UNAVAILABLE);

    yams_mobile_context_destroy(ctx);
}

TEST_CASE("Mobile daemon mode context fails gracefully when socket is missing", "[mobile][abi]") {
    const auto workingDir = make_unique_temp_dir();
    TempDirGuard guard(workingDir);

    yams_mobile_context_config config = yams_mobile_context_config_default();
    config.working_directory = workingDir.c_str();
    config.backend_mode = YAMS_MOBILE_BACKEND_DAEMON;
    config.daemon_socket_path = "/tmp/yams_mobile_nonexistent_socket_test.sock";

    yams_mobile_context_t* ctx = nullptr;
    const auto status = yams_mobile_context_create(&config, &ctx);

    CHECK(status != YAMS_MOBILE_STATUS_OK);
    CHECK(ctx == nullptr);

    const char* err = yams_mobile_last_error_message();
    REQUIRE(err != nullptr);
    CHECK(std::strlen(err) > 0U);
}

TEST_CASE("Mobile invalid backend mode is rejected", "[mobile][abi]") {
    const auto workingDir = make_unique_temp_dir();
    TempDirGuard guard(workingDir);

    yams_mobile_context_config config = yams_mobile_context_config_default();
    config.working_directory = workingDir.c_str();
    config.backend_mode = 99;

    yams_mobile_context_t* ctx = nullptr;
    const auto status = yams_mobile_context_create(&config, &ctx);
    CHECK(status == YAMS_MOBILE_STATUS_INVALID_ARGUMENT);
    CHECK(ctx == nullptr);
}

TEST_CASE("Embedded mobile graph query remains unavailable", "[mobile][abi]") {
    const auto workingDir = make_unique_temp_dir();
    TempDirGuard guard(workingDir);

    yams_mobile_context_config config = yams_mobile_context_config_default();
    config.working_directory = workingDir.c_str();
    config.backend_mode = YAMS_MOBILE_BACKEND_EMBEDDED;

    yams_mobile_context_t* ctx = nullptr;
    REQUIRE(yams_mobile_context_create(&config, &ctx) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(ctx != nullptr);

    yams_mobile_graph_query_request graphReq{};
    graphReq.header = yams_mobile_request_header_default();
    graphReq.document_hash = "deadbeef";
    graphReq.max_depth = 1;
    graphReq.max_results = 10;
    yams_mobile_graph_query_result_t* graphResult = nullptr;

    CHECK(yams_mobile_graph_query(ctx, &graphReq, &graphResult) ==
          YAMS_MOBILE_STATUS_UNAVAILABLE);
    CHECK(graphResult == nullptr);

    yams_mobile_context_destroy(ctx);
}

TEST_CASE("Daemon mobile get_document honors include_content flag", "[mobile][abi][daemon]") {
    yams::test::DaemonHarness harness({.enableAutoRepair = false});
    REQUIRE(harness.start(std::chrono::seconds(10)));

    const auto workingDir = make_unique_temp_dir();
    TempDirGuard guard(workingDir);
    const auto documentPath = std::filesystem::path(workingDir) / "daemon-mobile-note.txt";

    {
        std::ofstream out(documentPath);
        REQUIRE(out.is_open());
        out << "offline memory for daemon mobile bindings test\n";
    }

    yams_mobile_context_config config = yams_mobile_context_config_default();
    config.working_directory = workingDir.c_str();
    config.backend_mode = YAMS_MOBILE_BACKEND_DAEMON;
    const auto socketPath = harness.socketPath().string();
    config.daemon_socket_path = socketPath.c_str();

    yams_mobile_context_t* ctx = nullptr;
    REQUIRE(yams_mobile_context_create(&config, &ctx) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(ctx != nullptr);

    yams_mobile_document_store_request store{};
    store.header = yams_mobile_request_header_default();
    store.path = documentPath.c_str();

    yams_mobile_string_view stored_hash{};
    REQUIRE(yams_mobile_store_document(ctx, &store, &stored_hash) == YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(stored_hash.length > 0U);

    const auto hashString = std::string(stored_hash.data, stored_hash.length);
    REQUIRE(wait_for_document(ctx, hashString, std::chrono::seconds(5)));

    yams_mobile_document_get_request getWithoutContent{};
    getWithoutContent.header = yams_mobile_request_header_default();
    getWithoutContent.document_hash = hashString.c_str();
    getWithoutContent.include_content = 0;
    getWithoutContent.include_extracted_text = 0;

    yams_mobile_document_get_result_t* noContentResult = nullptr;
    REQUIRE(yams_mobile_get_document(ctx, &getWithoutContent, &noContentResult) ==
            YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(noContentResult != nullptr);

    const auto noContentPayload = yams_mobile_document_get_result_json(noContentResult);
    REQUIRE(noContentPayload.data != nullptr);
    const auto noContentBody = yams_mobile_document_get_result_content(noContentResult);
    CHECK((noContentBody.data == nullptr || noContentBody.length == 0U));
    yams_mobile_document_get_result_destroy(noContentResult);

    yams_mobile_document_get_request getWithContent{};
    getWithContent.header = yams_mobile_request_header_default();
    getWithContent.document_hash = hashString.c_str();
    getWithContent.include_content = 1;
    getWithContent.include_extracted_text = 0;

    yams_mobile_document_get_result_t* withContentResult = nullptr;
    REQUIRE(yams_mobile_get_document(ctx, &getWithContent, &withContentResult) ==
            YAMS_MOBILE_STATUS_OK);
    INFO(yams_mobile_last_error_message());
    REQUIRE(withContentResult != nullptr);

    const auto withContentBody = yams_mobile_document_get_result_content(withContentResult);
    REQUIRE(withContentBody.data != nullptr);
    CHECK(withContentBody.length > 0U);
    const auto withContentString = std::string(withContentBody.data, withContentBody.length);
    CHECK(withContentString.find("offline memory for daemon mobile bindings test") !=
          std::string::npos);
    yams_mobile_document_get_result_destroy(withContentResult);

    yams_mobile_context_destroy(ctx);
    harness.stop();
}
