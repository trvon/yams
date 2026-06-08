#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/download_metadata_entries.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams::app::services;
using yams::metadata::ConnectionPool;
using yams::metadata::ConnectionPoolConfig;
using yams::metadata::DocumentInfo;
using yams::metadata::MetadataRepository;
using yams::metadata::populatePathDerivedFields;

namespace {
std::filesystem::path tempDbPath(std::string_view prefix) {
    const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    return std::filesystem::temp_directory_path() / (std::string(prefix) + suffix + ".db");
}

DocumentInfo makeDocument(std::string path, std::string hash) {
    DocumentInfo doc;
    doc.filePath = std::move(path);
    doc.fileName = std::filesystem::path(doc.filePath).filename().string();
    doc.fileExtension = std::filesystem::path(doc.filePath).extension().string();
    doc.fileSize = 123;
    doc.sha256Hash = std::move(hash);
    doc.mimeType = "text/plain";
    const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.createdTime = now;
    doc.modifiedTime = now;
    doc.indexedTime = now;
    populatePathDerivedFields(doc);
    return doc;
}

std::optional<std::string> getStringMetadata(MetadataRepository& repository, int64_t documentId,
                                             const std::string& key) {
    auto result = repository.getMetadata(documentId, key);
    if (!result || !result.value().has_value()) {
        return std::nullopt;
    }
    return result.value()->asString();
}
} // namespace

TEST_CASE("Download metadata entries preserve current last-write-wins tag semantics",
          "[download][metadata][batch]") {
    const auto dbPath = tempDbPath("download_metadata_entries_");

    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 1;
    auto pool = std::make_unique<ConnectionPool>(dbPath.string(), cfg);
    REQUIRE(pool->initialize().has_value());
    MetadataRepository repository(*pool);

    auto insert = repository.insertDocument(
        makeDocument("/tmp/download-metadata-entries.txt", "download-metadata-entries-hash"));
    REQUIRE(insert.has_value());

    DownloadServiceRequest request;
    request.tags = {"user-tag-a", "user-tag-b"};
    request.metadata = {{"custom_key", "custom_value"}};

    yams::downloader::FinalResult result;
    result.url = "https://example.com/path/file.txt";
    result.httpStatus = 204;
    result.etag = "etag-value";
    result.lastModified = "Mon, 01 Jan 2024 00:00:00 GMT";
    result.contentType = "text/plain";
    result.suggestedName = "file.txt";
    result.checksumOk = true;

    auto entries = buildDownloadMetadataEntries(insert.value(), request, result);
    REQUIRE_FALSE(entries.empty());
    REQUIRE(repository.setMetadataBatch(entries).has_value());

    CHECK((getStringMetadata(repository, insert.value(), "source_url") == result.url));
    CHECK((getStringMetadata(repository, insert.value(), "etag") == result.etag.value()));
    CHECK((getStringMetadata(repository, insert.value(), "last_modified") ==
           result.lastModified.value()));
    CHECK((getStringMetadata(repository, insert.value(), "content_type") ==
           result.contentType.value()));
    CHECK((getStringMetadata(repository, insert.value(), "suggested_name") ==
           result.suggestedName.value()));
    CHECK((getStringMetadata(repository, insert.value(), "http_status") == "204"));
    CHECK((getStringMetadata(repository, insert.value(), "checksum_ok") == "true"));
    CHECK((getStringMetadata(repository, insert.value(), "custom_key") == "custom_value"));

    // Characterize current semantics: repeated key="tag" collapses to the last write.
    CHECK((getStringMetadata(repository, insert.value(), "tag") == "status:2xx"));

    pool->shutdown();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("Download metadata entries keep downloaded tag when URL has no scheme",
          "[download][metadata][batch]") {
    DownloadServiceRequest request;
    request.tags = {"user-tag"};

    yams::downloader::FinalResult result;
    result.url = "downloaded-file-without-scheme";

    auto entries = buildDownloadMetadataEntries(7, request, result);
    REQUIRE_FALSE(entries.empty());

    const auto& last = entries.back();
    CHECK((std::get<0>(last) == 7));
    CHECK((std::get<1>(last) == "tag"));
    CHECK((std::get<2>(last).asString() == "downloaded"));
}
