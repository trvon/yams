#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <filesystem>
#include <memory>

#include <nlohmann/json.hpp>
#include "../common/test_helpers_catch2.h"
#include <yams/mcp/mcp_server.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

namespace {

using json = nlohmann::json;
using yams::mcp::MCPServer;

class NullTransport : public yams::mcp::ITransport {
public:
    void send(const json&) override {}
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NotImplemented, "Null transport"};
    }
    bool isConnected() const override { return false; }
    void close() override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Disconnected;
    }
};

} // namespace

TEST_CASE("semantic_dedupe returns persisted groups from metadata",
          "[mcp][semantic-dedupe][catch2]") {
    auto tempDir = yams::test::make_temp_dir("yams_mcp_semantic_dedupe_");
    std::filesystem::create_directories(tempDir);

    yams::metadata::ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    auto pool = std::make_shared<yams::metadata::ConnectionPool>((tempDir / "yams.db").string(),
                                                                 poolConfig);
    auto init = pool->initialize();
    REQUIRE(init.has_value());

    yams::metadata::MetadataRepository repo(*pool);

    auto makeDoc = [](std::string path, std::string hash) {
        yams::metadata::DocumentInfo doc;
        doc.filePath = std::move(path);
        doc.fileName = std::filesystem::path(doc.filePath).filename().string();
        doc.fileExtension = std::filesystem::path(doc.filePath).extension().string();
        doc.sha256Hash = std::move(hash);
        doc.fileSize = 123;
        doc.mimeType = "text/plain";
        doc.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.modifiedTime = doc.createdTime;
        doc.indexedTime = doc.createdTime;
        return doc;
    };

    auto docA = repo.insertDocument(makeDoc("/tmp/a.txt", "hash-a"));
    auto docB = repo.insertDocument(makeDoc("/tmp/b.txt", "hash-b"));
    REQUIRE(docA.has_value());
    REQUIRE(docB.has_value());

    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    yams::metadata::SemanticDuplicateGroup group;
    group.groupKey = "semantic:test";
    group.algorithmVersion = "semantic-dedupe-v1";
    group.status = "suggested";
    group.reviewState = "pending";
    group.canonicalDocumentId = docA.value();
    group.memberCount = 2;
    group.maxPairScore = 0.97;
    group.threshold = 0.92;
    group.evidenceJson = R"({"source":"mcp-test"})";
    group.createdAt = now;
    group.updatedAt = now;
    group.lastComputedAt = now;
    auto groupId = repo.upsertSemanticDuplicateGroup(group);
    REQUIRE(groupId.has_value());

    yams::metadata::SemanticDuplicateGroupMember canonical;
    canonical.documentId = docA.value();
    canonical.role = "canonical";
    canonical.decision = "keep";
    canonical.reason = "keep-newest";
    canonical.createdAt = now;
    canonical.updatedAt = now;

    yams::metadata::SemanticDuplicateGroupMember duplicate;
    duplicate.documentId = docB.value();
    duplicate.role = "duplicate";
    duplicate.decision = "unknown";
    duplicate.similarityToCanonical = 0.98;
    duplicate.titleOverlap = 0.6;
    duplicate.pathOverlap = 0.2;
    duplicate.pairScore = 0.93;
    duplicate.createdAt = now;
    duplicate.updatedAt = now;
    REQUIRE(repo.replaceSemanticDuplicateGroupMembers(groupId.value(), {canonical, duplicate})
                .has_value());

    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    yams::daemon::ClientConfig cfg;
    cfg.dataDir = tempDir;
    server->testConfigureDaemonClient(cfg);

    boost::asio::io_context io;
    yams::mcp::MCPSemanticDedupeRequest req;
    req.limit = 10;

    auto future =
        boost::asio::co_spawn(io, server->testHandleSemanticDedupe(req), boost::asio::use_future);
    io.run();

    auto result = future.get();
    REQUIRE(result.has_value());
    REQUIRE(result.value().groups.size() == 1);
    CHECK(result.value().groups[0].groupKey == "semantic:test");
    CHECK(result.value().groups[0].canonicalDocumentId == docA.value());
    CHECK(result.value().groups[0].members.size() == 2);
}
