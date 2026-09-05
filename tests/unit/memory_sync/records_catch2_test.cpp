// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <string>

#include <nlohmann/json.hpp>

#include <yams/memory_sync/records.h>
#include <yams/memory_sync/task_record.h>

using namespace yams::memory_sync;

namespace {

constexpr std::string_view kHashA =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kHashB =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

MemoryIndexRecord indexRecord(std::string_view hash) {
    MemoryIndexRecord record;
    record.entryHash = std::string(hash);
    record.ts = HybridTs{42, 7};
    record.origin = "node-a";
    record.vv.increment(record.origin);
    record.corpusId = "corpus-a";
    record.corpusEpoch = 1;
    record.operationId = "node-a:1";
    record.logicalKey = "document/doc%2Fwith%20spaces";
    record.recordKind = "value";
    return record;
}

} // namespace

TEST_CASE("memory-sync store records round-trip through JSON", "[memory-sync][records]") {
    const DocumentRecord document{
        "doc/with spaces", "source.md", std::string(kHashA), {{"tag", "memory"}}};
    const EmbeddingRecord embedding{
        "chunk/0", document.documentId, "simeon-v1", 3, {0.25F, -1.0F, 2.5F}};
    const TopologyNodeRecord node{"symbol:main", "function", "main", {{"language", "cpp"}}};
    const TopologyEdgeRecord edge{"symbol:main", "calls", "symbol:helper", 0.75};
    const ContentBlobRecord blob{std::string(kHashB), "source.md", 128, "text/markdown"};

    CHECK(nlohmann::json(document).get<DocumentRecord>() == document);
    CHECK(nlohmann::json(embedding).get<EmbeddingRecord>() == embedding);
    CHECK(nlohmann::json(node).get<TopologyNodeRecord>() == node);
    CHECK(nlohmann::json(edge).get<TopologyEdgeRecord>() == edge);
    CHECK(nlohmann::json(blob).get<ContentBlobRecord>() == blob);

    const MetadataDocumentRecord metadataDocument{
        .documentId = "document-key",
        .filePath = "/corpus/source.md",
        .fileName = "source.md",
        .fileExtension = ".md",
        .fileSize = 128,
        .contentHash = std::string(kHashA),
        .mimeType = "text/markdown",
        .createdTime = 100,
        .modifiedTime = 101,
        .indexedTime = 102,
        .contentExtracted = true,
        .extractionStatus = 1,
        .repairStatus = 2,
        .repairAttemptedAt = 103,
        .repairAttempts = 3,
        .metadata = {{"priority", MetadataValueRecord{"7", 1}}},
    };
    CHECK(nlohmann::json(metadataDocument).get<MetadataDocumentRecord>() == metadataDocument);
}

TEST_CASE("memory-sync record keys are deterministic and store-scoped", "[memory-sync][records]") {
    const auto envelope = indexRecord(kHashA);
    const DocumentRecord document{"doc/with spaces", "source.md", std::string(kHashA), {}};
    const EmbeddingRecord embedding{"doc/with spaces", "document", "model", 1, {1.0F}};

    const auto documentKey = recordIndexKey(MemoryStore::Document, document.id(), envelope);
    const auto embeddingKey = recordIndexKey(MemoryStore::Embedding, embedding.id(), envelope);

    REQUIRE(documentKey.has_value());
    REQUIRE(embeddingKey.has_value());
    CHECK(documentKey.value() ==
          "index/document/doc%2Fwith%20spaces/"
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const auto repeatedDocumentKey = recordIndexKey(MemoryStore::Document, document.id(), envelope);
    REQUIRE(repeatedDocumentKey.has_value());
    CHECK(repeatedDocumentKey.value() == documentKey.value());
    CHECK(documentKey.value() != embeddingKey.value());
}

TEST_CASE("memory-sync topology edge identity is unambiguous and path-safe",
          "[memory-sync][records]") {
    const TopologyEdgeRecord edge{"source/a", "calls/indirect", "target%z", 1.0};
    const auto envelope = indexRecord(kHashB);

    REQUIRE(edge.id() == "source%2Fa/calls%2Findirect/target%25z");
    const auto key = recordIndexKey(MemoryStore::TopologyEdge, edge.id(), envelope);
    REQUIRE(key.has_value());
    CHECK(key.value() == "index/topology-edge/source%252Fa%2Fcalls%252Findirect%2Ftarget%2525z/"
                         "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
}

TEST_CASE("memory-sync record keys reject malformed envelope hashes", "[memory-sync][records]") {
    auto envelope = indexRecord(kHashA);
    envelope.entryHash = "not-a-sha256";

    const auto key = recordIndexKey(MemoryStore::ContentBlob, "blob", envelope);
    REQUIRE_FALSE(key.has_value());
    CHECK(key.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("task records bind traceability and descriptive ownership to a revision key",
          "[memory-sync][records][task-record]") {
    const std::string taskId = "123e4567-e89b-42d3-a456-426614174000";
    const std::string key = "task-record/" + taskId + "/" + std::string(kHashA);
    nlohmann::json record = {
        {"schema", "yams.task-record/v1"},
        {"task_id", taskId},
        {"kind", "claim"},
        {"owner", "agent-a"},
        {"source_uri", "repo:src/search"},
        {"source_revision", "git:9883a438"},
        {"recorded_at_ms", 2000},
        {"source_timestamp_ms", 1000},
        {"supersedes", nlohmann::json::array({kHashB})},
        {"ownership_semantics", "record_only"},
        {"body", "Investigating retrieval"},
    };
    SECTION("valid revision") {
        CHECK(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
    }
    SECTION("key cannot alias another task or revision") {
        CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashB).has_value());
        record["task_id"] = "223e4567-e89b-42d3-a456-426614174000";
        CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
    }
    SECTION("claims cannot advertise acquisition") {
        record["ownership_semantics"] = "acquired";
        CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
        record["ownership_semantics"] = "record_only";
        record["fencing_token"] = 1;
        CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
    }
    SECTION("required provenance cannot be omitted") {
        record.erase("source_revision");
        CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
    }
    SECTION("timestamps are positive integer milliseconds") {
        for (const auto& value :
             {nlohmann::json(0), nlohmann::json(-1), nlohmann::json(1.5), nlohmann::json("2000")}) {
            record["recorded_at_ms"] = value;
            CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
        }
    }
    SECTION("supersession links must be distinct non-self hashes") {
        for (const auto& links :
             {nlohmann::json::array({kHashA}), nlohmann::json::array({kHashB, kHashB}),
              nlohmann::json::array({"unknown"})}) {
            record["supersedes"] = links;
            CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
        }
    }
    SECTION("supersession references use canonical lowercase hashes") {
        record["supersedes"] = nlohmann::json::array({std::string(64, 'B')});
        CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
    }
    SECTION("invalid JSON and unknown schema are rejected") {
        CHECK_FALSE(validateTaskRecordPublication(key, "not-json", kHashA).has_value());
        auto duplicate = record.dump();
        duplicate.insert(1, "\"ownership_semantics\":\"acquired\",");
        CHECK_FALSE(validateTaskRecordPublication(key, duplicate, kHashA).has_value());
        record["schema"] = "yams.task-record/v2";
        CHECK_FALSE(validateTaskRecordPublication(key, record.dump(), kHashA).has_value());
    }
}
