// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <string>

#include <nlohmann/json.hpp>

#include <yams/memory_sync/records.h>

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
