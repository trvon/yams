// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_codec.h>

#include <nlohmann/json.hpp>
#include <string>
#include <vector>

using yams::topology::ClusterArtifact;
using yams::topology::ClusterRepresentative;
using yams::topology::ClusterRoutingRepresentative;
using yams::topology::decodeBase64;
using yams::topology::deserializeTopologyBatchBinary;
using yams::topology::deserializeTopologyBatchCompressed;
using yams::topology::DocumentClusterMembership;
using yams::topology::DocumentTopologyRole;
using yams::topology::encodeBase64;
using yams::topology::serializeTopologyBatchBinary;
using yams::topology::serializeTopologyBatchCompressed;
using yams::topology::TopologyArtifactBatch;
using yams::topology::TopologyInputKind;

namespace {

TopologyArtifactBatch createSampleBatch() {
    TopologyArtifactBatch batch;
    batch.snapshotId = "topology-test-2026";
    batch.algorithm = "connected_components_v1";
    batch.inputKind = TopologyInputKind::Hybrid;
    batch.embeddingSpaceIdentity = "test-space-v1";
    batch.protectedRelationIdentity = "pr-identity-v1";
    batch.generatedAtUnixSeconds = 1785471700;
    batch.topologyEpoch = 42;

    // Cluster 1
    ClusterArtifact c1;
    c1.clusterId = "cluster:001";
    c1.parentClusterId = std::nullopt;
    c1.level = 0;
    c1.memberCount = 2;
    c1.persistenceScore = 0.85;
    c1.cohesionScore = 0.92;
    c1.densityScore = 0.77;
    c1.bridgeMass = 0.12;
    c1.protectedPairCount = 4;
    c1.preservedProtectedPairCount = 3;

    ClusterRepresentative medoid1;
    medoid1.clusterId = "cluster:001";
    medoid1.documentHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    medoid1.filePath = "src/search/search_engine.cpp";
    medoid1.representativeScore = 0.98;
    c1.medoid = medoid1;

    c1.memberDocumentHashes = {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                               "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
    c1.overlapClusterIds = {"cluster:002"};
    c1.centroidEmbedding = {0.1f, 0.2f, -0.3f, 0.4f, -0.5f};

    ClusterRoutingRepresentative rep1;
    rep1.documentHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    rep1.embedding = {0.11f, 0.22f, -0.33f, 0.44f, -0.55f};
    c1.routingRepresentatives = {rep1};

    // Cluster 2
    ClusterArtifact c2;
    c2.clusterId = "cluster:002";
    c2.parentClusterId = "cluster:001";
    c2.level = 1;
    c2.memberCount = 1;
    c2.persistenceScore = 0.65;
    c2.cohesionScore = 0.81;
    c2.densityScore = 0.90;
    c2.bridgeMass = 0.05;
    c2.protectedPairCount = 2;
    c2.preservedProtectedPairCount = 2;
    c2.memberDocumentHashes = {"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"};

    batch.clusters = {c1, c2};

    // Memberships
    DocumentClusterMembership m1;
    m1.documentHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    m1.clusterId = "cluster:001";
    m1.parentClusterId = std::nullopt;
    m1.clusterLevel = 0;
    m1.persistenceScore = 0.85;
    m1.cohesionScore = 0.92;
    m1.bridgeScore = 0.10;
    m1.role = DocumentTopologyRole::Medoid;
    m1.overlapClusterIds = {"cluster:002"};

    DocumentClusterMembership m2;
    m2.documentHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    m2.clusterId = "cluster:001";
    m2.parentClusterId = std::nullopt;
    m2.clusterLevel = 0;
    m2.persistenceScore = 0.80;
    m2.cohesionScore = 0.88;
    m2.bridgeScore = 0.25;
    m2.role = DocumentTopologyRole::Bridge;

    DocumentClusterMembership m3;
    m3.documentHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    m3.clusterId = "cluster:002";
    m3.parentClusterId = "cluster:001";
    m3.clusterLevel = 1;
    m3.persistenceScore = 0.65;
    m3.cohesionScore = 0.81;
    m3.bridgeScore = 0.05;
    m3.role = DocumentTopologyRole::Core;

    batch.memberships = {m1, m2, m3};

    return batch;
}

} // namespace

TEST_CASE("Base64 encode and decode roundtrip", "[topology][codec][catch2]") {
    SECTION("Empty input") {
        std::vector<std::byte> empty;
        auto encoded = encodeBase64(empty);
        CHECK(encoded.empty());
        auto decoded = decodeBase64(encoded);
        REQUIRE(decoded);
        CHECK(decoded.value().empty());
    }

    SECTION("Arbitrary binary data") {
        std::vector<std::byte> data;
        for (int i = 0; i < 256; ++i) {
            data.push_back(static_cast<std::byte>(i));
        }
        auto encoded = encodeBase64(data);
        CHECK(!encoded.empty());

        auto decoded = decodeBase64(encoded);
        REQUIRE(decoded);
        CHECK(decoded.value().size() == data.size());
        CHECK(decoded.value() == data);
    }

    SECTION("Malformed base64 returns error") {
        CHECK(!decodeBase64("abc"));   // invalid length
        CHECK(!decodeBase64("abc!"));  // invalid character
        CHECK(!decodeBase64("=====")); // invalid padding
    }
}

TEST_CASE("Topology binary codec roundtrip fidelity", "[topology][codec][catch2]") {
    const auto original = createSampleBatch();

    auto serialized = serializeTopologyBatchBinary(original);
    REQUIRE(serialized);
    REQUIRE(!serialized.value().empty());

    auto deserialized = deserializeTopologyBatchBinary(serialized.value());
    REQUIRE(deserialized);

    const auto& actual = deserialized.value();
    CHECK(actual.snapshotId == original.snapshotId);
    CHECK(actual.algorithm == original.algorithm);
    CHECK(actual.inputKind == original.inputKind);
    CHECK(actual.embeddingSpaceIdentity == original.embeddingSpaceIdentity);
    CHECK(actual.protectedRelationIdentity == original.protectedRelationIdentity);
    CHECK(actual.generatedAtUnixSeconds == original.generatedAtUnixSeconds);
    CHECK(actual.topologyEpoch == original.topologyEpoch);

    REQUIRE(actual.clusters.size() == original.clusters.size());
    for (size_t i = 0; i < original.clusters.size(); ++i) {
        const auto& exp = original.clusters[i];
        const auto& act = actual.clusters[i];
        CHECK(act.clusterId == exp.clusterId);
        CHECK(act.parentClusterId == exp.parentClusterId);
        CHECK(act.level == exp.level);
        CHECK(act.memberCount == exp.memberCount);
        CHECK(act.persistenceScore == exp.persistenceScore);
        CHECK(act.cohesionScore == exp.cohesionScore);
        CHECK(act.densityScore == exp.densityScore);
        CHECK(act.bridgeMass == exp.bridgeMass);
        CHECK(act.protectedPairCount == exp.protectedPairCount);
        CHECK(act.preservedProtectedPairCount == exp.preservedProtectedPairCount);
        CHECK(act.memberDocumentHashes == exp.memberDocumentHashes);
        CHECK(act.overlapClusterIds == exp.overlapClusterIds);
        CHECK(act.centroidEmbedding == exp.centroidEmbedding);

        REQUIRE(act.medoid.has_value() == exp.medoid.has_value());
        if (exp.medoid.has_value()) {
            CHECK(act.medoid->clusterId == exp.medoid->clusterId);
            CHECK(act.medoid->documentHash == exp.medoid->documentHash);
            CHECK(act.medoid->filePath == exp.medoid->filePath);
            CHECK(act.medoid->representativeScore == exp.medoid->representativeScore);
        }

        REQUIRE(act.routingRepresentatives.size() == exp.routingRepresentatives.size());
        for (size_t j = 0; j < exp.routingRepresentatives.size(); ++j) {
            CHECK(act.routingRepresentatives[j].documentHash ==
                  exp.routingRepresentatives[j].documentHash);
            CHECK(act.routingRepresentatives[j].embedding ==
                  exp.routingRepresentatives[j].embedding);
        }
    }

    REQUIRE(actual.memberships.size() == original.memberships.size());
    for (size_t i = 0; i < original.memberships.size(); ++i) {
        const auto& exp = original.memberships[i];
        const auto& act = actual.memberships[i];
        CHECK(act.documentHash == exp.documentHash);
        CHECK(act.clusterId == exp.clusterId);
        CHECK(act.parentClusterId == exp.parentClusterId);
        CHECK(act.clusterLevel == exp.clusterLevel);
        CHECK(act.persistenceScore == exp.persistenceScore);
        CHECK(act.cohesionScore == exp.cohesionScore);
        CHECK(act.bridgeScore == exp.bridgeScore);
        CHECK(act.role == exp.role);
        CHECK(act.overlapClusterIds == exp.overlapClusterIds);
    }
}

TEST_CASE("Topology compressed serialization roundtrip and compression ratio",
          "[topology][codec][catch2]") {
    const auto original = createSampleBatch();

    auto compressed = serializeTopologyBatchCompressed(original, 3);
    REQUIRE(compressed);
    const auto& payload = compressed.value();

    // Verify valid JSON envelope
    auto j = nlohmann::json::parse(payload, nullptr, false);
    REQUIRE(!j.is_discarded());
    CHECK(j["format"] == "zstd_binary_v1");
    CHECK(j["snapshot_id"] == original.snapshotId);
    CHECK(j["algorithm"] == original.algorithm);
    CHECK(j["topology_epoch"] == original.topologyEpoch);
    CHECK(j["cluster_count"] == 2);
    CHECK(j["membership_count"] == 3);
    CHECK(j.contains("data_b64"));

    // Verify deserialization recovers identical batch
    auto recovered = deserializeTopologyBatchCompressed(payload);
    REQUIRE(recovered);
    CHECK(recovered.value().snapshotId == original.snapshotId);
    CHECK(recovered.value().clusters.size() == original.clusters.size());
    CHECK(recovered.value().memberships.size() == original.memberships.size());
    CHECK(recovered.value().clusters[0].centroidEmbedding ==
          original.clusters[0].centroidEmbedding);
}

TEST_CASE("Topology codec backward compatibility with legacy raw JSON",
          "[topology][codec][catch2]") {
    nlohmann::json legacy;
    legacy["snapshot_id"] = "legacy-snapshot-001";
    legacy["algorithm"] = "connected_components_v1";
    legacy["input_kind"] = "hybrid";
    legacy["embedding_space_identity"] = "legacy-space";
    legacy["protected_relation_identity"] = "";
    legacy["generated_at_unix_seconds"] = 1700000000;
    legacy["topology_epoch"] = 1;

    nlohmann::json c;
    c["cluster_id"] = "c1";
    c["parent_cluster_id"] = nullptr;
    c["level"] = 0;
    c["member_count"] = 1;
    c["persistence_score"] = 0.5;
    c["cohesion_score"] = 0.5;
    c["density_score"] = 0.5;
    c["bridge_mass"] = 0.0;
    c["protected_pair_count"] = 0;
    c["preserved_protected_pair_count"] = 0;
    c["member_document_hashes"] = nlohmann::json::array({"hash1"});
    legacy["clusters"] = nlohmann::json::array({c});

    nlohmann::json m;
    m["document_hash"] = "hash1";
    m["cluster_id"] = "c1";
    m["parent_cluster_id"] = nullptr;
    m["cluster_level"] = 0;
    m["persistence_score"] = 0.5;
    m["cohesion_score"] = 0.5;
    m["bridge_score"] = 0.0;
    m["role"] = "core";
    legacy["memberships"] = nlohmann::json::array({m});

    std::string legacyStr = legacy.dump();

    auto deserialized = deserializeTopologyBatchCompressed(legacyStr);
    REQUIRE(deserialized);
    CHECK(deserialized.value().snapshotId == "legacy-snapshot-001");
    CHECK(deserialized.value().clusters.size() == 1);
    CHECK(deserialized.value().clusters[0].clusterId == "c1");
    CHECK(deserialized.value().memberships.size() == 1);
    CHECK(deserialized.value().memberships[0].documentHash == "hash1");
}

TEST_CASE("Topology codec robustness on corrupted payloads", "[topology][codec][catch2]") {
    SECTION("Empty payload") {
        auto res = deserializeTopologyBatchCompressed("");
        CHECK(!res);
    }

    SECTION("Invalid JSON") {
        auto res = deserializeTopologyBatchCompressed("{invalid_json");
        CHECK(!res);
    }

    SECTION("Corrupted binary buffer") {
        std::vector<std::byte> garbage = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
        auto res = deserializeTopologyBatchBinary(garbage);
        CHECK(!res);
    }

    SECTION("Compressed envelope with corrupted base64") {
        nlohmann::json j;
        j["format"] = "zstd_binary_v1";
        j["data_b64"] = "corrupted!base64";
        auto res = deserializeTopologyBatchCompressed(j.dump());
        CHECK(!res);
    }
}

namespace {

void appendU32(std::vector<std::byte>& out, uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

void appendU64(std::vector<std::byte>& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

// Valid header up to (but excluding) the string-table count.
std::vector<std::byte> binaryHeaderPrefix() {
    std::vector<std::byte> out;
    appendU32(out, 0x59414D54); // magic
    appendU32(out, 1);          // version
    out.push_back(std::byte{0});
    out.push_back(std::byte{0});
    out.push_back(std::byte{0});
    out.push_back(std::byte{0});
    appendU64(out, 0);
    appendU64(out, 0);
    for (int i = 0; i < 4; ++i) {
        appendU32(out, 0); // empty metadata strings
    }
    return out;
}

std::string envelopeFor(const nlohmann::json& overrides) {
    auto batch = createSampleBatch();
    auto compressed = serializeTopologyBatchCompressed(batch);
    REQUIRE(compressed);
    auto envelope = nlohmann::json::parse(compressed.value());
    for (auto it = overrides.begin(); it != overrides.end(); ++it) {
        envelope[it.key()] = it.value();
    }
    return envelope.dump();
}

} // namespace

TEST_CASE("Topology binary decode rejects hostile counts without throwing",
          "[topology][codec][catch2]") {
    SECTION("String table count larger than remaining bytes") {
        auto bytes = binaryHeaderPrefix();
        appendU32(bytes, 0xFFFFFFFFu);
        auto res = deserializeTopologyBatchBinary(bytes);
        CHECK(!res);
    }

    SECTION("Cluster count larger than remaining bytes") {
        auto bytes = binaryHeaderPrefix();
        appendU32(bytes, 0); // empty string table
        appendU32(bytes, 0xFFFFFFFFu);
        auto res = deserializeTopologyBatchBinary(bytes);
        CHECK(!res);
    }

    SECTION("Membership count larger than remaining bytes") {
        auto bytes = binaryHeaderPrefix();
        appendU32(bytes, 0); // empty string table
        appendU32(bytes, 0); // no clusters
        appendU32(bytes, 0xFFFFFFFFu);
        auto res = deserializeTopologyBatchBinary(bytes);
        CHECK(!res);
    }

    SECTION("Trailing bytes after a complete batch") {
        auto binary = serializeTopologyBatchBinary(createSampleBatch());
        REQUIRE(binary);
        auto bytes = binary.value();
        bytes.push_back(std::byte{0x00});
        auto res = deserializeTopologyBatchBinary(bytes);
        CHECK(!res);
    }

    SECTION("Every truncation of a valid batch fails cleanly") {
        auto binary = serializeTopologyBatchBinary(createSampleBatch());
        REQUIRE(binary);
        const auto& full = binary.value();
        for (size_t len = 0; len < full.size(); ++len) {
            auto res = deserializeTopologyBatchBinary(std::span(full.data(), len));
            CHECK(!res);
        }
    }
}

TEST_CASE("Topology compressed envelope rejects malformed fields without throwing",
          "[topology][codec][catch2]") {
    SECTION("Wrong-typed format field") {
        auto res = deserializeTopologyBatchCompressed(R"({"format": 7})");
        CHECK(!res);
    }

    SECTION("Wrong-typed compression field") {
        auto res = deserializeTopologyBatchCompressed(envelopeFor({{"compression", 1}}));
        CHECK(!res);
    }

    SECTION("Negative uncompressed_bytes") {
        auto res = deserializeTopologyBatchCompressed(envelopeFor({{"uncompressed_bytes", -1}}));
        CHECK(!res);
    }

    SECTION("String uncompressed_bytes") {
        auto res = deserializeTopologyBatchCompressed(envelopeFor({{"uncompressed_bytes", "10"}}));
        CHECK(!res);
    }

    SECTION("Absurd uncompressed_bytes") {
        auto res = deserializeTopologyBatchCompressed(
            envelopeFor({{"uncompressed_bytes", uint64_t{8} * 1024 * 1024 * 1024}}));
        CHECK(!res);
    }

    SECTION("Legacy JSON with wrong-typed fields") {
        auto res = deserializeTopologyBatchCompressed(
            R"({"snapshot_id": 3, "clusters": [{"cluster_id": []}]})");
        CHECK(!res);
    }

    SECTION("Legacy JSON with wrong-typed nested member list") {
        auto res = deserializeTopologyBatchCompressed(
            R"({"snapshot_id": "s", "clusters": [{"cluster_id": "c", "member_document_hashes": [1, 2]}]})");
        CHECK(!res);
    }
}
