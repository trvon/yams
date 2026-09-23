// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/profiling.h>
#include <yams/topology/topology_codec.h>

#include <nlohmann/json.hpp>
#include <yams/compression/compressor_interface.h>
#include <yams/core/assert.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace yams::topology {

namespace {

using json = nlohmann::json;

inline uint32_t toU32(size_t n) {
    YAMS_ASSERT(n <= std::numeric_limits<uint32_t>::max(), "size exceeds uint32_t range");
    return static_cast<uint32_t>(n);
}

constexpr uint32_t kTopologyBinaryMagic = 0x59414D54; // 'Y','A','M','T'
constexpr uint32_t kTopologyBinaryVersion = 1;
constexpr uint32_t kNullStringId = 0xFFFFFFFF;

// Lower bounds on encoded record sizes, used to validate counts before reserving.
constexpr size_t kMinStringBytes = 4;    // length prefix
constexpr size_t kMinStringRefBytes = 4; // string-table index
constexpr size_t kMinFloatBytes = 4;
constexpr size_t kMinRoutingRepBytes = 4 + 4; // hash index + embedding dim
constexpr size_t kMinClusterBytes = 4 * 4 + 8 * 4 + 4 * 2 + 1 + 4 * 4;
constexpr size_t kMinMembershipBytes = 4 * 4 + 8 * 3 + 1 + 4;

// Upper bound on a decompressed topology snapshot; also bounded by a compression-ratio cap.
constexpr size_t kMaxSnapshotUncompressedBytes = size_t{1} << 30; // 1 GiB
constexpr size_t kMaxSnapshotCompressionRatio = 1000;

[[maybe_unused]] const char* roleToString(DocumentTopologyRole role) {
    switch (role) {
        case DocumentTopologyRole::Core:
            return "core";
        case DocumentTopologyRole::Bridge:
            return "bridge";
        case DocumentTopologyRole::Medoid:
            return "medoid";
        case DocumentTopologyRole::Outlier:
            return "outlier";
    }
    return "core";
}

DocumentTopologyRole roleFromString(std::string_view value) {
    if (value == "bridge") {
        return DocumentTopologyRole::Bridge;
    }
    if (value == "medoid") {
        return DocumentTopologyRole::Medoid;
    }
    if (value == "outlier") {
        return DocumentTopologyRole::Outlier;
    }
    return DocumentTopologyRole::Core;
}

[[maybe_unused]] const char* inputKindToString(TopologyInputKind kind) {
    switch (kind) {
        case TopologyInputKind::SemanticNeighborGraph:
            return "semantic_neighbor_graph";
        case TopologyInputKind::EmbeddingNeighborhood:
            return "embedding_neighborhood";
        case TopologyInputKind::Hybrid:
            return "hybrid";
    }
    return "hybrid";
}

TopologyInputKind inputKindFromString(std::string_view value) {
    if (value == "semantic_neighbor_graph") {
        return TopologyInputKind::SemanticNeighborGraph;
    }
    if (value == "embedding_neighborhood") {
        return TopologyInputKind::EmbeddingNeighborhood;
    }
    return TopologyInputKind::Hybrid;
}

DocumentClusterMembership membershipFromJson(const json& j) {
    DocumentClusterMembership membership;
    membership.documentHash = j.value("document_hash", "");
    membership.clusterId = j.value("cluster_id", "");
    if (j.contains("parent_cluster_id") && !j["parent_cluster_id"].is_null()) {
        membership.parentClusterId = j["parent_cluster_id"].get<std::string>();
    }
    membership.clusterLevel = j.value("cluster_level", std::size_t{0});
    membership.persistenceScore = j.value("persistence_score", 0.0);
    membership.cohesionScore = j.value("cohesion_score", 0.0);
    membership.bridgeScore = j.value("bridge_score", 0.0);
    membership.role = roleFromString(j.value("role", std::string{"core"}));
    if (j.contains("overlap_cluster_ids") && j["overlap_cluster_ids"].is_array()) {
        membership.overlapClusterIds = j["overlap_cluster_ids"].get<std::vector<std::string>>();
    }
    return membership;
}

ClusterRepresentative representativeFromJson(const json& j) {
    ClusterRepresentative representative;
    representative.clusterId = j.value("cluster_id", "");
    representative.documentHash = j.value("document_hash", "");
    representative.filePath = j.value("file_path", "");
    representative.representativeScore = j.value("representative_score", 0.0);
    return representative;
}

ClusterRoutingRepresentative routingRepresentativeFromJson(const json& j) {
    ClusterRoutingRepresentative representative;
    representative.documentHash = j.value("document_hash", "");
    if (j.contains("embedding") && j["embedding"].is_array()) {
        representative.embedding = j["embedding"].get<std::vector<float>>();
    }
    return representative;
}

ClusterArtifact clusterFromJson(const json& j) {
    ClusterArtifact cluster;
    cluster.clusterId = j.value("cluster_id", "");
    if (j.contains("parent_cluster_id") && !j["parent_cluster_id"].is_null()) {
        cluster.parentClusterId = j["parent_cluster_id"].get<std::string>();
    }
    cluster.level = j.value("level", std::size_t{0});
    cluster.memberCount = j.value("member_count", std::size_t{0});
    cluster.persistenceScore = j.value("persistence_score", 0.0);
    cluster.cohesionScore = j.value("cohesion_score", 0.0);
    cluster.densityScore = j.value("density_score", 0.0);
    cluster.bridgeMass = j.value("bridge_mass", 0.0);
    cluster.protectedPairCount = j.value("protected_pair_count", std::size_t{0});
    cluster.preservedProtectedPairCount = j.value("preserved_protected_pair_count", std::size_t{0});
    if (j.contains("medoid") && j["medoid"].is_object()) {
        cluster.medoid = representativeFromJson(j["medoid"]);
    }
    if (j.contains("member_document_hashes") && j["member_document_hashes"].is_array()) {
        cluster.memberDocumentHashes = j["member_document_hashes"].get<std::vector<std::string>>();
    }
    if (j.contains("overlap_cluster_ids") && j["overlap_cluster_ids"].is_array()) {
        cluster.overlapClusterIds = j["overlap_cluster_ids"].get<std::vector<std::string>>();
    }
    if (j.contains("centroid_embedding") && j["centroid_embedding"].is_array()) {
        cluster.centroidEmbedding = j["centroid_embedding"].get<std::vector<float>>();
    }
    if (j.contains("routing_representatives") && j["routing_representatives"].is_array()) {
        for (const auto& representative : j["routing_representatives"]) {
            if (representative.is_object()) {
                cluster.routingRepresentatives.push_back(
                    routingRepresentativeFromJson(representative));
            }
        }
    }
    return cluster;
}

Result<TopologyArtifactBatch> legacyBatchFromJson(const json& j) {
    if (!j.is_object()) {
        return Error{ErrorCode::InvalidData, "topology batch JSON must be an object"};
    }
    TopologyArtifactBatch batch;
    batch.snapshotId = j.value("snapshot_id", "");
    batch.algorithm = j.value("algorithm", "");
    batch.inputKind = inputKindFromString(j.value("input_kind", std::string{"hybrid"}));
    batch.embeddingSpaceIdentity = j.value("embedding_space_identity", "");
    batch.protectedRelationIdentity = j.value("protected_relation_identity", "");
    batch.generatedAtUnixSeconds = j.value("generated_at_unix_seconds", uint64_t{0});
    batch.topologyEpoch = j.value("topology_epoch", uint64_t{0});
    if (j.contains("clusters") && j["clusters"].is_array()) {
        for (const auto& clusterJson : j["clusters"]) {
            batch.clusters.push_back(clusterFromJson(clusterJson));
        }
    }
    if (j.contains("memberships") && j["memberships"].is_array()) {
        for (const auto& membershipJson : j["memberships"]) {
            batch.memberships.push_back(membershipFromJson(membershipJson));
        }
    }
    return batch;
}

class ByteWriter {
public:
    void writeU8(uint8_t v) { buf_.push_back(static_cast<std::byte>(v)); }

    void writeU16(uint16_t v) {
        buf_.push_back(static_cast<std::byte>(v & 0xFF));
        buf_.push_back(static_cast<std::byte>((v >> 8) & 0xFF));
    }

    void writeU32(uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            buf_.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    }

    void writeU64(uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            buf_.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    }

    void writeFloat(float v) {
        uint32_t raw = 0;
        std::memcpy(&raw, &v, sizeof(v));
        writeU32(raw);
    }

    void writeDouble(double v) {
        uint64_t raw = 0;
        std::memcpy(&raw, &v, sizeof(v));
        writeU64(raw);
    }

    void writeString(std::string_view s) {
        writeU32(toU32(s.size()));
        const auto* ptr = reinterpret_cast<const std::byte*>(s.data());
        buf_.insert(buf_.end(), ptr, ptr + s.size());
    }

    std::vector<std::byte> take() { return std::move(buf_); }
    void reserve(size_t n) { buf_.reserve(n); }

private:
    std::vector<std::byte> buf_;
};

class ByteReader {
public:
    explicit ByteReader(std::span<const std::byte> data) : data_(data), pos_(0) {}

    bool hasRemaining(size_t bytes) const { return bytes <= data_.size() - pos_; }
    bool atEnd() const { return pos_ == data_.size(); }

    // Reads an element count and rejects counts that cannot fit in the remaining bytes, so
    // corrupt payloads cannot drive reserve() into huge allocations.
    Result<uint32_t> readCount(size_t minElementBytes, std::string_view what) {
        auto count = readU32();
        if (!count) {
            return count.error();
        }
        const size_t remaining = data_.size() - pos_;
        if (minElementBytes > 0 && count.value() > remaining / minElementBytes) {
            return Error{ErrorCode::InvalidData, "Topology binary " + std::string(what) +
                                                     " count exceeds remaining payload"};
        }
        return count;
    }

    Result<uint8_t> readU8() {
        if (!hasRemaining(1)) {
            return Error{ErrorCode::InvalidData, "Truncated U8 in topology binary"};
        }
        return static_cast<uint8_t>(data_[pos_++]);
    }

    Result<uint16_t> readU16() {
        if (!hasRemaining(2)) {
            return Error{ErrorCode::InvalidData, "Truncated U16 in topology binary"};
        }
        uint16_t v = static_cast<uint8_t>(data_[pos_]) |
                     (static_cast<uint16_t>(static_cast<uint8_t>(data_[pos_ + 1])) << 8);
        pos_ += 2;
        return v;
    }

    Result<uint32_t> readU32() {
        if (!hasRemaining(4)) {
            return Error{ErrorCode::InvalidData, "Truncated U32 in topology binary"};
        }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v |= (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + i])) << (i * 8));
        }
        pos_ += 4;
        return v;
    }

    Result<uint64_t> readU64() {
        if (!hasRemaining(8)) {
            return Error{ErrorCode::InvalidData, "Truncated U64 in topology binary"};
        }
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= (static_cast<uint64_t>(static_cast<uint8_t>(data_[pos_ + i])) << (i * 8));
        }
        pos_ += 8;
        return v;
    }

    Result<float> readFloat() {
        auto u = readU32();
        if (!u) {
            return u.error();
        }
        float f = 0.0f;
        auto raw = u.value();
        std::memcpy(&f, &raw, sizeof(f));
        return f;
    }

    Result<double> readDouble() {
        auto u = readU64();
        if (!u) {
            return u.error();
        }
        double d = 0.0;
        auto raw = u.value();
        std::memcpy(&d, &raw, sizeof(d));
        return d;
    }

    Result<std::string> readString() {
        auto len = readU32();
        if (!len) {
            return len.error();
        }
        if (!hasRemaining(len.value())) {
            return Error{ErrorCode::InvalidData, "Truncated string in topology binary"};
        }
        std::string s(reinterpret_cast<const char*>(data_.data() + pos_), len.value());
        pos_ += len.value();
        return s;
    }

private:
    std::span<const std::byte> data_;
    size_t pos_;
};

} // namespace

std::string encodeBase64(std::span<const std::byte> bytes) {
    static constexpr char kBase64Chars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    const size_t len = bytes.size();
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        uint32_t n = static_cast<uint32_t>(bytes[i]) << 16;
        if (i + 1 < len) {
            n |= static_cast<uint32_t>(bytes[i + 1]) << 8;
        }
        if (i + 2 < len) {
            n |= static_cast<uint32_t>(bytes[i + 2]);
        }

        out += kBase64Chars[(n >> 18) & 0x3F];
        out += kBase64Chars[(n >> 12) & 0x3F];
        out += (i + 1 < len) ? kBase64Chars[(n >> 6) & 0x3F] : '=';
        out += (i + 2 < len) ? kBase64Chars[n & 0x3F] : '=';
    }
    return out;
}

Result<std::vector<std::byte>> decodeBase64(std::string_view s) {
    std::string clean;
    clean.reserve(s.size());
    for (char c : s) {
        if (!std::isspace(static_cast<unsigned char>(c))) {
            clean.push_back(c);
        }
    }
    if (clean.empty()) {
        return std::vector<std::byte>{};
    }
    if (clean.size() % 4 != 0) {
        return Error{ErrorCode::InvalidData, "Invalid base64 length: not a multiple of 4"};
    }

    auto decodeChar = [](char c) -> int {
        if (c >= 'A' && c <= 'Z')
            return c - 'A';
        if (c >= 'a' && c <= 'z')
            return c - 'a' + 26;
        if (c >= '0' && c <= '9')
            return c - '0' + 52;
        if (c == '+')
            return 62;
        if (c == '/')
            return 63;
        if (c == '=')
            return -2; // padding
        return -1;     // invalid
    };

    std::vector<std::byte> out;
    out.reserve((clean.size() / 4) * 3);

    for (size_t i = 0; i < clean.size(); i += 4) {
        int c0 = decodeChar(clean[i]);
        int c1 = decodeChar(clean[i + 1]);
        int c2 = decodeChar(clean[i + 2]);
        int c3 = decodeChar(clean[i + 3]);

        if (c0 < 0 || c1 < 0) {
            return Error{ErrorCode::InvalidData, "Invalid base64 character in chunk"};
        }

        uint32_t n = (static_cast<uint32_t>(c0) << 18) | (static_cast<uint32_t>(c1) << 12);
        out.push_back(static_cast<std::byte>((n >> 16) & 0xFF));

        if (c2 >= 0) {
            n |= (static_cast<uint32_t>(c2) << 6);
            out.push_back(static_cast<std::byte>((n >> 8) & 0xFF));
            if (c3 >= 0) {
                n |= static_cast<uint32_t>(c3);
                out.push_back(static_cast<std::byte>(n & 0xFF));
            } else if (c3 != -2) {
                return Error{ErrorCode::InvalidData, "Invalid base64 character in 4th byte"};
            }
        } else if (c2 == -2) {
            if (c3 != -2) {
                return Error{ErrorCode::InvalidData, "Invalid base64 padding sequence"};
            }
        } else {
            return Error{ErrorCode::InvalidData, "Invalid base64 character in 3rd byte"};
        }
    }
    return out;
}

Result<std::vector<std::byte>> serializeTopologyBatchBinary(const TopologyArtifactBatch& batch) {
    YAMS_ZONE_SCOPED_N("topology::codec::serializeBinary");
    ByteWriter writer;
    writer.reserve(1024 + batch.clusters.size() * 128 + batch.memberships.size() * 64);

    // 1. Header
    writer.writeU32(kTopologyBinaryMagic);
    writer.writeU32(kTopologyBinaryVersion);
    writer.writeU8(static_cast<uint8_t>(batch.inputKind));
    writer.writeU8(0); // Reserved padding
    writer.writeU8(0);
    writer.writeU8(0);
    writer.writeU64(batch.generatedAtUnixSeconds);
    writer.writeU64(batch.topologyEpoch);

    // 2. Batch-level metadata strings
    writer.writeString(batch.snapshotId);
    writer.writeString(batch.algorithm);
    writer.writeString(batch.embeddingSpaceIdentity);
    writer.writeString(batch.protectedRelationIdentity);

    // 3. String Table (interning repeated hashes and cluster IDs)
    std::unordered_map<std::string, uint32_t> stringToId;
    std::vector<std::string_view> idToString;

    auto internString = [&](const std::string& s) -> uint32_t {
        if (auto it = stringToId.find(s); it != stringToId.end()) {
            return it->second;
        }
        uint32_t id = toU32(idToString.size());
        stringToId.emplace(s, id);
        idToString.push_back(s);
        return id;
    };

    auto internOptionalString = [&](const std::optional<std::string>& opt) -> uint32_t {
        if (!opt.has_value()) {
            return kNullStringId;
        }
        return internString(*opt);
    };

    // Pre-populate string table from clusters and memberships
    for (const auto& cluster : batch.clusters) {
        internString(cluster.clusterId);
        internOptionalString(cluster.parentClusterId);
        if (cluster.medoid.has_value()) {
            internString(cluster.medoid->clusterId);
            internString(cluster.medoid->documentHash);
            internString(cluster.medoid->filePath);
        }
        for (const auto& hash : cluster.memberDocumentHashes) {
            internString(hash);
        }
        for (const auto& overlapId : cluster.overlapClusterIds) {
            internString(overlapId);
        }
        for (const auto& routingRep : cluster.routingRepresentatives) {
            internString(routingRep.documentHash);
        }
    }

    for (const auto& membership : batch.memberships) {
        internString(membership.documentHash);
        internString(membership.clusterId);
        internOptionalString(membership.parentClusterId);
        for (const auto& overlapId : membership.overlapClusterIds) {
            internString(overlapId);
        }
    }

    // Write string table
    writer.writeU32(toU32(idToString.size()));
    for (const auto& s : idToString) {
        writer.writeString(s);
    }

    // 4. Clusters
    writer.writeU32(toU32(batch.clusters.size()));
    for (const auto& cluster : batch.clusters) {
        writer.writeU32(internString(cluster.clusterId));
        writer.writeU32(internOptionalString(cluster.parentClusterId));
        writer.writeU32(toU32(cluster.level));
        writer.writeU32(toU32(cluster.memberCount));
        writer.writeDouble(cluster.persistenceScore);
        writer.writeDouble(cluster.cohesionScore);
        writer.writeDouble(cluster.densityScore);
        writer.writeDouble(cluster.bridgeMass);
        writer.writeU32(toU32(cluster.protectedPairCount));
        writer.writeU32(toU32(cluster.preservedProtectedPairCount));

        if (cluster.medoid.has_value()) {
            writer.writeU8(1);
            writer.writeU32(internString(cluster.medoid->clusterId));
            writer.writeU32(internString(cluster.medoid->documentHash));
            writer.writeU32(internString(cluster.medoid->filePath));
            writer.writeDouble(cluster.medoid->representativeScore);
        } else {
            writer.writeU8(0);
        }

        writer.writeU32(toU32(cluster.memberDocumentHashes.size()));
        for (const auto& hash : cluster.memberDocumentHashes) {
            writer.writeU32(internString(hash));
        }

        writer.writeU32(toU32(cluster.overlapClusterIds.size()));
        for (const auto& overlapId : cluster.overlapClusterIds) {
            writer.writeU32(internString(overlapId));
        }

        writer.writeU32(toU32(cluster.centroidEmbedding.size()));
        for (float val : cluster.centroidEmbedding) {
            writer.writeFloat(val);
        }

        writer.writeU32(toU32(cluster.routingRepresentatives.size()));
        for (const auto& rep : cluster.routingRepresentatives) {
            writer.writeU32(internString(rep.documentHash));
            writer.writeU32(toU32(rep.embedding.size()));
            for (float val : rep.embedding) {
                writer.writeFloat(val);
            }
        }
    }

    // 5. Memberships
    writer.writeU32(toU32(batch.memberships.size()));
    for (const auto& membership : batch.memberships) {
        writer.writeU32(internString(membership.documentHash));
        writer.writeU32(internString(membership.clusterId));
        writer.writeU32(internOptionalString(membership.parentClusterId));
        writer.writeU32(toU32(membership.clusterLevel));
        writer.writeDouble(membership.persistenceScore);
        writer.writeDouble(membership.cohesionScore);
        writer.writeDouble(membership.bridgeScore);
        writer.writeU8(static_cast<uint8_t>(membership.role));

        writer.writeU32(toU32(membership.overlapClusterIds.size()));
        for (const auto& overlapId : membership.overlapClusterIds) {
            writer.writeU32(internString(overlapId));
        }
    }

    return writer.take();
}

Result<TopologyArtifactBatch> deserializeTopologyBatchBinary(std::span<const std::byte> bytes) {
    YAMS_ZONE_SCOPED_N("topology::codec::deserializeBinary");
    ByteReader reader(bytes);

    // 1. Header
    auto magic = reader.readU32();
    if (!magic) {
        return magic.error();
    }
    if (magic.value() != kTopologyBinaryMagic) {
        return Error{ErrorCode::InvalidData, "Invalid magic in topology binary"};
    }

    auto version = reader.readU32();
    if (!version) {
        return version.error();
    }
    if (version.value() != kTopologyBinaryVersion) {
        return Error{ErrorCode::InvalidData,
                     "Unsupported topology binary version: " + std::to_string(version.value())};
    }

    auto inputKindRaw = reader.readU8();
    if (!inputKindRaw) {
        return inputKindRaw.error();
    }
    auto pad0 = reader.readU8();
    auto pad1 = reader.readU8();
    auto pad2 = reader.readU8();
    if (!pad0 || !pad1 || !pad2) {
        return Error{ErrorCode::InvalidData, "Truncated header padding"};
    }

    auto genAt = reader.readU64();
    if (!genAt) {
        return genAt.error();
    }
    auto epoch = reader.readU64();
    if (!epoch) {
        return epoch.error();
    }

    TopologyArtifactBatch batch;
    batch.inputKind = static_cast<TopologyInputKind>(inputKindRaw.value());
    batch.generatedAtUnixSeconds = genAt.value();
    batch.topologyEpoch = epoch.value();

    // 2. Metadata strings
    auto snapId = reader.readString();
    if (!snapId) {
        return snapId.error();
    }
    batch.snapshotId = std::move(snapId.value());

    auto algo = reader.readString();
    if (!algo) {
        return algo.error();
    }
    batch.algorithm = std::move(algo.value());

    auto embSpace = reader.readString();
    if (!embSpace) {
        return embSpace.error();
    }
    batch.embeddingSpaceIdentity = std::move(embSpace.value());

    auto protRel = reader.readString();
    if (!protRel) {
        return protRel.error();
    }
    batch.protectedRelationIdentity = std::move(protRel.value());

    // 3. String Table
    auto stringCountRes = reader.readCount(kMinStringBytes, "string table");
    if (!stringCountRes) {
        return stringCountRes.error();
    }
    const uint32_t stringCount = stringCountRes.value();
    std::vector<std::string> stringTable;
    stringTable.reserve(stringCount);
    for (uint32_t i = 0; i < stringCount; ++i) {
        auto strRes = reader.readString();
        if (!strRes) {
            return strRes.error();
        }
        stringTable.push_back(std::move(strRes.value()));
    }

    auto resolveString = [&](uint32_t idx) -> Result<const std::string*> {
        if (idx >= stringTable.size()) {
            return Error{ErrorCode::InvalidData,
                         "String table index out of bounds: " + std::to_string(idx)};
        }
        return &stringTable[idx];
    };

    auto resolveOptionalString = [&](uint32_t idx) -> Result<std::optional<std::string>> {
        if (idx == kNullStringId) {
            return std::optional<std::string>{std::nullopt};
        }
        if (idx >= stringTable.size()) {
            return Error{ErrorCode::InvalidData,
                         "String table index out of bounds: " + std::to_string(idx)};
        }
        return std::optional<std::string>{stringTable[idx]};
    };

    // 4. Clusters
    auto clusterCountRes = reader.readCount(kMinClusterBytes, "cluster");
    if (!clusterCountRes) {
        return clusterCountRes.error();
    }
    const uint32_t clusterCount = clusterCountRes.value();
    batch.clusters.reserve(clusterCount);

    for (uint32_t i = 0; i < clusterCount; ++i) {
        ClusterArtifact cluster;

        auto cIdIdx = reader.readU32();
        if (!cIdIdx)
            return cIdIdx.error();
        auto cIdStr = resolveString(cIdIdx.value());
        if (!cIdStr)
            return cIdStr.error();
        cluster.clusterId = *cIdStr.value();

        auto pIdIdx = reader.readU32();
        if (!pIdIdx)
            return pIdIdx.error();
        auto pIdStr = resolveOptionalString(pIdIdx.value());
        if (!pIdStr)
            return pIdStr.error();
        cluster.parentClusterId = std::move(pIdStr.value());

        auto level = reader.readU32();
        if (!level)
            return level.error();
        cluster.level = level.value();

        auto memberCount = reader.readU32();
        if (!memberCount)
            return memberCount.error();
        cluster.memberCount = memberCount.value();

        auto persistence = reader.readDouble();
        if (!persistence)
            return persistence.error();
        cluster.persistenceScore = persistence.value();

        auto cohesion = reader.readDouble();
        if (!cohesion)
            return cohesion.error();
        cluster.cohesionScore = cohesion.value();

        auto density = reader.readDouble();
        if (!density)
            return density.error();
        cluster.densityScore = density.value();

        auto bridgeMass = reader.readDouble();
        if (!bridgeMass)
            return bridgeMass.error();
        cluster.bridgeMass = bridgeMass.value();

        auto protectedPairCount = reader.readU32();
        if (!protectedPairCount)
            return protectedPairCount.error();
        cluster.protectedPairCount = protectedPairCount.value();

        auto preservedProtectedPairCount = reader.readU32();
        if (!preservedProtectedPairCount)
            return preservedProtectedPairCount.error();
        cluster.preservedProtectedPairCount = preservedProtectedPairCount.value();

        auto hasMedoid = reader.readU8();
        if (!hasMedoid)
            return hasMedoid.error();
        if (hasMedoid.value() == 1) {
            ClusterRepresentative medoid;
            auto mcId = reader.readU32();
            if (!mcId)
                return mcId.error();
            auto mcStr = resolveString(mcId.value());
            if (!mcStr)
                return mcStr.error();
            medoid.clusterId = *mcStr.value();

            auto mdHash = reader.readU32();
            if (!mdHash)
                return mdHash.error();
            auto mdStr = resolveString(mdHash.value());
            if (!mdStr)
                return mdStr.error();
            medoid.documentHash = *mdStr.value();

            auto mfPath = reader.readU32();
            if (!mfPath)
                return mfPath.error();
            auto mfStr = resolveString(mfPath.value());
            if (!mfStr)
                return mfStr.error();
            medoid.filePath = *mfStr.value();

            auto mScore = reader.readDouble();
            if (!mScore)
                return mScore.error();
            medoid.representativeScore = mScore.value();

            cluster.medoid = std::move(medoid);
        }

        auto memberHashCount = reader.readCount(kMinStringRefBytes, "member hash");
        if (!memberHashCount)
            return memberHashCount.error();
        cluster.memberDocumentHashes.reserve(memberHashCount.value());
        for (uint32_t j = 0; j < memberHashCount.value(); ++j) {
            auto hIdx = reader.readU32();
            if (!hIdx)
                return hIdx.error();
            auto hStr = resolveString(hIdx.value());
            if (!hStr)
                return hStr.error();
            cluster.memberDocumentHashes.push_back(*hStr.value());
        }

        auto overlapCount = reader.readCount(kMinStringRefBytes, "overlap cluster");
        if (!overlapCount)
            return overlapCount.error();
        cluster.overlapClusterIds.reserve(overlapCount.value());
        for (uint32_t j = 0; j < overlapCount.value(); ++j) {
            auto oIdx = reader.readU32();
            if (!oIdx)
                return oIdx.error();
            auto oStr = resolveString(oIdx.value());
            if (!oStr)
                return oStr.error();
            cluster.overlapClusterIds.push_back(*oStr.value());
        }

        auto centroidDim = reader.readCount(kMinFloatBytes, "centroid dimension");
        if (!centroidDim)
            return centroidDim.error();
        cluster.centroidEmbedding.reserve(centroidDim.value());
        for (uint32_t j = 0; j < centroidDim.value(); ++j) {
            auto f = reader.readFloat();
            if (!f)
                return f.error();
            cluster.centroidEmbedding.push_back(f.value());
        }

        auto routingRepCount = reader.readCount(kMinRoutingRepBytes, "routing representative");
        if (!routingRepCount)
            return routingRepCount.error();
        cluster.routingRepresentatives.reserve(routingRepCount.value());
        for (uint32_t j = 0; j < routingRepCount.value(); ++j) {
            ClusterRoutingRepresentative rep;
            auto rHash = reader.readU32();
            if (!rHash)
                return rHash.error();
            auto rStr = resolveString(rHash.value());
            if (!rStr)
                return rStr.error();
            rep.documentHash = *rStr.value();

            auto rDim = reader.readCount(kMinFloatBytes, "routing embedding dimension");
            if (!rDim)
                return rDim.error();
            rep.embedding.reserve(rDim.value());
            for (uint32_t k = 0; k < rDim.value(); ++k) {
                auto rf = reader.readFloat();
                if (!rf)
                    return rf.error();
                rep.embedding.push_back(rf.value());
            }
            cluster.routingRepresentatives.push_back(std::move(rep));
        }

        batch.clusters.push_back(std::move(cluster));
    }

    // 5. Memberships
    auto membershipCountRes = reader.readCount(kMinMembershipBytes, "membership");
    if (!membershipCountRes) {
        return membershipCountRes.error();
    }
    const uint32_t membershipCount = membershipCountRes.value();
    batch.memberships.reserve(membershipCount);

    for (uint32_t i = 0; i < membershipCount; ++i) {
        DocumentClusterMembership membership;

        auto dHashIdx = reader.readU32();
        if (!dHashIdx)
            return dHashIdx.error();
        auto dHashStr = resolveString(dHashIdx.value());
        if (!dHashStr)
            return dHashStr.error();
        membership.documentHash = *dHashStr.value();

        auto cIdIdx = reader.readU32();
        if (!cIdIdx)
            return cIdIdx.error();
        auto cIdStr = resolveString(cIdIdx.value());
        if (!cIdStr)
            return cIdStr.error();
        membership.clusterId = *cIdStr.value();

        auto pIdIdx = reader.readU32();
        if (!pIdIdx)
            return pIdIdx.error();
        auto pIdStr = resolveOptionalString(pIdIdx.value());
        if (!pIdStr)
            return pIdStr.error();
        membership.parentClusterId = std::move(pIdStr.value());

        auto clusterLevel = reader.readU32();
        if (!clusterLevel)
            return clusterLevel.error();
        membership.clusterLevel = clusterLevel.value();

        auto persistence = reader.readDouble();
        if (!persistence)
            return persistence.error();
        membership.persistenceScore = persistence.value();

        auto cohesion = reader.readDouble();
        if (!cohesion)
            return cohesion.error();
        membership.cohesionScore = cohesion.value();

        auto bridge = reader.readDouble();
        if (!bridge)
            return bridge.error();
        membership.bridgeScore = bridge.value();

        auto role = reader.readU8();
        if (!role)
            return role.error();
        membership.role = static_cast<DocumentTopologyRole>(role.value());

        auto overlapCount = reader.readCount(kMinStringRefBytes, "overlap cluster");
        if (!overlapCount)
            return overlapCount.error();
        membership.overlapClusterIds.reserve(overlapCount.value());
        for (uint32_t j = 0; j < overlapCount.value(); ++j) {
            auto oIdx = reader.readU32();
            if (!oIdx)
                return oIdx.error();
            auto oStr = resolveString(oIdx.value());
            if (!oStr)
                return oStr.error();
            membership.overlapClusterIds.push_back(*oStr.value());
        }

        batch.memberships.push_back(std::move(membership));
    }

    if (!reader.atEnd()) {
        return Error{ErrorCode::InvalidData, "Trailing bytes after topology binary payload"};
    }
    return batch;
}

Result<std::string> serializeTopologyBatchCompressed(const TopologyArtifactBatch& batch,
                                                     int compressionLevel) {
    YAMS_ZONE_SCOPED_N("topology::codec::serializeCompressed");
    auto binaryRes = serializeTopologyBatchBinary(batch);
    if (!binaryRes) {
        return binaryRes.error();
    }
    const auto& binaryBytes = binaryRes.value();

    auto compressor = compression::CompressionRegistry::instance().createCompressor(
        compression::CompressionAlgorithm::Zstandard);
    if (!compressor) {
        return Error{ErrorCode::InternalError, "Failed to create Zstandard compressor"};
    }

    auto compRes = compressor->compress(binaryBytes, static_cast<uint8_t>(compressionLevel));
    if (!compRes) {
        return compRes.error();
    }
    const auto& compResult = compRes.value();
    const bool isZstd = (compResult.algorithm == compression::CompressionAlgorithm::Zstandard);
    std::string b64 = encodeBase64(compResult.data);

    json envelope;
    envelope["format"] = "zstd_binary_v1";
    envelope["compression"] = isZstd ? "zstd" : "none";
    envelope["snapshot_id"] = batch.snapshotId;
    envelope["algorithm"] = batch.algorithm;
    envelope["topology_epoch"] = batch.topologyEpoch;
    envelope["cluster_count"] = batch.clusters.size();
    envelope["membership_count"] = batch.memberships.size();
    envelope["uncompressed_bytes"] = binaryBytes.size();
    envelope["compressed_bytes"] = compResult.data.size();
    envelope["generated_at_unix_seconds"] = batch.generatedAtUnixSeconds;
    envelope["data_b64"] = std::move(b64);

    return envelope.dump();
}

namespace {

// Typed envelope accessors: a present field of the wrong type is corruption, not a default.
Result<std::string> envelopeString(const json& envelope, const char* key, std::string fallback) {
    auto it = envelope.find(key);
    if (it == envelope.end()) {
        return fallback;
    }
    if (!it->is_string()) {
        return Error{ErrorCode::InvalidData, std::string("Topology snapshot envelope field '") +
                                                 key + "' must be a string"};
    }
    return it->get<std::string>();
}

Result<size_t> envelopeUnsigned(const json& envelope, const char* key) {
    auto it = envelope.find(key);
    if (it == envelope.end()) {
        return size_t{0};
    }
    if (!it->is_number_unsigned()) {
        return Error{ErrorCode::InvalidData, std::string("Topology snapshot envelope field '") +
                                                 key + "' must be an unsigned integer"};
    }
    const auto value = it->get<uint64_t>();
    if (value > std::numeric_limits<size_t>::max()) {
        return Error{ErrorCode::InvalidData,
                     std::string("Topology snapshot envelope field '") + key + "' is too large"};
    }
    return static_cast<size_t>(value);
}

bool startsWithTopologyMagic(std::span<const std::byte> bytes) {
    if (bytes.size() < 4) {
        return false;
    }
    uint32_t magic = 0;
    for (int i = 0; i < 4; ++i) {
        magic |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[i])) << (i * 8);
    }
    return magic == kTopologyBinaryMagic;
}

Result<TopologyArtifactBatch> deserializeCompressedEnvelope(const json& envelope) {
    auto b64 = envelopeString(envelope, "data_b64", "");
    if (!b64) {
        return b64.error();
    }
    if (b64.value().empty()) {
        return Error{ErrorCode::InvalidData, "Compressed topology snapshot missing data_b64"};
    }
    auto compAlgo = envelopeString(envelope, "compression", "zstd");
    if (!compAlgo) {
        return compAlgo.error();
    }
    auto expectedSize = envelopeUnsigned(envelope, "uncompressed_bytes");
    if (!expectedSize) {
        return expectedSize.error();
    }

    auto decodedBytes = decodeBase64(b64.value());
    if (!decodedBytes) {
        return decodedBytes.error();
    }
    const auto& compressed = decodedBytes.value();

    // Data stored uncompressed (tiny batch where the compressor opted out of a zstd frame) is
    // deserialized directly.
    if (compAlgo.value() == "none" || startsWithTopologyMagic(compressed)) {
        return deserializeTopologyBatchBinary(compressed);
    }
    if (compAlgo.value() != "zstd") {
        return Error{ErrorCode::InvalidData,
                     "Unsupported topology snapshot compression: " + compAlgo.value()};
    }

    // The envelope's size hint is untrusted: bound it before it sizes the output buffer.
    const size_t maxByRatio =
        compressed.size() > kMaxSnapshotUncompressedBytes / kMaxSnapshotCompressionRatio
            ? kMaxSnapshotUncompressedBytes
            : compressed.size() * kMaxSnapshotCompressionRatio;
    if (expectedSize.value() > std::min(kMaxSnapshotUncompressedBytes, maxByRatio)) {
        return Error{ErrorCode::InvalidData,
                     "Topology snapshot uncompressed_bytes exceeds the allowed bound"};
    }

    auto compressor = compression::CompressionRegistry::instance().createCompressor(
        compression::CompressionAlgorithm::Zstandard);
    if (!compressor) {
        return Error{ErrorCode::InternalError, "Failed to create Zstandard compressor"};
    }
    auto decompressed = compressor->decompress(compressed, expectedSize.value());
    if (!decompressed) {
        return decompressed.error();
    }
    if (expectedSize.value() != 0 && decompressed.value().size() != expectedSize.value()) {
        return Error{ErrorCode::InvalidData,
                     "Topology snapshot decompressed size does not match uncompressed_bytes"};
    }
    return deserializeTopologyBatchBinary(decompressed.value());
}

} // namespace

Result<TopologyArtifactBatch> deserializeTopologyBatchCompressed(std::string_view payload) {
    YAMS_ZONE_SCOPED_N("topology::codec::deserializeCompressed");
    if (payload.empty()) {
        return Error{ErrorCode::InvalidArgument, "Empty topology snapshot payload"};
    }

    auto parsed = json::parse(payload, nullptr, false);
    if (parsed.is_discarded()) {
        return Error{ErrorCode::SerializationError, "Failed to parse topology snapshot JSON"};
    }

    if (parsed.is_object() && parsed.contains("format")) {
        auto format = envelopeString(parsed, "format", "");
        if (!format) {
            return format.error();
        }
        if (format.value() == "zstd_binary_v1") {
            return deserializeCompressedEnvelope(parsed);
        }
    }

    // Fallback: legacy uncompressed JSON. Its field accessors throw on wrong-typed values.
    try {
        return legacyBatchFromJson(parsed);
    } catch (const json::exception& e) {
        return Error{ErrorCode::InvalidData,
                     std::string("Malformed legacy topology snapshot JSON: ") + e.what()};
    }
}

} // namespace yams::topology
