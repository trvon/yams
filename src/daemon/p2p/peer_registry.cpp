// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/peer_registry.h>

// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>

#include <limits>
#include <utility>

namespace yams::daemon::p2p {

namespace {

constexpr std::string_view kCreatePeerRegistry = R"sql(
CREATE TABLE IF NOT EXISTS p2p_peers (
    node_id TEXT PRIMARY KEY NOT NULL,
    spki_hash TEXT NOT NULL CHECK(length(spki_hash) = 64),
    corpus_id TEXT NOT NULL DEFAULT '',
    corpus_epoch INTEGER NOT NULL DEFAULT 0,
    last_seen_vv TEXT NOT NULL DEFAULT '{"counters_":{}}',
    last_connected_ms INTEGER NOT NULL DEFAULT 0,
    pinned_by_operator INTEGER NOT NULL DEFAULT 0 CHECK(pinned_by_operator IN (0, 1))
)
)sql";

Result<std::string> serializeVersion(const memory_sync::VersionVector& version) {
    try {
        return nlohmann::json(version).dump();
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError, error.what()};
    }
}

Result<memory_sync::VersionVector> deserializeVersion(std::string_view encoded) {
    try {
        return nlohmann::json::parse(encoded).get<memory_sync::VersionVector>();
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError,
                     std::string("invalid persisted peer version: ") + error.what()};
    }
}

Result<metadata::Statement> prepareNodeStatement(metadata::Database& database, std::string_view sql,
                                                 std::string_view nodeId) {
    auto prepared = database.prepare(std::string(sql));
    if (!prepared) {
        return prepared.error();
    }
    auto statement = std::move(prepared.value());
    if (auto bound = statement.bind(1, nodeId); !bound) {
        return bound.error();
    }
    return statement;
}

} // namespace

PeerRegistry::PeerRegistry(std::unique_ptr<metadata::Database> database, std::size_t maxPeers)
    : database_(std::move(database)), maxPeers_(maxPeers) {}

PeerRegistry::~PeerRegistry() = default;

Result<std::unique_ptr<PeerRegistry>> PeerRegistry::open(const std::filesystem::path& databasePath,
                                                         std::size_t maxPeers) {
    if (databasePath.empty() || maxPeers == 0) {
        return Error{ErrorCode::InvalidArgument,
                     "peer registry requires a database path and positive capacity"};
    }
    auto database = std::make_unique<metadata::Database>();
    if (auto opened = database->open(databasePath.string(), metadata::ConnectionMode::Create);
        !opened) {
        return opened.error();
    }
    auto registry = std::unique_ptr<PeerRegistry>(new PeerRegistry(std::move(database), maxPeers));
    if (auto initialized = registry->initializeSchema(); !initialized) {
        return initialized.error();
    }
    return registry;
}

Result<void> PeerRegistry::initializeSchema() {
    std::lock_guard<std::mutex> lock(mutex_);
    return database_->execute(std::string(kCreatePeerRegistry));
}

Result<std::optional<std::string>> PeerRegistry::findPinLocked(std::string_view nodeId) const {
    auto prepared = prepareNodeStatement(
        *database_, "SELECT spki_hash FROM p2p_peers WHERE node_id = ?", nodeId);
    if (!prepared) {
        return prepared.error();
    }
    auto statement = std::move(prepared.value());
    auto row = statement.step();
    if (!row) {
        return row.error();
    }
    return row.value() ? std::optional<std::string>{statement.getString(0)}
                       : std::optional<std::string>{};
}

Result<std::size_t> PeerRegistry::peerCountLocked() const {
    auto prepared = database_->prepare("SELECT COUNT(*) FROM p2p_peers");
    if (!prepared) {
        return prepared.error();
    }
    auto statement = std::move(prepared.value());
    auto row = statement.step();
    if (!row) {
        return row.error();
    }
    if (!row.value()) {
        return Error{ErrorCode::DatabaseError, "peer registry count returned no row"};
    }
    const auto count = statement.getInt64(0);
    if (count < 0) {
        return Error{ErrorCode::DatabaseError, "peer registry count is negative"};
    }
    return static_cast<std::size_t>(count);
}

Result<PeerTrustDecision> PeerRegistry::verifyOrPin(std::string_view nodeId,
                                                    std::string_view spkiPin,
                                                    bool allowFirstContact) {
    if (nodeId.empty()) {
        return Error{ErrorCode::InvalidArgument, "p2p trust node id is empty"};
    }
    auto normalized = normalizePeerSpkiPin(spkiPin);
    if (!normalized) {
        return normalized.error();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = findPinLocked(nodeId);
    if (!existing) {
        return existing.error();
    }
    auto decision = evaluatePeerPin(existing.value(), normalized.value(), allowFirstContact);
    if (!decision || !decision.value().firstContactPinned) {
        return decision;
    }
    auto count = peerCountLocked();
    if (!count) {
        return count.error();
    }
    if (count.value() >= maxPeers_) {
        return Error{ErrorCode::ResourceExhausted, "p2p peer registry capacity reached"};
    }
    auto prepared = database_->prepare("INSERT INTO p2p_peers(node_id, spki_hash) VALUES(?, ?)");
    if (!prepared) {
        return prepared.error();
    }
    auto statement = std::move(prepared.value());
    if (auto bound = statement.bindAll(nodeId, normalized.value()); !bound) {
        return bound.error();
    }
    if (auto inserted = statement.execute(); !inserted) {
        return inserted.error();
    }
    return PeerTrustDecision{.firstContactPinned = true};
}

Result<void> PeerRegistry::updatePeerState(std::string_view nodeId, std::string_view corpusId,
                                           std::uint64_t corpusEpoch,
                                           const memory_sync::VersionVector& lastSeenVersion,
                                           std::int64_t lastConnectedMs, bool pinnedByOperator) {
    if (nodeId.empty() || corpusId.empty() || corpusEpoch == 0 ||
        corpusEpoch > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        lastConnectedMs < 0) {
        return Error{ErrorCode::InvalidArgument, "invalid persisted p2p peer state"};
    }
    auto encodedVersion = serializeVersion(lastSeenVersion);
    if (!encodedVersion) {
        return encodedVersion.error();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = findPinLocked(nodeId);
    if (!existing) {
        return existing.error();
    }
    if (!existing.value()) {
        return Error{ErrorCode::NotFound, "p2p peer is not pinned"};
    }
    auto prepared = database_->prepare(R"sql(
UPDATE p2p_peers
SET corpus_id = ?, corpus_epoch = ?, last_seen_vv = ?, last_connected_ms = ?,
    pinned_by_operator = MAX(pinned_by_operator, ?)
WHERE node_id = ?
)sql");
    if (!prepared) {
        return prepared.error();
    }
    auto statement = std::move(prepared.value());
    if (auto bound = statement.bindAll(corpusId, static_cast<std::int64_t>(corpusEpoch),
                                       encodedVersion.value(), lastConnectedMs,
                                       pinnedByOperator ? 1 : 0, nodeId);
        !bound) {
        return bound.error();
    }
    return statement.execute();
}

Result<std::vector<PeerRegistryRecord>> PeerRegistry::listPeers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto prepared = database_->prepare(R"sql(
SELECT node_id, spki_hash, corpus_id, corpus_epoch, last_seen_vv,
       last_connected_ms, pinned_by_operator
FROM p2p_peers
ORDER BY node_id
)sql");
    if (!prepared) {
        return prepared.error();
    }
    auto statement = std::move(prepared.value());
    std::vector<PeerRegistryRecord> peers;
    for (;;) {
        auto row = statement.step();
        if (!row) {
            return row.error();
        }
        if (!row.value()) {
            return peers;
        }
        auto version = deserializeVersion(statement.getString(4));
        if (!version) {
            return version.error();
        }
        const auto epoch = statement.getInt64(3);
        if (epoch < 0) {
            return Error{ErrorCode::DataCorruption, "persisted peer epoch is negative"};
        }
        peers.push_back(PeerRegistryRecord{.nodeId = statement.getString(0),
                                           .spkiPin = statement.getString(1),
                                           .corpusId = statement.getString(2),
                                           .corpusEpoch = static_cast<std::uint64_t>(epoch),
                                           .lastSeenVersion = std::move(version.value()),
                                           .lastConnectedMs = statement.getInt64(5),
                                           .pinnedByOperator = statement.getInt(6) != 0});
    }
}

Result<void> PeerRegistry::removePeer(std::string_view nodeId) {
    if (nodeId.empty()) {
        return Error{ErrorCode::InvalidArgument, "p2p peer node id is empty"};
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto prepared =
        prepareNodeStatement(*database_, "DELETE FROM p2p_peers WHERE node_id = ?", nodeId);
    if (!prepared) {
        return prepared.error();
    }
    return prepared.value().execute();
}

} // namespace yams::daemon::p2p
