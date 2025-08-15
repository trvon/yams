#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace yams::wal {

/**
 * WAL Entry structure for write-ahead logging
 *
 * Each entry represents an operation that modifies storage state.
 * Entries are written to the log before being applied to ensure durability.
 */
struct WALEntry {
    // Operation types
    enum class OpType : uint8_t {
        BeginTransaction = 1,
        StoreBlock = 2,
        DeleteBlock = 3,
        UpdateReference = 4,
        UpdateMetadata = 5,
        CommitTransaction = 6,
        Rollback = 7,
        Checkpoint = 8
    };

    // WAL entry header with fixed size for efficient reading
    struct Header {
        uint32_t magic = 0x57414C31; // "WAL1" in hex
        uint32_t version = 1;        // Format version
        uint64_t sequenceNum;        // Monotonic sequence number
        uint64_t timestamp;          // Microseconds since epoch
        uint64_t transactionId;      // Transaction this entry belongs to
        OpType operation;            // Operation type
        uint8_t flags = 0;           // Reserved flags
        uint16_t reserved = 0;       // Reserved for alignment
        uint32_t dataSize;           // Size of payload data
        uint32_t checksum;           // CRC32 of header + data

        // Validate header integrity
        [[nodiscard]] bool isValid() const noexcept {
            return magic == 0x57414C31 && version == 1 && sequenceNum > 0 &&
                   static_cast<uint8_t>(operation) >= 1 && static_cast<uint8_t>(operation) <= 8;
        }

        // Calculate header size
        static constexpr size_t size() noexcept { return sizeof(Header); }
    };

    // Entry data
    Header header;
    std::vector<std::byte> data;

    // Constructors
    WALEntry() = default;

    WALEntry(OpType op, uint64_t seqNum, uint64_t txnId, std::span<const std::byte> payload = {})
        : header{.sequenceNum = seqNum,
                 .timestamp =
                     static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                               std::chrono::system_clock::now().time_since_epoch())
                                               .count()),
                 .transactionId = txnId,
                 .operation = op,
                 .dataSize = static_cast<uint32_t>(payload.size())},
          data(payload.begin(), payload.end()) {
        updateChecksum();
    }

    // Serialization
    [[nodiscard]] std::vector<std::byte> serialize() const;

    // Deserialization
    [[nodiscard]] static std::optional<WALEntry> deserialize(std::span<const std::byte> buffer);

    // Checksum calculation
    void updateChecksum();
    [[nodiscard]] bool verifyChecksum() const;

    // Helpers
    [[nodiscard]] bool isTransactional() const noexcept {
        return header.operation == OpType::BeginTransaction ||
               header.operation == OpType::CommitTransaction ||
               header.operation == OpType::Rollback;
    }

    [[nodiscard]] size_t totalSize() const noexcept { return Header::size() + data.size(); }

    // Operation-specific data structures
    struct StoreBlockData {
        char hash[HASH_SIZE]; // Content hash
        uint32_t size;        // Block size
        uint32_t refCount;    // Initial reference count

        [[nodiscard]] static std::vector<std::byte> encode(const std::string& hash, uint32_t size,
                                                           uint32_t refCount = 1);
        [[nodiscard]] static std::optional<StoreBlockData> decode(std::span<const std::byte> data);
    };

    struct DeleteBlockData {
        char hash[HASH_SIZE]; // Content hash to delete

        [[nodiscard]] static std::vector<std::byte> encode(const std::string& hash);
        [[nodiscard]] static std::optional<DeleteBlockData> decode(std::span<const std::byte> data);
    };

    struct UpdateReferenceData {
        char hash[HASH_SIZE]; // Content hash
        int32_t delta;        // Reference count change (+/-)

        [[nodiscard]] static std::vector<std::byte> encode(const std::string& hash, int32_t delta);
        [[nodiscard]] static std::optional<UpdateReferenceData>
        decode(std::span<const std::byte> data);
    };

    struct UpdateMetadataData {
        char hash[HASH_SIZE]; // Content hash
        uint32_t keySize;     // Metadata key size
        uint32_t valueSize;   // Metadata value size
        // Followed by key and value data

        [[nodiscard]] static std::vector<std::byte>
        encode(const std::string& hash, const std::string& key, const std::string& value);
        [[nodiscard]] static std::optional<UpdateMetadataData>
        decode(std::span<const std::byte> data);
    };

    struct TransactionData {
        uint64_t transactionId;
        uint32_t participantCount; // Number of operations in transaction

        [[nodiscard]] static std::vector<std::byte> encode(uint64_t txnId, uint32_t count = 0);
        [[nodiscard]] static std::optional<TransactionData> decode(std::span<const std::byte> data);
    };

    struct CheckpointData {
        uint64_t sequenceNum; // Last applied sequence number
        uint64_t timestamp;   // Checkpoint timestamp

        [[nodiscard]] static std::vector<std::byte> encode(uint64_t seqNum, uint64_t timestamp);
        [[nodiscard]] static std::optional<CheckpointData> decode(std::span<const std::byte> data);
    };
};

// Stream operators for logging
std::ostream& operator<<(std::ostream& os, WALEntry::OpType op);
std::ostream& operator<<(std::ostream& os, const WALEntry& entry);

} // namespace yams::wal