#include <yams/wal/wal_entry.h>

#include <array>
#include <cstring>
#include <iomanip>
#include <iterator>
#include <limits>
#include <span>
#include <sstream>
#include <stdexcept>
#include <type_traits>

// CRC32 implementation (simplified - in production use a library)
namespace {

uint32_t crc32(const void* data, size_t length) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < length; ++i) {
        crc ^= bytes[i];
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0xEDB88320 * (crc & 1));
        }
    }

    return ~crc;
}

uint32_t checkedMetadataPartSize(size_t size, const char* field) {
    if (size > std::numeric_limits<uint32_t>::max()) {
        throw std::length_error(std::string("WAL metadata ") + field +
                                " exceeds uint32 size limit");
    }
    return static_cast<uint32_t>(size);
}

template <typename T> std::span<const std::byte> objectBytes(const T& value) noexcept {
    static_assert(std::is_trivially_copyable_v<T>,
                  "WAL serialization only supports trivially-copyable POD payloads");
    return std::as_bytes(std::span<const T>(&value, 1));
}

template <typename T> void appendObjectBytes(std::vector<std::byte>& out, const T& value) {
    const auto bytes = objectBytes(value);
    out.insert(out.end(), bytes.begin(), bytes.end());
}

std::array<std::byte, yams::wal::WALEntry::Header::size()>
stableHeaderBytes(yams::wal::WALEntry::Header header) {
    // Header has natural padding on some targets. Zeroing a copy makes checksum bytes stable
    // without changing the on-disk layout expected by existing WAL files.
    yams::wal::WALEntry::Header stable{};
    stable.magic = header.magic;
    stable.version = header.version;
    stable.sequenceNum = header.sequenceNum;
    stable.timestamp = header.timestamp;
    stable.transactionId = header.transactionId;
    stable.operation = header.operation;
    stable.flags = header.flags;
    stable.reserved = header.reserved;
    stable.dataSize = header.dataSize;
    stable.checksum = header.checksum;

    std::array<std::byte, yams::wal::WALEntry::Header::size()> bytes{};
    std::memcpy(bytes.data(), &stable, sizeof(stable));
    return bytes;
}

void appendHeaderBytes(std::vector<std::byte>& out, const yams::wal::WALEntry::Header& header) {
    const auto bytes = stableHeaderBytes(header);
    out.insert(out.end(), bytes.begin(), bytes.end());
}

template <typename T> std::optional<T> readObject(std::span<const std::byte> in) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "WAL deserialization only supports trivially-copyable POD payloads");
    if (in.size() < sizeof(T)) {
        return std::nullopt;
    }
    T result{};
    std::memcpy(&result, in.data(),
                sizeof(T)); // nosemgrep: yams.cpp.memcpy-non-pod-object
                            // payload decoder with static_assert.
    return result;
}

} // anonymous namespace

namespace yams::wal {

std::vector<std::byte> WALEntry::serialize() const {
    std::vector<std::byte> result;
    result.reserve(totalSize());

    // Copy header (update checksum first)
    auto headerCopy = header;
    headerCopy.checksum = 0; // Zero out for calculation

    appendHeaderBytes(result, headerCopy);

    // Copy data
    result.insert(result.end(), data.begin(), data.end());

    // Calculate and set checksum
    uint32_t checksum = crc32(result.data(), result.size());
    std::memcpy(result.data() + offsetof(Header, checksum), &checksum, sizeof(checksum));

    return result;
}

std::optional<WALEntry> WALEntry::deserialize(std::span<const std::byte> buffer) {
    constexpr size_t headerSize = sizeof(Header);

    // Check minimum size
    if (buffer.size() < headerSize) {
        return std::nullopt;
    }

    // Read header
    WALEntry entry;
    auto header = readObject<Header>(buffer);
    if (!header) {
        return std::nullopt;
    }
    entry.header = *header;

    // Validate header
    if (!entry.header.isValid()) {
        return std::nullopt;
    }

    // Check buffer has enough data
    if (buffer.size() < headerSize + entry.header.dataSize) {
        return std::nullopt;
    }

    // Read data
    const auto dataBegin = std::next(buffer.begin(), static_cast<std::ptrdiff_t>(headerSize));
    entry.data.assign(dataBegin,
                      std::next(dataBegin, static_cast<std::ptrdiff_t>(entry.header.dataSize)));

    // Verify checksum
    if (!entry.verifyChecksum()) {
        return std::nullopt;
    }

    return entry;
}

void WALEntry::updateChecksum() {
    // Temporarily zero checksum
    header.checksum = 0;

    // Calculate checksum over header and data
    std::vector<std::byte> temp;
    temp.reserve(totalSize());

    appendHeaderBytes(temp, header);
    temp.insert(temp.end(), data.begin(), data.end());

    header.checksum = crc32(temp.data(), temp.size());
}

bool WALEntry::verifyChecksum() const {
    // Save current checksum
    uint32_t savedChecksum = header.checksum;

    // Zero it out for calculation
    auto headerCopy = header;
    headerCopy.checksum = 0;

    // Calculate checksum
    std::vector<std::byte> temp;
    temp.reserve(totalSize());

    appendHeaderBytes(temp, headerCopy);
    temp.insert(temp.end(), data.begin(), data.end());

    uint32_t calculatedChecksum = crc32(temp.data(), temp.size());

    if (savedChecksum == calculatedChecksum) {
        return true;
    }

    // Older WAL entries were checksummed over raw Header bytes, including padding.
    // Keep recovery compatible while all new writes use stable zero-padded bytes.
    temp.clear();
    appendObjectBytes(temp, headerCopy);
    temp.insert(temp.end(), data.begin(), data.end());
    return savedChecksum == crc32(temp.data(), temp.size());
}

// StoreBlockData implementation
std::vector<std::byte> WALEntry::StoreBlockData::encode(const std::string& hash, uint32_t size,
                                                        uint32_t refCount) {
    StoreBlockData data{};

    // Copy hash (truncate if needed)
    std::memset(data.hash, 0, HASH_SIZE);
    std::memcpy(data.hash, hash.data(), std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));

    data.size = size;
    data.refCount = refCount;

    std::vector<std::byte> result;
    result.reserve(sizeof(StoreBlockData));
    appendObjectBytes(result, data);
    return result;
}

std::optional<WALEntry::StoreBlockData>
WALEntry::StoreBlockData::decode(std::span<const std::byte> data) {
    if (data.size() < sizeof(StoreBlockData)) {
        return std::nullopt;
    }

    return readObject<StoreBlockData>(data);
}

// DeleteBlockData implementation
std::vector<std::byte> WALEntry::DeleteBlockData::encode(const std::string& hash) {
    DeleteBlockData data{};

    std::memset(data.hash, 0, HASH_SIZE);
    std::memcpy(data.hash, hash.data(), std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));

    std::vector<std::byte> result;
    result.reserve(sizeof(DeleteBlockData));
    appendObjectBytes(result, data);
    return result;
}

std::optional<WALEntry::DeleteBlockData>
WALEntry::DeleteBlockData::decode(std::span<const std::byte> data) {
    if (data.size() < sizeof(DeleteBlockData)) {
        return std::nullopt;
    }

    return readObject<DeleteBlockData>(data);
}

// UpdateReferenceData implementation
std::vector<std::byte> WALEntry::UpdateReferenceData::encode(const std::string& hash,
                                                             int32_t delta) {
    UpdateReferenceData data{};

    std::memset(data.hash, 0, HASH_SIZE);
    std::memcpy(data.hash, hash.data(), std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));

    data.delta = delta;

    std::vector<std::byte> result;
    result.reserve(sizeof(UpdateReferenceData));
    appendObjectBytes(result, data);
    return result;
}

std::optional<WALEntry::UpdateReferenceData>
WALEntry::UpdateReferenceData::decode(std::span<const std::byte> data) {
    if (data.size() < sizeof(UpdateReferenceData)) {
        return std::nullopt;
    }

    return readObject<UpdateReferenceData>(data);
}

// UpdateMetadataData implementation
std::vector<std::byte> WALEntry::UpdateMetadataData::encode(const std::string& hash,
                                                            const std::string& key,
                                                            const std::string& value) {
    UpdateMetadataData data{};

    std::memset(data.hash, 0, HASH_SIZE);
    std::memcpy(data.hash, hash.data(), std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));

    data.keySize = checkedMetadataPartSize(key.size(), "key");
    data.valueSize = checkedMetadataPartSize(value.size(), "value");

    std::vector<std::byte> result;
    result.reserve(sizeof(UpdateMetadataData) + key.size() + value.size());
    appendObjectBytes(result, data);

    // Copy key and value
    const auto* keyPtr = reinterpret_cast<const std::byte*>(key.data());
    result.insert(result.end(), keyPtr, keyPtr + key.size());

    const auto* valuePtr = reinterpret_cast<const std::byte*>(value.data());
    result.insert(result.end(), valuePtr, valuePtr + value.size());

    return result;
}

std::optional<WALEntry::UpdateMetadataData>
WALEntry::UpdateMetadataData::decode(std::span<const std::byte> data) {
    if (data.size() < sizeof(UpdateMetadataData)) {
        return std::nullopt;
    }

    auto result = readObject<UpdateMetadataData>(data);
    if (!result) {
        return std::nullopt;
    }

    // Verify size
    size_t expectedSize = sizeof(UpdateMetadataData) + result->keySize + result->valueSize;
    if (data.size() < expectedSize) {
        return std::nullopt;
    }

    return result;
}

// TransactionData implementation
std::vector<std::byte> WALEntry::TransactionData::encode(uint64_t txnId, uint32_t count) {
    TransactionData data{};
    data.transactionId = txnId;
    data.participantCount = count;

    std::vector<std::byte> result;
    result.reserve(sizeof(TransactionData));
    appendObjectBytes(result, data);
    return result;
}

std::optional<WALEntry::TransactionData>
WALEntry::TransactionData::decode(std::span<const std::byte> data) {
    if (data.size() < sizeof(TransactionData)) {
        return std::nullopt;
    }

    return readObject<TransactionData>(data);
}

// CheckpointData implementation
std::vector<std::byte> WALEntry::CheckpointData::encode(uint64_t seqNum, uint64_t timestamp) {
    CheckpointData data{};
    data.sequenceNum = seqNum;
    data.timestamp = timestamp;

    std::vector<std::byte> result;
    result.reserve(sizeof(CheckpointData));
    appendObjectBytes(result, data);
    return result;
}

std::optional<WALEntry::CheckpointData>
WALEntry::CheckpointData::decode(std::span<const std::byte> data) {
    if (data.size() < sizeof(CheckpointData)) {
        return std::nullopt;
    }

    return readObject<CheckpointData>(data);
}

// Stream operators
std::ostream& operator<<(std::ostream& os, WALEntry::OpType op) {
    switch (op) {
        case WALEntry::OpType::BeginTransaction:
            return os << "BeginTransaction";
        case WALEntry::OpType::StoreBlock:
            return os << "StoreBlock";
        case WALEntry::OpType::DeleteBlock:
            return os << "DeleteBlock";
        case WALEntry::OpType::UpdateReference:
            return os << "UpdateReference";
        case WALEntry::OpType::UpdateMetadata:
            return os << "UpdateMetadata";
        case WALEntry::OpType::CommitTransaction:
            return os << "CommitTransaction";
        case WALEntry::OpType::Rollback:
            return os << "Rollback";
        case WALEntry::OpType::Checkpoint:
            return os << "Checkpoint";
        default:
            return os << "Unknown(" << static_cast<int>(op) << ")";
    }
}

std::ostream& operator<<(std::ostream& os, const WALEntry& entry) {
    os << "WALEntry{"
       << "seq=" << entry.header.sequenceNum << ", op=" << entry.header.operation
       << ", txn=" << entry.header.transactionId << ", size=" << entry.header.dataSize
       << ", checksum=" << std::hex << entry.header.checksum << std::dec << "}";
    return os;
}

} // namespace yams::wal
