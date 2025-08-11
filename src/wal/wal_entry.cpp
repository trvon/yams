#include <yams/wal/wal_entry.h>

#include <cstring>
#include <iomanip>
#include <sstream>

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

} // anonymous namespace

namespace yams::wal {

std::vector<std::byte> WALEntry::serialize() const {
    std::vector<std::byte> result;
    result.reserve(totalSize());
    
    // Copy header (update checksum first)
    auto headerCopy = header;
    headerCopy.checksum = 0;  // Zero out for calculation
    
    const auto* headerBytes = reinterpret_cast<const std::byte*>(&headerCopy);
    result.insert(result.end(), headerBytes, headerBytes + sizeof(Header));
    
    // Copy data
    result.insert(result.end(), data.begin(), data.end());
    
    // Calculate and set checksum
    uint32_t checksum = crc32(result.data(), result.size());
    auto* checksumPtr = reinterpret_cast<uint32_t*>(
        result.data() + offsetof(Header, checksum));
    *checksumPtr = checksum;
    
    return result;
}

std::optional<WALEntry> WALEntry::deserialize(std::span<const std::byte> buffer) {
    // Check minimum size
    if (buffer.size() < sizeof(Header)) {
        return std::nullopt;
    }
    
    // Read header
    WALEntry entry;
    std::memcpy(&entry.header, buffer.data(), sizeof(Header));
    
    // Validate header
    if (!entry.header.isValid()) {
        return std::nullopt;
    }
    
    // Check buffer has enough data
    if (buffer.size() < sizeof(Header) + entry.header.dataSize) {
        return std::nullopt;
    }
    
    // Read data
    entry.data.assign(
        buffer.begin() + sizeof(Header),
        buffer.begin() + sizeof(Header) + entry.header.dataSize
    );
    
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
    
    const auto* headerBytes = reinterpret_cast<const std::byte*>(&header);
    temp.insert(temp.end(), headerBytes, headerBytes + sizeof(Header));
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
    
    const auto* headerBytes = reinterpret_cast<const std::byte*>(&headerCopy);
    temp.insert(temp.end(), headerBytes, headerBytes + sizeof(Header));
    temp.insert(temp.end(), data.begin(), data.end());
    
    uint32_t calculatedChecksum = crc32(temp.data(), temp.size());
    
    return savedChecksum == calculatedChecksum;
}

// StoreBlockData implementation
std::vector<std::byte> WALEntry::StoreBlockData::encode(
    const std::string& hash, uint32_t size, uint32_t refCount) {
    
    std::vector<std::byte> result(sizeof(StoreBlockData));
    auto* data = reinterpret_cast<StoreBlockData*>(result.data());
    
    // Copy hash (truncate if needed)
    std::memset(data->hash, 0, HASH_SIZE);
    std::memcpy(data->hash, hash.data(), 
                std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));
    
    data->size = size;
    data->refCount = refCount;
    
    return result;
}

std::optional<WALEntry::StoreBlockData> WALEntry::StoreBlockData::decode(
    std::span<const std::byte> data) {
    
    if (data.size() < sizeof(StoreBlockData)) {
        return std::nullopt;
    }
    
    StoreBlockData result;
    std::memcpy(&result, data.data(), sizeof(StoreBlockData));
    return result;
}

// DeleteBlockData implementation
std::vector<std::byte> WALEntry::DeleteBlockData::encode(const std::string& hash) {
    std::vector<std::byte> result(sizeof(DeleteBlockData));
    auto* data = reinterpret_cast<DeleteBlockData*>(result.data());
    
    std::memset(data->hash, 0, HASH_SIZE);
    std::memcpy(data->hash, hash.data(), 
                std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));
    
    return result;
}

std::optional<WALEntry::DeleteBlockData> WALEntry::DeleteBlockData::decode(
    std::span<const std::byte> data) {
    
    if (data.size() < sizeof(DeleteBlockData)) {
        return std::nullopt;
    }
    
    DeleteBlockData result;
    std::memcpy(&result, data.data(), sizeof(DeleteBlockData));
    return result;
}

// UpdateReferenceData implementation
std::vector<std::byte> WALEntry::UpdateReferenceData::encode(
    const std::string& hash, int32_t delta) {
    
    std::vector<std::byte> result(sizeof(UpdateReferenceData));
    auto* data = reinterpret_cast<UpdateReferenceData*>(result.data());
    
    std::memset(data->hash, 0, HASH_SIZE);
    std::memcpy(data->hash, hash.data(), 
                std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));
    
    data->delta = delta;
    
    return result;
}

std::optional<WALEntry::UpdateReferenceData> WALEntry::UpdateReferenceData::decode(
    std::span<const std::byte> data) {
    
    if (data.size() < sizeof(UpdateReferenceData)) {
        return std::nullopt;
    }
    
    UpdateReferenceData result;
    std::memcpy(&result, data.data(), sizeof(UpdateReferenceData));
    return result;
}

// UpdateMetadataData implementation
std::vector<std::byte> WALEntry::UpdateMetadataData::encode(
    const std::string& hash, const std::string& key, const std::string& value) {
    
    size_t totalSize = sizeof(UpdateMetadataData) + key.size() + value.size();
    std::vector<std::byte> result(totalSize);
    
    auto* data = reinterpret_cast<UpdateMetadataData*>(result.data());
    
    std::memset(data->hash, 0, HASH_SIZE);
    std::memcpy(data->hash, hash.data(), 
                std::min(hash.size(), static_cast<size_t>(HASH_SIZE)));
    
    data->keySize = static_cast<uint32_t>(key.size());
    data->valueSize = static_cast<uint32_t>(value.size());
    
    // Copy key and value
    auto* keyPtr = result.data() + sizeof(UpdateMetadataData);
    std::memcpy(keyPtr, key.data(), key.size());
    
    auto* valuePtr = keyPtr + key.size();
    std::memcpy(valuePtr, value.data(), value.size());
    
    return result;
}

std::optional<WALEntry::UpdateMetadataData> WALEntry::UpdateMetadataData::decode(
    std::span<const std::byte> data) {
    
    if (data.size() < sizeof(UpdateMetadataData)) {
        return std::nullopt;
    }
    
    UpdateMetadataData result;
    std::memcpy(&result, data.data(), sizeof(UpdateMetadataData));
    
    // Verify size
    size_t expectedSize = sizeof(UpdateMetadataData) + result.keySize + result.valueSize;
    if (data.size() < expectedSize) {
        return std::nullopt;
    }
    
    return result;
}

// TransactionData implementation
std::vector<std::byte> WALEntry::TransactionData::encode(
    uint64_t txnId, uint32_t count) {
    
    std::vector<std::byte> result(sizeof(TransactionData));
    auto* data = reinterpret_cast<TransactionData*>(result.data());
    
    data->transactionId = txnId;
    data->participantCount = count;
    
    return result;
}

std::optional<WALEntry::TransactionData> WALEntry::TransactionData::decode(
    std::span<const std::byte> data) {
    
    if (data.size() < sizeof(TransactionData)) {
        return std::nullopt;
    }
    
    TransactionData result;
    std::memcpy(&result, data.data(), sizeof(TransactionData));
    return result;
}

// CheckpointData implementation
std::vector<std::byte> WALEntry::CheckpointData::encode(
    uint64_t seqNum, uint64_t timestamp) {
    
    std::vector<std::byte> result(sizeof(CheckpointData));
    auto* data = reinterpret_cast<CheckpointData*>(result.data());
    
    data->sequenceNum = seqNum;
    data->timestamp = timestamp;
    
    return result;
}

std::optional<WALEntry::CheckpointData> WALEntry::CheckpointData::decode(
    std::span<const std::byte> data) {
    
    if (data.size() < sizeof(CheckpointData)) {
        return std::nullopt;
    }
    
    CheckpointData result;
    std::memcpy(&result, data.data(), sizeof(CheckpointData));
    return result;
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
       << "seq=" << entry.header.sequenceNum
       << ", op=" << entry.header.operation
       << ", txn=" << entry.header.transactionId
       << ", size=" << entry.header.dataSize
       << ", checksum=" << std::hex << entry.header.checksum << std::dec
       << "}";
    return os;
}

} // namespace yams::wal