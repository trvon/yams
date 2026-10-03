#include <yams/common/crc32.h>
#include <yams/wal/wal_entry.h>

#include <array>
#include <cstddef>
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
    return yams::common::crc32(
        std::span<const std::byte>(static_cast<const std::byte*>(data), length));
}

uint32_t checkedMetadataPartSize(size_t size, const char* field) {
    if (size > std::numeric_limits<uint32_t>::max()) {
        throw std::length_error(std::string("WAL metadata ") + field +
                                " exceeds uint32 size limit");
    }
    return static_cast<uint32_t>(size);
}

using Header = yams::wal::WALEntry::Header;
using HeaderBytes = std::array<std::byte, Header::size()>;
using TransactionData = yams::wal::WALEntry::TransactionData;

// On-disk v1 entry header: the in-memory Header layout on LP64/LLP64 targets, host byte order.
// Bytes 44..47 are alignment padding and are always written as zero. Header and payload
// encoding goes field by field so padding contents never reach disk or the checksum; the
// assertions pin the layout so a struct change cannot silently change the file format.
constexpr size_t kHeaderMagicOffset = 0;
constexpr size_t kHeaderVersionOffset = 4;
constexpr size_t kHeaderSequenceOffset = 8;
constexpr size_t kHeaderTimestampOffset = 16;
constexpr size_t kHeaderTransactionOffset = 24;
constexpr size_t kHeaderOperationOffset = 32;
constexpr size_t kHeaderFlagsOffset = 33;
constexpr size_t kHeaderReservedOffset = 34;
constexpr size_t kHeaderDataSizeOffset = 36;
constexpr size_t kHeaderChecksumOffset = 40;
constexpr size_t kHeaderEncodedSize = 48;

static_assert(std::is_standard_layout_v<Header> && std::is_trivially_copyable_v<Header>);
static_assert(Header::size() == kHeaderEncodedSize);
static_assert(offsetof(Header, magic) == kHeaderMagicOffset && sizeof(Header::magic) == 4);
static_assert(offsetof(Header, version) == kHeaderVersionOffset && sizeof(Header::version) == 4);
static_assert(offsetof(Header, sequenceNum) == kHeaderSequenceOffset &&
              sizeof(Header::sequenceNum) == 8);
static_assert(offsetof(Header, timestamp) == kHeaderTimestampOffset &&
              sizeof(Header::timestamp) == 8);
static_assert(offsetof(Header, transactionId) == kHeaderTransactionOffset &&
              sizeof(Header::transactionId) == 8);
static_assert(offsetof(Header, operation) == kHeaderOperationOffset &&
              sizeof(Header::operation) == 1);
static_assert(offsetof(Header, flags) == kHeaderFlagsOffset && sizeof(Header::flags) == 1);
static_assert(offsetof(Header, reserved) == kHeaderReservedOffset && sizeof(Header::reserved) == 2);
static_assert(offsetof(Header, dataSize) == kHeaderDataSizeOffset && sizeof(Header::dataSize) == 4);
static_assert(offsetof(Header, checksum) == kHeaderChecksumOffset && sizeof(Header::checksum) == 4);

// TransactionData: uint64 + uint32 followed by 4 bytes of padding, encoded as 16 bytes.
constexpr size_t kTxnIdOffset = 0;
constexpr size_t kTxnCountOffset = 8;
constexpr size_t kTxnEncodedSize = 16;
static_assert(std::is_standard_layout_v<TransactionData>);
static_assert(sizeof(TransactionData) == kTxnEncodedSize);
static_assert(offsetof(TransactionData, transactionId) == kTxnIdOffset &&
              sizeof(TransactionData::transactionId) == 8);
static_assert(offsetof(TransactionData, participantCount) == kTxnCountOffset &&
              sizeof(TransactionData::participantCount) == 4);

template <typename T> void putScalar(std::span<std::byte> out, size_t offset, T value) noexcept {
    static_assert(std::is_scalar_v<T>);
    std::memcpy(out.data() + offset, &value, sizeof(T));
}

template <typename T> T getScalar(std::span<const std::byte> in, size_t offset) noexcept {
    static_assert(std::is_scalar_v<T>);
    T value;
    std::memcpy(&value, in.data() + offset, sizeof(T));
    return value;
}

HeaderBytes encodeHeader(const Header& header, uint32_t checksum) noexcept {
    HeaderBytes out{};
    putScalar(out, kHeaderMagicOffset, header.magic);
    putScalar(out, kHeaderVersionOffset, header.version);
    putScalar(out, kHeaderSequenceOffset, header.sequenceNum);
    putScalar(out, kHeaderTimestampOffset, header.timestamp);
    putScalar(out, kHeaderTransactionOffset, header.transactionId);
    putScalar(out, kHeaderOperationOffset, header.operation);
    putScalar(out, kHeaderFlagsOffset, header.flags);
    putScalar(out, kHeaderReservedOffset, header.reserved);
    putScalar(out, kHeaderDataSizeOffset, header.dataSize);
    putScalar(out, kHeaderChecksumOffset, checksum);
    return out;
}

// Precondition: in.size() >= Header::size().
Header decodeHeaderBytes(std::span<const std::byte> in) noexcept {
    Header header{};
    header.magic = getScalar<uint32_t>(in, kHeaderMagicOffset);
    header.version = getScalar<uint32_t>(in, kHeaderVersionOffset);
    header.sequenceNum = getScalar<uint64_t>(in, kHeaderSequenceOffset);
    header.timestamp = getScalar<uint64_t>(in, kHeaderTimestampOffset);
    header.transactionId = getScalar<uint64_t>(in, kHeaderTransactionOffset);
    header.operation = getScalar<yams::wal::WALEntry::OpType>(in, kHeaderOperationOffset);
    header.flags = getScalar<uint8_t>(in, kHeaderFlagsOffset);
    header.reserved = getScalar<uint16_t>(in, kHeaderReservedOffset);
    header.dataSize = getScalar<uint32_t>(in, kHeaderDataSizeOffset);
    header.checksum = getScalar<uint32_t>(in, kHeaderChecksumOffset);
    return header;
}

// CRC32 over the encoded header (checksum field zero, padding zero) followed by the payload.
uint32_t entryChecksum(const Header& header, std::span<const std::byte> data) {
    const auto headerBytes = encodeHeader(header, 0);
    std::vector<std::byte> temp;
    temp.reserve(headerBytes.size() + data.size());
    temp.insert(temp.end(), headerBytes.begin(), headerBytes.end());
    temp.insert(temp.end(), data.begin(), data.end());
    return crc32(temp.data(), temp.size());
}

// Payload structs other than TransactionData are copied whole; they must have no padding.
template <typename T> void appendObjectBytes(std::vector<std::byte>& out, const T& value) {
    static_assert(std::is_trivially_copyable_v<T> && std::has_unique_object_representations_v<T>,
                  "WAL payloads copied as raw bytes must be trivially copyable without padding");
    const auto bytes = std::as_bytes(std::span<const T>(&value, 1));
    out.insert(out.end(), bytes.begin(), bytes.end());
}

template <typename T> std::optional<T> readObject(std::span<const std::byte> in) {
    static_assert(std::is_trivially_copyable_v<T> && std::has_unique_object_representations_v<T>,
                  "WAL payloads read as raw bytes must be trivially copyable without padding");
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

std::optional<WALEntry::Header> WALEntry::decodeHeader(std::span<const std::byte> buffer) {
    if (buffer.size() < Header::size()) {
        return std::nullopt;
    }
    return decodeHeaderBytes(buffer);
}

std::vector<std::byte> WALEntry::serialize() const {
    std::vector<std::byte> result;
    result.reserve(totalSize());

    const auto headerBytes = encodeHeader(header, entryChecksum(header, data));
    result.insert(result.end(), headerBytes.begin(), headerBytes.end());
    result.insert(result.end(), data.begin(), data.end());
    return result;
}

std::optional<WALEntry> WALEntry::deserialize(std::span<const std::byte> buffer) {
    constexpr size_t headerSize = Header::size();

    auto header = decodeHeader(buffer);
    if (!header || !header->isValid()) {
        return std::nullopt;
    }

    if (buffer.size() - headerSize < header->dataSize) {
        return std::nullopt;
    }

    WALEntry entry;
    entry.header = *header;
    const auto dataBegin = std::next(buffer.begin(), static_cast<std::ptrdiff_t>(headerSize));
    const auto dataEnd = std::next(dataBegin, static_cast<std::ptrdiff_t>(entry.header.dataSize));
    entry.data.assign(dataBegin, dataEnd);

    if (entry.verifyChecksum()) {
        return entry;
    }

    // Builds whose compiler left the header padding uninitialized (e.g. GCC) wrote those
    // bytes to disk and checksummed them. Accept such an entry when the CRC over the bytes as
    // stored (checksum field zeroed) matches, then normalize the in-memory checksum to the
    // canonical encoding so verifyChecksum() holds for every returned entry.
    std::vector<std::byte> stored(buffer.begin(), dataEnd);
    putScalar(std::span<std::byte>(stored), kHeaderChecksumOffset, uint32_t{0});
    if (crc32(stored.data(), stored.size()) != entry.header.checksum) {
        return std::nullopt;
    }
    entry.updateChecksum();
    return entry;
}

void WALEntry::updateChecksum() {
    header.checksum = entryChecksum(header, data);
}

bool WALEntry::verifyChecksum() const {
    return header.checksum == entryChecksum(header, data);
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

    const size_t metadataHeaderSize = sizeof(UpdateMetadataData);
    std::vector<std::byte> result;
    result.reserve(metadataHeaderSize + key.size() + value.size());
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
    const size_t metadataHeaderSize = sizeof(UpdateMetadataData);
    size_t expectedSize = metadataHeaderSize + result->keySize + result->valueSize;
    if (data.size() < expectedSize) {
        return std::nullopt;
    }

    return result;
}

// TransactionData implementation
std::vector<std::byte> WALEntry::TransactionData::encode(uint64_t txnId, uint32_t count) {
    std::vector<std::byte> result(kTxnEncodedSize, std::byte{0});
    putScalar(std::span<std::byte>(result), kTxnIdOffset, txnId);
    putScalar(std::span<std::byte>(result), kTxnCountOffset, count);
    return result;
}

std::optional<WALEntry::TransactionData>
WALEntry::TransactionData::decode(std::span<const std::byte> data) {
    if (data.size() < kTxnEncodedSize) {
        return std::nullopt;
    }

    TransactionData result{};
    result.transactionId = getScalar<uint64_t>(data, kTxnIdOffset);
    result.participantCount = getScalar<uint32_t>(data, kTxnCountOffset);
    return result;
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
