#include <catch2/catch_test_macros.hpp>
#include <yams/wal/wal_entry.h>
#include <cstring>
#include <sstream>

using namespace yams::wal;

TEST_CASE("WALEntry::Header::isValid accepts OpTypes 1-8", "[wal][header][catch2]") {
    WALEntry::Header h{};
    h.sequenceNum = 1;
    h.version = 1;

    CHECK(WALEntry::Header::size() == sizeof(WALEntry::Header));

    for (uint8_t opv = 1; opv <= 8; ++opv) {
        h.operation = static_cast<WALEntry::OpType>(opv);
        CHECK(h.isValid());
    }
}

TEST_CASE("WALEntry::Header::isValid rejects bad magic", "[wal][header][catch2]") {
    WALEntry::Header h{};
    h.sequenceNum = 1;
    h.version = 1;
    h.operation = WALEntry::OpType::StoreBlock;

    h.magic = 0;
    CHECK_FALSE(h.isValid());
}

TEST_CASE("WALEntry::Header::isValid rejects bad version (not 1 or 2)", "[wal][header][catch2]") {
    WALEntry::Header h{};
    h.sequenceNum = 1;
    h.operation = WALEntry::OpType::StoreBlock;

    h.version = 0;
    CHECK_FALSE(h.isValid());
    h.version = 3;
    CHECK_FALSE(h.isValid());
}

TEST_CASE("WALEntry::Header::isValid accepts version 2 (dual-read compat)",
          "[wal][header][catch2]") {
    WALEntry::Header h{};
    h.sequenceNum = 1;
    h.version = 2;
    h.operation = WALEntry::OpType::StoreBlock;

    CHECK(h.isValid());
}

TEST_CASE("WALEntry::Header::isValid rejects zero sequenceNum", "[wal][header][catch2]") {
    WALEntry::Header h{};
    h.sequenceNum = 0;
    h.version = 1;
    h.operation = WALEntry::OpType::StoreBlock;

    CHECK_FALSE(h.isValid());
}

TEST_CASE("WALEntry::Header::isValid rejects out-of-range OpType (0 and 9)",
          "[wal][header][catch2]") {
    WALEntry::Header h{};
    h.sequenceNum = 1;
    h.version = 1;

    h.operation = static_cast<WALEntry::OpType>(0);
    CHECK_FALSE(h.isValid());

    h.operation = static_cast<WALEntry::OpType>(9);
    CHECK_FALSE(h.isValid());
}

TEST_CASE("WALEntry constructor sets header fields", "[wal][ctor][catch2]") {
    std::vector<std::byte> payload;
    payload.resize(3);
    payload[0] = std::byte{0x01};
    payload[1] = std::byte{0x02};
    payload[2] = std::byte{0x03};

    const auto op = WALEntry::OpType::UpdateReference;
    const uint64_t seq = 42;
    const uint64_t txn = 99;
    WALEntry e(op, seq, txn, std::span<const std::byte>(payload.data(), payload.size()));

    CHECK(e.header.magic == 0x57414C31);
    CHECK(e.header.version == 1);
    CHECK(e.header.sequenceNum == seq);
    CHECK(e.header.transactionId == txn);
    CHECK(e.header.operation == op);
    CHECK(e.header.flags == 0);
    CHECK(e.header.reserved == 0);
    CHECK(e.header.dataSize == static_cast<uint32_t>(payload.size()));
    CHECK(e.data == payload);
    CHECK(e.header.timestamp != 0);
    CHECK(e.verifyChecksum());
}

TEST_CASE("WALEntry::isTransactional true for Begin/Commit/Rollback", "[wal][ctor][catch2]") {
    WALEntry begin(WALEntry::OpType::BeginTransaction, 1, 123);
    WALEntry commit(WALEntry::OpType::CommitTransaction, 2, 123);
    WALEntry rollback(WALEntry::OpType::Rollback, 3, 123);

    CHECK(begin.isTransactional());
    CHECK(commit.isTransactional());
    CHECK(rollback.isTransactional());

    WALEntry store(WALEntry::OpType::StoreBlock, 4, 123);
    WALEntry del(WALEntry::OpType::DeleteBlock, 5, 123);
    WALEntry ref(WALEntry::OpType::UpdateReference, 6, 123);
    WALEntry meta(WALEntry::OpType::UpdateMetadata, 7, 123);
    WALEntry cp(WALEntry::OpType::Checkpoint, 8, 123);

    CHECK_FALSE(store.isTransactional());
    CHECK_FALSE(del.isTransactional());
    CHECK_FALSE(ref.isTransactional());
    CHECK_FALSE(meta.isTransactional());
    CHECK_FALSE(cp.isTransactional());
}

TEST_CASE("WALEntry serialize/deserialize round-trip with empty payload", "[wal][serde][catch2]") {
    WALEntry e(WALEntry::OpType::StoreBlock, 1, 7);
    REQUIRE(e.data.empty());

    auto buf = e.serialize();
    auto out = WALEntry::deserialize(std::span<const std::byte>(buf.data(), buf.size()));
    REQUIRE(out.has_value());

    CHECK(out->header.magic == e.header.magic);
    CHECK(out->header.version == e.header.version);
    CHECK(out->header.sequenceNum == e.header.sequenceNum);
    CHECK(out->header.transactionId == e.header.transactionId);
    CHECK(out->header.operation == e.header.operation);
    CHECK(out->header.dataSize == 0);
    CHECK(out->data.empty());
    CHECK(out->verifyChecksum());
}

TEST_CASE("WALEntry serialize/deserialize round-trip with 128-byte payload",
          "[wal][serde][catch2]") {
    std::vector<std::byte> payload;
    payload.resize(128);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = std::byte{static_cast<unsigned char>(i & 0xFF)};
    }

    WALEntry e(WALEntry::OpType::UpdateMetadata, 2, 99,
               std::span<const std::byte>(payload.data(), payload.size()));
    auto buf = e.serialize();

    auto out = WALEntry::deserialize(std::span<const std::byte>(buf.data(), buf.size()));
    REQUIRE(out.has_value());
    CHECK(out->header.operation == e.header.operation);
    CHECK(out->header.sequenceNum == e.header.sequenceNum);
    CHECK(out->header.transactionId == e.header.transactionId);
    CHECK(out->header.dataSize == static_cast<uint32_t>(payload.size()));
    CHECK(out->data == payload);
    CHECK(out->verifyChecksum());
}

TEST_CASE("WALEntry serialize/deserialize round-trip for every OpType (1-8)",
          "[wal][serde][catch2]") {
    for (uint8_t opv = 1; opv <= 8; ++opv) {
        auto op = static_cast<WALEntry::OpType>(opv);
        WALEntry e(op, 100 + opv, 5000 + opv);
        auto buf = e.serialize();

        auto out = WALEntry::deserialize(std::span<const std::byte>(buf.data(), buf.size()));
        REQUIRE(out.has_value());
        CHECK(out->header.operation == op);
        CHECK(out->header.sequenceNum == e.header.sequenceNum);
        CHECK(out->header.transactionId == e.header.transactionId);
        CHECK(out->verifyChecksum());
    }
}

TEST_CASE("WALEntry::verifyChecksum succeeds on valid entry", "[wal][checksum][catch2]") {
    std::vector<std::byte> payload;
    payload.resize(4);
    payload[0] = std::byte{0xDE};
    payload[1] = std::byte{0xAD};
    payload[2] = std::byte{0xBE};
    payload[3] = std::byte{0xEF};

    WALEntry e(WALEntry::OpType::StoreBlock, 1, 1,
               std::span<const std::byte>(payload.data(), payload.size()));
    CHECK(e.verifyChecksum());
}

TEST_CASE("WALEntry::verifyChecksum fails after corrupting data[0]", "[wal][checksum][catch2]") {
    std::vector<std::byte> payload;
    payload.resize(1);
    payload[0] = std::byte{0xAA};

    WALEntry e(WALEntry::OpType::StoreBlock, 10, 77,
               std::span<const std::byte>(payload.data(), payload.size()));
    REQUIRE_FALSE(e.data.empty());
    CHECK(e.verifyChecksum());

    e.data[0] = std::byte{0xAB};
    CHECK_FALSE(e.verifyChecksum());
}

TEST_CASE("WALEntry updateChecksum then verify after mutation", "[wal][checksum][catch2]") {
    std::vector<std::byte> payload;
    payload.resize(2);
    payload[0] = std::byte{0x10};
    payload[1] = std::byte{0x20};

    WALEntry e(WALEntry::OpType::DeleteBlock, 11, 88,
               std::span<const std::byte>(payload.data(), payload.size()));
    CHECK(e.verifyChecksum());

    e.data[1] = std::byte{0x21};
    CHECK_FALSE(e.verifyChecksum());

    e.updateChecksum();
    CHECK(e.verifyChecksum());
}

TEST_CASE("WALEntry::deserialize rejects buffer smaller than header",
          "[wal][serde][error][catch2]") {
    std::vector<std::byte> buf;
    buf.resize(WALEntry::Header::size() - 1);
    auto out = WALEntry::deserialize(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(out.has_value());
}

TEST_CASE("WALEntry::deserialize rejects invalid header (corrupt magic)",
          "[wal][serde][error][catch2]") {
    WALEntry e(WALEntry::OpType::StoreBlock, 1, 1);
    auto buf = e.serialize();
    REQUIRE(buf.size() >= 1);
    buf[0] ^= std::byte{0xFF};

    auto out = WALEntry::deserialize(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(out.has_value());
}

TEST_CASE("WALEntry::deserialize rejects truncated data region", "[wal][serde][error][catch2]") {
    std::vector<std::byte> payload;
    payload.resize(16);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = std::byte{0x5A};
    }
    WALEntry e(WALEntry::OpType::UpdateReference, 2, 3,
               std::span<const std::byte>(payload.data(), payload.size()));
    auto buf = e.serialize();

    REQUIRE(buf.size() == WALEntry::Header::size() + payload.size());
    buf.resize(buf.size() - 1);

    auto out = WALEntry::deserialize(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(out.has_value());
}

TEST_CASE("WALEntry::deserialize rejects corrupted checksum (flip a byte)",
          "[wal][serde][error][catch2]") {
    std::vector<std::byte> payload;
    payload.resize(8);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = std::byte{static_cast<unsigned char>(i)};
    }

    WALEntry e(WALEntry::OpType::UpdateMetadata, 3, 4,
               std::span<const std::byte>(payload.data(), payload.size()));
    auto buf = e.serialize();

    REQUIRE(buf.size() > WALEntry::Header::size());
    buf.back() ^= std::byte{0xFF};

    auto out = WALEntry::deserialize(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(out.has_value());
}

TEST_CASE("StoreBlockData encode/decode round-trip", "[wal][store][catch2]") {
    const std::string hash = "0123456789abcdef0123456789abcdef"; // 32 bytes
    auto enc = WALEntry::StoreBlockData::encode(hash, 4096, 2);
    auto dec = WALEntry::StoreBlockData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());

    CHECK(std::memcmp(dec->hash, hash.data(), yams::HASH_SIZE) == 0);
    CHECK(dec->size == 4096);
    CHECK(dec->refCount == 2);
}

TEST_CASE("StoreBlockData short hash is zero-padded", "[wal][store][catch2]") {
    const std::string hash = "abc";
    auto enc = WALEntry::StoreBlockData::encode(hash, 1, 1);
    auto dec = WALEntry::StoreBlockData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());

    CHECK(dec->hash[0] == 'a');
    CHECK(dec->hash[1] == 'b');
    CHECK(dec->hash[2] == 'c');
    for (size_t i = 3; i < yams::HASH_SIZE; ++i) {
        CHECK(dec->hash[i] == 0);
    }
}

TEST_CASE("StoreBlockData default refCount is 1", "[wal][store][catch2]") {
    const std::string hash = "0123456789abcdef0123456789abcdef";
    auto enc = WALEntry::StoreBlockData::encode(hash, 4096);
    auto dec = WALEntry::StoreBlockData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());
    CHECK(dec->refCount == 1);
}

TEST_CASE("StoreBlockData decode rejects too-small buffer", "[wal][store][catch2]") {
    std::vector<std::byte> buf;
    buf.resize(sizeof(WALEntry::StoreBlockData) - 1);
    auto dec = WALEntry::StoreBlockData::decode(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(dec.has_value());
}

TEST_CASE("DeleteBlockData encode/decode round-trip", "[wal][delete][catch2]") {
    const std::string hash = "0123456789abcdef0123456789abcdef";
    auto enc = WALEntry::DeleteBlockData::encode(hash);
    auto dec =
        WALEntry::DeleteBlockData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());
    CHECK(std::memcmp(dec->hash, hash.data(), yams::HASH_SIZE) == 0);
}

TEST_CASE("DeleteBlockData decode rejects too-small buffer", "[wal][delete][catch2]") {
    std::vector<std::byte> buf;
    buf.resize(sizeof(WALEntry::DeleteBlockData) - 1);
    auto dec =
        WALEntry::DeleteBlockData::decode(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(dec.has_value());
}

TEST_CASE("UpdateReferenceData encode/decode round-trip with positive delta",
          "[wal][ref][catch2]") {
    const std::string hash = "0123456789abcdef0123456789abcdef";
    auto enc = WALEntry::UpdateReferenceData::encode(hash, 5);
    auto dec =
        WALEntry::UpdateReferenceData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());
    CHECK(std::memcmp(dec->hash, hash.data(), yams::HASH_SIZE) == 0);
    CHECK(dec->delta == 5);
}

TEST_CASE("UpdateReferenceData encode/decode round-trip with negative delta",
          "[wal][ref][catch2]") {
    const std::string hash = "0123456789abcdef0123456789abcdef";
    auto enc = WALEntry::UpdateReferenceData::encode(hash, -3);
    auto dec =
        WALEntry::UpdateReferenceData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());
    CHECK(std::memcmp(dec->hash, hash.data(), yams::HASH_SIZE) == 0);
    CHECK(dec->delta == -3);
}

TEST_CASE("UpdateReferenceData decode rejects too-small buffer", "[wal][ref][catch2]") {
    std::vector<std::byte> buf;
    buf.resize(sizeof(WALEntry::UpdateReferenceData) - 1);
    auto dec =
        WALEntry::UpdateReferenceData::decode(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(dec.has_value());
}

TEST_CASE("UpdateMetadataData encode/decode round-trip with key/value sizes",
          "[wal][meta][catch2]") {
    const std::string hash = "0123456789abcdef0123456789abcdef";
    const std::string key = "author";
    const std::string value = "alice";

    auto enc = WALEntry::UpdateMetadataData::encode(hash, key, value);
    auto dec =
        WALEntry::UpdateMetadataData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());

    CHECK(std::memcmp(dec->hash, hash.data(), yams::HASH_SIZE) == 0);
    CHECK(dec->keySize == static_cast<uint32_t>(key.size()));
    CHECK(dec->valueSize == static_cast<uint32_t>(value.size()));
    CHECK(enc.size() == sizeof(WALEntry::UpdateMetadataData) + key.size() + value.size());

    const auto* keyPtr = enc.data() + sizeof(WALEntry::UpdateMetadataData);
    const auto* valPtr = keyPtr + key.size();
    CHECK(std::memcmp(keyPtr, key.data(), key.size()) == 0);
    CHECK(std::memcmp(valPtr, value.data(), value.size()) == 0);
}

TEST_CASE("UpdateMetadataData decode rejects truncated key/value region", "[wal][meta][catch2]") {
    const std::string hash = "0123456789abcdef0123456789abcdef";
    const std::string key = "author";
    const std::string value = "alice";

    auto enc = WALEntry::UpdateMetadataData::encode(hash, key, value);
    REQUIRE(enc.size() > sizeof(WALEntry::UpdateMetadataData));
    enc.resize(enc.size() - 1);

    auto dec =
        WALEntry::UpdateMetadataData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    CHECK_FALSE(dec.has_value());
}

TEST_CASE("UpdateMetadataData decode rejects too-small buffer", "[wal][meta][catch2]") {
    std::vector<std::byte> buf;
    buf.resize(sizeof(WALEntry::UpdateMetadataData) - 1);
    auto dec =
        WALEntry::UpdateMetadataData::decode(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(dec.has_value());
}

TEST_CASE("TransactionData encode/decode round-trip", "[wal][txn][catch2]") {
    auto enc = WALEntry::TransactionData::encode(12345, 10);
    auto dec =
        WALEntry::TransactionData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());
    CHECK(dec->transactionId == 12345);
    CHECK(dec->participantCount == 10);
}

TEST_CASE("TransactionData default count is 0", "[wal][txn][catch2]") {
    auto enc = WALEntry::TransactionData::encode(12345);
    auto dec =
        WALEntry::TransactionData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());
    CHECK(dec->transactionId == 12345);
    CHECK(dec->participantCount == 0);
}

TEST_CASE("TransactionData decode rejects too-small buffer", "[wal][txn][catch2]") {
    std::vector<std::byte> buf;
    buf.resize(sizeof(WALEntry::TransactionData) - 1);
    auto dec =
        WALEntry::TransactionData::decode(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(dec.has_value());
}

TEST_CASE("CheckpointData encode/decode round-trip", "[wal][checkpoint][catch2]") {
    auto enc = WALEntry::CheckpointData::encode(500, 1700000000);
    auto dec = WALEntry::CheckpointData::decode(std::span<const std::byte>(enc.data(), enc.size()));
    REQUIRE(dec.has_value());
    CHECK(dec->sequenceNum == 500);
    CHECK(dec->timestamp == 1700000000);
}

TEST_CASE("CheckpointData decode rejects too-small buffer", "[wal][checkpoint][catch2]") {
    std::vector<std::byte> buf;
    buf.resize(sizeof(WALEntry::CheckpointData) - 1);
    auto dec = WALEntry::CheckpointData::decode(std::span<const std::byte>(buf.data(), buf.size()));
    CHECK_FALSE(dec.has_value());
}

TEST_CASE("OpType stream operator prints all named values and Unknown", "[wal][stream][catch2]") {
    struct Expected {
        WALEntry::OpType op;
        const char* s;
    };

    const Expected expected[] = {
        {WALEntry::OpType::BeginTransaction, "BeginTransaction"},
        {WALEntry::OpType::StoreBlock, "StoreBlock"},
        {WALEntry::OpType::DeleteBlock, "DeleteBlock"},
        {WALEntry::OpType::UpdateReference, "UpdateReference"},
        {WALEntry::OpType::UpdateMetadata, "UpdateMetadata"},
        {WALEntry::OpType::CommitTransaction, "CommitTransaction"},
        {WALEntry::OpType::Rollback, "Rollback"},
        {WALEntry::OpType::Checkpoint, "Checkpoint"},
    };

    for (const auto& e : expected) {
        std::ostringstream oss;
        oss << e.op;
        CHECK(oss.str() == e.s);
    }

    {
        std::ostringstream oss;
        oss << static_cast<WALEntry::OpType>(9);
        CHECK(oss.str() == "Unknown(9)");
    }
}

TEST_CASE("WALEntry stream includes key fields", "[wal][stream][catch2]") {
    WALEntry e(WALEntry::OpType::CommitTransaction, 123, 456);
    std::ostringstream oss;
    oss << e;
    const auto s = oss.str();

    CHECK(s.find("WALEntry{") != std::string::npos);
    CHECK(s.find("seq=") != std::string::npos);
    CHECK(s.find("txn=") != std::string::npos);
}
