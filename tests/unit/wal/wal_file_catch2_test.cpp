#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <yams/core/types.h>
#include <yams/wal/wal_file.h>

using namespace yams;
using namespace yams::wal;

namespace {

struct TempDir {
    TempDir() {
        auto stamp = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        path = std::filesystem::temp_directory_path() / ("yams_wal_file_test_" + stamp);
        std::filesystem::create_directories(path);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    std::filesystem::path path;
};

std::string makeHash(char fill) {
    return std::string(HASH_SIZE, fill);
}

} // namespace

TEST_CASE("WALFile read mode open fails for missing file", "[unit][wal][file]") {
    TempDir temp;
    auto logPath = temp.path / "missing.log";

    WALFile file(logPath, WALFile::Mode::Read);
    auto openResult = file.open();

    REQUIRE_FALSE(openResult);
    CHECK_FALSE(file.isOpen());
    CHECK(openResult.error().code == ErrorCode::FileNotFound);
}

TEST_CASE("WALFile append and iterate round trip", "[unit][wal][file]") {
    TempDir temp;
    auto logPath = temp.path / "roundtrip.log";

    WALFile writer(logPath, WALFile::Mode::Write);
    REQUIRE(writer.open());
    CHECK(writer.isOpen());
    CHECK(writer.canWrite());
    CHECK(writer.getPath() == logPath);

    auto payloadA = WALEntry::StoreBlockData::encode(makeHash('a'), 111, 2);
    auto payloadB = WALEntry::UpdateReferenceData::encode(makeHash('b'), -3);

    WALEntry entryA(WALEntry::OpType::StoreBlock, 1, 0, payloadA);
    WALEntry entryB(WALEntry::OpType::UpdateReference, 2, 0, payloadB);

    auto appendA = writer.append(entryA);
    auto appendB = writer.append(entryB);
    REQUIRE(appendA);
    REQUIRE(appendB);
    CHECK(appendA.value() == entryA.totalSize());
    CHECK(appendB.value() == entryB.totalSize());
    CHECK(writer.getSize() >= entryA.totalSize() + entryB.totalSize());
    REQUIRE(writer.sync());

    std::vector<WALEntry::OpType> opsInWriteMode;
    for (auto it = writer.begin(); it != writer.end(); ++it) {
        auto value = *it;
        REQUIRE(value.has_value());
        opsInWriteMode.push_back(value->header.operation);
    }
    REQUIRE(opsInWriteMode.size() == 2);
    CHECK(opsInWriteMode[0] == WALEntry::OpType::StoreBlock);
    CHECK(opsInWriteMode[1] == WALEntry::OpType::UpdateReference);

    REQUIRE(writer.close());
    CHECK_FALSE(writer.isOpen());

    WALFile reader(logPath, WALFile::Mode::Read);
    REQUIRE(reader.open());
    CHECK(reader.isOpen());
    CHECK_FALSE(reader.canWrite());

    auto appendInReadMode = reader.append(entryA);
    REQUIRE_FALSE(appendInReadMode);
    CHECK(appendInReadMode.error().code == ErrorCode::InvalidOperation);

    std::vector<WALEntry::OpType> opsInReadMode;
    for (auto it = reader.begin(); it != reader.end(); ++it) {
        auto value = *it;
        REQUIRE(value.has_value());
        opsInReadMode.push_back(value->header.operation);
    }
    REQUIRE(opsInReadMode.size() == 2);
    CHECK(opsInReadMode[0] == WALEntry::OpType::StoreBlock);
    CHECK(opsInReadMode[1] == WALEntry::OpType::UpdateReference);

    REQUIRE(reader.close());
}

TEST_CASE("WALFile sync requires writable open file", "[unit][wal][file]") {
    TempDir temp;
    auto logPath = temp.path / "sync.log";

    WALFile unopened(logPath, WALFile::Mode::Write);
    auto unopenedSync = unopened.sync();
    REQUIRE_FALSE(unopenedSync);
    CHECK(unopenedSync.error().code == ErrorCode::InvalidOperation);

    WALFile writer(logPath, WALFile::Mode::Write);
    REQUIRE(writer.open());
    auto payload = WALEntry::DeleteBlockData::encode(makeHash('s'));
    WALEntry entry(WALEntry::OpType::DeleteBlock, 1, 0, payload);
    REQUIRE(writer.append(entry));
    REQUIRE(writer.sync());
    REQUIRE(writer.close());

    WALFile readOnly(logPath, WALFile::Mode::Read);
    REQUIRE(readOnly.open());
    auto readSync = readOnly.sync();
    REQUIRE_FALSE(readSync);
    CHECK(readSync.error().code == ErrorCode::InvalidOperation);
    REQUIRE(readOnly.close());
}