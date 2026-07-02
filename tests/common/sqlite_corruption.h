// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "../../include/yams/metadata/database.h"
#include "metadata_test_db.h"

namespace yams::test {

inline constexpr std::uint64_t kSqliteHeaderSize = 100;
inline constexpr std::uint64_t kHeaderCorruptOffset = 20;
inline constexpr std::uint64_t kWalHeaderSize = 32;
inline constexpr std::uint64_t kWalFrameHeaderSize = 24;
inline constexpr char kGarbageByte = '\xff';

inline void corruptOffset(const std::filesystem::path& file, std::uint64_t offset,
                          std::uint64_t length) {
    std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
    if (!f.is_open()) {
        throw std::runtime_error("corruptOffset: cannot open " + file.string());
    }
    f.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    const std::string garbage(static_cast<std::size_t>(length), kGarbageByte);
    f.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
    if (!f.good()) {
        throw std::runtime_error("corruptOffset: write failed for " + file.string());
    }
}

inline void corruptHeader(const std::filesystem::path& dbPath) {
    corruptOffset(dbPath, kHeaderCorruptOffset, 8);
}

inline std::uint32_t readBigEndian(const std::filesystem::path& file, std::uint64_t offset,
                                   std::uint32_t bytes) {
    std::ifstream f(file, std::ios::binary);
    if (!f.is_open()) {
        throw std::runtime_error("readBigEndian: cannot open " + file.string());
    }
    f.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    std::uint32_t value = 0;
    for (std::uint32_t i = 0; i < bytes; ++i) {
        char c = 0;
        f.get(c);
        value = (value << 8) | static_cast<std::uint8_t>(c);
    }
    if (!f.good()) {
        throw std::runtime_error("readBigEndian: read failed for " + file.string());
    }
    return value;
}

inline std::uint32_t sqlitePageSize(const std::filesystem::path& dbPath) {
    const auto raw = readBigEndian(dbPath, 16, 2);
    return raw == 1 ? 65536u : raw;
}

inline std::uint32_t sqlitePageCount(const std::filesystem::path& dbPath) {
    return readBigEndian(dbPath, 28, 4);
}

inline std::int64_t tableRootPage(const std::filesystem::path& dbPath, std::string_view table) {
    metadata::Database db;
    auto open = db.open(dbPath.string(), metadata::ConnectionMode::ReadOnly);
    if (!open) {
        throw std::runtime_error("tableRootPage: open failed: " + open.error().message);
    }
    auto stmt = db.prepare("SELECT rootpage FROM sqlite_master WHERE name = ?");
    if (!stmt) {
        throw std::runtime_error("tableRootPage: prepare failed: " + stmt.error().message);
    }
    auto bound = stmt.value().bind(1, std::string(table));
    if (!bound) {
        throw std::runtime_error("tableRootPage: bind failed: " + bound.error().message);
    }
    auto row = stmt.value().step();
    if (!row || !row.value()) {
        throw std::runtime_error("tableRootPage: no such table: " + std::string(table));
    }
    return stmt.value().getInt64(0);
}

inline void corruptPage(const std::filesystem::path& dbPath, std::uint64_t pageNo) {
    const auto pageSize = sqlitePageSize(dbPath);
    const auto start = (pageNo - 1) * static_cast<std::uint64_t>(pageSize);
    corruptOffset(dbPath, start, 64);
    corruptOffset(dbPath, start + pageSize - (pageSize / 4), pageSize / 4);
}

inline void truncateFile(const std::filesystem::path& file, double fraction) {
    const auto size = std::filesystem::file_size(file);
    const auto newSize = static_cast<std::uintmax_t>(static_cast<double>(size) * fraction);
    std::filesystem::resize_file(file, newSize);
}

inline std::filesystem::path walPath(const std::filesystem::path& dbPath) {
    return {dbPath.string() + "-wal"};
}

inline std::filesystem::path shmPath(const std::filesystem::path& dbPath) {
    return {dbPath.string() + "-shm"};
}

inline void corruptWalFrame(const std::filesystem::path& dbPath) {
    const auto wal = walPath(dbPath);
    if (!std::filesystem::exists(wal) ||
        std::filesystem::file_size(wal) <= kWalHeaderSize + kWalFrameHeaderSize) {
        throw std::runtime_error("corruptWalFrame: no uncheckpointed WAL frame at " +
                                 wal.string());
    }
    corruptOffset(wal, kWalHeaderSize + kWalFrameHeaderSize - 8, 8);
}

inline void corruptShm(const std::filesystem::path& dbPath) {
    const auto shm = shmPath(dbPath);
    if (!std::filesystem::exists(shm)) {
        throw std::runtime_error("corruptShm: no -shm file at " + shm.string());
    }
    corruptOffset(shm, 0, 48);
}

inline std::filesystem::path copySqliteFiles(const std::filesystem::path& dbPath,
                                             std::string_view prefix) {
    namespace fs = std::filesystem;
    auto target = make_temp_sqlite_path(prefix);
    fs::copy_file(dbPath, target, fs::copy_options::overwrite_existing);
    for (const char* suffix : {"-wal", "-shm"}) {
        const auto source = fs::path(dbPath.string() + suffix);
        if (fs::exists(source)) {
            fs::copy_file(source, fs::path(target.string() + suffix),
                          fs::copy_options::overwrite_existing);
        }
    }
    return target;
}

inline std::filesystem::path
snapshotMidTransaction(const std::filesystem::path& dbPath,
                       const std::function<void(metadata::Database&)>& mutate,
                       std::string_view prefix) {
    metadata::Database db;
    auto open = db.open(dbPath.string(), metadata::ConnectionMode::ReadWrite);
    if (!open) {
        throw std::runtime_error("snapshotMidTransaction: open failed: " + open.error().message);
    }
    auto begin = db.execute("BEGIN IMMEDIATE");
    if (!begin) {
        throw std::runtime_error("snapshotMidTransaction: begin failed: " +
                                 begin.error().message);
    }
    mutate(db);

    auto target = copySqliteFiles(dbPath, prefix);

    auto rollback = db.execute("ROLLBACK");
    if (!rollback) {
        throw std::runtime_error("snapshotMidTransaction: rollback failed: " +
                                 rollback.error().message);
    }
    db.close();
    return target;
}

} // namespace yams::test
