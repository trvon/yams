// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#pragma once

#include <chrono>
#include <filesystem>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>

#include "../../include/yams/metadata/database.h"
#include "../../include/yams/metadata/migration.h"
#include <yams/config/config_helpers.h>

namespace yams::test {

inline std::filesystem::path make_temp_sqlite_path(std::string_view prefix = "yams_test_") {
    namespace fs = std::filesystem;
    const auto environment = yams::config::getenv_nonempty("YAMS_TEST_TMPDIR");
    const auto base = environment ? fs::path(*environment) : fs::temp_directory_path();
    std::error_code ec;
    fs::create_directories(base, ec);

    std::uniform_int_distribution<int> dist(0, 9999);
    thread_local std::mt19937_64 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 512; ++attempt) {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto candidate = base / (std::string(prefix) + std::to_string(stamp) + "_" +
                                 std::to_string(dist(rng)) + ".db");
        fs::remove(candidate, ec);
        if (!fs::exists(candidate)) {
            return candidate;
        }
    }
    throw std::runtime_error("failed to allocate temporary sqlite path");
}

inline void remove_sqlite_artifacts(const std::filesystem::path& dbPath) {
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
    std::filesystem::remove(std::filesystem::path(dbPath.string() + "-wal"), ec);
    std::filesystem::remove(std::filesystem::path(dbPath.string() + "-shm"), ec);
}

class MetadataDbTemplate {
public:
    explicit MetadataDbTemplate(std::string_view prefix = "yams_metadata_template_")
        : templatePath_(make_temp_sqlite_path(prefix)) {
        metadata::Database db;
        auto openResult = db.open(templatePath_.string(), metadata::ConnectionMode::Create);
        if (!openResult) {
            throw std::runtime_error("failed to open metadata template db: " +
                                     openResult.error().message);
        }

        metadata::MigrationManager manager(db);
        auto initResult = manager.initialize();
        if (!initResult) {
            db.close();
            throw std::runtime_error("failed to initialize metadata template migrations: " +
                                     initResult.error().message);
        }

        manager.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = manager.migrate();
        if (!migrateResult) {
            db.close();
            throw std::runtime_error("failed to migrate metadata template db: " +
                                     migrateResult.error().message);
        }

        db.close();
    }

    ~MetadataDbTemplate() { remove_sqlite_artifacts(templatePath_); }

    MetadataDbTemplate(const MetadataDbTemplate&) = delete;
    MetadataDbTemplate& operator=(const MetadataDbTemplate&) = delete;

    [[nodiscard]] std::filesystem::path clone(std::string_view prefix) const {
        auto target = make_temp_sqlite_path(prefix);
        // VACUUM INTO produces a self-contained single-file copy, checkpointing any WAL in the
        // template. Raw copy_file of the main file plus -wal/-shm sidecars is incorrect: the
        // -shm is a derived shared-memory index that must be rebuilt on open, and a stale -shm
        // corrupts WAL replay on Windows.
        metadata::Database db;
        auto openResult = db.open(templatePath_.string(), metadata::ConnectionMode::ReadWrite);
        if (!openResult) {
            throw std::runtime_error("failed to open metadata template db for clone: " +
                                     openResult.error().message);
        }
        auto vacuumResult =
            db.execute("VACUUM INTO '" + escapeSqlStringLiteral(target.string()) + "'");
        db.close();
        if (!vacuumResult) {
            throw std::runtime_error("failed to clone metadata template db: " +
                                     vacuumResult.error().message);
        }
        verifyCloneIntegrity(target);
        return target;
    }

private:
    static std::string escapeSqlStringLiteral(const std::string& raw) {
        std::string escaped;
        escaped.reserve(raw.size() + 2);
        for (const char c : raw) {
            escaped.push_back(c);
            if (c == '\'') {
                escaped.push_back('\'');
            }
        }
        return escaped;
    }

    // Fail loudly if the clone is missing the KG schema, so a broken clone surfaces here with a
    // clear message instead of an opaque "no such table" later in a test.
    void verifyCloneIntegrity(const std::filesystem::path& target) const {
        metadata::Database db;
        auto openResult = db.open(target.string(), metadata::ConnectionMode::ReadWrite);
        if (!openResult) {
            throw std::runtime_error("failed to open cloned metadata db: " +
                                     openResult.error().message);
        }
        auto probeResult = db.execute("SELECT 1 FROM kg_nodes LIMIT 0");
        db.close();
        if (!probeResult) {
            throw std::runtime_error("cloned metadata db is missing the KG schema: " +
                                     probeResult.error().message);
        }
    }

    std::filesystem::path templatePath_;
};

inline const MetadataDbTemplate& migrated_metadata_db_template() {
    static const MetadataDbTemplate templateDb{};
    return templateDb;
}

} // namespace yams::test
