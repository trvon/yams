// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#pragma once

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>

#include "../../include/yams/metadata/database.h"
#include "../../include/yams/metadata/migration.h"

namespace yams::test {

inline std::filesystem::path make_temp_sqlite_path(std::string_view prefix = "yams_test_") {
    namespace fs = std::filesystem;
    const char* env = std::getenv("YAMS_TEST_TMPDIR");
    const auto base = (env && *env) ? fs::path(env) : fs::temp_directory_path();
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
        std::filesystem::copy_file(templatePath_, target,
                                   std::filesystem::copy_options::overwrite_existing);
        copyOptionalSidecar("-wal", target);
        copyOptionalSidecar("-shm", target);
        return target;
    }

private:
    void copyOptionalSidecar(const char* suffix, const std::filesystem::path& target) const {
        const auto source = std::filesystem::path(templatePath_.string() + suffix);
        if (!std::filesystem::exists(source)) {
            return;
        }
        std::filesystem::copy_file(source, std::filesystem::path(target.string() + suffix),
                                   std::filesystem::copy_options::overwrite_existing);
    }

    std::filesystem::path templatePath_;
};

inline const MetadataDbTemplate& migrated_metadata_db_template() {
    static const MetadataDbTemplate templateDb{};
    return templateDb;
}

} // namespace yams::test
