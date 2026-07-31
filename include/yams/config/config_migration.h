#pragma once

#include <filesystem>
#include <map>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::config {

struct ConfigVersion {
    int major = 3;
    int minor = 0;
    int patch = 0;

    std::string toString() const {
        return std::to_string(major) + "." + std::to_string(minor) + "." + std::to_string(patch);
    }
};

struct MigrationEntry {
    std::string old_key;
    std::string new_key;
    std::string default_value;
    std::string description;
    bool required = false;
};

class ConfigMigrator {
public:
    ConfigMigrator() = default;
    ~ConfigMigrator() = default;

    Result<bool> needsMigration(const std::filesystem::path& configPath);
    Result<void> migrateToLatest(const std::filesystem::path& configPath, bool createBackup = true);
    Result<ConfigVersion> getConfigVersion(const std::filesystem::path& configPath);
    Result<void> createDefaultLatestConfig(const std::filesystem::path& configPath);
    Result<void> validateLatestConfig(const std::filesystem::path& configPath);
    static std::vector<MigrationEntry> getV1ToLatestMigrationMap();
    static std::map<std::string, std::map<std::string, std::string>> getLatestConfigDefaults();
    static std::map<std::string, std::map<std::string, std::string>> getLatestAdditiveDefaults();

    Result<std::vector<std::string>>
    updateLatestSchemaAdditive(const std::filesystem::path& configPath, bool makeBackup = true,
                               bool dryRun = false);

    // Compatibility wrappers for pre-v3 naming.
    Result<void> migrateToV2(const std::filesystem::path& configPath, bool createBackup = true) {
        return migrateToLatest(configPath, createBackup);
    }

    Result<void> createDefaultV2Config(const std::filesystem::path& configPath) {
        return createDefaultLatestConfig(configPath);
    }

    Result<void> validateV2Config(const std::filesystem::path& configPath) {
        return validateLatestConfig(configPath);
    }

    static std::vector<MigrationEntry> getV1ToV2MigrationMap() {
        return getV1ToLatestMigrationMap();
    }

    static std::map<std::string, std::map<std::string, std::string>> getV2ConfigDefaults() {
        return getLatestConfigDefaults();
    }

    static std::map<std::string, std::map<std::string, std::string>> getV2AdditiveDefaults() {
        return getLatestAdditiveDefaults();
    }

    Result<std::vector<std::string>> updateV2SchemaAdditive(const std::filesystem::path& configPath,
                                                            bool makeBackup = true,
                                                            bool dryRun = false) {
        return updateLatestSchemaAdditive(configPath, makeBackup, dryRun);
    }

    Result<std::map<std::string, std::map<std::string, std::string>>>
    parseTomlConfig(const std::filesystem::path& path);

private:
    Result<void>
    writeTomlConfig(const std::filesystem::path& path,
                    const std::map<std::string, std::map<std::string, std::string>>& config,
                    const ConfigVersion& version);
    Result<std::filesystem::path> createBackup(const std::filesystem::path& configPath);
    std::map<std::string, std::map<std::string, std::string>>
    mergeConfigs(const std::map<std::string, std::map<std::string, std::string>>& oldConfig,
                 const std::map<std::string, std::map<std::string, std::string>>& newDefaults);
    void logMigration(const std::string& action, const std::string& details);
};

} // namespace yams::config
