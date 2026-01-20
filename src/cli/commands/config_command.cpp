#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <yams/cli/command.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>

namespace yams::cli {

namespace fs = std::filesystem;

class ConfigCommand : public ICommand {
public:
    std::string getName() const override { return "config"; }

    std::string getDescription() const override { return "Manage YAMS configuration settings"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("config", getDescription());
        cmd->require_subcommand();

        // Get subcommand
        auto* getCmd = cmd->add_subcommand("get", "Get a configuration value");
        getCmd->add_option("key", key_, "Configuration key to retrieve")->required();
        getCmd->callback([this]() {
            auto result = executeGet();
            if (!result) {
                spdlog::error("Config get failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Set subcommand
        auto* setCmd = cmd->add_subcommand("set", "Set a configuration value");
        setCmd->add_option("key", key_, "Configuration key")->required();
        setCmd->add_option("value", value_, "Configuration value")->required();
        setCmd->callback([this]() {
            auto result = executeSet();
            if (!result) {
                spdlog::error("Config set failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // List subcommand
        auto* listCmd = cmd->add_subcommand("list", "List all configuration settings");
        listCmd->callback([this]() {
            auto result = executeList();
            if (!result) {
                spdlog::error("Config list failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Validate subcommand
        auto* validateCmd = cmd->add_subcommand("validate", "Validate configuration file");
        validateCmd->add_option("--config-path", configPath_, "Path to config file");
        validateCmd->callback([this]() {
            auto result = executeValidate();
            if (!result) {
                spdlog::error("Config validate failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Export subcommand
        auto* exportCmd = cmd->add_subcommand("export", "Export configuration");
        exportCmd->add_option("--format", format_, "Export format (toml, json)")
            ->default_val("toml")
            ->check(CLI::IsMember({"toml", "json"}));
        exportCmd->callback([this]() {
            auto result = executeExport();
            if (!result) {
                spdlog::error("Config export failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Embeddings subcommand with various actions
        auto* embeddingsCmd = cmd->add_subcommand("embeddings", "Manage embedding configuration");
        embeddingsCmd->require_subcommand();

        // Enable auto-generation
        auto* enableCmd =
            embeddingsCmd->add_subcommand("enable", "Enable automatic embedding generation");
        enableCmd->callback([this]() {
            auto result = executeEmbeddingsEnable();
            if (!result) {
                spdlog::error("Enable embeddings failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Disable auto-generation
        auto* disableCmd =
            embeddingsCmd->add_subcommand("disable", "Disable automatic embedding generation");
        disableCmd->callback([this]() {
            auto result = executeEmbeddingsDisable();
            if (!result) {
                spdlog::error("Disable embeddings failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Show status
        auto* statusCmd =
            embeddingsCmd->add_subcommand("status", "Show embedding configuration status");
        statusCmd->callback([this]() {
            auto result = executeEmbeddingsStatus();
            if (!result) {
                spdlog::error("Embeddings status failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Set preferred model
        auto* modelCmd = embeddingsCmd->add_subcommand("model", "Set preferred embedding model");
        modelCmd->add_option("model_name", embeddingModel_, "Model name (e.g., all-MiniLM-L6-v2)")
            ->required();
        modelCmd->callback([this]() {
            auto result = executeEmbeddingsModel();
            if (!result) {
                spdlog::error("Set embeddings model failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Tune performance
        auto* tuneCmd = embeddingsCmd->add_subcommand("tune", "Apply performance preset");
        tuneCmd->add_option("preset", embeddingPreset_, "Preset (performance, quality, balanced)")
            ->required()
            ->check(CLI::IsMember({"performance", "quality", "balanced"}));
        tuneCmd->callback([this]() {
            auto result = executeEmbeddingsTune();
            if (!result) {
                spdlog::error("Tune embeddings failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Search subcommands (path-tree traversal controls)
        auto* searchCmd = cmd->add_subcommand("search", "Manage search configuration");
        searchCmd->require_subcommand();

        auto* pathTreeCmd =
            searchCmd->add_subcommand("path-tree", "Configure hierarchical path-tree traversal");
        pathTreeCmd->require_subcommand();

        auto* pathTreeEnableCmd =
            pathTreeCmd->add_subcommand("enable", "Enable path-tree traversal");
        pathTreeEnableCmd
            ->add_option("--mode", pathTreeMode_, "Mode when enabled (fallback|preferred)")
            ->check(CLI::IsMember({"fallback", "preferred"}));
        pathTreeEnableCmd->callback([this]() {
            auto result = executePathTreeEnable();
            if (!result) {
                spdlog::error("Enable path-tree failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* pathTreeDisableCmd =
            pathTreeCmd->add_subcommand("disable", "Disable path-tree traversal");
        pathTreeDisableCmd->callback([this]() {
            auto result = executePathTreeDisable();
            if (!result) {
                spdlog::error("Disable path-tree failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* pathTreeModeCmd = pathTreeCmd->add_subcommand("mode", "Set path-tree traversal mode");
        pathTreeModeCmd->add_option("mode", pathTreeMode_, "fallback|preferred")
            ->required()
            ->check(CLI::IsMember({"fallback", "preferred"}));
        pathTreeModeCmd->callback([this]() {
            auto result = executePathTreeMode();
            if (!result) {
                spdlog::error("Set path-tree mode failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* pathTreeStatusCmd =
            pathTreeCmd->add_subcommand("status", "Show path-tree configuration");
        pathTreeStatusCmd->callback([this]() {
            auto result = executePathTreeStatus();
            if (!result) {
                spdlog::error("Path-tree status failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Migration subcommand
        auto* migrateCmd = cmd->add_subcommand("migrate", "Migrate configuration to v2");
        migrateCmd->add_option("--config-path", configPath_, "Path to config file");
        migrateCmd->add_flag("--no-backup", noBackup_, "Skip creating backup");
        migrateCmd->callback([this]() {
            auto result = executeMigrate();
            if (!result) {
                spdlog::error("Config migration failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Update subcommand (add new v2 keys additively)
        auto* updateCmd = cmd->add_subcommand(
            "update", "Add newly introduced v2 keys without overwriting existing values");
        updateCmd->add_option("--config-path", configPath_, "Path to config file");
        updateCmd->add_flag("--no-backup", noBackup_, "Skip creating backup");
        updateCmd->add_flag("--dry-run", dryRun_, "Show changes without writing");
        updateCmd->callback([this]() {
            auto result = executeUpdate();
            if (!result) {
                spdlog::error("Config update failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Grammar subcommands for tree-sitter symbol extraction
        auto* grammarCmd =
            cmd->add_subcommand("grammar", "Manage tree-sitter grammars for symbol extraction");
        grammarCmd->require_subcommand();

        auto* grammarListCmd =
            grammarCmd->add_subcommand("list", "List available and installed grammars");
        grammarListCmd->callback([this]() {
            auto result = executeGrammarList();
            if (!result) {
                spdlog::error("Grammar list failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* grammarDownloadCmd =
            grammarCmd->add_subcommand("download", "Download tree-sitter grammar");
        grammarDownloadCmd
            ->add_option("language", grammarLanguage_,
                         "Language (cpp, python, rust, go, javascript, typescript, java)")
            ->required();
        grammarDownloadCmd->callback([this]() {
            auto result = executeGrammarDownload();
            if (!result) {
                spdlog::error("Grammar download failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* grammarPathCmd = grammarCmd->add_subcommand("path", "Show grammar installation path");
        grammarPathCmd->callback([this]() {
            auto result = executeGrammarPath();
            if (!result) {
                spdlog::error("Grammar path failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* grammarAutoEnableCmd =
            grammarCmd->add_subcommand("auto-enable", "Enable automatic grammar downloads");
        grammarAutoEnableCmd->callback([this]() {
            auto result = executeGrammarAutoEnable();
            if (!result) {
                spdlog::error("Enable auto-download failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* grammarAutoDisableCmd =
            grammarCmd->add_subcommand("auto-disable", "Disable automatic grammar downloads");
        grammarAutoDisableCmd->callback([this]() {
            auto result = executeGrammarAutoDisable();
            if (!result) {
                spdlog::error("Disable auto-download failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Check subcommand to verify if migration is needed
        auto* checkCmd = cmd->add_subcommand("check", "Check if config needs migration");
        checkCmd->add_option("--config-path", configPath_, "Path to config file");
        checkCmd->callback([this]() {
            auto result = executeCheck();
            if (!result) {
                spdlog::error("Config check failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Tuning subcommands for centralized tuning configuration
        auto* tuningCmd = cmd->add_subcommand("tuning", "Manage daemon tuning settings");
        tuningCmd->require_subcommand();

        auto* tuningProfileCmd = tuningCmd->add_subcommand("profile", "Set tuning profile preset");
        tuningProfileCmd
            ->add_option("preset", tuningProfile_, "Preset (efficient|balanced|aggressive)")
            ->required()
            ->check(CLI::IsMember({"efficient", "balanced", "aggressive"}));
        tuningProfileCmd->callback([this]() {
            auto result = executeTuningProfile();
            if (!result) {
                spdlog::error("Set tuning profile failed: {}", result.error().message);
                std::exit(1);
            }
        });

        auto* tuningStatusCmd =
            tuningCmd->add_subcommand("status", "Show current tuning configuration");
        tuningStatusCmd->callback([this]() {
            auto result = executeTuningStatus();
            if (!result) {
                spdlog::error("Tuning status failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Storage subcommand
        auto* storageCmd = cmd->add_subcommand("storage", "Configure storage settings");
        storageCmd->add_option("--engine", storageEngine_, "Storage engine (local or s3)");
        storageCmd->add_option("--s3-url", s3Url_, "S3 URL (e.g., s3://bucket/prefix)");
        storageCmd->add_option("--s3-region", s3Region_, "S3 region");
        storageCmd->add_option("--s3-endpoint", s3Endpoint_, "S3 custom endpoint");
        storageCmd->add_option("--s3-access-key", s3AccessKey_, "S3 access key");
        storageCmd->add_option("--s3-secret-key", s3SecretKey_, "S3 secret key");
        storageCmd->add_flag("--s3-use-path-style", s3UsePathStyle_,
                             "Enable S3 path style addressing");
        storageCmd->callback([this]() {
            auto result = executeStorage();
            if (!result) {
                spdlog::error("Config storage failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        // This shouldn't be reached as subcommands handle execution
        return Error{ErrorCode::NotImplemented, "Config command requires a subcommand"};
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string key_;
    std::string value_;
    std::string configPath_;
    std::string format_;
    std::string embeddingModel_;
    std::string embeddingPreset_;
    std::string tuningProfile_;
    std::string pathTreeMode_;
    std::string grammarLanguage_;
    bool noBackup_ = false;
    bool dryRun_ = false;

    // Storage config members
    std::string storageEngine_;
    std::string s3Url_;
    std::string s3Region_;
    std::string s3Endpoint_;
    std::string s3AccessKey_;
    std::string s3SecretKey_;
    bool s3UsePathStyle_ = false;

    // Template helper for setting single config value with validation
    template <typename Validator = std::nullptr_t>
    Result<void> setConfigValue(const std::string& key, const std::string& value,
                                Validator validator = nullptr) {
        try {
            if constexpr (!std::is_same_v<Validator, std::nullptr_t>) {
                auto validationResult = validator(key, value);
                if (!validationResult) {
                    return validationResult;
                }
            }

            auto result = writeConfigValue(key, value);
            if (!result)
                return result;

            std::cout << ui::status_ok("Updated " + key + " = " + value + " in " +
                                       getConfigPath().string())
                      << "\n";
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    // Template helper for setting multiple config values
    template <typename... Pairs> Result<void> setConfigValues(Pairs&&... pairs) {
        try {
            auto results = {setConfigValue(std::forward<Pairs>(pairs).first,
                                           std::forward<Pairs>(pairs).second)...};
            for (const auto& result : results) {
                if (!result)
                    return result;
            }
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    // Template helper for setting multiple config key-value pairs from a map/vector
    Result<void>
    setMultipleConfigs(const std::vector<std::pair<std::string, std::string>>& configs) {
        try {
            for (const auto& [key, value] : configs) {
                auto result = writeConfigValue(key, value);
                if (!result)
                    return result;
            }
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    // Template helper for showing status with config values
    template <typename ConfigMap>
    void printStatusSection(const std::string& title, const ConfigMap& items) {
        std::cout << title << "\n";
        std::cout << std::string(title.length(), '=') << "\n\n";
        for (const auto& [label, value] : items) {
            std::cout << label << ": " << value << "\n";
        }
        std::cout << "\n";
    }

    // Template helper for enable/disable operations
    Result<void> setBooleanConfig(const std::string& key, bool enabled,
                                  const std::string& enableMsg, const std::string& disableMsg) {
        try {
            auto result = writeConfigValue(key, enabled ? "true" : "false");
            if (!result)
                return result;

            std::cout << ui::status_ok(enabled ? enableMsg : disableMsg) << "\n";
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    fs::path getConfigPath() const { return yams::config::get_config_path(configPath_); }

    // Parse all config values into a map (section.key format)
    std::map<std::string, std::string> parseSimpleToml(const fs::path& path) const {
        return yams::config::parse_simple_toml(path);
    }

    static std::vector<std::string> parseStringList(const std::string& raw) {
        std::vector<std::string> items;
        std::string s = raw;
        auto trim_inplace = [](std::string& str) {
            while (!str.empty() && std::isspace(static_cast<unsigned char>(str.front())))
                str.erase(str.begin());
            while (!str.empty() && std::isspace(static_cast<unsigned char>(str.back())))
                str.pop_back();
        };

        trim_inplace(s);
        if (s.size() >= 2 && s.front() == '[' && s.back() == ']') {
            s = s.substr(1, s.size() - 2);
        }

        std::string current;
        bool inQuote = false;
        char quoteChar = '\0';
        for (char ch : s) {
            if ((ch == '"' || ch == '\'') && (!inQuote || ch == quoteChar)) {
                inQuote = !inQuote;
                quoteChar = inQuote ? ch : '\0';
                continue;
            }
            if (ch == ',' && !inQuote) {
                trim_inplace(current);
                if (!current.empty()) {
                    items.push_back(current);
                }
                current.clear();
                continue;
            }
            current.push_back(ch);
        }
        trim_inplace(current);
        if (!current.empty()) {
            items.push_back(current);
        }

        items.erase(std::remove_if(items.begin(), items.end(),
                                   [](const std::string& v) { return v.empty(); }),
                    items.end());
        return items;
    }

    static std::string formatStringList(const std::vector<std::string>& items) {
        std::ostringstream out;
        out << "[";
        for (size_t i = 0; i < items.size(); ++i) {
            if (i > 0)
                out << ", ";
            out << "\"" << items[i] << "\"";
        }
        out << "]";
        return out.str();
    }

    Result<void> executeGet() {
        try {
            auto configPath = getConfigPath();
            if (!fs::exists(configPath)) {
                return Error{ErrorCode::FileNotFound,
                             "Configuration file not found: " + configPath.string()};
            }

            auto config = parseSimpleToml(configPath);

            // Look for the key
            if (config.find(key_) != config.end()) {
                std::cout << config[key_] << "\n";
            } else {
                return Error{ErrorCode::InvalidArgument,
                             "Key '" + key_ + "' not found in configuration"};
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeSet() {
        try {
            auto configPath = getConfigPath();

            // Validate compression settings
            if (key_.starts_with("compression.")) {
                auto validationResult = validateCompressionSetting(key_, value_);
                if (!validationResult) {
                    return validationResult;
                }
            }

            // Basic numeric validation for tuning keys where applicable
            if (key_.starts_with("tuning.")) {
                auto sub = key_.substr(std::string("tuning.").size());
                if (sub == "profile") {
                    // Accept only efficient|balanced|aggressive (case-insensitive)
                    std::string v = value_;
                    for (auto& c : v)
                        c = static_cast<char>(std::tolower(c));
                    if (v != "efficient" && v != "balanced" && v != "aggressive") {
                        return Error{
                            ErrorCode::InvalidArgument,
                            "tuning.profile must be one of: efficient|balanced|aggressive"};
                    }
                } else {
                    auto is_number = [&](const std::string& s) {
                        if (s.empty())
                            return false;
                        char* end = nullptr;
                        std::strtod(s.c_str(), &end);
                        return end && *end == '\0';
                    };
                    if (!is_number(value_)) {
                        return Error{ErrorCode::InvalidArgument, "tuning value must be numeric"};
                    }
                }
            }

            auto res = writeConfigValue(key_, value_);
            if (!res)
                return res;
            std::cout << ui::status_ok("Updated " + key_ + " = " + value_ + " in " +
                                       configPath.string())
                      << "\n";
            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeList() {
        try {
            auto configPath = getConfigPath();
            if (!fs::exists(configPath)) {
                return Error{ErrorCode::FileNotFound,
                             "Configuration file not found: " + configPath.string()};
            }

            auto config = parseSimpleToml(configPath);

            std::cout << "Current configuration:\n";
            std::cout << ui::horizontal_rule(21) << "\n";

            for (const auto& [key, value] : config) {
                std::cout << std::left << std::setw(30) << key << " = " << value << "\n";
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeValidate() {
        try {
            auto configPath = getConfigPath();
            if (!fs::exists(configPath)) {
                return Error{ErrorCode::FileNotFound,
                             "Configuration file not found: " + configPath.string()};
            }

            auto config = parseSimpleToml(configPath);

            // Check for required fields
            std::vector<std::string> errors;

            if (config.find("core.data_dir") == config.end()) {
                errors.push_back("Missing required field: core.data_dir");
            }

            if (config.find("auth.private_key_path") == config.end()) {
                errors.push_back("Missing required field: auth.private_key_path");
            }

            // Validate compression settings if present
            validateCompressionConfig(config, errors);

            if (errors.empty()) {
                std::cout << ui::status_ok("Configuration is valid") << "\n";
                std::cout << "  Config file: " << configPath << "\n";

                // Show compression settings if present
                if (config.find("compression.enable") != config.end()) {
                    std::cout << "\n  Compression Settings:\n";
                    std::cout << "    Enabled: " << config["compression.enable"] << "\n";
                    if (config.find("compression.algorithm") != config.end()) {
                        std::cout << "    Algorithm: " << config["compression.algorithm"] << "\n";
                    }
                    if (config.find("compression.zstd_level") != config.end()) {
                        std::cout << "    Zstd Level: " << config["compression.zstd_level"] << "\n";
                    }
                    if (config.find("compression.lzma_level") != config.end()) {
                        std::cout << "    LZMA Level: " << config["compression.lzma_level"] << "\n";
                    }
                }
            } else {
                std::cerr << ui::status_error("Configuration validation failed:") << "\n";
                for (const auto& error : errors) {
                    std::cerr << "  - " << error << "\n";
                }
                return Error{ErrorCode::InvalidData, "Configuration validation failed"};
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeExport() {
        try {
            auto configPath = getConfigPath();
            if (!fs::exists(configPath)) {
                return Error{ErrorCode::FileNotFound,
                             "Configuration file not found: " + configPath.string()};
            }

            if (format_ == "toml") {
                // Just output the file contents
                std::ifstream file(configPath);
                std::cout << file.rdbuf();
            } else if (format_ == "json") {
                // Convert to simple JSON
                auto config = parseSimpleToml(configPath);

                std::cout << "{\n";
                bool first = true;
                for (const auto& [key, value] : config) {
                    if (!first)
                        std::cout << ",\n";
                    std::cout << "  \"" << key << "\": \"" << value << "\"";
                    first = false;
                }
                std::cout << "\n}\n";
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> validateCompressionSetting(const std::string& key, const std::string& value) {
        if (key == "compression.enable" || key == "compression.async_compression") {
            if (value != "true" && value != "false") {
                return Error{ErrorCode::InvalidArgument, key + " must be 'true' or 'false'"};
            }
        } else if (key == "compression.algorithm") {
            if (value != "zstd" && value != "lzma") {
                return Error{ErrorCode::InvalidArgument,
                             "compression.algorithm must be 'zstd' or 'lzma'"};
            }
        } else if (key == "compression.zstd_level") {
            try {
                int level = std::stoi(value);
                if (level < 1 || level > 22) {
                    return Error{ErrorCode::InvalidArgument,
                                 "compression.zstd_level must be between 1 and 22"};
                }
            } catch (...) {
                return Error{ErrorCode::InvalidArgument,
                             "compression.zstd_level must be a number between 1 and 22"};
            }
        } else if (key == "compression.lzma_level") {
            try {
                int level = std::stoi(value);
                if (level < 0 || level > 9) {
                    return Error{ErrorCode::InvalidArgument,
                                 "compression.lzma_level must be between 0 and 9"};
                }
            } catch (...) {
                return Error{ErrorCode::InvalidArgument,
                             "compression.lzma_level must be a number between 0 and 9"};
            }
        } else if (key == "compression.max_concurrent_compressions") {
            try {
                int count = std::stoi(value);
                if (count < 1 || count > 32) {
                    return Error{
                        ErrorCode::InvalidArgument,
                        "compression.max_concurrent_compressions must be between 1 and 32"};
                }
            } catch (...) {
                return Error{
                    ErrorCode::InvalidArgument,
                    "compression.max_concurrent_compressions must be a number between 1 and 32"};
            }
        } else if (key.starts_with("compression.") &&
                   (key.ends_with("_threshold") || key.ends_with("_above") ||
                    key.ends_with("_below") || key.ends_with("_days"))) {
            try {
                int num = std::stoi(value);
                if (num < 0) {
                    return Error{ErrorCode::InvalidArgument, key + " must be a positive number"};
                }
            } catch (...) {
                return Error{ErrorCode::InvalidArgument, key + " must be a positive number"};
            }
        }

        return Result<void>();
    }

    // Helper method to write config values
    Result<void> writeConfigValue(const std::string& key, const std::string& value) {
        auto configPath = getConfigPath();
        if (yams::config::write_config_value(configPath, key, value)) {
            return Result<void>();
        }
        return Error{ErrorCode::WriteError, "Failed to write config key: " + key};
    }

    Result<void> executeTuningProfile() {
        try {
            std::string normalized = tuningProfile_;
            for (auto& ch : normalized)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));

            auto result = writeConfigValue("tuning.profile", normalized);
            if (!result)
                return result;

            std::cout << ui::status_ok("Set tuning.profile = " + normalized) << "\n";
            std::cout << "  Presets: efficient | balanced | aggressive\n";
            std::cout << "  Changes apply the next time the daemon reloads its tuning config.\n";
            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeTuningStatus() {
        try {
            auto configPath = getConfigPath();
            auto config = parseSimpleToml(configPath);

            auto getValue = [&config](const std::string& key,
                                      const std::string& fallback) -> std::string {
                auto it = config.find(key);
                if (it != config.end() && !it->second.empty())
                    return it->second;
                return fallback;
            };

            const auto profile = getValue("tuning.profile", "(not set – defaults to balanced)");
            const auto queueMax = getValue("tuning.post_ingest_queue_max", "(auto)");
            const auto threadsMin = getValue("tuning.post_ingest_threads", "(auto)");
            const auto poolCooldown = getValue("tuning.pool_cooldown_ms", "500");

            std::cout << ui::section_header("Tuning Configuration") << "\n";
            std::cout << "Profile: " << profile << "\n";
            std::cout << "Post-ingest worker cap: " << threadsMin << "\n";
            std::cout << "Post-ingest queue max: " << queueMax << "\n";
            std::cout << "Pool cooldown (ms): " << poolCooldown << "\n";

            std::cout << "\nCommands:\n";
            std::cout
                << "  yams config tuning profile <preset>   # efficient|balanced|aggressive\n";
            std::cout << "  yams config tuning status            # show current values\n";

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    // Check if embedding models are available
    std::vector<std::string> getAvailableModels() {
        std::vector<std::string> models;
        std::set<std::string> seen; // Track unique model names

        // Collect model directories from multiple potential paths
        std::vector<fs::path> searchPaths;

        // Priority 0: core.data_dir from config file (user's configured storage)
        try {
            auto configPath = getConfigPath();
            auto config = parseSimpleToml(configPath);
            if (config.contains("core.data_dir")) {
                fs::path dataDir = config["core.data_dir"];
                searchPaths.emplace_back(dataDir / "models");
            }
        } catch (...) {
            // Ignore config parsing errors, fall through to other paths
        }

        // Priority 1: YAMS_STORAGE environment variable
        if (const char* storage = std::getenv("YAMS_STORAGE")) {
            searchPaths.emplace_back(fs::path(storage) / "models");
        }

        // Priority 2: XDG_DATA_HOME or ~/.local/share/yams
        searchPaths.emplace_back(yams::config::get_data_dir() / "models");

        // Priority 3: ~/.yams/models (legacy path)
        if (const char* home = std::getenv("HOME")) {
            searchPaths.emplace_back(fs::path(home) / ".yams" / "models");
        }

        // Scan all paths for models
        for (const auto& modelsPath : searchPaths) {
            if (!fs::exists(modelsPath))
                continue;

            for (const auto& entry : fs::directory_iterator(modelsPath)) {
                if (entry.is_directory()) {
                    fs::path modelFile = entry.path() / "model.onnx";
                    if (fs::exists(modelFile)) {
                        std::string name = entry.path().filename().string();
                        if (seen.insert(name).second) {
                            models.push_back(name);
                        }
                    }
                }
            }
        }

        return models;
    }

    Result<void> executeEmbeddingsEnable() {
        try {
            auto models = getAvailableModels();
            if (models.empty()) {
                std::cout << ui::status_warning("No embedding models found.") << "\n";
                std::cout << "Download a model first: yams model download nomic-embed-text-v1.5\n";
                std::cout << "  (or use --hf nomic-ai/nomic-embed-text-v1.5)\n";
                return Error{ErrorCode::NotFound, "No embedding models available"};
            }

            // Preserve existing preferred_model if set and valid, otherwise use first available
            auto config = parseSimpleToml(getConfigPath());
            std::string preferredModel = config["embeddings.preferred_model"];
            if (preferredModel.empty() ||
                std::ranges::find(models, preferredModel) == models.end()) {
                preferredModel = models[0];
            }

            // Set default configurations for auto-generation
            auto result = setMultipleConfigs({{"embeddings.auto_generate", "true"},
                                              {"embeddings.preferred_model", preferredModel},
                                              {"embeddings.batch_size", "16"},
                                              {"embeddings.generation_delay_ms", "1000"}});
            if (!result)
                return result;

            std::cout << ui::status_ok("Automatic embedding generation enabled") << "\n";
            std::cout << "  Model: " << preferredModel << "\n";
            std::cout << "  Batch size: 16\n";
            std::cout << "  Processing delay: 1000ms\n\n";
            std::cout << "Documents added with 'yams add' will now automatically\n";
            std::cout << "generate embeddings in the background.\n";

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeEmbeddingsDisable() {
        return setBooleanConfig("embeddings.auto_generate", false,
                                "Automatic embedding generation disabled",
                                "Automatic embedding generation disabled\n"
                                "  Use 'yams repair --embeddings' to manually generate embeddings");
    }

    Result<void> executeEmbeddingsStatus() {
        try {
            auto configPath = getConfigPath();
            auto config = parseSimpleToml(configPath);
            auto models = getAvailableModels();

            std::cout << ui::section_header("Embedding Configuration Status") << "\n";

            // Auto-generation status
            bool autoEnabled = config["embeddings.auto_generate"] == "true";
            std::cout << "Auto-generation: "
                      << (autoEnabled ? ui::status_ok("Enabled") : ui::status_error("Disabled"))
                      << "\n";

            // Available models
            std::cout << "Available models: ";
            if (models.empty()) {
                std::cout << "None\n";
                std::cout << "  → Download: yams model download nomic-embed-text-v1.5\n";
            } else {
                std::cout << models.size() << " found\n";
                for (const auto& model : models) {
                    std::cout << "  - " << model << "\n";
                }
            }

            // Current settings
            if (autoEnabled) {
                std::cout << "\nCurrent settings:\n";
                std::cout << "  Preferred model: " << config["embeddings.preferred_model"] << "\n";
                std::cout << "  Batch size: " << config["embeddings.batch_size"] << "\n";
                std::cout << "  Processing delay: " << config["embeddings.generation_delay_ms"]
                          << "ms\n";
            }

            std::cout << "\nCommands:\n";
            std::cout << "  yams config embeddings enable   - Enable auto-generation\n";
            std::cout << "  yams config embeddings disable  - Disable auto-generation\n";
            std::cout << "  yams config embeddings model <name> - Change preferred model\n";
            std::cout << "  yams config embeddings tune <preset> - Apply performance preset\n";

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeEmbeddingsModel() {
        try {
            auto models = getAvailableModels();

            // Check if the requested model is available
            if (std::ranges::find(models, embeddingModel_) == models.end()) {
                std::cout << ui::status_error("Model '" + embeddingModel_ + "' not found") << "\n";
                std::cout << "\nAvailable models:\n";
                for (const auto& model : models) {
                    std::cout << "  - " << model << "\n";
                }
                if (models.empty()) {
                    std::cout << "  (none - download with: yams model --download <model-name>)\n";
                }
                return Error{ErrorCode::NotFound, "Model not available: " + embeddingModel_};
            }

            // Build config updates: preferred_model + dimension
            std::vector<std::pair<std::string, std::string>> configs;
            configs.emplace_back("embeddings.preferred_model", embeddingModel_);

            // Keep preferred model in the daemon preload list as the first entry.
            auto config = parseSimpleToml(getConfigPath());
            auto preloadRaw = config["daemon.models.preload_models"];
            auto preloadModels = parseStringList(preloadRaw);
            if (!preloadModels.empty()) {
                preloadModels.erase(preloadModels.begin());
            }
            preloadModels.erase(
                std::remove(preloadModels.begin(), preloadModels.end(), embeddingModel_),
                preloadModels.end());
            preloadModels.insert(preloadModels.begin(), embeddingModel_);
            configs.emplace_back("daemon.models.preload_models", formatStringList(preloadModels));

            // Get dimension from model metadata (preferred) or fall back to heuristic
            std::optional<size_t> dim;
            fs::path dataDir = cli_->getDataPath();
            dim = vecutil::getModelDimensionFromMetadata(dataDir, embeddingModel_);
            if (!dim) {
                dim = vecutil::getModelDimensionHeuristic(embeddingModel_);
            }

            // Update dimension configs if we found a dimension
            if (dim) {
                std::string dimStr = std::to_string(*dim);
                configs.emplace_back("embeddings.embedding_dim", dimStr);
                configs.emplace_back("vector_database.embedding_dim", dimStr);
                configs.emplace_back("vector_index.dimension", dimStr);
            }

            auto result = setMultipleConfigs(configs);
            if (!result)
                return result;

            std::cout << ui::status_ok("Preferred embedding model set to: " + embeddingModel_)
                      << "\n";

            // Show dimension info
            if (dim) {
                std::cout << "  Embedding dimension updated to: " << *dim << "\n";
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeEmbeddingsTune() {
        try {
            std::vector<std::pair<std::string, std::string>> configs;
            std::string description;

            // Apply preset configurations
            if (embeddingPreset_ == "performance") {
                configs = {{"embeddings.batch_size", "32"},
                           {"embeddings.generation_delay_ms", "500"}};
                // Keep user's preferred model unless unset
                if (parseSimpleToml(getConfigPath())["embeddings.preferred_model"].empty()) {
                    configs.push_back({"embeddings.preferred_model", "nomic-embed-text-v1.5"});
                }
                description = "Performance preset applied\n"
                              "  - Larger batch size (32)\n"
                              "  - Faster processing (500ms delay)\n"
                              "  - Lightweight model (MiniLM)";
            } else if (embeddingPreset_ == "quality") {
                configs = {{"embeddings.batch_size", "8"},
                           {"embeddings.generation_delay_ms", "2000"},
                           {"embeddings.preferred_model", "all-mpnet-base-v2"}};
                description = "Quality preset applied\n"
                              "  - Smaller batch size (8)\n"
                              "  - Slower processing (2000ms delay)\n"
                              "  - High-quality model (MPNet)";
            } else if (embeddingPreset_ == "balanced") {
                configs = {{"embeddings.batch_size", "16"},
                           {"embeddings.generation_delay_ms", "1000"},
                           {"embeddings.preferred_model", "all-MiniLM-L6-v2"}};
                description = "Balanced preset applied\n"
                              "  - Medium batch size (16)\n"
                              "  - Moderate processing (1000ms delay)\n"
                              "  - Efficient model (MiniLM)";
            } else {
                return Error{ErrorCode::InvalidArgument, "Unknown preset: " + embeddingPreset_};
            }

            auto result = setMultipleConfigs(configs);
            if (!result)
                return result;

            std::cout << ui::status_ok(description) << "\n";
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    std::string normalizePathTreeMode(const std::string& value, const std::string& fallback) const {
        if (value.empty())
            return fallback;
        std::string mode = value;
        std::ranges::transform(mode, mode.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (mode == "fallback" || mode == "preferred")
            return mode;
        return fallback;
    }

    Result<void> executePathTreeEnable() {
        auto mode = normalizePathTreeMode(pathTreeMode_, "preferred");

        auto result = writeConfigValue("search.path_tree.enable", "true");
        if (!result)
            return result;

        result = writeConfigValue("search.path_tree.mode", mode);
        if (!result)
            return result;

        std::cout << ui::status_ok("Path-tree traversal enabled") << "\n";
        std::cout << "  Mode: " << mode << "\n";
        if (mode == "fallback") {
            std::cout << "  Baseline scans remain as a safety net when tree nodes are missing.\n";
        } else {
            std::cout << "  Path-tree results will be preferred for eligible workloads.\n";
        }

        pathTreeMode_.clear();
        return Result<void>();
    }

    Result<void> executePathTreeDisable() {
        return setBooleanConfig("search.path_tree.enable", false, "Path-tree traversal disabled",
                                "Path-tree traversal disabled\n"
                                "  Workloads will continue to use the legacy scan engine.");
    }

    Result<void> executePathTreeMode() {
        try {
            auto mode = normalizePathTreeMode(pathTreeMode_, "");
            if (mode.empty()) {
                return Error{ErrorCode::InvalidArgument,
                             "Mode must be either 'fallback' or 'preferred'"};
            }

            auto result = writeConfigValue("search.path_tree.mode", mode);
            if (!result)
                return result;

            std::cout << ui::status_ok("Path-tree mode set to: " + mode) << "\n";
            if (mode == "fallback") {
                std::cout
                    << "  Baseline scans will still be used when tree coverage is incomplete.\n";
            } else {
                std::cout << "  Path-tree will be preferred whenever the index is available.\n";
            }

            pathTreeMode_.clear();
            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executePathTreeStatus() {
        try {
            auto configPath = getConfigPath();
            bool hasConfig = fs::exists(configPath);
            auto config = parseSimpleToml(configPath);

            auto toLower = [](std::string v) {
                std::ranges::transform(v, v.begin(), [](unsigned char c) {
                    return static_cast<char>(std::tolower(c));
                });
                return v;
            };

            bool enabled = false;
            if (auto it = config.find("search.path_tree.enable"); it != config.end()) {
                auto lowered = toLower(it->second);
                enabled = (lowered == "true" || lowered == "1" || lowered == "yes");
            }

            std::string mode = "fallback";
            if (auto it = config.find("search.path_tree.mode");
                it != config.end() && !it->second.empty()) {
                mode = normalizePathTreeMode(it->second, mode);
            }

            std::cout << ui::section_header("Path-tree traversal configuration") << "\n";
            std::cout << "Config file: " << configPath
                      << (hasConfig ? "" : " (not found, showing defaults)") << "\n";
            std::cout << "Enabled    : "
                      << (enabled ? ui::status_ok("yes") : ui::status_error("no")) << "\n";
            std::cout << "Mode       : " << mode << "\n";

            if (!enabled) {
                std::cout << "\nEnable with: yams config search path-tree enable\n";
            } else if (mode == "fallback") {
                std::cout << "\nMode note: fallback keeps baseline scans as a safety net.\n";
            } else {
                std::cout
                    << "\nMode note: preferred routes eligible workloads through the tree first.\n";
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    void validateCompressionConfig(const std::map<std::string, std::string>& config,
                                   std::vector<std::string>& errors) {
        // Check compression level ranges
        if (config.find("compression.zstd_level") != config.end()) {
            try {
                int level = std::stoi(config.at("compression.zstd_level"));
                if (level < 1 || level > 22) {
                    errors.push_back("compression.zstd_level must be between 1 and 22");
                }
            } catch (...) {
                errors.push_back("compression.zstd_level must be a valid number");
            }
        }

        if (config.find("compression.lzma_level") != config.end()) {
            try {
                int level = std::stoi(config.at("compression.lzma_level"));
                if (level < 0 || level > 9) {
                    errors.push_back("compression.lzma_level must be between 0 and 9");
                }
            } catch (...) {
                errors.push_back("compression.lzma_level must be a valid number");
            }
        }

        // Check algorithm validity
        if (config.find("compression.algorithm") != config.end()) {
            const auto& algo = config.at("compression.algorithm");
            if (algo != "zstd" && algo != "lzma") {
                errors.push_back("compression.algorithm must be 'zstd' or 'lzma'");
            }
        }

        // Check boolean values
        for (const auto& [key, value] : config) {
            if ((key == "compression.enable" || key == "compression.async_compression") &&
                value != "true" && value != "false") {
                errors.push_back(key + " must be 'true' or 'false'");
            }
        }
    }

    Result<void> executeMigrate() {
        try {
            auto configPath = getConfigPath();
            config::ConfigMigrator migrator;

            // Check if migration is needed
            auto needsResult = migrator.needsMigration(configPath);
            if (!needsResult) {
                return Error{needsResult.error()};
            }

            if (!needsResult.value()) {
                std::cout << ui::status_ok("Configuration is already at version 2") << "\n";
                // Still perform additive update of any newly introduced v2 keys
                auto added = migrator.updateV2SchemaAdditive(configPath, !noBackup_, false);
                if (!added) {
                    return Error{added.error()};
                }
                if (added.value().empty()) {
                    std::cout << "  No new keys to add\n";
                } else {
                    std::cout << "  Added " << added.value().size() << " new key(s):\n";
                    for (const auto& k : added.value()) {
                        std::cout << "    - " << k << "\n";
                    }
                }
                return Result<void>();
            }

            // Get current version
            auto versionResult = migrator.getConfigVersion(configPath);
            if (versionResult) {
                std::cout << "Current config version: " << versionResult.value().toString() << "\n";
            } else {
                std::cout << "Current config version: 1.0.0 (assumed)\n";
            }

            std::cout << "Migrating to version 2...\n";

            // Perform migration
            auto migrateResult = migrator.migrateToV2(configPath, !noBackup_);
            if (!migrateResult) {
                return Error{migrateResult.error()};
            }

            std::cout << ui::status_ok("Successfully migrated configuration to v2") << "\n";
            std::cout << "  Config file: " << configPath << "\n";

            if (!noBackup_) {
                std::cout << "  Backup created with .backup.* extension\n";
            }

            // Validate new config
            auto validateResult = migrator.validateV2Config(configPath);
            if (!validateResult) {
                std::cout << ui::status_warning("Config validation failed: ")
                          << validateResult.error().message << "\n";
            } else {
                std::cout << ui::status_ok("Configuration validated successfully") << "\n";
            }

            std::cout << "\nNew features available in v2:\n";
            std::cout << "  • Vector database configuration\n";
            std::cout << "  • Embedding service settings\n";
            std::cout << "  • MCP server configuration\n";
            std::cout << "  • WAL (Write-Ahead Logging)\n";
            std::cout << "  • Experimental features section\n";
            std::cout << "  • Performance tuning options\n";
            std::cout << "\nEdit " << configPath << " to customize these settings.\n";

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeUpdate() {
        try {
            auto configPath = getConfigPath();
            config::ConfigMigrator migrator;

            if (!fs::exists(configPath)) {
                std::cout << ui::status_error("No configuration file found at: " +
                                              configPath.string())
                          << "\n";
                std::cout << "  Run 'yams config migrate' to create a v2 config\n";
                return Error{ErrorCode::FileNotFound, "Config file not found"};
            }

            auto versionResult = migrator.getConfigVersion(configPath);
            if (!versionResult) {
                std::cout << ui::status_warning("Cannot determine config version; attempting "
                                                "additive update for v2 keys")
                          << "\n";
            } else if (versionResult.value().major < 2) {
                std::cout << ui::status_warning("Config is v1; run 'yams config migrate' first")
                          << "\n";
                return Error{ErrorCode::InvalidData, "Config is not v2"};
            }

            auto added = migrator.updateV2SchemaAdditive(configPath, !noBackup_, dryRun_);
            if (!added) {
                return Error{added.error()};
            }

            if (added.value().empty()) {
                if (dryRun_) {
                    std::cout << "No changes (up-to-date)\n";
                } else {
                    std::cout << ui::status_ok("Configuration already has all known v2 keys")
                              << "\n";
                }
            } else {
                if (dryRun_) {
                    std::cout << "Would add " << added.value().size() << " key(s):\n";
                } else {
                    std::cout << ui::status_ok("Added " + std::to_string(added.value().size()) +
                                               " key(s):")
                              << "\n";
                }
                for (const auto& k : added.value()) {
                    std::cout << "  - " << k << "\n";
                }
                if (!dryRun_ && !noBackup_) {
                    std::cout << "  Backup created with .backup.* extension\n";
                }
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeCheck() {
        try {
            auto configPath = getConfigPath();
            config::ConfigMigrator migrator;

            if (!fs::exists(configPath)) {
                std::cout << ui::status_error("No configuration file found at: " +
                                              configPath.string())
                          << "\n";
                std::cout << "  Run 'yams config migrate' to create a v2 config\n";
                return Result<void>();
            }

            // Get current version
            auto versionResult = migrator.getConfigVersion(configPath);
            if (!versionResult) {
                std::cout << ui::status_warning("Cannot determine config version") << "\n";
                std::cout << "  Assuming v1 configuration\n";
                std::cout << "  Run 'yams config migrate' to upgrade to v2\n";
                return Result<void>();
            }

            auto version = versionResult.value();
            std::cout << "Configuration version: " << version.toString() << "\n";
            std::cout << "Latest version: 2.0.0\n";

            // Check if migration is needed
            auto needsResult = migrator.needsMigration(configPath);
            if (!needsResult) {
                return Error{needsResult.error()};
            }

            if (needsResult.value()) {
                std::cout << "\n" << ui::status_warning("Migration needed!") << "\n";
                std::cout << "  Your configuration is at version " << version.toString() << "\n";
                std::cout << "  Run 'yams config migrate' to upgrade to v2\n";
                std::cout << "\nVersion 2 adds support for:\n";
                std::cout << "  • Vector embeddings and semantic search\n";
                std::cout << "  • MCP server integration\n";
                std::cout << "  • Advanced performance tuning\n";
                std::cout << "  • Experimental BERT NER features\n";
                std::cout << "  • Smart chunking strategies\n";
            } else {
                std::cout << "\n" << ui::status_ok("Configuration is up to date") << "\n";

                // Validate current config
                auto validateResult = migrator.validateV2Config(configPath);
                if (!validateResult) {
                    std::cout << ui::status_warning("Config validation issues found:") << "\n";
                    std::cout << "  " << validateResult.error().message << "\n";
                } else {
                    std::cout << ui::status_ok("Configuration is valid") << "\n";
                }
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeStorage() {
        try {
            if (!storageEngine_.empty()) {
                auto r = writeConfigValue("storage.engine", storageEngine_);
                if (!r)
                    return r;
            }
            if (!s3Url_.empty()) {
                auto r = writeConfigValue("storage.s3.url", s3Url_);
                if (!r)
                    return r;
            }
            if (!s3Region_.empty()) {
                auto r = writeConfigValue("storage.s3.region", s3Region_);
                if (!r)
                    return r;
            }
            if (!s3Endpoint_.empty()) {
                auto r = writeConfigValue("storage.s3.endpoint", s3Endpoint_);
                if (!r)
                    return r;
            }
            if (!s3AccessKey_.empty()) {
                auto r = writeConfigValue("storage.s3.access_key", s3AccessKey_);
                if (!r)
                    return r;
            }
            if (!s3SecretKey_.empty()) {
                auto r = writeConfigValue("storage.s3.secret_key", s3SecretKey_);
                if (!r)
                    return r;
            }
            if (s3UsePathStyle_) {
                auto r = writeConfigValue("storage.s3.use_path_style", "true");
                if (!r)
                    return r;
            }

            std::cout << "Storage configuration updated successfully." << std::endl;
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    fs::path getGrammarPath() const { return yams::config::get_data_dir() / "grammars"; }

    Result<void> executeGrammarList() {
        try {
            auto grammarPath = getGrammarPath();

            std::cout << ui::section_header("Tree-sitter Grammar Status") << "\n";
            std::cout << "Installation path: " << grammarPath << "\n\n";

            // Supported languages (must match grammar_loader.h kGrammarRepos)
            std::vector<std::pair<std::string, std::string>> languages = {
                {"c", "libtree-sitter-c.so"},
                {"cpp", "libtree-sitter-cpp.so"},
                {"python", "libtree-sitter-python.so"},
                {"rust", "libtree-sitter-rust.so"},
                {"go", "libtree-sitter-go.so"},
                {"javascript", "libtree-sitter-javascript.so"},
                {"typescript", "libtree-sitter-typescript.so"},
                {"java", "libtree-sitter-java.so"},
                {"csharp", "libtree-sitter-c-sharp.so"},
                {"php", "libtree-sitter-php.so"},
                {"kotlin", "libtree-sitter-kotlin.so"},
                {"perl", "libtree-sitter-perl.so"},
                {"r", "libtree-sitter-r.so"},
                {"sql", "libtree-sitter-sql.so"},
                {"solidity", "libtree-sitter-solidity.so"},
                {"dart", "libtree-sitter-dart.so"},
                {"p4", "libtree-sitter-p4.so"},
                {"zig", "libtree-sitter-zig.so"},
                {"swift", "libtree-sitter-swift.so"}};

            std::cout << "Supported Languages:\n";
            for (const auto& [lang, lib] : languages) {
                fs::path libPath = grammarPath / lib;
                bool installed = fs::exists(libPath);
                std::cout << "  " << std::left << std::setw(15) << lang
                          << (installed ? ui::status_ok("installed")
                                        : ui::status_error("not installed"))
                          << "\n";
            }

            std::cout << "\nCommands:\n";
            std::cout << "  yams config grammar download <language>  - Download grammar\n";
            std::cout << "  yams config grammar path                 - Show installation path\n";

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeGrammarDownload() {
        try {
            // Supported grammars mapped to their repository URLs
            // Must match grammar_loader.h kGrammarRepos
            std::map<std::string, std::string> grammarRepos = {
                {"c", "tree-sitter/tree-sitter-c"},
                {"cpp", "tree-sitter/tree-sitter-cpp"},
                {"python", "tree-sitter/tree-sitter-python"},
                {"rust", "tree-sitter/tree-sitter-rust"},
                {"go", "tree-sitter/tree-sitter-go"},
                {"javascript", "tree-sitter/tree-sitter-javascript"},
                {"typescript", "tree-sitter/tree-sitter-typescript"},
                {"java", "tree-sitter/tree-sitter-java"},
                {"csharp", "tree-sitter/tree-sitter-c-sharp"},
                {"php", "tree-sitter/tree-sitter-php"},
                {"kotlin", "fwcd/tree-sitter-kotlin"},
                {"perl", "tree-sitter-perl/tree-sitter-perl"},
                {"r", "r-lib/tree-sitter-r"},
                {"sql", "DerekStride/tree-sitter-sql"},
                {"solidity", "JoranHonig/tree-sitter-solidity"},
                {"dart", "UserNobody14/tree-sitter-dart"},
                {"p4", "prona-p4-learning-platform/tree-sitter-p4"},
                {"zig", "maxxnino/tree-sitter-zig"},
                {"swift", "alex-pinkus/tree-sitter-swift"}};

            if (grammarRepos.find(grammarLanguage_) == grammarRepos.end()) {
                std::cout << ui::status_error("Unsupported language: " + grammarLanguage_) << "\n";
                std::cout << "Supported: c, cpp, python, rust, go, javascript, typescript, java, "
                          << "csharp, php, kotlin, perl, r, sql, solidity, dart, p4, zig, swift\n";
                return Error{ErrorCode::InvalidArgument, "Unsupported language"};
            }

            auto grammarPath = getGrammarPath();
            fs::create_directories(grammarPath);

            std::cout << "Downloading tree-sitter grammar for " << grammarLanguage_ << "...\n";
            std::cout << "Repository: " << grammarRepos[grammarLanguage_] << "\n";
            std::cout << "Target: " << grammarPath << "\n\n";

            // Instructions since we can't directly download/compile here
            std::cout << "Manual installation steps:\n";
            std::cout << "1. Clone the repository:\n";
            std::cout << "   git clone https://github.com/" << grammarRepos[grammarLanguage_]
                      << " /tmp/tree-sitter-" << grammarLanguage_ << "\n\n";
            std::cout << "2. Build the shared library:\n";
            std::cout << "   cd /tmp/tree-sitter-" << grammarLanguage_ << "\n";
            std::cout << "   gcc -shared -fPIC -o libtree-sitter-" << grammarLanguage_
                      << ".so src/parser.c -I.\n\n";
            std::cout << "3. Copy to YAMS grammar directory:\n";
            std::cout << "   cp libtree-sitter-" << grammarLanguage_ << ".so " << grammarPath
                      << "/\n\n";

            std::cout << "Or set environment variable:\n";
            std::cout << "   export YAMS_TS_" << grammarLanguage_ << "_LIB=/path/to/libtree-sitter-"
                      << grammarLanguage_ << ".so\n\n";

            std::cout << ui::status_warning(
                             "Note: Automatic downloads will be added in a future update.")
                      << "\n";
            std::cout << "For now, follow the manual steps above or use system packages.\n";

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeGrammarPath() {
        try {
            auto grammarPath = getGrammarPath();
            std::cout << grammarPath << "\n";
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeGrammarAutoEnable() {
        try {
            auto result =
                writeConfigValue("plugins.symbol_extraction.auto_download_grammars", "true");
            if (!result) {
                return result;
            }
            std::cout << ui::status_ok("Automatic grammar downloads enabled") << "\n";
            std::cout << "Grammars will be downloaded automatically when needed.\n";
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeGrammarAutoDisable() {
        try {
            auto result =
                writeConfigValue("plugins.symbol_extraction.auto_download_grammars", "false");
            if (!result) {
                return result;
            }
            std::cout << ui::status_ok("Automatic grammar downloads disabled") << "\n";
            std::cout << "You can manually download grammars using: yams config grammar download "
                         "<language>\n";
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }
};

// Factory function
std::unique_ptr<ICommand> createConfigCommand() {
    return std::make_unique<ConfigCommand>();
}

} // namespace yams::cli
