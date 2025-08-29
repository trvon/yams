#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
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
    bool noBackup_ = false;
    bool dryRun_ = false;

    fs::path getConfigPath() const {
        if (!configPath_.empty()) {
            return fs::path(configPath_);
        }

        const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
        const char* homeEnv = std::getenv("HOME");

        fs::path configHome;
        if (xdgConfigHome) {
            configHome = fs::path(xdgConfigHome);
        } else if (homeEnv) {
            configHome = fs::path(homeEnv) / ".config";
        } else {
            return fs::path("~/.config") / "yams" / "config.toml";
        }

        return configHome / "yams" / "config.toml";
    }

    // Simple TOML parser for reading config
    std::map<std::string, std::string> parseSimpleToml(const fs::path& path) const {
        std::map<std::string, std::string> config;
        std::ifstream file(path);
        if (!file) {
            return config;
        }

        std::string line;
        std::string currentSection;

        while (std::getline(file, line)) {
            // Skip comments and empty lines
            if (line.empty() || line[0] == '#')
                continue;

            // Check for section headers
            if (line[0] == '[') {
                size_t end = line.find(']');
                if (end != std::string::npos) {
                    currentSection = line.substr(1, end - 1);
                    if (!currentSection.empty()) {
                        currentSection += ".";
                    }
                }
                continue;
            }

            // Parse key-value pairs
            size_t eq = line.find('=');
            if (eq != std::string::npos) {
                std::string key = line.substr(0, eq);
                std::string value = line.substr(eq + 1);

                // Trim whitespace
                key.erase(0, key.find_first_not_of(" \t"));
                key.erase(key.find_last_not_of(" \t") + 1);
                value.erase(0, value.find_first_not_of(" \t"));
                value.erase(value.find_last_not_of(" \t") + 1);

                // Remove quotes if present
                if (value.size() >= 2 && value[0] == '"' && value.back() == '"') {
                    value = value.substr(1, value.size() - 2);
                }

                // Remove comments from value
                size_t comment = value.find('#');
                if (comment != std::string::npos) {
                    value = value.substr(0, comment);
                    // Trim again after removing comment
                    value.erase(value.find_last_not_of(" \t") + 1);
                }

                config[currentSection + key] = value;
            }
        }

        return config;
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

            // For now, just display what would be set
            std::cout << "Would set " << key_ << " = " << value_ << "\n";
            std::cout << "Note: Config modification not yet fully implemented.\n";
            std::cout << "Please edit " << configPath << " manually.\n";

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
            std::cout << "─────────────────────\n";

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
                std::cout << "✓ Configuration is valid\n";
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
                std::cerr << "✗ Configuration validation failed:\n";
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
        try {
            auto configPath = getConfigPath();

            // Ensure config directory exists
            fs::create_directories(configPath.parent_path());

            // Read existing config
            auto config = parseSimpleToml(configPath);
            config[key] = value;

            // Write back to file
            std::ofstream file(configPath);
            if (!file) {
                return Error{ErrorCode::WriteError,
                             "Cannot write to config file: " + configPath.string()};
            }

            // Simple TOML writing (organized by sections)
            std::map<std::string, std::map<std::string, std::string>> sections;

            for (const auto& [fullKey, val] : config) {
                size_t dot = fullKey.find('.');
                if (dot != std::string::npos) {
                    std::string section = fullKey.substr(0, dot);
                    std::string subkey = fullKey.substr(dot + 1);
                    sections[section][subkey] = val;
                } else {
                    sections[""][fullKey] = val;
                }
            }

            // Write sections
            for (const auto& [section, values] : sections) {
                if (!section.empty()) {
                    file << "\n[" << section << "]\n";
                }
                for (const auto& [k, v] : values) {
                    file << k << " = \"" << v << "\"\n";
                }
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    // Check if embedding models are available
    std::vector<std::string> getAvailableModels() {
        std::vector<std::string> models;
        const char* home = std::getenv("HOME");
        if (!home)
            return models;

        fs::path modelsPath = fs::path(home) / ".yams" / "models";
        if (fs::exists(modelsPath)) {
            for (const auto& entry : fs::directory_iterator(modelsPath)) {
                if (entry.is_directory()) {
                    fs::path modelFile = entry.path() / "model.onnx";
                    if (fs::exists(modelFile)) {
                        models.push_back(entry.path().filename().string());
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
                std::cout << "⚠ No embedding models found.\n";
                std::cout << "Download a model first: yams model --download all-MiniLM-L6-v2\n";
                return Error{ErrorCode::NotFound, "No embedding models available"};
            }

            // Set default configurations for auto-generation
            auto result = writeConfigValue("embeddings.auto_generate", "true");
            if (!result)
                return result;

            result = writeConfigValue("embeddings.preferred_model", models[0]);
            if (!result)
                return result;

            result = writeConfigValue("embeddings.batch_size", "16");
            if (!result)
                return result;

            result = writeConfigValue("embeddings.generation_delay_ms", "1000");
            if (!result)
                return result;

            std::cout << "✓ Automatic embedding generation enabled\n";
            std::cout << "  Model: " << models[0] << "\n";
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
        try {
            auto result = writeConfigValue("embeddings.auto_generate", "false");
            if (!result)
                return result;

            std::cout << "✓ Automatic embedding generation disabled\n";
            std::cout << "  Use 'yams repair --embeddings' to manually generate embeddings\n";

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeEmbeddingsStatus() {
        try {
            auto configPath = getConfigPath();
            auto config = parseSimpleToml(configPath);
            auto models = getAvailableModels();

            std::cout << "Embedding Configuration Status\n";
            std::cout << "==============================\n\n";

            // Auto-generation status
            bool autoEnabled = config["embeddings.auto_generate"] == "true";
            std::cout << "Auto-generation: " << (autoEnabled ? "✓ Enabled" : "✗ Disabled") << "\n";

            // Available models
            std::cout << "Available models: ";
            if (models.empty()) {
                std::cout << "None\n";
                std::cout << "  → Download: yams model --download all-MiniLM-L6-v2\n";
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
            if (std::find(models.begin(), models.end(), embeddingModel_) == models.end()) {
                std::cout << "✗ Model '" << embeddingModel_ << "' not found\n";
                std::cout << "\nAvailable models:\n";
                for (const auto& model : models) {
                    std::cout << "  - " << model << "\n";
                }
                if (models.empty()) {
                    std::cout << "  (none - download with: yams model --download <model-name>)\n";
                }
                return Error{ErrorCode::NotFound, "Model not available: " + embeddingModel_};
            }

            auto result = writeConfigValue("embeddings.preferred_model", embeddingModel_);
            if (!result)
                return result;

            std::cout << "✓ Preferred embedding model set to: " << embeddingModel_ << "\n";

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    Result<void> executeEmbeddingsTune() {
        try {
            // Apply preset configurations
            if (embeddingPreset_ == "performance") {
                auto result = writeConfigValue("embeddings.batch_size", "32");
                if (!result)
                    return result;
                result = writeConfigValue("embeddings.generation_delay_ms", "500");
                if (!result)
                    return result;
                result = writeConfigValue("embeddings.preferred_model", "all-MiniLM-L6-v2");
                if (!result)
                    return result;

                std::cout << "✓ Performance preset applied\n";
                std::cout << "  - Larger batch size (32)\n";
                std::cout << "  - Faster processing (500ms delay)\n";
                std::cout << "  - Lightweight model (MiniLM)\n";

            } else if (embeddingPreset_ == "quality") {
                auto result = writeConfigValue("embeddings.batch_size", "8");
                if (!result)
                    return result;
                result = writeConfigValue("embeddings.generation_delay_ms", "2000");
                if (!result)
                    return result;
                result = writeConfigValue("embeddings.preferred_model", "all-mpnet-base-v2");
                if (!result)
                    return result;

                std::cout << "✓ Quality preset applied\n";
                std::cout << "  - Smaller batch size (8)\n";
                std::cout << "  - Slower processing (2000ms delay)\n";
                std::cout << "  - High-quality model (MPNet)\n";

            } else if (embeddingPreset_ == "balanced") {
                auto result = writeConfigValue("embeddings.batch_size", "16");
                if (!result)
                    return result;
                result = writeConfigValue("embeddings.generation_delay_ms", "1000");
                if (!result)
                    return result;
                result = writeConfigValue("embeddings.preferred_model", "all-MiniLM-L6-v2");
                if (!result)
                    return result;

                std::cout << "✓ Balanced preset applied\n";
                std::cout << "  - Medium batch size (16)\n";
                std::cout << "  - Moderate processing (1000ms delay)\n";
                std::cout << "  - Efficient model (MiniLM)\n";
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
                std::cout << "✓ Configuration is already at version 2\n";
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

            std::cout << "✓ Successfully migrated configuration to v2\n";
            std::cout << "  Config file: " << configPath << "\n";

            if (!noBackup_) {
                std::cout << "  Backup created with .backup.* extension\n";
            }

            // Validate new config
            auto validateResult = migrator.validateV2Config(configPath);
            if (!validateResult) {
                std::cout << "⚠ Warning: Config validation failed: "
                          << validateResult.error().message << "\n";
            } else {
                std::cout << "✓ Configuration validated successfully\n";
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
                std::cout << "✗ No configuration file found at: " << configPath << "\n";
                std::cout << "  Run 'yams config migrate' to create a v2 config\n";
                return Error{ErrorCode::FileNotFound, "Config file not found"};
            }

            auto versionResult = migrator.getConfigVersion(configPath);
            if (!versionResult) {
                std::cout << "⚠ Cannot determine config version; attempting additive update for v2 "
                             "keys\n";
            } else if (versionResult.value().major < 2) {
                std::cout << "⚠ Config is v1; run 'yams config migrate' first\n";
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
                    std::cout << "✓ Configuration already has all known v2 keys\n";
                }
            } else {
                if (dryRun_) {
                    std::cout << "Would add " << added.value().size() << " key(s):\n";
                } else {
                    std::cout << "✓ Added " << added.value().size() << " key(s):\n";
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
                std::cout << "✗ No configuration file found at: " << configPath << "\n";
                std::cout << "  Run 'yams config migrate' to create a v2 config\n";
                return Result<void>();
            }

            // Get current version
            auto versionResult = migrator.getConfigVersion(configPath);
            if (!versionResult) {
                std::cout << "⚠ Cannot determine config version\n";
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
                std::cout << "\n⚠ Migration needed!\n";
                std::cout << "  Your configuration is at version " << version.toString() << "\n";
                std::cout << "  Run 'yams config migrate' to upgrade to v2\n";
                std::cout << "\nVersion 2 adds support for:\n";
                std::cout << "  • Vector embeddings and semantic search\n";
                std::cout << "  • MCP server integration\n";
                std::cout << "  • Advanced performance tuning\n";
                std::cout << "  • Experimental BERT NER features\n";
                std::cout << "  • Smart chunking strategies\n";
            } else {
                std::cout << "\n✓ Configuration is up to date\n";

                // Validate current config
                auto validateResult = migrator.validateV2Config(configPath);
                if (!validateResult) {
                    std::cout << "⚠ Warning: Config validation issues found:\n";
                    std::cout << "  " << validateResult.error().message << "\n";
                } else {
                    std::cout << "✓ Configuration is valid\n";
                }
            }

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