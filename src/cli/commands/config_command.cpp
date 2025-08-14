#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <iomanip>

namespace yams::cli {

namespace fs = std::filesystem;

class ConfigCommand : public ICommand {
public:
    std::string getName() const override { return "config"; }
    
    std::string getDescription() const override { 
        return "Manage YAMS configuration settings";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("config", getDescription());
        cmd->require_subcommand();
        
        // Get subcommand
        auto* getCmd = cmd->add_subcommand("get", "Get a configuration value");
        getCmd->add_option("key", key_, "Configuration key to retrieve")
            ->required();
        getCmd->callback([this]() {
            auto result = executeGet();
            if (!result) {
                spdlog::error("Config get failed: {}", result.error().message);
                std::exit(1);
            }
        });
        
        // Set subcommand  
        auto* setCmd = cmd->add_subcommand("set", "Set a configuration value");
        setCmd->add_option("key", key_, "Configuration key")
            ->required();
        setCmd->add_option("value", value_, "Configuration value")
            ->required();
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
            if (line.empty() || line[0] == '#') continue;
            
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
                std::cout << std::left << std::setw(30) << key 
                         << " = " << value << "\n";
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
                    if (!first) std::cout << ",\n";
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
                return Error{ErrorCode::InvalidArgument, 
                           key + " must be 'true' or 'false'"};
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
                    return Error{ErrorCode::InvalidArgument, 
                               "compression.max_concurrent_compressions must be between 1 and 32"};
                }
            } catch (...) {
                return Error{ErrorCode::InvalidArgument, 
                           "compression.max_concurrent_compressions must be a number between 1 and 32"};
            }
        } else if (key.starts_with("compression.") && 
                  (key.ends_with("_threshold") || key.ends_with("_above") || 
                   key.ends_with("_below") || key.ends_with("_days"))) {
            try {
                int num = std::stoi(value);
                if (num < 0) {
                    return Error{ErrorCode::InvalidArgument, 
                               key + " must be a positive number"};
                }
            } catch (...) {
                return Error{ErrorCode::InvalidArgument, 
                           key + " must be a positive number"};
            }
        }
        
        return Result<void>();
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
};

// Factory function
std::unique_ptr<ICommand> createConfigCommand() {
    return std::make_unique<ConfigCommand>();
}

} // namespace yams::cli