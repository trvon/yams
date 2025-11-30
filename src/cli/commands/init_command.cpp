#include <yams/cli/command.h>
#include <yams/cli/prompt_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/downloader/downloader.hpp>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>
#include <fmt/format.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rand.h>

namespace yams::cli {

namespace fs = std::filesystem;

// static constexpr std::string_view DEFAULT_STORAGE_ENGINE = "local";  // Currently unused
static constexpr size_t DEFAULT_API_KEY_BYTES = 32;

// Available models for vector database
struct EmbeddingModel {
    std::string name;
    std::string url;
    std::string description;
    size_t size_mb;
    int dimensions;
};

static const std::vector<EmbeddingModel> EMBEDDING_MODELS = {
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight model for semantic search", 90, 384},
    {"multi-qa-MiniLM-L6-cos-v1",
     "https://huggingface.co/sentence-transformers/multi-qa-MiniLM-L6-cos-v1/resolve/main/onnx/"
     "model.onnx",
     "Optimized for semantic search on QA pairs (215M training samples)", 90, 384}};

class InitCommand : public ICommand {
public:
    std::string getName() const override { return "init"; }

    std::string getDescription() const override {
        return "Initialize YAMS storage and configuration (interactive or non-interactive)";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("init", getDescription());

        cmd->add_flag("--non-interactive", nonInteractive_,
                      "Run without prompts, using defaults and flags");
        cmd->add_flag("--auto", autoInit_,
                      "Auto-initialize with all defaults for containerized/headless environments "
                      "(enables vector DB, plugins, default model; skips S3)");
        cmd->add_flag("--force", force_, "Overwrite existing config/keys if already initialized");
        cmd->add_flag("--no-keygen", noKeygen_, "Skip authentication key generation");
        cmd->add_flag("--print", printConfig_,
                      "Print resulting configuration to stdout (secrets masked)");
        cmd->add_flag("--enable-plugins", enablePlugins_,
                      "Create and trust a local plugins directory (~/.local/lib/yams/plugins)");

        // Note: storage directory is a global option (--data-dir/--storage). Users can pass it
        // globally: yams --storage /path init We'll also allow overriding interactively.

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Initialization failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        try {
            // Handle --auto flag: sets sensible defaults for containerized environments
            if (autoInit_) {
                nonInteractive_ = true;
                enablePlugins_ = true;
                // noKeygen_ remains false (generate keys by default)
                spdlog::info("Auto-initialization mode: using defaults for headless environment");
            }

            // 1) Resolve directories using platform-specific helpers
            auto dataPath = cli_->getDataPath();

            // Use platform-specific config directory
            fs::path configDir = yams::config::get_config_dir();
            fs::path keysDir = configDir / "keys";
            fs::path configPath = configDir / "config.toml";

            if (!nonInteractive_) {
                dataPath = promptForDataDir(dataPath);
                if (!noKeygen_) {
                    noKeygen_ = !prompt_yes_no("Generate authentication keys? [Y/n]: ",
                                               YesNoOptions{.defaultYes = true});
                }
            }

            // Ensure we use the resolved dataPath in CLI
            cli_->setDataPath(dataPath);

            // 2) Create base directories
            createDirectoryIfMissing(dataPath);
            createDirectoryIfMissing(configDir);
            createDirectoryIfMissing(keysDir);

            // 3) Idempotency check
            const fs::path dbFile = dataPath / "yams.db";
            const fs::path storageDir = dataPath / "storage";
            const bool alreadyInitialized =
                fs::exists(dbFile) && fs::exists(storageDir) && fs::exists(configPath);

            if (alreadyInitialized && !force_) {
                spdlog::info("YAMS is already initialized at {} (use --force to overwrite).",
                             dataPath.string());
                return Result<void>();
            }

            // 4) Initialize storage (database + content store)
            {
                auto ensured = cli_->ensureStorageInitialized();
                if (!ensured) {
                    return ensured;
                }
                spdlog::debug("Storage initialized at {}", dataPath.string());
            }

            // 5) Authentication key generation
            fs::path privateKeyPath = keysDir / "ed25519.pem";
            fs::path publicKeyPath = keysDir / "ed25519.pub";
            if (!noKeygen_) {
                auto kg = generateEd25519Keypair(privateKeyPath, publicKeyPath, force_);
                if (!kg) {
                    // Keys already exist - warn but continue
                    if (kg.error().code == ErrorCode::InvalidState) {
                        spdlog::info("Authentication keys already exist at {}", keysDir.string());
                    } else {
                        // Actual error - report but don't fail init
                        spdlog::warn("Key generation failed: {}", kg.error().message);
                    }
                } else {
                    spdlog::info("Authentication keys generated under {}", keysDir.string());
                }
            } else {
                spdlog::debug("Skipping key generation (--no-keygen)");
            }

            // 6) Vector Database Setup
            bool enableVectorDB = autoInit_; // Auto mode enables vector DB by default
            std::string selectedModel;
            if (autoInit_) {
                // Use the default model (first in the list) for auto-init
                selectedModel = EMBEDDING_MODELS[0].name;
                spdlog::info("Using default embedding model: {}", selectedModel);
            } else if (!nonInteractive_) {
                enableVectorDB =
                    prompt_yes_no("\nEnable vector database for semantic search? [Y/n]: ",
                                  YesNoOptions{.defaultYes = true});
                if (enableVectorDB) {
                    selectedModel = promptForModel(dataPath);
                }
            }

            // 6a) S3 Storage Setup
            bool useS3 = false;
            std::string s3Url, s3Region, s3Endpoint, s3AccessKey, s3SecretKey;
            bool s3UsePathStyle = false;
            if (!nonInteractive_) {
                useS3 =
                    prompt_yes_no("\nConfigure S3 as the storage backend? (default: local) [y/N]: ",
                                  YesNoOptions{.defaultYes = false});
                if (useS3) {
                    std::cout << "Enter S3 URL (e.g., s3://my-bucket/my-prefix): ";
                    std::getline(std::cin, s3Url);
                    std::cout << "Enter S3 region [us-east-1]: ";
                    std::getline(std::cin, s3Region);
                    if (s3Region.empty())
                        s3Region = "us-east-1";
                    std::cout << "Enter S3 endpoint (optional, for R2/MinIO): ";
                    std::getline(std::cin, s3Endpoint);
                    std::cout << "Enter S3 Access Key ID (optional, uses env var if blank): ";
                    std::getline(std::cin, s3AccessKey);
                    std::cout << "Enter S3 Secret Access Key (optional, uses env var if blank): ";
                    std::getline(std::cin, s3SecretKey);
                    s3UsePathStyle = prompt_yes_no("Use path-style addressing? (for MinIO) [y/N]: ",
                                                   YesNoOptions{.defaultYes = false});
                }
            }

            // 6b) Plugins setup (local user directory + trust)
            bool setupPlugins = enablePlugins_;
            if (!nonInteractive_) {
                setupPlugins =
                    prompt_yes_no("Enable plugins and create a local plugins dir? [Y/n]: ",
                                  YesNoOptions{.defaultYes = true});
            }

            // 7) Generate an initial API key
            std::string apiKeyHex = generateApiKey(DEFAULT_API_KEY_BYTES);

            // 8) Write config.toml (v2 format)
            auto migrator = std::make_unique<config::ConfigMigrator>();
            auto createResult = migrator->createDefaultV2Config(configPath);
            if (!createResult) {
                return createResult;
            }

            // Update the v2 config with user choices
            auto updateResult =
                updateV2Config(configPath, dataPath, privateKeyPath, publicKeyPath, apiKeyHex,
                               enableVectorDB, selectedModel, useS3, s3Url, s3Region, s3Endpoint,
                               s3AccessKey, s3SecretKey, s3UsePathStyle);
            if (!updateResult) {
                return updateResult;
            }

            if (printConfig_) {
                // Print the v2 config (with secrets masked)
                std::ifstream configFile(configPath);
                if (configFile) {
                    std::string line;
                    while (std::getline(configFile, line)) {
                        // Mask API keys in output
                        if (line.find("api_keys = ") != std::string::npos) {
                            std::cout << "api_keys = [\"" << maskApiKey(apiKeyHex) << "\"]"
                                      << std::endl;
                        } else {
                            std::cout << line << std::endl;
                        }
                    }
                    configFile.close();
                }
            }

            // Initialize vector database if enabled
            if (enableVectorDB) {
                spdlog::info("Initializing vector database...");

                vector::VectorDatabaseConfig vdbConfig;
                vdbConfig.database_path = (dataPath / "vectors.db").string();

                // Set embedding dimension based on selected model
                for (const auto& model : EMBEDDING_MODELS) {
                    if (model.name == selectedModel) {
                        vdbConfig.embedding_dim = model.dimensions;
                        break;
                    }
                }

                auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
                if (!vectorDb->initialize()) {
                    spdlog::warn("Failed to initialize vector database: {}",
                                 vectorDb->getLastError());
                    spdlog::warn("Vector database can be initialized later using 'yams repair'");
                } else {
                    spdlog::info("Vector database initialized successfully");
                    // Test that tables exist
                    if (vectorDb->tableExists()) {
                        spdlog::debug("Vector database tables created successfully");
                    }
                }
            }

            // Seed plugins directory and trust list if requested
            if (setupPlugins) {
                setupUserPluginsDirAndTrust();
                // Also set plugin_name_policy to "spec" to enforce canonical naming on this machine
                try {
                    std::ifstream in(configPath);
                    std::stringstream buf;
                    buf << in.rdbuf();
                    in.close();
                    std::string content = buf.str();
                    // Ensure [daemon] section exists; if not, append one
                    if (content.find("[daemon]") == std::string::npos) {
                        content.append("\n[daemon]\n");
                    }
                    // Insert or replace plugin_name_policy
                    auto secPos = content.find("[daemon]");
                    if (secPos != std::string::npos) {
                        auto nextSec = content.find("[", secPos + 1);
                        auto rangeEnd = (nextSec == std::string::npos) ? content.size() : nextSec;
                        auto keyPos = content.find("plugin_name_policy", secPos);
                        if (keyPos == std::string::npos || keyPos > rangeEnd) {
                            content.insert(rangeEnd,
                                           std::string("plugin_name_policy = \"spec\"\n"));
                        } else {
                            auto lineEnd = content.find("\n", keyPos);
                            if (lineEnd == std::string::npos)
                                lineEnd = content.size();
                            content.replace(keyPos, lineEnd - keyPos,
                                            "plugin_name_policy = \"spec\"");
                        }
                        std::ofstream outCfg(configPath, std::ios::trunc);
                        outCfg << content;
                        outCfg.close();
                        spdlog::info("Configured [daemon].plugin_name_policy = spec");
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("Skipping plugin_name_policy write: {}", e.what());
                }
            }

            spdlog::info("YAMS initialization complete.");
            spdlog::info("Config file: {}", configPath.string());
            spdlog::info("Data dir:    {}", dataPath.string());
            return Result<void>();
        } catch (const std::exception& ex) {
            return Error{ErrorCode::Unknown, std::string("Init error: ") + ex.what()};
        }
    }

private:
    // Helpers

    static std::string maskApiKey(const std::string& key) {
        constexpr size_t VISIBLE_PREFIX = 6;
        if (key.size() <= VISIBLE_PREFIX) {
            return std::string(key.size(), '*');
        }
        std::string masked = key.substr(0, VISIBLE_PREFIX);
        masked.append(key.size() - VISIBLE_PREFIX, '*');
        return masked;
    }

    static void createDirectoryIfMissing(const fs::path& p) {
        if (!fs::exists(p)) {
            fs::create_directories(p);
        }
    }

    fs::path promptForDataDir(const fs::path& current) {
        std::cout << "Storage directory [" << current.string() << "]: ";
        std::string line;
        std::getline(std::cin, line);
        if (line.empty())
            return current;

        fs::path chosen = fs::path(line);
        return chosen;
    }

    // Removed legacy promptYesNo (replaced by prompt_yes_no in prompt_util.h)

    static Result<void> updateV2Config(const fs::path& configPath, const fs::path& dataDir,
                                       const fs::path& privateKeyPath,
                                       const fs::path& publicKeyPath, const std::string& apiKey,
                                       bool enableVectorDB, const std::string& selectedModel,
                                       bool useS3, const std::string& s3Url,
                                       const std::string& s3Region, const std::string& s3Endpoint,
                                       const std::string& s3AccessKey,
                                       const std::string& s3SecretKey, bool s3UsePathStyle) {
        try {
            // Read the existing v2 config
            std::ifstream in(configPath);
            if (!in) {
                return Error{ErrorCode::InvalidState, "Failed to read config file"};
            }
            std::stringstream buffer;
            buffer << in.rdbuf();
            in.close();

            std::string content = buffer.str();

            // Update specific values in the config
            // This is a simple string replacement approach
            // In production, we'd use a proper TOML parser

            size_t pos;

            // Update data_dir
            pos = content.find("data_dir = ");
            if (pos != std::string::npos) {
                size_t endPos = content.find("\n", pos);
                if (endPos != std::string::npos) {
                    content.replace(pos, endPos - pos,
                                    "data_dir = \"" + escapeTomlString(dataDir.string()) + "\"");
                }
            }

            // Update auth keys
            pos = content.find("private_key_path = ");
            if (pos != std::string::npos) {
                size_t endPos = content.find("\n", pos);
                if (endPos != std::string::npos) {
                    content.replace(pos, endPos - pos,
                                    "private_key_path = \"" +
                                        escapeTomlString(privateKeyPath.string()) + "\"");
                }
            }

            pos = content.find("public_key_path = ");
            if (pos != std::string::npos) {
                size_t endPos = content.find("\n", pos);
                if (endPos != std::string::npos) {
                    content.replace(pos, endPos - pos,
                                    "public_key_path = \"" +
                                        escapeTomlString(publicKeyPath.string()) + "\"");
                }
            }

            pos = content.find("api_keys = ");
            if (pos != std::string::npos) {
                size_t endPos = content.find("\n", pos);
                if (endPos != std::string::npos) {
                    content.replace(pos, endPos - pos,
                                    "api_keys = [\"" + escapeTomlString(apiKey) + "\"]");
                }
            }

            // Update vector database settings
            if (enableVectorDB && !selectedModel.empty()) {
                pos = content.find("[vector_database]");
                if (pos != std::string::npos) {
                    // Find the enabled line
                    size_t enabledPos = content.find("enabled = ", pos);
                    if (enabledPos != std::string::npos) {
                        size_t endPos = content.find("\n", enabledPos);
                        if (endPos != std::string::npos) {
                            content.replace(enabledPos, endPos - enabledPos, "enabled = true");
                        }
                    }

                    // Update model
                    size_t modelPos = content.find("model = ", pos);
                    if (modelPos != std::string::npos) {
                        size_t endPos = content.find("\n", modelPos);
                        if (endPos != std::string::npos) {
                            content.replace(modelPos, endPos - modelPos,
                                            "model = \"" + escapeTomlString(selectedModel) + "\"");
                        }
                    }

                    // Update model_path
                    size_t pathPos = content.find("model_path = ", pos);
                    if (pathPos != std::string::npos) {
                        size_t endPos = content.find("\n", pathPos);
                        if (endPos != std::string::npos) {
                            content.replace(pathPos, endPos - pathPos,
                                            "model_path = \"" +
                                                escapeTomlString((dataDir / "models" /
                                                                  selectedModel / "model.onnx")
                                                                     .string()) +
                                                "\"");
                        }
                    }
                }
            }

            if (useS3) {
                pos = content.find("engine = ");
                if (pos != std::string::npos) {
                    size_t endPos = content.find("\n", pos);
                    if (endPos != std::string::npos) {
                        content.replace(pos, endPos - pos, "engine = \"s3\"");
                    }
                }

                pos = content.find("url = ");
                if (pos != std::string::npos) {
                    size_t endPos = content.find("\n", pos);
                    if (endPos != std::string::npos) {
                        content.replace(pos, endPos - pos,
                                        "url = \"" + escapeTomlString(s3Url) + "\"");
                    }
                }

                pos = content.find("region = ");
                if (pos != std::string::npos) {
                    size_t endPos = content.find("\n", pos);
                    if (endPos != std::string::npos) {
                        content.replace(pos, endPos - pos,
                                        "region = \"" + escapeTomlString(s3Region) + "\"");
                    }
                }

                pos = content.find("endpoint = ");
                if (pos != std::string::npos) {
                    size_t endPos = content.find("\n", pos);
                    if (endPos != std::string::npos) {
                        content.replace(pos, endPos - pos,
                                        "endpoint = \"" + escapeTomlString(s3Endpoint) + "\"");
                    }
                }

                pos = content.find("access_key = ");
                if (pos != std::string::npos) {
                    size_t endPos = content.find("\n", pos);
                    if (endPos != std::string::npos) {
                        content.replace(pos, endPos - pos,
                                        "access_key = \"" + escapeTomlString(s3AccessKey) + "\"");
                    }
                }

                pos = content.find("secret_key = ");
                if (pos != std::string::npos) {
                    size_t endPos = content.find("\n", pos);
                    if (endPos != std::string::npos) {
                        content.replace(pos, endPos - pos,
                                        "secret_key = \"" + escapeTomlString(s3SecretKey) + "\"");
                    }
                }

                pos = content.find("use_path_style = ");
                if (pos != std::string::npos) {
                    size_t endPos = content.find("\n", pos);
                    if (endPos != std::string::npos) {
                        content.replace(pos, endPos - pos,
                                        std::string("use_path_style = ") +
                                            (s3UsePathStyle ? "true" : "false"));
                    }
                }
            }

            // Update storage base path and CAS directories based on selected data dir
            pos = content.find("base_path = ");
            if (pos != std::string::npos) {
                size_t endPos = content.find("\n", pos);
                if (endPos != std::string::npos) {
                    content.replace(pos, endPos - pos,
                                    "base_path = \"" + escapeTomlString(dataDir.string()) + "\"");
                }
            }

            // Ensure CAS subdirectories are explicitly set (relative to base_path)
            pos = content.find("objects_dir = ");
            if (pos != std::string::npos) {
                size_t endPos = content.find("\n", pos);
                if (endPos != std::string::npos) {
                    content.replace(pos, endPos - pos, "objects_dir = \"objects\"");
                }
            }

            pos = content.find("staging_dir = ");
            if (pos != std::string::npos) {
                size_t endPos = content.find("\n", pos);
                if (endPos != std::string::npos) {
                    content.replace(pos, endPos - pos, "staging_dir = \"staging\"");
                }
            }

            // Ensure downloader store-only behavior and temp extension are set
            size_t dpos = content.find("[downloader]");
            if (dpos != std::string::npos) {
                size_t storePos = content.find("store_only = ", dpos);
                if (storePos != std::string::npos) {
                    size_t endPos = content.find("\n", storePos);
                    if (endPos != std::string::npos) {
                        content.replace(storePos, endPos - storePos, "store_only = true");
                    }
                }
                size_t tempPos = content.find("temp_extension = ", dpos);
                if (tempPos != std::string::npos) {
                    size_t endPos = content.find("\n", tempPos);
                    if (endPos != std::string::npos) {
                        content.replace(tempPos, endPos - tempPos, "temp_extension = \".part\"");
                    }
                }
            }

            // Write updated config
            std::ofstream out(configPath, std::ios::trunc);
            if (!out) {
                return Error{ErrorCode::WriteError, "Failed to write updated config"};
            }
            out << content;
            out.close();

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::WriteError,
                         std::string("Failed to update config: ") + e.what()};
        }
    }

    static std::string escapeTomlString(const std::string& s) {
        std::string out;
        out.reserve(s.size());
        for (char c : s) {
            switch (c) {
                case '\\':
                    out += "\\\\";
                    break;
                case '"':
                    out += "\\\"";
                    break;
                case '\n':
                    out += "\\n";
                    break;
                case '\t':
                    out += "\\t";
                    break;
                default:
                    out += c;
                    break;
            }
        }
        return out;
    }

    static Result<void> generateEd25519Keypair(const fs::path& privateKeyPath,
                                               const fs::path& publicKeyPath, bool force) {
        try {
            if (!force) {
                if (fs::exists(privateKeyPath) || fs::exists(publicKeyPath)) {
                    return Error{ErrorCode::InvalidState,
                                 "Key files already exist (use --force to overwrite)"};
                }
            }

            // Create key
            EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, nullptr);
            if (!pctx) {
                return Error{ErrorCode::InternalError, "Failed to create EVP_PKEY_CTX"};
            }

            if (EVP_PKEY_keygen_init(pctx) <= 0) {
                EVP_PKEY_CTX_free(pctx);
                return Error{ErrorCode::InternalError, "EVP_PKEY_keygen_init failed"};
            }

            EVP_PKEY* pkey = nullptr;
            if (EVP_PKEY_keygen(pctx, &pkey) <= 0) {
                EVP_PKEY_CTX_free(pctx);
                return Error{ErrorCode::InternalError, "EVP_PKEY_keygen failed"};
            }
            EVP_PKEY_CTX_free(pctx);

            // Ensure parent dirs
            if (privateKeyPath.has_parent_path()) {
                fs::create_directories(privateKeyPath.parent_path());
            }
            if (publicKeyPath.has_parent_path()) {
                fs::create_directories(publicKeyPath.parent_path());
            }

            // Write private key (PKCS#8 PEM, unencrypted)
            {
                FILE* fp = std::fopen(privateKeyPath.string().c_str(), "wb");
                if (!fp) {
                    EVP_PKEY_free(pkey);
                    return Error{ErrorCode::WriteError,
                                 "Failed to open private key file for writing"};
                }
                if (!PEM_write_PrivateKey(fp, pkey, nullptr, nullptr, 0, nullptr, nullptr)) {
                    std::fclose(fp);
                    EVP_PKEY_free(pkey);
                    return Error{ErrorCode::InternalError, "PEM_write_PrivateKey failed"};
                }
                std::fclose(fp);

                // Restrict permissions to 0600
                std::error_code ec;
                fs::permissions(privateKeyPath, fs::perms::owner_read | fs::perms::owner_write,
                                fs::perm_options::replace, ec);
                (void)ec; // best-effort
            }

            // Write public key (SubjectPublicKeyInfo PEM)
            {
                FILE* fp = std::fopen(publicKeyPath.string().c_str(), "wb");
                if (!fp) {
                    EVP_PKEY_free(pkey);
                    return Error{ErrorCode::WriteError,
                                 "Failed to open public key file for writing"};
                }
                if (!PEM_write_PUBKEY(fp, pkey)) {
                    std::fclose(fp);
                    EVP_PKEY_free(pkey);
                    return Error{ErrorCode::InternalError, "PEM_write_PUBKEY failed"};
                }
                std::fclose(fp);
            }

            EVP_PKEY_free(pkey);
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError, std::string("Keygen error: ") + e.what()};
        }
    }

    static std::string toHex(const unsigned char* data, size_t len) {
        static const char* kHex = "0123456789abcdef";
        std::string out;
        out.resize(len * 2);
        for (size_t i = 0; i < len; ++i) {
            out[2 * i] = kHex[(data[i] >> 4) & 0xF];
            out[2 * i + 1] = kHex[(data[i]) & 0xF];
        }
        return out;
    }

    static std::string generateApiKey(size_t numBytes) {
        std::vector<unsigned char> buf(numBytes);
        if (RAND_bytes(buf.data(), static_cast<int>(buf.size())) != 1) {
            // Fallback to std::random_device if OpenSSL RNG fails
            for (auto& b : buf) {
                b = static_cast<unsigned char>(std::rand() & 0xFF);
            }
        }
        return toHex(buf.data(), buf.size());
    }

    void setupUserPluginsDirAndTrust() {
        try {
            namespace fs = std::filesystem;
            // Determine user plugin directory: ~/.local/lib/yams/plugins
            fs::path userPlugins = fs::path(std::getenv("HOME") ? std::getenv("HOME") : "") /
                                   ".local" / "lib" / "yams" / "plugins";
            fs::create_directories(userPlugins);
            spdlog::info("Plugins directory: {}", userPlugins.string());

            // Write trust file entry so daemon will load from here by default
            // Trust file aligns with ServiceManager: XDG_CONFIG_HOME or
            // ~/.config/yams/plugins_trust.txt
            fs::path cfgHome;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                cfgHome = fs::path(xdg);
            else if (const char* home = std::getenv("HOME"))
                cfgHome = fs::path(home) / ".config";
            else
                cfgHome = fs::path(".config");
            fs::path trustFile = cfgHome / "yams" / "plugins_trust.txt";
            fs::create_directories(trustFile.parent_path());
            // Append only if not already present
            std::vector<std::string> lines;
            {
                std::ifstream in(trustFile);
                std::string line;
                while (in && std::getline(in, line)) {
                    if (!line.empty())
                        lines.push_back(line);
                }
            }
            auto canon = fs::weakly_canonical(userPlugins).string();
            bool present = false;
            for (const auto& l : lines) {
                if (fs::weakly_canonical(l).string() == canon) {
                    present = true;
                    break;
                }
            }
            if (!present) {
                std::ofstream out(trustFile, std::ios::app);
                out << canon << "\n";
                spdlog::info("Trusted plugin directory added: {}", canon);
            } else {
                spdlog::debug("Plugin directory already trusted: {}", canon);
            }

            // Also set [daemon].plugin_dir in config.toml to this path (best-effort)
            try {
                fs::path configPath = cfgHome / "yams" / "config.toml";
                if (fs::exists(configPath)) {
                    std::ifstream in(configPath);
                    std::stringstream buf;
                    buf << in.rdbuf();
                    in.close();
                    std::string content = buf.str();
                    // Ensure [daemon] section exists; if not, append one
                    if (content.find("[daemon]") == std::string::npos) {
                        content.append("\n[daemon]\nplugin_dir = \"\"\n");
                    }
                    // Replace plugin_dir value within [daemon]
                    auto secPos = content.find("[daemon]");
                    if (secPos != std::string::npos) {
                        auto nextSec = content.find("[", secPos + 1);
                        auto rangeEnd = (nextSec == std::string::npos) ? content.size() : nextSec;
                        auto keyPos = content.find("plugin_dir", secPos);
                        if (keyPos == std::string::npos || keyPos > rangeEnd) {
                            // Insert key at end of section
                            content.insert(rangeEnd,
                                           std::string("plugin_dir = \"") + canon + "\"\n");
                        } else {
                            // Replace the value on the line
                            auto lineEnd = content.find("\n", keyPos);
                            if (lineEnd == std::string::npos)
                                lineEnd = content.size();
                            content.replace(keyPos, lineEnd - keyPos,
                                            std::string("plugin_dir = \"") + canon + "\"");
                        }
                    }
                    std::ofstream outCfg(configPath, std::ios::trunc);
                    outCfg << content;
                    outCfg.close();
                    spdlog::info("Configured [daemon].plugin_dir = {}", canon);
                }
            } catch (const std::exception& e) {
                spdlog::debug("Skipping plugin_dir write to config: {}", e.what());
            }

            // Optional: if a system ONNX plugin exists, hint the user
            fs::path sys1 = fs::path("/usr/local/lib/yams/plugins/libyams_onnx_plugin.so");
            fs::path sys2 = fs::path("/usr/lib/yams/plugins/libyams_onnx_plugin.so");
            if (fs::exists(sys1) || fs::exists(sys2)) {
                spdlog::info("ONNX plugin found in system plugins. It will be auto-loaded.");
            } else {
                spdlog::info(
                    "You can place plugins under '{}' or set YAMS_PLUGIN_DIR for development.",
                    userPlugins.string());
            }
        } catch (const std::exception& e) {
            spdlog::warn("Plugins setup skipped: {}", e.what());
        }
    }

    // Download model using the unified downloader
    Result<void> downloadModelFiles(const EmbeddingModel& model, const fs::path& outputDir) {
        fs::create_directories(outputDir);
        fs::path modelFile = outputDir / "model.onnx";

        // Check if already downloaded
        if (fs::exists(modelFile)) {
            std::cout << "  Model already exists at: " << modelFile.string() << "\n";
            return Result<void>{};
        }

        // Setup downloader
        yams::downloader::StorageConfig storage{};
        try {
            fs::path dataDir = cli_ ? cli_->getDataPath() : fs::path{};
            if (!dataDir.empty()) {
                storage.objectsDir = dataDir / "storage" / "objects";
                storage.stagingDir = dataDir / "staging" / "downloader";
            } else {
                auto tmp = fs::temp_directory_path();
                storage.objectsDir = tmp / "yams" / "objects";
                storage.stagingDir = tmp / "yams" / "downloader";
            }
        } catch (...) {
            try {
                auto tmp = fs::temp_directory_path();
                storage.objectsDir = tmp / "yams" / "objects";
                storage.stagingDir = tmp / "yams" / "downloader";
            } catch (...) {
            }
        }

        yams::downloader::DownloaderConfig dcfg{};
        auto manager = yams::downloader::makeDownloadManager(storage, dcfg);

        // Download helper with progress
        auto downloadFile = [&](const std::string& url, const fs::path& outPath,
                                const std::string& label) -> Result<void> {
            yams::downloader::DownloadRequest req{};
            req.url = url;
            req.storeOnly = true;
            req.exportPath = outPath;

            size_t lastLen = 0;
            auto onProgress = [&label, &lastLen](const yams::downloader::ProgressEvent& ev) {
                auto stageName = [](yams::downloader::ProgressStage s) {
                    switch (s) {
                        case yams::downloader::ProgressStage::Resolving:
                            return "resolving";
                        case yams::downloader::ProgressStage::Connecting:
                            return "connecting";
                        case yams::downloader::ProgressStage::Downloading:
                            return "downloading";
                        case yams::downloader::ProgressStage::Verifying:
                            return "verifying";
                        case yams::downloader::ProgressStage::Finalizing:
                            return "finalizing";
                        default:
                            return "";
                    }
                };
                float pct = ev.percentage.value_or(0.0f);
                double done_mb = static_cast<double>(ev.downloadedBytes) / (1024.0 * 1024.0);
                std::string content;
                if (ev.totalBytes) {
                    double total_mb = static_cast<double>(*ev.totalBytes) / (1024.0 * 1024.0);
                    content = fmt::format("  {} {:11s} {:3.0f}% [{:.1f}/{:.1f} MB]", label,
                                          stageName(ev.stage), pct, done_mb, total_mb);
                } else {
                    content =
                        fmt::format("  {} {:11s} [{:.1f} MB]", label, stageName(ev.stage), done_mb);
                }
                std::string out = "\r" + content;
                if (lastLen > content.size())
                    out += std::string(lastLen - content.size(), ' ');
                fmt::print("{}", out);
                std::fflush(stdout);
                lastLen = content.size();
                if (ev.stage == yams::downloader::ProgressStage::Finalizing) {
                    fmt::print("\n");
                    lastLen = 0;
                }
            };

            auto res = manager->download(req, onProgress, [] { return false; }, {});
            if (!res.ok() || !res.value().success) {
                std::string msg =
                    res.ok() ? (res.value().error ? res.value().error->message : "download failed")
                             : res.error().message;
                return Error{ErrorCode::InternalError, label + ": " + msg};
            }
            return Result<void>{};
        };

        // Map model name to HuggingFace repo
        auto getHfRepo = [](const std::string& name) -> std::string {
            if (name == "all-MiniLM-L6-v2")
                return "sentence-transformers/all-MiniLM-L6-v2";
            if (name == "multi-qa-MiniLM-L6-cos-v1")
                return "sentence-transformers/multi-qa-MiniLM-L6-cos-v1";
            if (name == "all-mpnet-base-v2")
                return "sentence-transformers/all-mpnet-base-v2";
            return "";
        };

        std::string repo = getHfRepo(model.name);
        if (repo.empty()) {
            return Error{ErrorCode::InvalidArgument, "Unknown model: " + model.name};
        }

        std::cout << "\nDownloading model: " << model.name << " (~" << model.size_mb << " MB)...\n";

        // Download model.onnx (try multiple paths)
        std::vector<std::string> modelPaths = {"onnx/model.onnx", "model.onnx"};
        bool downloaded = false;
        for (const auto& path : modelPaths) {
            std::string url = "https://huggingface.co/" + repo + "/resolve/main/" + path;
            auto result = downloadFile(url, modelFile, "model.onnx");
            if (result) {
                downloaded = true;
                break;
            }
        }
        if (!downloaded) {
            return Error{ErrorCode::NetworkError, "Failed to download model.onnx"};
        }

        // Download optional companion files (best effort)
        std::vector<std::pair<std::string, std::string>> companions = {
            {"config.json", "config.json"},
            {"tokenizer.json", "tokenizer.json"},
            {"sentence_bert_config.json", "sentence_bert_config.json"}};
        for (const auto& [filename, localName] : companions) {
            std::string url = "https://huggingface.co/" + repo + "/resolve/main/" + filename;
            (void)downloadFile(url, outputDir / localName, filename);
        }

        std::cout << "✓ Model downloaded successfully\n";
        return Result<void>{};
    }

    std::string promptForModel(const fs::path& dataPath) {
        // Build choice items
        std::vector<ChoiceItem> items;
        items.reserve(EMBEDDING_MODELS.size());
        for (const auto& m : EMBEDDING_MODELS) {
            ChoiceItem ci;
            ci.value = m.name;
            ci.label = m.name + " (" + std::to_string(m.size_mb) +
                       " MB, dim=" + std::to_string(m.dimensions) + ")";
            ci.description = m.description;
            items.push_back(std::move(ci));
        }

        size_t defaultIndex = 0; // first model default
        // Prefer a nomic* model as default if present (future friendly heuristic)
        for (size_t i = 0; i < EMBEDDING_MODELS.size(); ++i) {
            if (EMBEDDING_MODELS[i].name.find("nomic-embed-text") != std::string::npos) {
                defaultIndex = i;
                break;
            }
        }

        size_t chosenIdx = prompt_choice("\nAvailable embedding models (recommended first):", items,
                                         ChoiceOptions{.defaultIndex = defaultIndex,
                                                       .allowEmpty = true,
                                                       .retryOnInvalid = true});

        const auto& selectedModel = EMBEDDING_MODELS[chosenIdx];

        // Check if model already exists
        fs::path modelDir = dataPath / "models" / selectedModel.name;
        fs::path modelPath = modelDir / "model.onnx";

        if (fs::exists(modelPath)) {
            std::cout << "\nModel already downloaded at: " << modelPath.string() << "\n";
        } else {
            // Ask if user wants to download now
            bool downloadNow = prompt_yes_no("\nDownload the model now? [Y/n]: ",
                                             YesNoOptions{.defaultYes = true});

            if (downloadNow) {
                auto result = downloadModelFiles(selectedModel, modelDir);
                if (!result) {
                    spdlog::warn("Model download failed: {}", result.error().message);
                    std::cout << "\nYou can download the model later with:\n"
                              << "  yams model download " << selectedModel.name << "\n";
                }
            } else {
                std::cout << "\nTo download this model later:\n"
                          << "  yams model download " << selectedModel.name << "\n";
            }
        }
        return selectedModel.name;
    }

    bool downloadModel(const EmbeddingModel& /*model*/, const fs::path& /*outputDir*/) {
        return false;
    }

    YamsCLI* cli_ = nullptr;
    bool nonInteractive_ = false;
    bool autoInit_ = false;
    bool force_ = false;
    bool noKeygen_ = false;
    bool printConfig_ = false;
    bool enablePlugins_ = false;
};

// Factory function
std::unique_ptr<ICommand> createInitCommand() {
    return std::make_unique<InitCommand>();
}

} // namespace yams::cli
