#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_migration.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>

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
    {"all-mpnet-base-v2",
     "https://huggingface.co/sentence-transformers/all-mpnet-base-v2/resolve/main/onnx/model.onnx",
     "High-quality embeddings for better accuracy", 420, 768},
    {"nomic-embed-text-v1.5",
     "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/main/onnx/model.onnx",
     "State-of-the-art lightweight embeddings", 138, 768}};

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
        cmd->add_flag("--force", force_, "Overwrite existing config/keys if already initialized");
        cmd->add_flag("--no-keygen", noKeygen_, "Skip authentication key generation");
        cmd->add_flag("--print", printConfig_,
                      "Print resulting configuration to stdout (secrets masked)");

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
            // 1) Resolve directories using XDG Base Directory specification
            auto dataPath = cli_->getDataPath();
            auto homeEnv = std::getenv("HOME");
            fs::path homeDir = homeEnv ? fs::path(homeEnv) : fs::current_path();

            // Use XDG_CONFIG_HOME if set, otherwise ~/.config
            auto xdgConfigEnv = std::getenv("XDG_CONFIG_HOME");
            fs::path configHome = xdgConfigEnv ? fs::path(xdgConfigEnv) : (homeDir / ".config");

            fs::path configDir = configHome / "yams";
            fs::path keysDir = configDir / "keys";
            fs::path configPath = configDir / "config.toml";

            if (!nonInteractive_) {
                dataPath = promptForDataDir(dataPath);
                if (!noKeygen_) {
                    noKeygen_ = !promptYesNo("Generate authentication keys? [Y/n]: ", true);
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
                if (!kg)
                    return kg;
                spdlog::info("Authentication keys generated under {}", keysDir.string());
            } else {
                spdlog::debug("Skipping key generation (--no-keygen)");
            }

            // 6) Vector Database Setup
            bool enableVectorDB = false;
            std::string selectedModel;
            if (!nonInteractive_) {
                enableVectorDB =
                    promptYesNo("\nEnable vector database for semantic search? [Y/n]: ", true);
                if (enableVectorDB) {
                    selectedModel = promptForModel(dataPath);
                }
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
            auto updateResult = updateV2Config(configPath, dataPath, privateKeyPath, publicKeyPath,
                                               apiKeyHex, enableVectorDB, selectedModel);
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

    static bool promptYesNo(const std::string& prompt, bool defaultYes) {
        std::cout << prompt;
        std::string line;
        std::getline(std::cin, line);

        if (line.empty())
            return defaultYes;
        char c = static_cast<char>(std::tolower(line[0]));
        if (c == 'y')
            return true;
        if (c == 'n')
            return false;
        return defaultYes;
    }

    static Result<void> updateV2Config(const fs::path& configPath, const fs::path& dataDir,
                                       const fs::path& privateKeyPath,
                                       const fs::path& publicKeyPath, const std::string& apiKey,
                                       bool enableVectorDB, const std::string& selectedModel) {
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

            // Update data_dir
            size_t pos = content.find("data_dir = ");
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

    std::string promptForModel(const fs::path& dataPath) {
        std::cout << "\nAvailable embedding models:\n";
        for (size_t i = 0; i < EMBEDDING_MODELS.size(); ++i) {
            const auto& model = EMBEDDING_MODELS[i];
            std::cout << "  " << (i + 1) << ". " << model.name << " (" << model.size_mb << " MB)\n";
            std::cout << "     " << model.description << "\n";
            std::cout << "     Dimensions: " << model.dimensions << "\n";
        }

        std::cout << "\nSelect a model (1-" << EMBEDDING_MODELS.size() << "): ";
        std::string line;
        std::getline(std::cin, line);

        int choice = 0;
        try {
            choice = std::stoi(line);
        } catch (...) {
            choice = 1; // Default to first model
        }

        if (choice < 1 || choice > static_cast<int>(EMBEDDING_MODELS.size())) {
            choice = 1; // Default to first model
        }

        const auto& selectedModel = EMBEDDING_MODELS[choice - 1];

        // Download the model
        fs::path modelDir = dataPath / "models" / selectedModel.name;
        fs::path modelPath = modelDir / "model.onnx";

        if (fs::exists(modelPath)) {
            std::cout << "\nModel already downloaded at: " << modelPath.string() << "\n";
        } else {
            std::cout << "\nDownloading " << selectedModel.name << "...\n";
            if (!downloadModel(selectedModel, modelDir)) {
                spdlog::error("Failed to download model");
                return "";
            }
            std::cout << "Model downloaded successfully to: " << modelPath.string() << "\n";
        }

        return selectedModel.name;
    }

    bool downloadModel(const EmbeddingModel& model, const fs::path& outputDir) {
        try {
            // Create model directory
            fs::create_directories(outputDir);
            fs::path outputPath = outputDir / "model.onnx";

            // Download using curl or wget
            std::string command =
                "curl -L --progress-bar \"" + model.url + "\" -o \"" + outputPath.string() + "\"";

            int result = std::system(command.c_str());
            if (result != 0) {
                // Try wget as fallback
                command = "wget -q --show-progress \"" + model.url + "\" -O \"" +
                          outputPath.string() + "\"";
                result = std::system(command.c_str());
            }

            return result == 0 && fs::exists(outputPath);
        } catch (const std::exception& e) {
            spdlog::error("Error downloading model: {}", e.what());
            return false;
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    bool nonInteractive_ = false;
    bool force_ = false;
    bool noKeygen_ = false;
    bool printConfig_ = false;
};

// Factory function
std::unique_ptr<ICommand> createInitCommand() {
    return std::make_unique<InitCommand>();
}

} // namespace yams::cli