#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/cli/command.h>
#include <yams/cli/prompt_util.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/downloader/downloader.hpp>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>
#include <fmt/format.h>

// For grammar downloading on Windows
#ifdef _WIN32
#include <cstdlib>
#endif

#include <algorithm>
#include <atomic>
#include <cctype>
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
    {"mxbai-edge-colbert-v0-17m",
     "https://huggingface.co/ryandono/mxbai-edge-colbert-v0-17m-onnx-int8/resolve/main/onnx/"
     "model_quantized.onnx",
     "Lightweight ColBERT (token-level, MaxSim) optimized for edge use", 17, 48},
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight model for semantic search", 90, 384},
    {"multi-qa-MiniLM-L6-cos-v1",
     "https://huggingface.co/sentence-transformers/multi-qa-MiniLM-L6-cos-v1/resolve/main/onnx/"
     "model.onnx",
     "Optimized for semantic search on QA pairs (215M training samples)", 90, 384}};

// Available GLiNER models for named entity recognition (NER)
struct GlinerModel {
    std::string name;
    std::string repo;       // HuggingFace repo (e.g., "onnx-community/gliner_small-v2.1")
    std::string model_file; // Model filename (e.g., "model_quantized.onnx" or "model.onnx")
    std::string description;
    size_t size_mb; // Approximate total size (model + tokenizer)
};

static const std::vector<GlinerModel> GLINER_MODELS = {
    {"gliner_small-v2.1-quantized", "onnx-community/gliner_small-v2.1", "model_quantized.onnx",
     "Small GLiNER quantized (fast, recommended)", 175},
    {"gliner_small-v2.1", "onnx-community/gliner_small-v2.1", "model.onnx",
     "Small GLiNER full precision (~580MB)", 580},
    {"gliner_medium-v2.1-quantized", "onnx-community/gliner_medium-v2.1", "model_quantized.onnx",
     "Medium GLiNER quantized (balanced)", 220},
    {"gliner_medium-v2.1", "onnx-community/gliner_medium-v2.1", "model.onnx",
     "Medium GLiNER full precision (~745MB)", 745}};

// Files required for GLiNER model (tokenizer files at repo root)
static const std::vector<std::string> GLINER_TOKENIZER_FILES = {"tokenizer.json", "config.json",
                                                                "gliner_config.json"};

// Available reranker models for hybrid search two-stage retrieval
struct RerankerModel {
    std::string name;
    std::string repo;       // HuggingFace repo (e.g., "BAAI/bge-reranker-base")
    std::string model_file; // Model filename (e.g., "onnx/model.onnx")
    std::string description;
    size_t size_mb;
    int max_tokens; // Maximum input length
};

static const std::vector<RerankerModel> RERANKER_MODELS = {
    {"bge-reranker-base", "BAAI/bge-reranker-base", "onnx/model.onnx",
     "Cross-encoder reranker for hybrid search (recommended)", 278, 512},
    {"bge-reranker-large", "BAAI/bge-reranker-large", "onnx/model.onnx",
     "High-quality cross-encoder reranker (larger, more accurate)", 560, 512}};

// Files required for reranker model
static const std::vector<std::string> RERANKER_TOKENIZER_FILES = {"tokenizer.json", "config.json",
                                                                  "tokenizer_config.json"};

// Available tree-sitter grammars for symbol extraction
struct GrammarInfo {
    std::string_view language;
    std::string_view repo;
    std::string_view description;
    bool recommended; // Show in default selection
};

// Embedded YAMS skill file for AI agents (Claude Code, OpenCode)
static constexpr std::string_view YAMS_SKILL_CONTENT = R"skill(---
name: yams
description: Code indexing, semantic search, and knowledge graph for project memory
license: GPL-3.0
compatibility: claude-code, opencode
metadata:
  tools: cli, mcp
  categories: search, indexing, memory, knowledge-graph
---

# YAMS Skill

## Quick Reference

```bash
# Status & Health
yams status                    # Check daemon and index status
yams daemon start              # Start background daemon
yams doctor                    # Diagnose issues

# Indexing
yams add <file>                # Index single file
yams add . -r --include "*.py" # Index directory recursively
yams watch                     # Auto-index on file changes

# Search (use grep first, search for semantic)
yams grep "pattern"            # Code pattern search (fast, exact)
yams search "query"            # Semantic/hybrid search

# Graph
yams graph --name <file>       # Show file relationships
yams graph --list-type symbol  # List symbols
yams graph --relations         # List relation types with counts
yams graph --search "pattern"  # Search nodes by label pattern
```

## Code Indexing

### Index Project Files

```bash
# Index specific file types
yams add . -r --include "*.ts,*.tsx,*.js"

# Index with exclusions
yams add . -r --include "*.py" --exclude "venv/**,__pycache__/**"

# Index with metadata for tracking
yams add src/ -r --metadata "task=feature-auth"
```

### Auto-Index with Watch

```bash
yams watch                     # Start watching current directory
yams watch --interval 2000     # Custom interval (ms)
yams watch --stop              # Stop watching
```

## Search Patterns

### Decision Tree

1. **Code patterns** -> `yams grep` (fast, regex)
2. **Semantic/concept** -> `yams search` (embeddings)
3. **No results from grep** -> Try `yams search`

### grep (Code Search)

```bash
# Exact pattern
yams grep "function authenticate"

# Regex pattern
yams grep "async.*await.*fetch"

# Fuzzy matching
yams grep "authentcation" --fuzzy

# With context lines
yams grep "TODO" -A 2 -B 2

# Filter by extension
yams grep "import" --ext py
```

### search (Semantic Search)

```bash
# Concept search
yams search "error handling patterns"

# Hybrid search (default)
yams search "authentication flow" --type hybrid

# Limit results
yams search "database connection" --limit 5
```

## Knowledge Management

### Store Research

```bash
# Index documentation
curl -s "https://docs.example.com/api" | yams add - --name "api-docs.md"

# Store with metadata
yams add notes.md --metadata "source=research,topic=auth"
```

## Session Management

```bash
# Start named session
yams session start --name "feature-auth"

# List sessions
yams session ls

# Switch session
yams session use "feature-auth"

# Warm session cache (faster searches)
yams session warm --limit 100
```

## Graph Queries

```bash
# Show file dependencies
yams graph --name src/auth/login.ts --depth 2

# List all symbols of type
yams graph --list-type function --limit 50

# Find isolated nodes (potential dead code)
yams graph --list-type symbol --isolated

# Explore graph structure
yams graph --relations              # List all relation types with counts
yams graph --search "Request*"      # Find nodes matching pattern (wildcards: *, ?)
yams graph --search "*Controller"   # Find all controller nodes
```

## MCP Integration

YAMS exposes tools via Model Context Protocol for programmatic access.

```bash
yams serve                     # Start MCP server (quiet mode)
```

### MCP Configuration

```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"]
    }
  }
}
```

## Troubleshooting

```bash
yams daemon status -d          # Check daemon status
yams daemon log -n 50          # View daemon logs
yams doctor                    # Full diagnostic
yams doctor repair --all       # Repair index
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `YAMS_DATA_DIR` | Storage directory |
| `YAMS_SOCKET` | Daemon socket path |
| `YAMS_LOG_LEVEL` | Logging verbosity |
| `YAMS_SESSION_CURRENT` | Default session |
)skill";

// Must match grammar_loader.h kGrammarRepos
static constexpr GrammarInfo SUPPORTED_GRAMMARS[] = {
    {"c", "tree-sitter/tree-sitter-c", "C language", true},
    {"cpp", "tree-sitter/tree-sitter-cpp", "C++ language", true},
    {"python", "tree-sitter/tree-sitter-python", "Python language", true},
    {"javascript", "tree-sitter/tree-sitter-javascript", "JavaScript/JSX", true},
    {"typescript", "tree-sitter/tree-sitter-typescript", "TypeScript/TSX", true},
    {"rust", "tree-sitter/tree-sitter-rust", "Rust language", true},
    {"go", "tree-sitter/tree-sitter-go", "Go language", true},
    {"swift", "alex-pinkus/tree-sitter-swift", "Swift language", true},
    {"java", "tree-sitter/tree-sitter-java", "Java language", false},
    {"csharp", "tree-sitter/tree-sitter-c-sharp", "C# language", false},
    {"php", "tree-sitter/tree-sitter-php", "PHP language", false},
    {"kotlin", "fwcd/tree-sitter-kotlin", "Kotlin language", false},
    {"perl", "tree-sitter-perl/tree-sitter-perl", "Perl language", false},
    {"r", "r-lib/tree-sitter-r", "R language", false},
    {"dart", "UserNobody14/tree-sitter-dart", "Dart/Flutter", false},
    {"sql", "DerekStride/tree-sitter-sql", "SQL queries", false},
    {"solidity", "JoranHonig/tree-sitter-solidity", "Solidity (Ethereum)", false},
    {"p4", "prona-p4-learning-platform/tree-sitter-p4", "P4 network language", false},
    {"zig", "maxxnino/tree-sitter-zig", "Zig language", false},
};

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
                tuningProfile_ = promptForTuningProfile();
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

                if (!nonInteractive_) {
                    tuningProfile_ = promptForTuningProfile();
                    auto writeOk =
                        config::write_config_value(configPath, "tuning.profile", tuningProfile_);
                    if (!writeOk) {
                        spdlog::warn("Failed to update tuning.profile in config");
                    }
                }

                const auto preferredModel =
                    config::parse_config_value(configPath, "embeddings", "preferred_model");
                const bool colbertPreferred = isColbertModelName(preferredModel);

                // Still offer GLiNER model, reranker model, grammar download, and skill install
                // even if already initialized
                maybeSetupGlinerModel(dataPath, configPath);
                if (colbertPreferred) {
                    auto selected = maybeSetupRerankerModel(dataPath, configPath);
                    if (selected.empty()) {
                        updateColbertRerankingConfig(configPath);
                        spdlog::info(
                            "ColBERT preferred model detected; enabling MaxSim reranking via "
                            "ONNX plugin");
                    }
                } else {
                    (void)maybeSetupRerankerModel(dataPath, configPath);
                }
                maybeSetupGrammars(dataPath);
                maybeSetupAgentSkill();
                maybeBootstrapProjectSession();
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
            bool colbertSelected = false;
            if (autoInit_) {
                // Use the default model (first in the list) for auto-init
                selectedModel = EMBEDDING_MODELS[0].name;
                spdlog::info("Using default embedding model: {}", selectedModel);
                colbertSelected = isColbertModelName(selectedModel);
            } else if (!nonInteractive_) {
                enableVectorDB =
                    prompt_yes_no("\nEnable vector database for semantic search? [Y/n]: ",
                                  YesNoOptions{.defaultYes = true});
                if (enableVectorDB) {
                    selectedModel = promptForModel(dataPath);
                    colbertSelected = isColbertModelName(selectedModel);
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
                               s3AccessKey, s3SecretKey, s3UsePathStyle, tuningProfile_);
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

                // 7a) GLiNER Model Setup (for NER in Glint plugin)
                maybeSetupGlinerModel(dataPath, configPath);
            }

            // 7b) Reranker Model Setup (for two-stage hybrid search)
            if (enableVectorDB && colbertSelected) {
                auto selected = maybeSetupRerankerModel(dataPath, configPath);
                if (selected.empty()) {
                    updateColbertRerankingConfig(configPath);
                    spdlog::info("ColBERT selected; enabling MaxSim reranking via ONNX plugin");
                }
            } else {
                (void)maybeSetupRerankerModel(dataPath, configPath);
            }

            // 7) Tree-sitter Grammar Setup (for symbol extraction)
            maybeSetupGrammars(dataPath);

            // 8) AI Agent Skill Installation (Claude Code, OpenCode)
            maybeSetupAgentSkill();

            maybeBootstrapProjectSession();
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

    static bool envTruthy(const char* val) {
        if (!val || !*val)
            return false;
        std::string v(val);
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return v == "1" || v == "true" || v == "yes" || v == "on";
    }

    static fs::path findGitRoot(const fs::path& start) {
        std::error_code ec;
        fs::path cur = fs::absolute(start, ec);
        if (ec)
            cur = start;
        while (!cur.empty()) {
            auto candidate = cur / ".git";
            if (fs::exists(candidate, ec)) {
                return cur;
            }
            auto parent = cur.parent_path();
            if (parent == cur)
                break;
            cur = parent;
        }
        return {};
    }

    static std::string sanitizeName(std::string s) {
        if (s.empty())
            return "project";
        for (auto& c : s) {
            if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_')) {
                c = '-';
            } else {
                c = static_cast<char>(std::tolower(c));
            }
        }
        return s;
    }

    static std::string shortHash(const std::string& s) {
        // FNV-1a 64-bit
        std::uint64_t h = 1469598103934665603ull;
        for (unsigned char c : s) {
            h ^= static_cast<std::uint64_t>(c);
            h *= 1099511628211ull;
        }
        std::ostringstream oss;
        oss << std::hex << std::nouppercase << (h & 0xffffffffull);
        return oss.str();
    }

    std::string promptForTuningProfile() {
        std::vector<ChoiceItem> items = {
            {"balanced", "Balanced (Recommended)",
             "Default profile for general workloads and typical machines."},
            {"efficient", "Efficient", "Lower resource usage, slower background processing."},
            {"aggressive", "Aggressive", "Higher throughput, more background work."}};
        ChoiceOptions opts;
        opts.defaultIndex = 0;
        auto choice = prompt_choice("\nSelect a tuning profile:", items, opts);
        return items[choice].value;
    }

    void maybeBootstrapProjectSession() {
        if (envTruthy(std::getenv("YAMS_DISABLE_PROJECT_SESSION")))
            return;
        if (const char* envSession = std::getenv("YAMS_SESSION_CURRENT"); envSession && *envSession)
            return;

        std::error_code ec;
        fs::path cwd = fs::current_path(ec);
        if (ec)
            return;

        auto root = findGitRoot(cwd);
        if (root.empty())
            root = cwd;
        auto absRoot = fs::absolute(root, ec);
        if (!ec)
            root = absRoot;

        std::string base = root.filename().string();
        if (base.empty())
            base = "project";
        std::string sessionName = "proj-" + sanitizeName(base) + "-" + shortHash(root.string());

        auto svc = app::services::makeSessionService(nullptr);
        if (!svc)
            return;

        if (!svc->exists(sessionName)) {
            svc->init(sessionName, "auto: " + root.string());
        } else {
            svc->use(sessionName);
        }

        // Ensure selector for project root
        svc->use(sessionName);
        auto selectors = svc->listPathSelectors(sessionName);
        const std::string rootPath = root.string();
        if (std::find(selectors.begin(), selectors.end(), rootPath) == selectors.end()) {
            svc->addPathSelector(rootPath, {}, {});
        }

        // Enable watch by default (daemon watcher consumes this)
        if (!svc->watchEnabled(sessionName)) {
            svc->enableWatch(true, sessionName);
        }

        spdlog::info("Project session '{}' ready (root='{}')", sessionName, rootPath);
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

    static Result<void> updateV2Config(
        const fs::path& configPath, const fs::path& dataDir, const fs::path& privateKeyPath,
        const fs::path& publicKeyPath, const std::string& apiKey, bool enableVectorDB,
        const std::string& selectedModel, bool useS3, const std::string& s3Url,
        const std::string& s3Region, const std::string& s3Endpoint, const std::string& s3AccessKey,
        const std::string& s3SecretKey, bool s3UsePathStyle, const std::string& tuningProfile) {
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

            if (!tuningProfile.empty()) {
                const auto sectionPos = content.find("[tuning]");
                if (sectionPos != std::string::npos) {
                    const auto nextSection = content.find("[", sectionPos + 1);
                    const auto sectionEnd =
                        (nextSection == std::string::npos) ? content.size() : nextSection;
                    auto keyPos = content.find("profile = ", sectionPos);
                    if (keyPos == std::string::npos || keyPos > sectionEnd) {
                        content.insert(sectionEnd,
                                       "profile = \"" + escapeTomlString(tuningProfile) + "\"\n");
                    } else {
                        const auto lineEnd = content.find("\n", keyPos);
                        const auto replaceEnd =
                            (lineEnd == std::string::npos) ? content.size() : lineEnd;
                        content.replace(keyPos, replaceEnd - keyPos,
                                        "profile = \"" + escapeTomlString(tuningProfile) + "\"");
                    }
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

    static bool isColbertModelName(const std::string& name) {
        if (name.empty())
            return false;
        std::string lower = name;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return lower.find("colbert") != std::string::npos;
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
            fs::path yamsConfigDir = yams::config::get_config_dir();
            fs::path trustFile = yamsConfigDir / "plugins_trust.txt";
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
                fs::path configPath = yamsConfigDir / "config.toml";
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
#ifdef __APPLE__
            fs::path sys1 = fs::path("/opt/homebrew/lib/yams/plugins/libyams_onnx_plugin.dylib");
            fs::path sys2 = fs::path("/usr/local/lib/yams/plugins/libyams_onnx_plugin.dylib");
#else
            fs::path sys1 = fs::path("/usr/local/lib/yams/plugins/libyams_onnx_plugin.so");
            fs::path sys2 = fs::path("/usr/lib/yams/plugins/libyams_onnx_plugin.so");
#endif
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
            if (name == "mxbai-edge-colbert-v0-17m")
                return "ryandono/mxbai-edge-colbert-v0-17m-onnx-int8";
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

        std::cout << "\n" << cli::ui::section_header("Downloading Model") << "\n";
        std::cout << cli::ui::key_value("Model", model.name) << "\n";
        std::cout << cli::ui::key_value("Size", "~" + std::to_string(model.size_mb) + " MB")
                  << "\n\n";

        // Download model.onnx (try multiple paths)
        std::vector<std::string> modelPaths = {"onnx/model.onnx", "model.onnx"};
        if (model.name == "mxbai-edge-colbert-v0-17m") {
            modelPaths.insert(modelPaths.begin(), "onnx/model_quantized.onnx");
        }
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
        if (model.name == "mxbai-edge-colbert-v0-17m") {
            companions.emplace_back("tokenizer_config.json", "tokenizer_config.json");
            companions.emplace_back("special_tokens_map.json", "special_tokens_map.json");
            companions.emplace_back("skiplist.json", "skiplist.json");
        }
        for (const auto& [filename, localName] : companions) {
            std::string url = "https://huggingface.co/" + repo + "/resolve/main/" + filename;
            (void)downloadFile(url, outputDir / localName, filename);
        }

        std::cout << ui::status_ok("Model downloaded successfully") << "\n";
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

    // Download GLiNER model files using the unified downloader
    Result<void> downloadGlinerModelFiles(const GlinerModel& model, const fs::path& outputDir) {
        fs::create_directories(outputDir);
        fs::path modelFile = outputDir / "model.onnx";

        // Check if already downloaded
        if (fs::exists(modelFile) && fs::exists(outputDir / "tokenizer.json")) {
            std::cout << "  GLiNER model already exists at: " << outputDir.string() << "\n";
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

        std::cout << "\n" << cli::ui::section_header("Downloading GLiNER Model") << "\n";
        std::cout << cli::ui::key_value("Model", model.name) << "\n";
        std::cout << cli::ui::key_value("Size", "~" + std::to_string(model.size_mb) + " MB")
                  << "\n\n";

        // Base URL for this model's repo
        std::string baseUrl = "https://huggingface.co/" + model.repo + "/resolve/main";

        // Download the ONNX model file (from onnx/ subfolder)
        {
            std::string url = baseUrl + "/onnx/" + model.model_file;
            fs::path outPath = outputDir / "model.onnx"; // Always save as model.onnx locally

            auto result = downloadFile(url, outPath, model.model_file);
            if (!result) {
                return Error{ErrorCode::NetworkError,
                             "Failed to download model: " + model.model_file};
            }
        }

        // Download tokenizer files (from repo root)
        for (const auto& filename : GLINER_TOKENIZER_FILES) {
            std::string url = baseUrl + "/" + filename;
            fs::path outPath = outputDir / filename;

            auto result = downloadFile(url, outPath, filename);
            if (!result) {
                // tokenizer.json is required, others are optional
                if (filename == "tokenizer.json") {
                    spdlog::warn(
                        "Required tokenizer.json not available, model may not work correctly");
                } else {
                    spdlog::debug("Optional file {} not available", filename);
                }
            }
        }

        std::cout << ui::status_ok("GLiNER model downloaded successfully") << "\n";
        return Result<void>{};
    }

    std::string promptForGlinerModel(const fs::path& dataPath) {
        // Build choice items
        std::vector<ChoiceItem> items;
        items.reserve(GLINER_MODELS.size() + 1);

        // Add "skip" option first
        ChoiceItem skipItem;
        skipItem.value = "";
        skipItem.label = "Skip GLiNER model download";
        skipItem.description = "You can download a model later with: yams model download --gliner";
        items.push_back(std::move(skipItem));

        for (const auto& m : GLINER_MODELS) {
            ChoiceItem ci;
            ci.value = m.name;
            ci.label = m.name + " (~" + std::to_string(m.size_mb) + " MB)";
            ci.description = m.description;
            items.push_back(std::move(ci));
        }

        size_t chosenIdx = prompt_choice(
            "\nGLiNER model for named entity extraction (NER):", items,
            ChoiceOptions{.defaultIndex = 1, .allowEmpty = true, .retryOnInvalid = true});

        if (chosenIdx == 0) {
            // Skip selected
            std::cout << "\nSkipping GLiNER model download.\n";
            std::cout << "You can download a model later with: yams model download --gliner\n";
            return "";
        }

        const auto& selectedModel = GLINER_MODELS[chosenIdx - 1];

        // Check if model already exists
        fs::path modelDir = dataPath / "models" / "gliner" / selectedModel.name;
        fs::path modelPath = modelDir / "model.onnx";

        if (fs::exists(modelPath)) {
            std::cout << "\nGLiNER model already downloaded at: " << modelDir.string() << "\n";
        } else {
            // Ask if user wants to download now
            bool downloadNow = prompt_yes_no("\nDownload the GLiNER model now? [Y/n]: ",
                                             YesNoOptions{.defaultYes = true});

            if (downloadNow) {
                auto result = downloadGlinerModelFiles(selectedModel, modelDir);
                if (!result) {
                    spdlog::warn("GLiNER model download failed: {}", result.error().message);
                    std::cout << "\nYou can download the model later with:\n"
                              << "  yams model download --gliner " << selectedModel.name << "\n";
                }
            } else {
                std::cout << "\nTo download this model later:\n"
                          << "  yams model download --gliner " << selectedModel.name << "\n";
            }
        }
        return selectedModel.name;
    }

    /**
     * @brief Unified GLiNER model setup entry point for both init and already-initialized states.
     *
     * Handles the prompt logic based on nonInteractive_ and autoInit_ flags:
     * - Interactive mode: prompts user for model selection
     * - Auto mode: downloads default model without prompting
     * - Non-interactive (non-auto): skips GLiNER setup entirely
     */
    void maybeSetupGlinerModel(const fs::path& dataPath, const fs::path& configPath) {
        std::string selectedGlinerModel;

        if (autoInit_) {
            // Auto mode: download the small model by default
            selectedGlinerModel = GLINER_MODELS[0].name;
            spdlog::info("Using default GLiNER model: {}", selectedGlinerModel);
            fs::path glinerModelDir = dataPath / "models" / "gliner" / selectedGlinerModel;
            auto result = downloadGlinerModelFiles(GLINER_MODELS[0], glinerModelDir);
            if (!result) {
                spdlog::warn("GLiNER model download failed: {}", result.error().message);
                spdlog::warn("You can download the model later with: yams model download --gliner");
            }
        } else if (!nonInteractive_) {
            // Interactive mode: prompt user
            bool setupGliner =
                prompt_yes_no("\nDownload GLiNER model for named entity extraction? [Y/n]: ",
                              YesNoOptions{.defaultYes = true});
            if (setupGliner) {
                selectedGlinerModel = promptForGlinerModel(dataPath);
            }
        }
        // Non-interactive non-auto: skip GLiNER setup

        // Update config with GLiNER model path if selected
        if (!selectedGlinerModel.empty()) {
            updateGlinerConfig(configPath, dataPath, selectedGlinerModel);
        }
    }

    /**
     * @brief Updates config.toml with GLiNER model path.
     */
    void updateGlinerConfig(const fs::path& configPath, const fs::path& dataPath,
                            const std::string& selectedGlinerModel) {
        try {
            std::ifstream in(configPath);
            std::stringstream buf;
            buf << in.rdbuf();
            in.close();
            std::string content = buf.str();

            // Ensure [plugins] section exists
            if (content.find("[plugins]") == std::string::npos) {
                content.append("\n[plugins]\n");
            }

            // Ensure [plugins.glint] section exists
            if (content.find("[plugins.glint]") == std::string::npos) {
                auto pluginsPos = content.find("[plugins]");
                if (pluginsPos != std::string::npos) {
                    auto nextSec = content.find("\n[", pluginsPos + 1);
                    auto insertPos = (nextSec == std::string::npos) ? content.size() : nextSec;
                    content.insert(insertPos, "\n[plugins.glint]\nenabled = true\n");
                }
            }

            // Set model_path in [plugins.glint]
            fs::path glinerModelPath =
                dataPath / "models" / "gliner" / selectedGlinerModel / "model.onnx";
            auto secPos = content.find("[plugins.glint]");
            if (secPos != std::string::npos) {
                auto nextSec = content.find("\n[", secPos + 1);
                auto rangeEnd = (nextSec == std::string::npos) ? content.size() : nextSec;
                auto keyPos = content.find("model_path", secPos);
                std::string modelPathLine =
                    "model_path = \"" + escapeTomlString(glinerModelPath.string()) + "\"";
                if (keyPos == std::string::npos || keyPos > rangeEnd) {
                    content.insert(rangeEnd, modelPathLine + "\n");
                } else {
                    auto lineEnd = content.find("\n", keyPos);
                    if (lineEnd == std::string::npos)
                        lineEnd = content.size();
                    content.replace(keyPos, lineEnd - keyPos, modelPathLine);
                }
            }

            std::ofstream outCfg(configPath, std::ios::trunc);
            outCfg << content;
            outCfg.close();
            spdlog::info("Configured [plugins.glint].model_path");
        } catch (const std::exception& e) {
            spdlog::debug("Skipping GLiNER config write: {}", e.what());
        }
    }

    // ==========================================================================
    // Reranker Model Setup (for two-stage hybrid search)
    // ==========================================================================

    /**
     * @brief Download reranker model files using the unified downloader.
     */
    Result<void> downloadRerankerModelFiles(const RerankerModel& model, const fs::path& outputDir) {
        fs::create_directories(outputDir);
        fs::path modelFile = outputDir / "model.onnx";

        // Check if already downloaded
        if (fs::exists(modelFile) && fs::exists(outputDir / "tokenizer.json")) {
            std::cout << "  Reranker model already exists at: " << outputDir.string() << "\n";
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

        std::cout << "\n" << cli::ui::section_header("Downloading Reranker Model") << "\n";
        std::cout << cli::ui::key_value("Model", model.name) << "\n";
        std::cout << cli::ui::key_value("Size", "~" + std::to_string(model.size_mb) + " MB")
                  << "\n";
        std::cout << cli::ui::key_value("Max tokens", std::to_string(model.max_tokens)) << "\n\n";

        // Build base URL
        std::string baseUrl = "https://huggingface.co/" + model.repo + "/resolve/main/";

        // Download model.onnx
        std::string modelUrl = baseUrl + model.model_file;
        auto result = downloadFile(modelUrl, modelFile, "model.onnx");
        if (!result) {
            return Error{ErrorCode::NetworkError,
                         "Failed to download model.onnx: " + result.error().message};
        }

        // Download tokenizer files (required for reranker)
        for (const auto& filename : RERANKER_TOKENIZER_FILES) {
            std::string url = baseUrl + filename;
            auto r = downloadFile(url, outputDir / filename, filename);
            // tokenizer.json is required, others are optional
            if (!r && filename == "tokenizer.json") {
                return Error{ErrorCode::NetworkError,
                             "Failed to download tokenizer.json: " + r.error().message};
            }
        }

        std::cout << ui::status_ok("Reranker model downloaded successfully") << "\n";
        std::cout << "\n"
                  << ui::status_info("Reranker improves hybrid search by re-scoring "
                                     "candidates with cross-attention")
                  << "\n";
        return Result<void>{};
    }

    /**
     * @brief Prompt user to select a reranker model.
     */
    std::string promptForRerankerModel(const fs::path& dataPath) {
        // Build choice items
        std::vector<ChoiceItem> items;
        items.reserve(RERANKER_MODELS.size());
        for (const auto& m : RERANKER_MODELS) {
            ChoiceItem ci;
            ci.value = m.name;
            ci.label = m.name + " (" + std::to_string(m.size_mb) + " MB)";
            ci.description = m.description;
            items.push_back(std::move(ci));
        }

        size_t defaultIndex = 0; // bge-reranker-base is first (recommended)

        size_t chosenIdx = prompt_choice("\nAvailable reranker models:", items,
                                         ChoiceOptions{.defaultIndex = defaultIndex,
                                                       .allowEmpty = true,
                                                       .retryOnInvalid = true});

        const auto& selectedModel = RERANKER_MODELS[chosenIdx];

        // Check if model already exists
        fs::path modelDir = dataPath / "models" / "reranker" / selectedModel.name;
        fs::path modelPath = modelDir / "model.onnx";

        if (fs::exists(modelPath)) {
            std::cout << "\nReranker model already downloaded at: " << modelPath.string() << "\n";
        } else {
            // Ask if user wants to download now
            bool downloadNow = prompt_yes_no("\nDownload the reranker model now? [Y/n]: ",
                                             YesNoOptions{.defaultYes = true});

            if (downloadNow) {
                auto result = downloadRerankerModelFiles(selectedModel, modelDir);
                if (!result) {
                    spdlog::warn("Reranker model download failed: {}", result.error().message);
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

    /**
     * @brief Unified reranker model setup entry point for both init and already-initialized states.
     *
     * Handles the prompt logic based on nonInteractive_ and autoInit_ flags:
     * - Interactive mode: prompts user for model selection
     * - Auto mode: downloads default model without prompting
     * - Non-interactive (non-auto): skips reranker setup entirely
     */
    std::string maybeSetupRerankerModel(const fs::path& dataPath, const fs::path& configPath) {
        std::string selectedRerankerModel;

        if (autoInit_) {
            // Auto mode: download the base model by default
            selectedRerankerModel = RERANKER_MODELS[0].name;
            spdlog::info("Using default reranker model: {}", selectedRerankerModel);
            fs::path rerankerModelDir = dataPath / "models" / "reranker" / selectedRerankerModel;
            auto result = downloadRerankerModelFiles(RERANKER_MODELS[0], rerankerModelDir);
            if (!result) {
                spdlog::warn("Reranker model download failed: {}", result.error().message);
                spdlog::warn(
                    "You can download the model later with: yams model download bge-reranker-base");
            }
        } else if (!nonInteractive_) {
            // Interactive mode: prompt user
            bool setupReranker =
                prompt_yes_no("\nDownload reranker model for improved hybrid search? [Y/n]: ",
                              YesNoOptions{.defaultYes = true});
            if (setupReranker) {
                selectedRerankerModel = promptForRerankerModel(dataPath);
            }
        }
        // Non-interactive non-auto: skip reranker setup

        // Update config with reranker model path if selected
        if (!selectedRerankerModel.empty()) {
            updateRerankerConfig(configPath, dataPath, selectedRerankerModel);
        }

        return selectedRerankerModel;
    }

    /**
     * @brief Updates config.toml with reranker model path.
     */
    void updateRerankerConfig(const fs::path& configPath, const fs::path& dataPath,
                              const std::string& selectedRerankerModel) {
        try {
            std::ifstream in(configPath);
            std::stringstream buf;
            buf << in.rdbuf();
            in.close();
            std::string content = buf.str();

            // Ensure [search] section exists
            if (content.find("[search]") == std::string::npos) {
                content.append("\n[search]\n");
            }

            // Set reranker_model and reranker_model_path in [search]
            fs::path rerankerModelPath =
                dataPath / "models" / "reranker" / selectedRerankerModel / "model.onnx";
            auto secPos = content.find("[search]");
            if (secPos != std::string::npos) {
                auto nextSec = content.find("\n[", secPos + 1);
                auto rangeEnd = (nextSec == std::string::npos) ? content.size() : nextSec;

                // Add reranker_model
                auto namePos = content.find("reranker_model =", secPos);
                std::string modelNameLine =
                    "reranker_model = \"" + escapeTomlString(selectedRerankerModel) + "\"";
                if (namePos == std::string::npos || namePos > rangeEnd) {
                    content.insert(rangeEnd, modelNameLine + "\n");
                } else {
                    auto lineEnd = content.find('\n', namePos);
                    if (lineEnd == std::string::npos)
                        lineEnd = content.size();
                    content.replace(namePos, lineEnd - namePos, modelNameLine);
                }

                // Add reranker_model_path
                auto keyPos = content.find("reranker_model_path", secPos);
                std::string modelPathLine = "reranker_model_path = \"" +
                                            escapeTomlString(rerankerModelPath.string()) + "\"";
                if (keyPos == std::string::npos || keyPos > rangeEnd) {
                    content.insert(rangeEnd, modelPathLine + "\n");
                } else {
                    auto lineEnd = content.find('\n', keyPos);
                    if (lineEnd == std::string::npos)
                        lineEnd = content.size();
                    content.replace(keyPos, lineEnd - keyPos, modelPathLine);
                }

                // Also enable reranking by default
                auto enablePos = content.find("enable_reranking", secPos);
                if (enablePos == std::string::npos || enablePos > rangeEnd) {
                    // Find position after reranker_model_path (re-find since string changed)
                    secPos = content.find("[search]");
                    nextSec = content.find("\n[", secPos + 1);
                    rangeEnd = (nextSec == std::string::npos) ? content.size() : nextSec;
                    content.insert(rangeEnd, "enable_reranking = true\n");
                }
            }

            std::ofstream outCfg(configPath, std::ios::trunc);
            outCfg << content;
            outCfg.close();
            spdlog::info("Configured [search].reranker_model_path");
        } catch (const std::exception& e) {
            spdlog::debug("Skipping reranker config write: {}", e.what());
        }
    }

    void updateColbertRerankingConfig(const fs::path& configPath) {
        try {
            std::ifstream in(configPath);
            std::stringstream buf;
            buf << in.rdbuf();
            in.close();
            std::string content = buf.str();

            // Ensure [search] section exists
            if (content.find("[search]") == std::string::npos) {
                content.append("\n[search]\n");
            }

            auto secPos = content.find("[search]");
            if (secPos != std::string::npos) {
                auto nextSec = content.find("\n[", secPos + 1);
                auto rangeEnd = (nextSec == std::string::npos) ? content.size() : nextSec;

                auto enablePos = content.find("enable_reranking", secPos);
                if (enablePos == std::string::npos || enablePos > rangeEnd) {
                    content.insert(rangeEnd, "enable_reranking = true\n");
                } else {
                    auto lineEnd = content.find('\n', enablePos);
                    if (lineEnd == std::string::npos)
                        lineEnd = content.size();
                    content.replace(enablePos, lineEnd - enablePos, "enable_reranking = true");
                }

                auto keyPos = content.find("reranker_model_path", secPos);
                if (keyPos != std::string::npos && keyPos < rangeEnd) {
                    auto lineEnd = content.find('\n', keyPos);
                    if (lineEnd == std::string::npos)
                        lineEnd = content.size();
                    content.erase(keyPos, lineEnd - keyPos + 1);
                }

                auto namePos = content.find("reranker_model =", secPos);
                if (namePos != std::string::npos && namePos < rangeEnd) {
                    auto lineEnd = content.find('\n', namePos);
                    if (lineEnd == std::string::npos)
                        lineEnd = content.size();
                    content.erase(namePos, lineEnd - namePos + 1);
                }
            }

            std::ofstream outCfg(configPath, std::ios::trunc);
            outCfg << content;
            outCfg.close();
            spdlog::info("Configured [search].enable_reranking for ColBERT MaxSim");
        } catch (const std::exception& e) {
            spdlog::debug("Skipping ColBERT reranking config write: {}", e.what());
        }
    }

    /**
     * @brief Unified grammar setup entry point for both init and already-initialized states.
     *
     * Handles the prompt logic based on nonInteractive_ and autoInit_ flags:
     * - Interactive mode: prompts user, then shows menu
     * - Auto mode: downloads recommended grammars without prompting
     * - Non-interactive (non-auto): skips grammar setup entirely
     */
    void maybeSetupGrammars(const fs::path& dataPath) {
        if (autoInit_) {
            // Auto mode: download recommended grammars without prompting
            downloadGrammars(dataPath, true);
        } else if (!nonInteractive_) {
            // Interactive mode: prompt user
            bool setupGrammars =
                prompt_yes_no("\nDownload tree-sitter grammars for symbol extraction? [Y/n]: ",
                              YesNoOptions{.defaultYes = true});
            if (setupGrammars) {
                downloadGrammars(dataPath, false);
            }
        }
        // Non-interactive non-auto: skip grammar setup
    }

    /**
     * @brief Downloads and builds tree-sitter grammars.
     *
     * @param dataPath Base data directory for fallback grammar path
     * @param useDefaults If true, downloads recommended grammars without menu
     */
    void downloadGrammars([[maybe_unused]] const fs::path& dataPath, bool useDefaults) {
        // Determine grammar output directory (platform-aware via config helper)
        fs::path grammarDir = yams::config::get_data_dir() / "grammars";
        fs::create_directories(grammarDir);

        std::vector<std::string> selectedLanguages;

        if (useDefaults) {
            // Auto mode: download all recommended grammars
            for (const auto& g : SUPPORTED_GRAMMARS) {
                if (g.recommended) {
                    selectedLanguages.emplace_back(g.language);
                }
            }
            std::cout << "\nDownloading recommended grammars: ";
            for (size_t i = 0; i < selectedLanguages.size(); ++i) {
                std::cout << selectedLanguages[i];
                if (i + 1 < selectedLanguages.size())
                    std::cout << ", ";
            }
            std::cout << "\n";
        } else {
            // Interactive mode: show menu
            std::cout << "\n" << cli::ui::section_header("Tree-sitter Grammars") << "\n";
            std::cout << cli::ui::numbered_item(
                             "Recommended (C, C++, Python, JS, TS, Rust, Go, Swift)", 1, 2)
                      << "\n";
            std::cout << cli::ui::numbered_item("All supported grammars", 2, 2) << "\n";
            std::cout << cli::ui::numbered_item("Select specific languages", 3, 2) << "\n";
            std::cout << cli::ui::numbered_item("Skip grammar download", 4, 2) << "\n";
            std::cout << "  Choice [1]: ";

            std::string choice;
            std::getline(std::cin, choice);
            if (choice.empty())
                choice = "1";

            if (choice == "1") {
                for (const auto& g : SUPPORTED_GRAMMARS) {
                    if (g.recommended) {
                        selectedLanguages.emplace_back(g.language);
                    }
                }
            } else if (choice == "2") {
                for (const auto& g : SUPPORTED_GRAMMARS) {
                    selectedLanguages.emplace_back(g.language);
                }
            } else if (choice == "3") {
                std::cout << "\nEnter language names separated by commas (e.g., c,cpp,python):\n";
                std::cout << "Available: ";
                for (size_t i = 0; i < std::size(SUPPORTED_GRAMMARS); ++i) {
                    std::cout << SUPPORTED_GRAMMARS[i].language;
                    if (i + 1 < std::size(SUPPORTED_GRAMMARS))
                        std::cout << ", ";
                }
                std::cout << "\nLanguages: ";
                std::string langs;
                std::getline(std::cin, langs);

                // Parse comma-separated list
                std::istringstream iss(langs);
                std::string lang;
                while (std::getline(iss, lang, ',')) {
                    yams::config::trim(lang);
                    if (!lang.empty()) {
                        selectedLanguages.push_back(lang);
                    }
                }
            } else {
                std::cout << "Skipping grammar download.\n";
                std::cout
                    << "You can download grammars later with: yams grammar download <language>\n";
                return;
            }
        }

        if (selectedLanguages.empty()) {
            std::cout << "No grammars selected.\n";
            return;
        }

        // Check if build tools are available
        bool canBuild = checkBuildToolsAvailable();
        if (!canBuild) {
            std::cout << "\n"
                      << cli::ui::status_warning("Build tools not found (git + compiler required).")
                      << "\n";
            std::cout << "Please install:\n";
#ifdef _WIN32
            std::cout << "  "
                      << cli::ui::bullet("Git for Windows: https://git-scm.com/download/win")
                      << "\n";
            std::cout << "  " << cli::ui::bullet("Visual Studio Build Tools or MinGW-w64") << "\n";
#else
            std::cout << "  " << cli::ui::bullet("git, gcc/g++ or clang") << "\n";
#endif
            std::cout
                << "\nYou can download grammars later with: yams grammar download <language>\n";
            return;
        }

        // Check if tree-sitter CLI is available (needed for some grammars)
        bool hasTreeSitter = false;
#ifdef _WIN32
        hasTreeSitter = std::system("where tree-sitter > NUL 2>&1") == 0;
#else
        hasTreeSitter = std::system("which tree-sitter > /dev/null 2>&1") == 0;
#endif

        std::cout << "\n" << cli::ui::section_header("Building Grammars") << "\n";
        std::cout << cli::ui::key_value("Output", grammarDir.string()) << "\n";
        if (!hasTreeSitter) {
            std::cout << cli::ui::colorize(
                             "  Note: Some grammars (swift, perl, p4) require tree-sitter CLI",
                             cli::ui::Ansi::DIM)
                      << "\n";
            std::cout << cli::ui::colorize("        Install with: npm i -g tree-sitter-cli",
                                           cli::ui::Ansi::DIM)
                      << "\n";
        }
        std::cout << "\n";

        size_t succeeded = 0;
        size_t failed = 0;

        for (const auto& lang : selectedLanguages) {
            std::cout << "  " << cli::ui::pad_right(lang, 12) << " ";
            std::cout.flush();

            auto result = downloadAndBuildGrammar(lang, grammarDir);
            if (result) {
                std::cout << cli::ui::status_ok("") << "\n";
                succeeded++;
            } else {
                std::cout << cli::ui::status_error(result.error().message) << "\n";
                failed++;
            }
        }

        std::cout << "\n";
        if (failed == 0) {
            std::cout << cli::ui::status_ok("Grammar setup complete: " + std::to_string(succeeded) +
                                            " succeeded")
                      << "\n";
        } else {
            std::cout << cli::ui::status_warning(
                             "Grammar setup complete: " +
                             cli::ui::colorize(std::to_string(succeeded) + " succeeded",
                                               cli::ui::Ansi::GREEN) +
                             ", " +
                             cli::ui::colorize(std::to_string(failed) + " failed",
                                               cli::ui::Ansi::RED))
                      << "\n";
        }
    }

    static bool checkBuildToolsAvailable() {
        auto have = [](const char* tool) {
#ifdef _WIN32
            std::string cmd = std::string("where ") + tool + " > NUL 2>&1";
#else
            std::string cmd = std::string("which ") + tool + " > /dev/null 2>&1";
#endif
            return std::system(cmd.c_str()) == 0;
        };

        bool hasGit = have("git");
#ifdef _WIN32
        bool hasCompiler = have("cl") || have("g++") || have("clang++");
#else
        bool hasCompiler = have("g++") || have("gcc") || have("clang++") || have("clang");
#endif
        return hasGit && hasCompiler;
    }

    static Result<void> downloadAndBuildGrammar(const std::string& language,
                                                const fs::path& outputDir) {
        // Find the grammar repo
        const GrammarInfo* grammarInfo = nullptr;
        for (const auto& g : SUPPORTED_GRAMMARS) {
            if (g.language == language) {
                grammarInfo = &g;
                break;
            }
        }

        if (!grammarInfo) {
            return Error{ErrorCode::NotFound, "Unknown language: " + language};
        }

        // Create temp directory for build
        auto tempDir = fs::temp_directory_path() / ("yams-grammar-" + language);
        fs::create_directories(tempDir);

        // Cleanup helper
        auto cleanup = [&tempDir]() {
            try {
                fs::remove_all(tempDir);
            } catch (...) {
            }
        };

        try {
            // Clone repository (some grammars need specific branches)
            std::string repoUrl = "https://github.com/" + std::string(grammarInfo->repo);
            std::string branch;

            // SQL grammar has generated files only on gh-pages branch
            if (language == "sql") {
                branch = " -b gh-pages";
            }

            std::string cloneCmd = "git clone --depth 1 --quiet" + branch + " " + repoUrl + " " +
                                   (tempDir / ("tree-sitter-" + language)).string();
#ifdef _WIN32
            cloneCmd += " 2>NUL";
#else
            cloneCmd += " 2>/dev/null";
#endif

            if (std::system(cloneCmd.c_str()) != 0) {
                cleanup();
                return Error{ErrorCode::NetworkError, "git clone failed"};
            }

            auto buildDir = tempDir / ("tree-sitter-" + language);

            // Some grammars have subdirectory structures
            if (language == "typescript") {
                buildDir = buildDir / "typescript";
            } else if (language == "php") {
                buildDir = buildDir / "php";
            }
            // Note: SQL uses gh-pages branch which has src/parser.c in root

            auto parserC = buildDir / "src" / "parser.c";
            auto scannerC = buildDir / "src" / "scanner.c";
            auto scannerCC = buildDir / "src" / "scanner.cc";

            // Some grammars don't ship pre-generated parser.c - need tree-sitter generate
            if (!fs::exists(parserC)) {
                // Check if tree-sitter CLI is available
#ifdef _WIN32
                bool hasTreeSitter = std::system("where tree-sitter > NUL 2>&1") == 0;
#else
                bool hasTreeSitter = std::system("which tree-sitter > /dev/null 2>&1") == 0;
#endif
                if (!hasTreeSitter) {
                    cleanup();
                    return Error{ErrorCode::NotFound, "parser.c missing, install tree-sitter CLI"};
                }

                // Run tree-sitter generate
                std::string genCmd = "cd \"" + buildDir.string() + "\" && tree-sitter generate";
#ifdef _WIN32
                genCmd += " > NUL 2>&1";
#else
                genCmd += " > /dev/null 2>&1";
#endif
                if (std::system(genCmd.c_str()) != 0) {
                    cleanup();
                    return Error{ErrorCode::InternalError, "tree-sitter generate failed"};
                }

                // Re-check for parser.c
                if (!fs::exists(parserC)) {
                    cleanup();
                    return Error{ErrorCode::NotFound, "parser.c not found after generate"};
                }
            }

            // Determine library name and compiler
#ifdef _WIN32
            std::string libName = "tree-sitter-" + language + ".dll";
            std::string compiler = "cl"; // Try MSVC first

            // Check if cl.exe is available, fallback to g++
            if (std::system("where cl > NUL 2>&1") != 0) {
                if (std::system("where g++ > NUL 2>&1") == 0) {
                    compiler = "g++";
                } else if (std::system("where clang++ > NUL 2>&1") == 0) {
                    compiler = "clang++";
                } else {
                    cleanup();
                    return Error{ErrorCode::NotFound, "No compiler found"};
                }
            }
#elif defined(__APPLE__)
            std::string libName = "libtree-sitter-" + language + ".dylib";
            std::string compiler = "clang++";
#else
            std::string libName = "libtree-sitter-" + language + ".so";
            std::string compiler = "g++";
#endif

            // Build command - use absolute paths for MSVC compatibility
            std::ostringstream buildCmd;

#ifdef _WIN32
            // MSVC needs to run from the build directory with pushd/popd
            // or use absolute paths. We'll use absolute paths.
            auto srcDir = buildDir / "src";
            auto parserCPath = srcDir / "parser.c";
            auto scannerCPath = srcDir / "scanner.c";
            auto scannerCCPath = srcDir / "scanner.cc";
            auto outputPath = buildDir / libName;

            // Determine if we have a C++ scanner - this affects our compilation strategy
            bool hasCppScanner = fs::exists(scannerCC);

            if (compiler == "cl") {
                // MSVC build with absolute paths, suppress all output
                // MSVC handles mixed C/C++ in a single invocation correctly via file extension
                buildCmd << "cl /nologo /LD /O2 /I\"" << srcDir.string() << "\" ";
                buildCmd << "\"" << parserCPath.string() << "\"";
                if (fs::exists(scannerC)) {
                    buildCmd << " \"" << scannerCPath.string() << "\"";
                } else if (hasCppScanner) {
                    buildCmd << " \"" << scannerCCPath.string() << "\"";
                }
                buildCmd << " /Fe:\"" << outputPath.string() << "\" >NUL 2>&1";
            } else if (hasCppScanner) {
                // MinGW/Clang with C++ scanner: compile separately then link
                auto parserObj = buildDir / "parser.o";
                auto scannerObj = buildDir / "scanner.o";

                // Compile parser.c as C
                std::ostringstream compileParserCmd;
                compileParserCmd << "gcc -c -O2 -I\"" << srcDir.string() << "\" ";
                compileParserCmd << "\"" << parserCPath.string() << "\" ";
                compileParserCmd << "-o \"" << parserObj.string() << "\" >NUL 2>&1";
                if (std::system(compileParserCmd.str().c_str()) != 0) {
                    cleanup();
                    return Error{ErrorCode::InternalError, "parser.c compile failed"};
                }

                // Compile scanner.cc as C++
                std::ostringstream compileScannerCmd;
                compileScannerCmd << "g++ -c -O2 -std=c++14 -I\"" << srcDir.string() << "\" ";
                compileScannerCmd << "\"" << scannerCCPath.string() << "\" ";
                compileScannerCmd << "-o \"" << scannerObj.string() << "\" >NUL 2>&1";
                if (std::system(compileScannerCmd.str().c_str()) != 0) {
                    cleanup();
                    return Error{ErrorCode::InternalError, "scanner.cc compile failed"};
                }

                // Link with g++
                buildCmd << "g++ -shared -O2 ";
                buildCmd << "\"" << parserObj.string() << "\" ";
                buildCmd << "\"" << scannerObj.string() << "\" ";
                buildCmd << "-o \"" << outputPath.string() << "\" >NUL 2>&1";
            } else {
                // MinGW/Clang pure C build: use gcc
                buildCmd << "gcc -shared -O2 -I\"" << srcDir.string() << "\" ";
                buildCmd << "\"" << parserCPath.string() << "\"";
                if (fs::exists(scannerC)) {
                    buildCmd << " \"" << scannerCPath.string() << "\"";
                }
                buildCmd << " -o \"" << outputPath.string() << "\" >NUL 2>&1";
            }
#elif defined(__APPLE__)
            // macOS build with absolute paths
            auto srcDir = buildDir / "src";
            auto parserCPath = srcDir / "parser.c";
            auto scannerCPath = srcDir / "scanner.c";
            auto scannerCCPath = srcDir / "scanner.cc";
            auto outputPath = buildDir / libName;

            // Determine if we have a C++ scanner - this affects our compilation strategy
            bool hasCppScanner = fs::exists(scannerCC);

            if (hasCppScanner) {
                // Mixed C/C++ build: compile separately then link
                auto parserObj = buildDir / "parser.o";
                auto scannerObj = buildDir / "scanner.o";

                // Compile parser.c as C
                std::ostringstream compileParserCmd;
                compileParserCmd << "clang -c -fPIC -O2 -I\"" << srcDir.string() << "\" ";
                compileParserCmd << "\"" << parserCPath.string() << "\" ";
                compileParserCmd << "-o \"" << parserObj.string() << "\" 2>/dev/null";
                if (std::system(compileParserCmd.str().c_str()) != 0) {
                    cleanup();
                    return Error{ErrorCode::InternalError, "parser.c compile failed"};
                }

                // Compile scanner.cc as C++
                std::ostringstream compileScannerCmd;
                compileScannerCmd << "clang++ -c -fPIC -O2 -std=c++14 -I\"" << srcDir.string()
                                  << "\" ";
                compileScannerCmd << "\"" << scannerCCPath.string() << "\" ";
                compileScannerCmd << "-o \"" << scannerObj.string() << "\" 2>/dev/null";
                if (std::system(compileScannerCmd.str().c_str()) != 0) {
                    cleanup();
                    return Error{ErrorCode::InternalError, "scanner.cc compile failed"};
                }

                // Link with clang++ (needed for C++ runtime)
                buildCmd << "clang++ -dynamiclib -O2 ";
                buildCmd << "\"" << parserObj.string() << "\" ";
                buildCmd << "\"" << scannerObj.string() << "\" ";
                buildCmd << "-o \"" << outputPath.string() << "\" 2>/dev/null";
            } else {
                // Pure C build: use clang (not clang++)
                buildCmd << "clang -dynamiclib -fPIC -O2 -I\"" << srcDir.string() << "\" ";
                buildCmd << "\"" << parserCPath.string() << "\"";
                if (fs::exists(scannerC)) {
                    buildCmd << " \"" << scannerCPath.string() << "\"";
                }
                buildCmd << " -o \"" << outputPath.string() << "\" 2>/dev/null";
            }
#else
            // Linux build with absolute paths
            auto srcDir = buildDir / "src";
            auto parserCPath = srcDir / "parser.c";
            auto scannerCPath = srcDir / "scanner.c";
            auto scannerCCPath = srcDir / "scanner.cc";
            auto outputPath = buildDir / libName;

            // Determine if we have a C++ scanner - this affects our compilation strategy
            bool hasCppScanner = fs::exists(scannerCC);

            if (hasCppScanner) {
                // Mixed C/C++ build: compile separately then link
                auto parserObj = buildDir / "parser.o";
                auto scannerObj = buildDir / "scanner.o";

                // Compile parser.c as C
                std::ostringstream compileParserCmd;
                compileParserCmd << "gcc -c -fPIC -O2 -I\"" << srcDir.string() << "\" ";
                compileParserCmd << "\"" << parserCPath.string() << "\" ";
                compileParserCmd << "-o \"" << parserObj.string() << "\" 2>/dev/null";
                if (std::system(compileParserCmd.str().c_str()) != 0) {
                    cleanup();
                    return Error{ErrorCode::InternalError, "parser.c compile failed"};
                }

                // Compile scanner.cc as C++
                std::ostringstream compileScannerCmd;
                compileScannerCmd << "g++ -c -fPIC -O2 -std=c++14 -I\"" << srcDir.string() << "\" ";
                compileScannerCmd << "\"" << scannerCCPath.string() << "\" ";
                compileScannerCmd << "-o \"" << scannerObj.string() << "\" 2>/dev/null";
                if (std::system(compileScannerCmd.str().c_str()) != 0) {
                    cleanup();
                    return Error{ErrorCode::InternalError, "scanner.cc compile failed"};
                }

                // Link with g++ (needed for C++ runtime)
                buildCmd << "g++ -shared -O2 ";
                buildCmd << "\"" << parserObj.string() << "\" ";
                buildCmd << "\"" << scannerObj.string() << "\" ";
                buildCmd << "-o \"" << outputPath.string() << "\" 2>/dev/null";
            } else {
                // Pure C build: use gcc (not g++)
                buildCmd << "gcc -shared -fPIC -O2 -I\"" << srcDir.string() << "\" ";
                buildCmd << "\"" << parserCPath.string() << "\"";
                if (fs::exists(scannerC)) {
                    buildCmd << " \"" << scannerCPath.string() << "\"";
                }
                buildCmd << " -o \"" << outputPath.string() << "\" 2>/dev/null";
            }
#endif

            if (std::system(buildCmd.str().c_str()) != 0) {
                cleanup();
                return Error{ErrorCode::InternalError, "build failed"};
            }

            // Copy to output directory (all platforms now use outputPath)
            if (!fs::exists(outputPath)) {
                cleanup();
                return Error{ErrorCode::NotFound, "built library not found"};
            }

            auto finalPath = outputDir / libName;
            if (outputPath != finalPath) {
                fs::copy_file(outputPath, finalPath, fs::copy_options::overwrite_existing);
            }

            cleanup();
            return Result<void>();

        } catch (const std::exception& e) {
            cleanup();
            return Error{ErrorCode::InternalError, e.what()};
        }
    }

    /**
     * @brief Installs YAMS skill file for AI agents (Claude Code, OpenCode).
     *
     * Handles the prompt logic based on nonInteractive_ and autoInit_ flags:
     * - Interactive mode: prompts user for installation
     * - Auto mode: installs skill automatically
     * - Non-interactive (non-auto): skips skill installation
     */
    void maybeSetupAgentSkill() {
        if (autoInit_) {
            // Auto mode: install skill without prompting
            installAgentSkill(true, true);
        } else if (!nonInteractive_) {
            // Interactive mode: prompt user
            bool installSkill =
                prompt_yes_no("\nInstall YAMS skill for AI agents (Claude Code, OpenCode)? [Y/n]: ",
                              YesNoOptions{.defaultYes = true});
            if (installSkill) {
                // Ask which agents to install for
                std::cout << "\n" << cli::ui::section_header("AI Agent Skill Installation") << "\n";
                std::cout << cli::ui::numbered_item("Claude Code only", 1, 2) << "\n";
                std::cout << cli::ui::numbered_item("OpenCode only", 2, 2) << "\n";
                std::cout << cli::ui::numbered_item("Both (recommended)", 3, 2) << "\n";
                std::cout << "  Choice [3]: ";

                std::string choice;
                std::getline(std::cin, choice);
                if (choice.empty())
                    choice = "3";

                bool installClaude = (choice == "1" || choice == "3");
                bool installOpenCode = (choice == "2" || choice == "3");
                installAgentSkill(installClaude, installOpenCode);
            }
        }
        // Non-interactive non-auto: skip skill installation
    }

    /**
     * @brief Writes the YAMS skill file to the specified agent skill directories.
     */
    void installAgentSkill(bool installClaude, bool installOpenCode) {
        const char* home = std::getenv("HOME");
        if (!home) {
#ifdef _WIN32
            home = std::getenv("USERPROFILE");
#endif
            if (!home) {
                spdlog::warn("Could not determine home directory for skill installation");
                return;
            }
        }

        fs::path homeDir(home);
        fs::path configHome;
        if (const char* xdgConfig = std::getenv("XDG_CONFIG_HOME"); xdgConfig && *xdgConfig) {
            configHome = fs::path(xdgConfig);
        } else {
            configHome = homeDir / ".config";
        }
        std::vector<std::pair<std::string, fs::path>> targets;

        if (installClaude) {
            targets.emplace_back("Claude Code",
                                 homeDir / ".claude" / "skills" / "yams" / "SKILL.md");
        }
        if (installOpenCode) {
            targets.emplace_back("OpenCode",
                                 configHome / "opencode" / "skill" / "yams" / "SKILL.md");
        }

        for (const auto& [name, skillPath] : targets) {
            try {
                fs::create_directories(skillPath.parent_path());

                // Check if already exists
                if (fs::exists(skillPath) && !force_) {
                    spdlog::info("{} skill already exists at {} (use --force to overwrite)", name,
                                 skillPath.string());
                    continue;
                }

                // Write skill file
                std::ofstream out(skillPath, std::ios::trunc);
                if (!out) {
                    spdlog::warn("Failed to write {} skill to {}", name, skillPath.string());
                    continue;
                }
                out << YAMS_SKILL_CONTENT;
                out.close();

                spdlog::info("{} skill installed: {}", name, skillPath.string());
            } catch (const std::exception& e) {
                spdlog::warn("Failed to install {} skill: {}", name, e.what());
            }
        }
    }

    YamsCLI* cli_ = nullptr;
    bool nonInteractive_ = false;
    bool autoInit_ = false;
    bool force_ = false;
    bool noKeygen_ = false;
    bool printConfig_ = false;
    bool enablePlugins_ = false;
    std::string tuningProfile_{"balanced"};
};

// Factory function
std::unique_ptr<ICommand> createInitCommand() {
    return std::make_unique<InitCommand>();
}

} // namespace yams::cli
