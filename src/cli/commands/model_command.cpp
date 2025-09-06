#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
#include <unistd.h>
#include <vector>
#include <sys/stat.h>
#include <sys/wait.h>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/profiling.h>

namespace yams::cli {

namespace fs = std::filesystem;

// Available models configuration
struct ModelInfo {
    std::string name;
    std::string url;
    std::string description;
    size_t size_mb;
    std::string type; // "embedding" or "generation"
};

static const std::vector<ModelInfo> AVAILABLE_MODELS = {
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight 384-dim embeddings for semantic search", 90, "embedding"},
    {"all-mpnet-base-v2",
     "https://huggingface.co/sentence-transformers/all-mpnet-base-v2/resolve/main/onnx/model.onnx",
     "High-quality 768-dim embeddings", 420, "embedding"},
    // Nomic model support (embedding). If the default URL changes, users can override with --url.
    {"nomic-embed-text-v1.5",
     "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/main/onnx/model.onnx",
     "Nomic embedding model (v1.5)", 0 /*unknown*/, "embedding"}};

class ModelCommand : public ICommand {
private:
    YamsCLI* cli_ = nullptr;
    bool list_ = false;
    std::string downloadModel_;
    std::string infoModel_;
    std::string customUrl_;
    bool check_ = false;

    // Helper function to check if a command exists in PATH
    bool commandExists(const std::string& cmd) const {
        const char* path_env = std::getenv("PATH");
        if (!path_env)
            return false;

        std::stringstream ss(path_env);
        std::string dir;
        while (std::getline(ss, dir, ':')) {
            fs::path full_path = fs::path(dir) / cmd;
            if (fs::exists(full_path) && fs::is_regular_file(full_path)) {
                struct stat st;
                if (stat(full_path.c_str(), &st) == 0 && (st.st_mode & S_IXUSR)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Helper function for safe command execution
    int executeCommand(const std::vector<std::string>& args) const {
        if (args.empty())
            return -1;

        // Convert to char* array for execvp
        std::vector<char*> argv;
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);

        pid_t pid = fork();
        if (pid == 0) {
            // Child process
            execvp(argv[0], argv.data());
            // If we get here, exec failed
            std::cerr << "Failed to execute: " << args[0] << std::endl;
            exit(127);
        } else if (pid > 0) {
            // Parent process - wait for child
            int status;
            waitpid(pid, &status, 0);
            if (WIFEXITED(status)) {
                return WEXITSTATUS(status);
            }
            return -1;
        } else {
            // Fork failed
            std::cerr << "Fork failed" << std::endl;
            return -1;
        }
    }

    // Helper function to validate paths for safety
    bool isPathSafe(const fs::path& path) const {
        // Check for directory traversal
        for (const auto& part : path) {
            if (part == "..") {
                return false;
            }
        }

        // Check for shell metacharacters that could be dangerous
        std::string path_str = path.string();
        if (path_str.find_first_of(";&|`$(){}[]<>*?!~\n\r") != std::string::npos) {
            return false;
        }

        return true;
    }
    std::string outputDir_;
    bool force_ = false;

public:
    std::string getName() const override { return "model"; }

    std::string getDescription() const override {
        return "Download and manage ONNX embedding models";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("model", getDescription());

        // Backward-compatible flags
        cmd->add_flag("--list", list_, "List available models");
        cmd->add_option("--download", downloadModel_, "Download a model by name");
        cmd->add_option("--info", infoModel_, "Show model information");
        cmd->add_option("--output", outputDir_, "Output directory for downloads");
        cmd->add_option("--url", customUrl_,
                        "Custom URL to download the model (use with --download <name>)");
        cmd->add_flag("--force", force_, "Force redownload if model exists");
        cmd->add_flag("--check", check_, "Show provider health and installed model status");

        // User-friendly subcommands: list | download <name> | info <name> | check
        auto* sub_list = cmd->add_subcommand("list", "List available and installed models");
        sub_list->callback([this]() {
            list_ = true;
            auto r = execute();
            if (!r) {
                spdlog::error("Model list failed: {}", r.error().message);
                std::exit(1);
            }
        });

        auto* sub_download = cmd->add_subcommand("download", "Download a model by name");
        std::string positional_download_name;
        sub_download->add_option("name", positional_download_name, "Model name");
        sub_download->add_option("--output", outputDir_, "Output directory for downloads");
        sub_download->add_option("--url", customUrl_, "Custom URL to download the model");
        sub_download->add_flag("--force", force_, "Force redownload if model exists");
        sub_download->callback([this, &positional_download_name]() {
            if (positional_download_name.empty()) {
                std::cout << "Model name is required. Usage: yams model download <name> [--url "
                             "<url>] [--output <dir>]"
                          << std::endl;
                std::exit(2);
            }
            downloadModel_ = positional_download_name;
            auto r = execute();
            if (!r) {
                spdlog::error("Model download failed: {}", r.error().message);
                std::exit(1);
            }
        });

        auto* sub_info = cmd->add_subcommand("info", "Show model details");
        std::string positional_info_name;
        sub_info->add_option("name", positional_info_name, "Model name");
        sub_info->callback([this, &positional_info_name]() {
            if (positional_info_name.empty()) {
                std::cout << "Model name is required. Usage: yams model info <name>" << std::endl;
                std::exit(2);
            }
            infoModel_ = positional_info_name;
            auto r = execute();
            if (!r) {
                spdlog::error("Model info failed: {}", r.error().message);
                std::exit(1);
            }
        });

        auto* sub_check =
            cmd->add_subcommand("check", "Check provider support and installed models");
        sub_check->callback([this]() {
            check_ = true;
            auto r = execute();
            if (!r) {
                spdlog::error("Model check failed: {}", r.error().message);
                std::exit(1);
            }
        });

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Model command failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("ModelCommand::execute");

        try {
            if (list_)
                return listModels();
            if (check_)
                return checkModels();

            if (!downloadModel_.empty()) {
                return downloadModel(downloadModel_, outputDir_, customUrl_);
            }

            if (!infoModel_.empty()) {
                return showModelInfo(infoModel_);
            }

            // Default: show help
            showHelp();
            return Result<void>{};

        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError, e.what()};
        }
    }

private:
    std::vector<ModelInfo> discoverLocalModels() const {
        std::vector<ModelInfo> locals;
        const char* home = std::getenv("HOME");
        if (!home)
            return locals;
        fs::path base = fs::path(home) / ".yams" / "models";
        std::error_code ec;
        if (!fs::exists(base, ec) || !fs::is_directory(base, ec))
            return locals;
        for (const auto& entry : fs::directory_iterator(base, ec)) {
            if (!entry.is_directory())
                continue;
            auto dir = entry.path();
            auto onnx = dir / "model.onnx";
            if (fs::exists(onnx, ec)) {
                size_t size_mb = 0;
                std::error_code ec2;
                auto sz = fs::file_size(onnx, ec2);
                if (!ec2)
                    size_mb = static_cast<size_t>(sz / (1024 * 1024));
                locals.push_back(ModelInfo{dir.filename().string(), "", "Installed local model",
                                           size_mb, "embedding"});
            }
        }
        return locals;
    }
    Result<void> listModels() {
        std::cout << "\nAvailable ONNX Models:\n";
        std::cout << "=====================\n\n";

        for (const auto& model : AVAILABLE_MODELS) {
            std::cout << "  " << model.name << " (" << model.type << ", " << model.size_mb
                      << " MB)\n";
            std::cout << "    " << model.description << "\n\n";
        }

        // Show installed models (autodiscovered)
        auto locals = discoverLocalModels();
        if (!locals.empty()) {
            std::cout << "Installed Models:\n";
            for (const auto& m : locals) {
                std::cout << "  " << m.name << " (installed";
                if (m.size_mb)
                    std::cout << ", ~" << m.size_mb << " MB";
                std::cout << ")\n";
            }
            std::cout << "\n";
        }

        std::cout << "To download a model:\n";
        std::cout << "  yams model --download <model-name>\n";
        std::cout << "Then set it as preferred:\n";
        std::cout << "  yams config embeddings model <model-name>\n\n";

        return Result<void>{};
    }

    Result<void> downloadModel(const std::string& model_name, const std::string& output_dir,
                               const std::string& override_url) {
        YAMS_ZONE_SCOPED_N("ModelCommand::downloadModel");

        // Find model info
        auto it = std::find_if(AVAILABLE_MODELS.begin(), AVAILABLE_MODELS.end(),
                               [&](const ModelInfo& m) { return m.name == model_name; });

        ModelInfo model;
        if (it == AVAILABLE_MODELS.end()) {
            // Allow custom download when URL is provided
            if (override_url.empty()) {
                std::cout << "Unknown model: " << model_name << "\n";
                std::cout << "Use 'yams model --list' to see available models or provide --url"
                          << "\n";
                return Error{ErrorCode::InvalidArgument,
                             "Unknown model and no --url provided: " + model_name};
            }
            model =
                ModelInfo{model_name, override_url, std::string{"Custom model"}, 0, "embedding"};
        } else {
            model = *it;
            if (!override_url.empty()) {
                model.url = override_url; // explicit user override
            }
        }

        // Determine output path
        fs::path output_path;
        if (!output_dir.empty()) {
            output_path = fs::path(output_dir) / model.name;

            // Validate user-provided path
            if (!isPathSafe(output_path)) {
                return Error{
                    ErrorCode::InvalidPath,
                    "Invalid output path - contains unsafe characters or directory traversal"};
            }
        } else {
            // Default to ~/.yams/models/
            const char* home = std::getenv("HOME");
            if (!home) {
                return Error{ErrorCode::InvalidPath, "Cannot determine home directory"};
            }
            output_path = fs::path(home) / ".yams" / "models" / model.name;
        }

        // Create directory if needed
        fs::create_directories(output_path);

        // Full path to model file
        fs::path model_file = output_path / "model.onnx";

        // Additional safety check on final path
        if (!isPathSafe(model_file)) {
            return Error{ErrorCode::InvalidPath, "Invalid model file path"};
        }

        // Check if already exists
        if (fs::exists(model_file) && !force_) {
            std::cout << "Model already exists at: " << model_file << "\n";
            std::cout << "Use --force to redownload\n";
            return Result<void>{};
        }

        std::cout << "Downloading " << model.name;
        if (model.size_mb > 0) {
            std::cout << " (" << model.size_mb << " MB)";
        }
        std::cout << "...\n";
        std::cout << "From: " << model.url << "\n";
        std::cout << "To: " << model_file << "\n\n";

        // Check for curl or wget using safe method
        bool has_curl = commandExists("curl");
        bool has_wget = commandExists("wget");

        if (!has_curl && !has_wget) {
            return Error{ErrorCode::NotFound,
                         "Neither curl nor wget found. Please install one to download models."};
        }

        // Build command arguments safely - no shell interpretation
        std::vector<std::string> download_args;
        if (has_curl) {
            download_args = {
                "curl",
                "-L",             // Follow redirects
                "--progress-bar", // Show progress
                "-o",
                model_file.string(), // Output file
                model.url            // URL to download
            };
        } else {
            download_args = {
                "wget",
                "--show-progress",         // Show progress
                "-O", model_file.string(), // Output file
                model.url                  // URL to download
            };
        }

        // Execute download safely without shell
        int result = executeCommand(download_args);

        if (result != 0) {
            // Clean up partial download
            if (fs::exists(model_file)) {
                fs::remove(model_file);
            }
            std::string reason;
            if (has_curl) {
                if (result == 6)
                    reason = " (Could not resolve host)";
                else if (result == 7)
                    reason = " (Failed to connect to host)";
            }
            return Error{ErrorCode::InternalError,
                         "Download failed with code: " + std::to_string(result) + reason};
        }

        // Create model config file
        fs::path config_file = output_path / "config.json";
        std::ofstream config(config_file);
        config << "{\n";
        config << "  \"name\": \"" << model.name << "\",\n";
        config << "  \"type\": \"" << model.type << "\",\n";
        config << "  \"model_path\": \"model.onnx\",\n";
        if (model.type == "embedding") {
            if (model.name.find("MiniLM") != std::string::npos) {
                config << "  \"embedding_dim\": 384,\n";
            } else if (model.name.find("mpnet") != std::string::npos) {
                config << "  \"embedding_dim\": 768,\n";
            }
            config << "  \"max_sequence_length\": 512,\n";
        }
        config << "  \"downloaded_at\": \""
               << std::chrono::system_clock::now().time_since_epoch().count() << "\"\n";
        config << "}\n";
        config.close();

        std::cout << "\n✓ Model downloaded successfully to: " << output_path << "\n";
        std::cout << "\nTo use this model:\n";
        if (model.type == "embedding") {
            std::cout << "  Configure YAMS to use: " << model_file << "\n";
            std::cout << "  for embedding generation in vector database operations\n";
            std::cout << "\n  Set this as the preferred embedding model:\n";
            std::cout << "    yams config embeddings model " << model.name << "\n";
            std::cout << "  Check embedding status:\n";
            std::cout << "    yams config embeddings status\n";
            std::cout << "  Enable automatic embedding generation (optional):\n";
            std::cout << "    yams config embeddings enable\n";
        } else {
            std::cout << "  Configure YAMS to use: " << model_file << "\n";
            std::cout << "  for text generation tasks\n";
        }

        return Result<void>{};
    }

    Result<void> showModelInfo(const std::string& model_name) {
        auto it = std::find_if(AVAILABLE_MODELS.begin(), AVAILABLE_MODELS.end(),
                               [&](const ModelInfo& m) { return m.name == model_name; });

        if (it == AVAILABLE_MODELS.end()) {
            return Error{ErrorCode::InvalidArgument, "Unknown model: " + model_name};
        }

        const auto& model = *it;

        std::cout << "\nModel: " << model.name << "\n";
        std::cout << "=====================================\n";
        std::cout << "Type: " << model.type << "\n";
        std::cout << "Size: " << model.size_mb << " MB\n";
        std::cout << "Description: " << model.description << "\n";
        std::cout << "URL: " << model.url << "\n";

        if (model.type == "embedding") {
            std::cout << "\nEmbedding Model Details:\n";
            if (model.name.find("MiniLM") != std::string::npos) {
                std::cout << "  - Embedding dimension: 384\n";
                std::cout << "  - Max sequence length: 512 tokens\n";
                std::cout << "  - Architecture: BERT-based\n";
                std::cout << "  - Training: Contrastive learning on 1B+ pairs\n";
            } else if (model.name.find("mpnet") != std::string::npos) {
                std::cout << "  - Embedding dimension: 768\n";
                std::cout << "  - Max sequence length: 512 tokens\n";
                std::cout << "  - Architecture: MPNet\n";
                std::cout << "  - Higher quality than MiniLM but slower\n";
            }
        }

        std::cout << "\nHint: set this as your preferred embedding model with:\n";
        std::cout << "  yams config embeddings model " << model.name << "\n";

        return Result<void>{};
    }

    Result<void> checkModels() {
        std::cout << "\nModel Provider Health\n";
        std::cout << "=====================\n\n";
#ifdef YAMS_USE_ONNX_RUNTIME
        std::cout << "ONNX runtime support: Enabled\n";
#else
        std::cout << "ONNX runtime support: Disabled (compiled without ONNX)\n";
#endif
        // Best-effort plugin presence check
        const char* plugin_env = std::getenv("YAMS_PLUGIN_DIR");
        fs::path plugin_dir =
            plugin_env ? fs::path(plugin_env) : fs::path("/usr/local/lib/yams/plugins");
        std::error_code ec;
        size_t plugin_files = 0;
        if (fs::exists(plugin_dir, ec) && fs::is_directory(plugin_dir, ec)) {
            for (const auto& entry : fs::directory_iterator(plugin_dir, ec)) {
                if (entry.is_regular_file()) {
                    auto ext = entry.path().extension().string();
                    if (ext == ".so" || ext == ".dylib" || ext == ".dll") {
                        plugin_files++;
                    }
                }
            }
            std::cout << "Plugin directory: " << plugin_dir << " (" << plugin_files
                      << " libraries)\n";
        } else {
            std::cout << "Plugin directory not found: " << plugin_dir << "\n";
        }
        auto locals = discoverLocalModels();
        if (locals.empty()) {
            std::cout << "No installed models found in ~/.yams/models\n";
        } else {
            std::cout << "Installed models:\n";
            for (const auto& m : locals) {
                std::cout << "  - " << m.name;
                if (m.size_mb)
                    std::cout << " (~" << m.size_mb << " MB)";
                std::cout << "\n";
            }
        }
        std::cout << "\nUse 'yams model download <name>' or 'yams model download <name> --url "
                     "<url>' to add models.\n";
        return Result<void>{};
    }

    void showHelp() {
        std::cout << R"(
YAMS Embedding Model Management

Commands:
  yams model --list                    List available embedding models
  yams model list                      List available embedding models (alias)
  yams model --download <name>         Download an embedding model
  yams model download <name>           Download an embedding model (alias)
  yams model --info <name>             Show model details
  yams model info <name>               Show model details (alias)
  yams model --download <name> --output <dir>  Download to specific directory
  yams model --download <name> --url <url>     Download from a specific URL (override)
  yams model check                     Check provider support and installed models

Examples:
  yams model --list
  yams model list
  yams model --download all-MiniLM-L6-v2
  yams model download all-MiniLM-L6-v2
  yams model --info all-mpnet-base-v2
  yams model info all-mpnet-base-v2
  yams model --download all-mpnet-base-v2 --output ~/my-models
  yams model --download nomic-embed-text-v1.5 --url https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/main/onnx/model.onnx
  yams model check

Available Models:
  - all-MiniLM-L6-v2: Fast 384-dim embeddings (90MB)
  - all-mpnet-base-v2: High-quality 768-dim embeddings (420MB)
  - nomic-embed-text-v1.5: Nomic embedding model (URL override supported)

Default download location: ~/.yams/models/<model-name>/
)";
    }
};

// Factory function
std::unique_ptr<ICommand> createModelCommand() {
    return std::make_unique<ModelCommand>();
}

} // namespace yams::cli
