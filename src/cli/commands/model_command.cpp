#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>
#include <thread>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#include <sys/wait.h>
#endif
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <sys/stat.h>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/downloader/downloader.hpp>
#include <yams/integrity/repair_utils.h>
#include <yams/profiling.h>

namespace yams::cli {

namespace fs = std::filesystem;

// Available models configuration
struct ModelInfo {
    std::string name;
    std::string url;
    std::string description;
    size_t size_mb;
    std::string type; // "embedding", "reranker", or "generation"
};

static const std::vector<ModelInfo> AVAILABLE_MODELS = {
    // Embedding models
    {"mxbai-edge-colbert-v0-17m",
     "https://huggingface.co/ryandono/mxbai-edge-colbert-v0-17m-onnx-int8/resolve/main/onnx/"
     "model_quantized.onnx",
     "Lightweight ColBERT (token-level, MaxSim) optimized for edge use", 17, "embedding"},
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight 384-dim embeddings for semantic search", 90, "embedding"},
    {"all-mpnet-base-v2",
     "https://huggingface.co/sentence-transformers/all-mpnet-base-v2/resolve/main/onnx/model.onnx",
     "High-quality 768-dim embeddings", 420, "embedding"},
    {"jina-embeddings-v2-base-code",
     "https://huggingface.co/jinaai/jina-embeddings-v2-base-code/resolve/main/onnx/model.onnx",
     "Code-aware 768-dim embeddings for 30+ programming languages", 550, "embedding"},
    // Reranker models (cross-encoder for two-stage retrieval)
    {"bge-reranker-base",
     "https://huggingface.co/BAAI/bge-reranker-base/resolve/main/onnx/model.onnx",
     "Cross-encoder reranker for hybrid search refinement (512 max tokens)", 278, "reranker"},
    {"bge-reranker-large",
     "https://huggingface.co/BAAI/bge-reranker-large/resolve/main/onnx/model.onnx",
     "High-quality cross-encoder reranker (512 max tokens)", 560, "reranker"}
    // Note: Nomic ONNX embedding model is currently not recommended due to provider limitations
    // (no batch embeddings/preload). You can still download via --hf or --url manually.
};

class ModelCommand : public ICommand {
private:
    YamsCLI* cli_ = nullptr;
    bool list_ = false;
    std::string downloadModel_;
    std::string infoModel_;
    // Positional argument storage (must outlive CLI11 option bindings)
    std::string positional_download_name_;
    std::string positional_info_name_;
    std::string customUrl_;
    std::string hfRepo_;
    std::string revision_ = "main";
    std::string token_;
    bool offline_ = false;
    bool check_ = false;
    bool applyConfig_ = false; // when true, write detected dims into config
    bool executed_ = false;

    // Helper function to check if a command exists in PATH
    bool commandExists(const std::string& cmd) const {
        const char* path_env = std::getenv("PATH");
        if (!path_env)
            return false;

#ifdef _WIN32
        std::stringstream ss(path_env);
        std::string dir;
        while (std::getline(ss, dir, ';')) {
            fs::path full_path = fs::path(dir) / cmd;
            // On Windows, check for .exe extension if not present
            if (full_path.extension() != ".exe")
                full_path += ".exe";

            if (fs::exists(full_path) && fs::is_regular_file(full_path)) {
                return true;
            }
        }
#else
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
#endif
        return false;
    }

    // Simple JSON loader
    static nlohmann::json load_json_file(const fs::path& p) {
        try {
            std::ifstream in(p);
            if (!in.good())
                return {};
            nlohmann::json j;
            in >> j;
            return j;
        } catch (...) {
            return {};
        }
    }

    struct DetectedProps {
        size_t dim = 0;
        size_t max_seq = 0;
        std::string pooling;
    };

    static DetectedProps detect_model_props(const std::string& model_name,
                                            const fs::path& model_dir) {
        DetectedProps out;
        auto cfg = load_json_file(model_dir / "config.json");
        auto sbert = load_json_file(model_dir / "sentence_bert_config.json");
        try {
            // Standard models use hidden_size, Nomic models use n_embd
            if (cfg.contains("hidden_size"))
                out.dim = static_cast<size_t>(cfg["hidden_size"].get<int>());
            else if (cfg.contains("n_embd"))
                out.dim = static_cast<size_t>(cfg["n_embd"].get<int>());
            if (cfg.contains("max_position_embeddings"))
                out.max_seq = static_cast<size_t>(cfg["max_position_embeddings"].get<int>());
        } catch (...) {
        }
        try {
            if (sbert.contains("output_embedding_size") && out.dim == 0)
                out.dim = static_cast<size_t>(sbert["output_embedding_size"].get<int>());
            if (sbert.contains("pooling_mode_mean_tokens") &&
                sbert["pooling_mode_mean_tokens"].get<bool>())
                out.pooling = "mean";
            if (sbert.contains("pooling_mode_cls_token") &&
                sbert["pooling_mode_cls_token"].get<bool>())
                out.pooling = out.pooling.empty() ? "cls" : (out.pooling + "+cls");
        } catch (...) {
        }
        // Heuristics by well-known model names (using extracted utility)
        if (out.dim == 0) {
            if (auto heuristic = vecutil::getModelDimensionHeuristic(model_name)) {
                out.dim = *heuristic;
            }
        }
        if (out.max_seq == 0)
            out.max_seq = 512; // safe default
        return out;
    }

    static fs::path resolveConfigPath() { return yams::config::get_config_path(); }

    static void
    toml_write(std::ostream& file,
               const std::map<std::string, std::map<std::string, std::string>>& config) {
        // Basic TOML writer (order not preserved; comments lost)
        for (const auto& [section, kv] : config) {
            file << "[" << section << "]\n";
            for (const auto& [k, v] : kv) {
                if (v == "true" || v == "false")
                    file << k << " = " << v << "\n";
                else {
                    // numeric?
                    bool is_num = !v.empty() && std::all_of(v.begin(), v.end(), [](char c) {
                        return std::isdigit(static_cast<unsigned char>(c));
                    });
                    if (!is_num) {
                        // float?
                        bool is_float =
                            !v.empty() && std::count(v.begin(), v.end(), '.') == 1 &&
                            std::all_of(v.begin(), v.end(), [](char c) {
                                return std::isdigit(static_cast<unsigned char>(c)) || c == '.';
                            });
                        if (is_float)
                            is_num = true;
                    }
                    if (is_num)
                        file << k << " = " << v << "\n";
                    else
                        file << k << " = \"" << v << "\"\n";
                }
            }
            file << "\n";
        }
    }

    Result<void> apply_detected_to_config(const std::string& model_name,
                                          const DetectedProps& props) {
        try {
            using yams::config::ConfigMigrator;
            fs::path cfgPath = resolveConfigPath();
            ConfigMigrator migrator;
            if (!fs::exists(cfgPath)) {
                // Create default v2 config
                auto mk = migrator.createDefaultV2Config(cfgPath);
                if (!mk)
                    return Error{mk.error()};
            }
            auto parsed = migrator.parseTomlConfig(cfgPath);
            if (!parsed)
                return Error{parsed.error()};
            auto c = parsed.value();
            auto& emb = c["embeddings"];
            emb["preferred_model"] = model_name;
            if (props.dim > 0) {
                emb["embedding_dim"] = std::to_string(props.dim);
            }
            if (props.max_seq > 0) {
                emb["max_sequence_length"] = std::to_string(props.max_seq);
            }
            auto& vdb = c["vector_database"];
            if (props.dim > 0)
                vdb["embedding_dim"] = std::to_string(props.dim);
            auto& vidx = c["vector_index"];
            if (props.dim > 0)
                vidx["dimension"] = std::to_string(props.dim);
            // Write back
            fs::create_directories(cfgPath.parent_path());
            std::ofstream out(cfgPath);
            if (!out.good())
                return Error{ErrorCode::WriteError,
                             "Failed to open config for write: " + cfgPath.string()};
            toml_write(out, c);
            out.close();
            std::cout << "✓ Updated config: embeddings.preferred_model='" << model_name
                      << "', embedding_dim=" << props.dim
                      << ", max_sequence_length=" << props.max_seq << "\n";
            std::cout
                << "  Also updated vector_database.embedding_dim and vector_index.dimension to "
                << props.dim << "\n";
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::WriteError, e.what()};
        }
    }

    // Helper function for safe command execution
    int executeCommand(const std::vector<std::string>& args) const {
        if (args.empty())
            return -1;

#ifdef _WIN32
        std::vector<const char*> argv;
        for (const auto& arg : args) {
            argv.push_back(arg.c_str());
        }
        argv.push_back(nullptr);

        intptr_t status = _spawnvp(_P_WAIT, argv[0], argv.data());
        return (int)status;
#else
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
#endif
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
            executed_ = true;
            auto r = execute();
            if (!r) {
                spdlog::error("Model list failed: {}", r.error().message);
                std::exit(1);
            }
        });

        auto* sub_download = cmd->add_subcommand("download", "Download a model by name");
        sub_download->add_option("name", positional_download_name_, "Model name");
        sub_download->add_option("--output", outputDir_, "Output directory for downloads");
        sub_download->add_option("--url", customUrl_, "Custom URL to download the model");
        sub_download->add_option("--hf", hfRepo_, "Hugging Face repo id (org/name)");
        sub_download->add_option("--revision", revision_, "Hugging Face revision (default: main)");
        sub_download->add_option("--token", token_, "Hugging Face access token");
        sub_download->add_flag("--offline", offline_, "Offline mode: use cache only");
        sub_download->add_flag("--force", force_, "Force redownload if model exists");
        sub_download->add_flag(
            "--apply-config", applyConfig_,
            "Update YAMS config with detected model dim/seq and set as preferred model");
        sub_download->callback([this]() {
            // Allow either a positional name, or --hf repo, or --url
            if (positional_download_name_.empty()) {
                if (!hfRepo_.empty()) {
                    // Derive a sensible name from repo when no positional name was given
                    auto pos = hfRepo_.find_last_of('/');
                    downloadModel_ = (pos == std::string::npos) ? hfRepo_ : hfRepo_.substr(pos + 1);
                } else if (!customUrl_.empty()) {
                    // Name from URL filename
                    try {
                        std::string fname = customUrl_;
                        auto p = fname.find_last_of('/');
                        if (p != std::string::npos)
                            fname = fname.substr(p + 1);
                        if (auto dot = fname.find('.'); dot != std::string::npos)
                            fname = fname.substr(0, dot);
                        downloadModel_ = fname.empty() ? std::string("model") : fname;
                    } catch (...) {
                        downloadModel_ = "model";
                    }
                } else {
                    std::cout << "Model name is required. Usage: yams model download <name> [--url "
                                 "<url>] [--output <dir>] or provide --hf <org/name>\n";
                    std::exit(2);
                }
            } else {
                downloadModel_ = positional_download_name_;
            }
            executed_ = true;
            auto r = execute();
            if (!r) {
                spdlog::error("Model download failed: {}", r.error().message);
                std::exit(1);
            }
        });

        auto* sub_info = cmd->add_subcommand("info", "Show model details");
        sub_info->add_option("name", positional_info_name_, "Model name");
        sub_info->callback([this]() {
            if (positional_info_name_.empty()) {
                std::cout << "Model name is required. Usage: yams model info <name>" << std::endl;
                std::exit(2);
            }
            infoModel_ = positional_info_name_;
            executed_ = true;
            cli_->setPendingCommand(this);
        });

        auto* sub_check =
            cmd->add_subcommand("check", "Check provider support and installed models");
        sub_check->callback([this]() {
            check_ = true;
            executed_ = true;
            cli_->setPendingCommand(this);
        });

        // model provider: print active backend and default/preferred model
        auto* sub_provider =
            cmd->add_subcommand("provider", "Show active model provider and preferred model");
        sub_provider->callback([this]() {
            auto printPreferredModel = []() {
                std::string preferred;
                auto cfgp = yams::config::get_config_path();
                auto config = yams::config::parse_simple_toml(cfgp);
                if (auto it = config.find("embeddings.preferred_model"); it != config.end()) {
                    preferred = it->second;
                }
                if (!preferred.empty())
                    std::cout << "Preferred model: " << preferred << "\n";
                else
                    std::cout << "Preferred model: (not set)\n";
            };
            try {
                using namespace yams::daemon;
                ClientConfig cfg;
                cfg.requestTimeout = std::chrono::milliseconds(3000);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
                    cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
                if (!leaseRes) {
                    std::cout << "Model Provider\n==============\n";
                    std::cout << "Status: unavailable - " << leaseRes.error().message << "\n";
                    printPreferredModel();
                    return;
                }
                auto leaseHandle = std::move(leaseRes.value());
                // Query model status (lists loaded models if any)
                ModelStatusRequest msr;
                msr.detailed = true;
                std::promise<Result<ModelStatusResponse>> prom;
                auto fut = prom.get_future();
                boost::asio::co_spawn(
                    getExecutor(),
                    [leaseHandle, msr, &prom]() mutable -> boost::asio::awaitable<void> {
                        auto& client = **leaseHandle;
                        auto r = co_await client.call(msr);
                        prom.set_value(std::move(r));
                        co_return;
                    },
                    boost::asio::detached);
                auto s =
                    (fut.wait_for(std::chrono::seconds(3)) == std::future_status::ready)
                        ? fut.get()
                        : Result<ModelStatusResponse>(Error{ErrorCode::Timeout, "status timeout"});
                std::cout << "Model Provider\n==============\n";
                if (!s) {
                    std::cout << "Status: unavailable - " << s.error().message << "\n";
                } else {
                    const auto& r = s.value();
                    std::cout << "Status: " << (r.models.empty() ? "idle" : "active") << "\n";
                    if (!r.models.empty()) {
                        std::cout << "Loaded models (" << r.models.size() << "):\n";
                        for (const auto& m : r.models)
                            std::cout << "  - " << m.name << (m.loaded ? " (loaded)" : "") << "\n";
                    }
                }
                printPreferredModel();
            } catch (const std::exception& e) {
                std::cout << "Model provider: error - " << e.what() << "\n";
            }
        });

        // Parent callback should not run when a subcommand already executed (avoids duplicate
        // output)
        cmd->callback([this]() {
            if (!executed_)
                cli_->setPendingCommand(this);
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
        if (!cli_)
            return locals;
        fs::path base = cli_->getDataPath() / "models";
        std::error_code ec;
        if (!fs::exists(base, ec) || !fs::is_directory(base, ec))
            return locals;
        for (const auto& entry : fs::directory_iterator(base, ec)) {
            if (!entry.is_directory())
                continue;
            const auto& dir = entry.path();
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
        using json = nlohmann::json;

        // Preferred model from config/env
        std::string preferred;
        try {
            auto cfgp = yams::config::get_config_path();
            auto config = yams::config::parse_simple_toml(cfgp);
            if (auto it = config.find("embeddings.preferred_model"); it != config.end()) {
                preferred = it->second;
            }
            if (preferred.empty()) {
                if (const char* p = std::getenv("YAMS_PREFERRED_MODEL"))
                    preferred = p;
            }
        } catch (...) {
        }

        // Query daemon for active models (dims/seq) — opt-in to avoid starting/stopping daemon
        // unexpectedly
        std::vector<std::string> activeLines;
        if (const char* withd = std::getenv("YAMS_MODEL_LIST_WITH_DAEMON")) {
            if (std::string(withd) == "1" || std::string(withd) == "true") {
                try {
                    yams::daemon::ClientConfig cfg;
                    cfg.requestTimeout = std::chrono::milliseconds(800);
                    auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
                        cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
                    if (!leaseRes) {
                        if (yams::cli::is_transport_failure(leaseRes.error())) {
                            spdlog::debug("model list: daemon transport unavailable: {}",
                                          leaseRes.error().message);
                        }
                        throw std::runtime_error(leaseRes.error().message);
                    }
                    auto leaseHandle = std::move(leaseRes.value());
                    yams::daemon::ModelStatusRequest msr;
                    msr.detailed = true;
                    std::promise<Result<yams::daemon::ModelStatusResponse>> prom;
                    auto fut = prom.get_future();
                    boost::asio::co_spawn(
                        getExecutor(),
                        [leaseHandle, msr, &prom]() mutable -> boost::asio::awaitable<void> {
                            auto& client = **leaseHandle;
                            auto r = co_await client.getModelStatus(msr);
                            prom.set_value(std::move(r));
                            co_return;
                        },
                        boost::asio::detached);
                    auto s =
                        (fut.wait_for(std::chrono::milliseconds(800)) == std::future_status::ready)
                            ? fut.get()
                            : Result<yams::daemon::ModelStatusResponse>(
                                  Error{ErrorCode::Timeout, "model status timeout"});
                    if (s) {
                        for (const auto& m : s.value().models) {
                            std::ostringstream ln;
                            ln << "  - " << m.name;
                            if (m.embeddingDim)
                                ln << "  dim=" << m.embeddingDim;
                            if (m.maxSequenceLength)
                                ln << "  seq=" << m.maxSequenceLength;
                            if (m.loaded)
                                ln << "  [loaded]";
                            activeLines.push_back(ln.str());
                        }
                    }
                } catch (...) {
                }
            }
        }

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
                // Try to read config.json or sentence_bert_config.json for dim/seq
                size_t dim = 0, seq = 0;
                try {
                    if (cli_) {
                        auto base = cli_->getDataPath() / "models" / m.name;
                        for (auto file : {std::string("config.json"),
                                          std::string("sentence_bert_config.json")}) {
                            auto p = base / file;
                            if (!std::filesystem::exists(p))
                                continue;
                            std::ifstream jin(p);
                            json j;
                            jin >> j;
                            if (!dim) {
                                if (j.contains("embedding_dimension") &&
                                    j["embedding_dimension"].is_number_integer())
                                    dim = j["embedding_dimension"].get<int>();
                                else if (j.contains("hidden_size") &&
                                         j["hidden_size"].is_number_integer())
                                    dim = j["hidden_size"].get<int>();
                                else if (j.contains("n_embd") && j["n_embd"].is_number_integer())
                                    dim = j["n_embd"].get<int>();
                            }
                            if (!seq) {
                                if (j.contains("max_seq_length") &&
                                    j["max_seq_length"].is_number_integer())
                                    seq = j["max_seq_length"].get<int>();
                                else if (j.contains("model_max_length") &&
                                         j["model_max_length"].is_number_integer())
                                    seq = j["model_max_length"].get<int>();
                                else if (j.contains("max_position_embeddings") &&
                                         j["max_position_embeddings"].is_number_integer())
                                    seq = j["max_position_embeddings"].get<int>();
                            }
                        }
                    }
                } catch (...) {
                }
                std::cout << "  " << m.name << " (installed";
                if (m.size_mb)
                    std::cout << ", ~" << m.size_mb << " MB";
                if (dim)
                    std::cout << ", dim=" << dim;
                if (seq)
                    std::cout << ", seq=" << seq;
                std::cout << ")\n";
            }
            std::cout << "\n";
        }

        // Show preferred and active models summary
        if (!preferred.empty() || !activeLines.empty()) {
            if (!preferred.empty())
                std::cout << "Preferred: " << preferred << "\n";
            if (!activeLines.empty()) {
                std::cout << "Active (daemon):\n";
                for (const auto& l : activeLines)
                    std::cout << l << "\n";
            }
            std::cout << "\n";
        }

        std::cout << "To download a model:\n";
        std::cout << "  yams model --download <model-name>\n";
        std::cout << "Then set it as preferred:\n";
        std::cout << "  yams config embeddings model <model-name>\n\n";

        return Result<void>{};
    }

    Result<void> downloadModel(const std::string& model_name,
                               [[maybe_unused]] const std::string& output_dir,
                               const std::string& override_url) {
        YAMS_ZONE_SCOPED_N("ModelCommand::downloadModel");

        // Built-in map: common names -> HF repos
        auto mapKnown = [](const std::string& name) -> std::string {
            if (name == "all-MiniLM-L6-v2")
                return "sentence-transformers/all-MiniLM-L6-v2";
            if (name == "all-mpnet-base-v2")
                return "sentence-transformers/all-mpnet-base-v2";
            if (name == "nomic-embed-text-v1")
                return "nomic-ai/nomic-embed-text-v1";
            if (name == "nomic-embed-text-v1.5")
                return "nomic-ai/nomic-embed-text-v1.5";
            return {};
        };

        // Find model info
        auto it = std::find_if(AVAILABLE_MODELS.begin(), AVAILABLE_MODELS.end(),
                               [&](const ModelInfo& m) { return m.name == model_name; });

        ModelInfo model;
        if (it == AVAILABLE_MODELS.end()) {
            // If no URL override, treat model_name (or --hf) as HF repo id
            if (override_url.empty()) {
                std::string repo = !hfRepo_.empty() ? hfRepo_ : model_name;
                if (repo.find('/') == std::string::npos) {
                    // Try known mappings
                    std::string mapped = mapKnown(repo);
                    if (!mapped.empty())
                        repo = mapped;
                }
                if (repo.find('/') == std::string::npos) {
                    std::cout << "Unknown model: " << model_name << "\n";
                    std::cout << "Use 'yams model --list', or provide --hf org/name, or a direct "
                                 "--url.\n";
                    return Error{ErrorCode::InvalidArgument, "Unknown model: " + model_name};
                }
                // In offline mode, we will copy from HF cache later; otherwise, build resolve URL
                std::string url =
                    offline_ ? std::string("")
                             : (std::string("https://huggingface.co/") + repo + "/resolve/" +
                                (revision_.empty() ? "main" : revision_) + "/onnx/model.onnx");
                model = ModelInfo{model_name, url, std::string{"HF model"}, 0, "embedding"};
            } else {
                model = ModelInfo{model_name, override_url, std::string{"Custom model"}, 0,
                                  "embedding"};
            }
        } else {
            model = *it;
            if (!override_url.empty()) {
                model.url = override_url; // explicit user override
            } else if (!revision_.empty() && revision_ != "main") {
                const std::string needle = "/resolve/main/";
                auto pos = model.url.find(needle);
                if (pos != std::string::npos) {
                    model.url.replace(pos, needle.size(),
                                      std::string("/resolve/") + revision_ + "/");
                }
            }
        }

        // Determine output path
        fs::path output_path;
        output_path = cli_->getDataPath() / "models" / model.name;

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

        // Download/copy model and companion files using downloader or HF cache
        auto summary = [&](const std::string& item, const fs::path& dst) {
            std::cout << "  - " << item << " -> " << dst << "\n";
        };

        if (offline_) {
            std::string repo = !hfRepo_.empty() ? hfRepo_ : model_name;
            const std::string rev = revision_.empty() ? std::string("main") : revision_;

            auto findInCacheFile = [&](const std::string& filename) -> fs::path {
                const char* home = std::getenv("HOME");
                const std::string homeDir = home ? home : "";
                const char* hfhome = std::getenv("HF_HOME");
                const char* ycache = std::getenv("YAMS_ONNX_HF_CACHE");
                std::vector<fs::path> roots;
                if (ycache)
                    roots.emplace_back(ycache);
                if (hfhome) {
                    roots.emplace_back(hfhome);
                    roots.emplace_back(fs::path(hfhome) / "hub");
                }
                if (!homeDir.empty()) {
                    roots.emplace_back(fs::path(homeDir) / ".cache" / "huggingface");
                    roots.emplace_back(fs::path(homeDir) / ".cache" / "huggingface" / "hub");
                }
                std::string repo_dir = "models--";
                for (char c : repo)
                    repo_dir += (c == '/') ? std::string("--") : std::string(1, c);
                for (const auto& root : roots) {
                    std::error_code ec;
                    fs::path base = root / repo_dir;
                    if (!fs::exists(base, ec))
                        continue;
                    fs::path snapdir = base / "snapshots" / rev;
                    fs::path ref = base / "refs" / rev;
                    if (fs::exists(ref, ec)) {
                        try {
                            std::ifstream in(ref);
                            std::string hash;
                            std::getline(in, hash);
                            if (!hash.empty())
                                snapdir = base / "snapshots" / hash;
                        } catch (...) {
                        }
                    }
                    if (fs::exists(snapdir, ec)) {
                        for (auto& p : fs::recursive_directory_iterator(snapdir, ec)) {
                            if (p.is_regular_file() && p.path().filename() == filename)
                                return p.path();
                        }
                    }
                }
                return {};
            };

            // Required: model.onnx
            auto cached = findInCacheFile("model.onnx");
            if (cached.empty())
                return Error{ErrorCode::NotFound, "model.onnx not found in HF cache"};
            std::error_code cec;
            fs::copy_file(cached, model_file, fs::copy_options::overwrite_existing, cec);
            if (cec)
                return Error{ErrorCode::IOError, std::string("Copy failed: ") + cec.message()};
            summary("model.onnx (cache)", model_file);

            // Optional companions
            fs::path cfg_dst = output_path / "config.json";
            if (auto p = findInCacheFile("config.json"); !p.empty()) {
                std::error_code ec;
                fs::copy_file(p, cfg_dst, fs::copy_options::overwrite_existing, ec);
                if (!ec)
                    summary("config.json (cache)", cfg_dst);
            }
            fs::path tok_dst = output_path / "tokenizer.json";
            if (auto p = findInCacheFile("tokenizer.json"); !p.empty()) {
                std::error_code ec;
                fs::copy_file(p, tok_dst, fs::copy_options::overwrite_existing, ec);
                if (!ec)
                    summary("tokenizer.json (cache)", tok_dst);
            }
            fs::path sb_dst = output_path / "sentence_bert_config.json";
            if (auto p = findInCacheFile("sentence_bert_config.json"); !p.empty()) {
                std::error_code ec;
                fs::copy_file(p, sb_dst, fs::copy_options::overwrite_existing, ec);
                if (!ec)
                    summary("sentence_bert_config.json (cache)", sb_dst);
            }

        } else {
            // Downloader-backed online fetch
            yams::downloader::StorageConfig storage{};
            // Prefer configured data dir for CAS objects; stage under configured staging or /tmp
            try {
                std::filesystem::path dataDir =
                    cli_ ? cli_->getDataPath() : std::filesystem::path{};
                if (!dataDir.empty()) {
                    storage.objectsDir = dataDir / "storage" / "objects";
                    storage.stagingDir = dataDir / "staging" / "downloader";
                } else {
                    auto tmp = std::filesystem::temp_directory_path();
                    storage.objectsDir = tmp / "yams" / "objects";
                    storage.stagingDir = tmp / "yams" / "downloader";
                }
            } catch (...) {
                // Fallback to /tmp if any path errors occur
                try {
                    auto tmp = std::filesystem::temp_directory_path();
                    storage.objectsDir = tmp / "yams" / "objects";
                    storage.stagingDir = tmp / "yams" / "downloader";
                } catch (...) {
                    // keep defaults if temp_directory_path throws
                }
            }

            yams::downloader::DownloaderConfig dcfg{};
            auto manager = yams::downloader::makeDownloadManager(storage, dcfg);

            std::vector<yams::downloader::Header> headers;
            if (!token_.empty())
                headers.push_back({"Authorization", std::string("Bearer ") + token_});

            auto dl_one = [&](const std::string& url, const fs::path& out,
                              const std::string& label) -> Result<void> {
                yams::downloader::DownloadRequest req{};
                req.url = url;
                req.headers = headers;
                req.storeOnly = true;
                req.exportPath = out;
                // In-place progress (single line), with proper spacing to avoid overlap
                auto onProgress = [label, lastStage = yams::downloader::ProgressStage::Resolving,
                                   lastPct = -1.0f, lastLen = std::size_t{0}](
                                      const yams::downloader::ProgressEvent& ev) mutable {
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
                    bool stageChanged = ev.stage != lastStage;
                    bool pctDelta = (pct - lastPct) >= 1.0f; // update every 1%
                    if (!stageChanged && !pctDelta &&
                        ev.stage == yams::downloader::ProgressStage::Downloading)
                        return;
                    lastStage = ev.stage;
                    lastPct = pct;

                    // Build content string with aligned fields
                    std::uint64_t done = ev.downloadedBytes;
                    double done_mb = static_cast<double>(done) / (1024.0 * 1024.0);
                    std::string content;
                    if (ev.totalBytes) {
                        double total_mb = static_cast<double>(*ev.totalBytes) / (1024.0 * 1024.0);
                        content = fmt::format("  - {}: {:11s} {:3.0f}% [{:6.1f}/{:6.1f} MiB]",
                                              label, stageName(ev.stage), pct, done_mb, total_mb);
                    } else {
                        content = fmt::format("  - {}: {:11s}       [{:6.1f} MiB]", label,
                                              stageName(ev.stage), done_mb);
                    }
                    // Clear previous line if new content is shorter
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
                    std::string msg;
                    int http = -1;
                    if (res.ok()) {
                        if (res.value().httpStatus)
                            http = *res.value().httpStatus;
                        msg = res.value().error ? res.value().error->message
                                                : std::string("download failed");
                    } else {
                        msg = res.error().message;
                    }
                    if (http == 401 || http == 403) {
                        msg += "; authentication may be required (try --token <HF_TOKEN>)";
                    }
                    return Error{ErrorCode::InternalError, label + ": " + msg};
                }
                summary(label, out);
                return Result<void>{};
            };

            auto dl_candidates = [&](const std::vector<std::string>& candidates,
                                     const fs::path& out,
                                     const std::string& label) -> Result<void> {
                // Try to parse repo/rev from a known HF URL if present
                auto parseHf = [](const std::string& url) -> std::pair<std::string, std::string> {
                    // Expect: https://huggingface.co/<repo>/resolve/<rev>/...
                    const std::string host = "https://huggingface.co/";
                    if (url.rfind(host, 0) != 0)
                        return {"", ""};
                    auto rest = url.substr(host.size());
                    auto p = rest.find("/resolve/");
                    if (p == std::string::npos)
                        return {"", ""};
                    std::string repo = rest.substr(0, p);
                    auto rest2 = rest.substr(p + std::string("/resolve/").size());
                    auto slash = rest2.find('/');
                    if (slash == std::string::npos)
                        return {repo, "main"};
                    std::string rev = rest2.substr(0, slash);
                    return {repo, rev.empty() ? "main" : rev};
                };
                std::string parsedRepo, parsedRev;
                if (!model.url.empty()) {
                    auto pr = parseHf(model.url);
                    parsedRepo = pr.first;
                    parsedRev = pr.second;
                }
                std::string rev = !parsedRev.empty()
                                      ? parsedRev
                                      : (revision_.empty() ? std::string("main") : revision_);
                if (!override_url.empty()) {
                    return dl_one(override_url, out, label);
                }
                std::string repo =
                    !parsedRepo.empty() ? parsedRepo : (!hfRepo_.empty() ? hfRepo_ : model_name);
                if (repo.find('/') == std::string::npos) {
                    // Try mapping
                    std::string mapped = mapKnown(repo);
                    if (!mapped.empty())
                        repo = mapped;
                }
                if (repo.find('/') == std::string::npos) {
                    // As a last resort, if we still lack a repo but we have a direct model URL, try
                    // that
                    if (!model.url.empty() && label == "model.onnx") {
                        return dl_one(model.url, out, label);
                    }
                    return Error{ErrorCode::InvalidArgument,
                                 "Missing --hf <org/name> (could not infer repo)"};
                }
                std::vector<std::string> tried;
                for (const auto& p : candidates) {
                    std::string url =
                        std::string("https://huggingface.co/") + repo + "/resolve/" + rev + "/" + p;
                    tried.push_back(url);
                    auto r = dl_one(url, out, label);
                    if (r)
                        return r;
                }
                std::ostringstream oss;
                oss << label << " not found at expected locations:\n";
                for (const auto& u : tried)
                    oss << "    - " << u << "\n";
                return Error{ErrorCode::NotFound, oss.str()};
            };

            // Required model: try multiple common names
            {
                std::vector<std::string> model_candidates = {
                    "onnx/model_quantized.onnx", "onnx/model.onnx",     "model.onnx",
                    "onnx/model_fp16.onnx",      "onnx/model-f16.onnx", "onnx/model_float32.onnx",
                };
                auto r = dl_candidates(model_candidates, model_file, "model.onnx");
                if (!r) {
                    // If the file appears to exist with non-zero size despite a reported error,
                    // treat as success
                    std::error_code fec;
                    auto sz = fs::file_size(model_file, fec);
                    if (fec || sz == 0)
                        return r; // truly failed
                }
            }
            // Optional companions
            (void)dl_candidates({"config.json"}, output_path / "config.json", "config.json");
            (void)dl_candidates({"tokenizer.json"}, output_path / "tokenizer.json",
                                "tokenizer.json");
            (void)dl_candidates({"sentence_bert_config.json"},
                                output_path / "sentence_bert_config.json",
                                "sentence_bert_config.json");
            if (model_name == "mxbai-edge-colbert-v0-17m") {
                (void)dl_candidates({"tokenizer_config.json"},
                                    output_path / "tokenizer_config.json", "tokenizer_config.json");
                (void)dl_candidates({"special_tokens_map.json"},
                                    output_path / "special_tokens_map.json",
                                    "special_tokens_map.json");
                (void)dl_candidates({"skiplist.json"}, output_path / "skiplist.json",
                                    "skiplist.json");
            }
        }

        std::cout << "\n✓ Model files ready in: " << output_path << "\n";

        // Detect properties for improved hints
        auto props = detect_model_props(model_name, output_path);
        if (props.dim > 0 || props.max_seq > 0) {
            std::cout << "\nDetected properties:\n";
            if (props.dim > 0)
                std::cout << "  • embedding_dim: " << props.dim << "\n";
            if (props.max_seq > 0)
                std::cout << "  • max_sequence_length: " << props.max_seq << "\n";
            if (!props.pooling.empty())
                std::cout << "  • pooling: " << props.pooling << "\n";
        }

        std::cout << "\nTo use this model:\n";
        {
            std::cout << "  Configure YAMS to use: " << model_file << "\n";
            std::cout << "  The ONNX provider auto-detects dim/seq/pooling from config.json if "
                         "present.\n";
            std::cout << "\n  Set this as the preferred embedding model:\n";
            std::cout << "    yams config embeddings model " << model_name << "\n";
            if (props.dim > 0) {
                std::cout << "  Update embedding/vector dimensions (recommended):\n";
                std::cout << "    yams config set embeddings.embedding_dim " << props.dim << "\n";
                std::cout << "    yams config set vector_database.embedding_dim " << props.dim
                          << "\n";
                std::cout << "    yams config set vector_index.dimension " << props.dim << "\n";
            }
            if (props.max_seq > 0) {
                std::cout << "  Update max sequence length (optional):\n";
                std::cout << "    yams config set embeddings.max_sequence_length " << props.max_seq
                          << "\n";
            }
            std::cout << "  Check embedding status:\n";
            std::cout << "    yams config embeddings status\n";
            std::cout << "  Enable automatic embedding generation (optional):\n";
            std::cout << "    yams config embeddings enable\n";
        }

        if (applyConfig_) {
            auto ar = apply_detected_to_config(model_name, props);
            if (!ar) {
                spdlog::warn("Failed to update config automatically: {}", ar.error().message);
            } else {
                // Align vector DB schema to new embedding dim by dropping/recreating tables
                try {
                    auto dataDir = cli_ ? cli_->getDataPath() : std::filesystem::path{};
                    if (!dataDir.empty() && props.dim > 0) {
                        auto al = yams::integrity::ensureVectorSchemaAligned(
                            dataDir, props.dim, /*recreateIfMismatch=*/true);
                        if (!al) {
                            spdlog::warn("Vector schema alignment failed: {}", al.error().message);
                        } else {
                            spdlog::info("Vector schema aligned to dim {} at {}", props.dim,
                                         (dataDir / "vectors.db").string());
                        }
                    }
                } catch (const std::exception& ex) {
                    spdlog::warn("Exception during vector schema alignment: {}", ex.what());
                }
            }
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
            } else if (model.name.find("jina") != std::string::npos) {
                std::cout << "  - Embedding dimension: 768\n";
                std::cout << "  - Max sequence length: 8192 tokens\n";
                std::cout << "  - Architecture: BERT-based with ALiBi\n";
                std::cout << "  - Optimized for code search across 30+ languages\n";
            }
            std::cout << "\nHint: set this as your preferred embedding model with:\n";
            std::cout << "  yams config embeddings model " << model.name << "\n";
        } else if (model.type == "reranker") {
            std::cout << "\nReranker Model Details:\n";
            std::cout << "  - Max input length: 512 tokens\n";
            std::cout << "  - Architecture: Cross-encoder (BERT-based)\n";
            std::cout << "  - Use case: Two-stage retrieval for hybrid search\n\n";
            std::cout << "How it works:\n";
            std::cout << "  1. First stage: Fast retrieval using keyword + vector search\n";
            std::cout << "  2. Second stage: Reranker scores query-document pairs with\n";
            std::cout << "     cross-attention, improving ranking quality\n\n";
            if (model.name.find("large") != std::string::npos) {
                std::cout << "Note: The 'large' model provides higher accuracy but\n";
                std::cout << "      is slower than 'base'. Use for quality-critical tasks.\n\n";
            } else {
                std::cout << "Note: This is the recommended model for most use cases.\n";
                std::cout << "      Good balance of speed and accuracy.\n\n";
            }
            std::cout << "To enable reranking:\n";
            std::cout << "  1. Download the model: yams model download " << model.name << "\n";
            std::cout << "  2. Configure YAMS:\n";
            std::cout << "     yams config search reranker set " << model.name << "\n";
            std::cout
                << "     (Optional) yams config set search.reranker_model_path <path>/model.onnx\n";
            std::cout << "     (Reranking auto-activates when the model is detected.)\n";
        }

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
        fs::path plugin_dir;
        if (plugin_env) {
            plugin_dir = fs::path(plugin_env);
        } else {
#ifdef __APPLE__
            plugin_dir = fs::path("/opt/homebrew/lib/yams/plugins");
#else
            plugin_dir = fs::path("/usr/local/lib/yams/plugins");
#endif
        }
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
            std::cout << "No installed models found in "
                      << (cli_->getDataPath() / "models").string() << "\n";
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
  Embedding models:
  - all-MiniLM-L6-v2: Fast 384-dim embeddings (90MB)
  - all-mpnet-base-v2: High-quality 768-dim embeddings (420MB)
  - nomic-embed-text-v1.5: Nomic embedding model (URL override supported)

  Reranker models (cross-encoder for hybrid search):
  - bge-reranker-base: Cross-encoder reranker (278MB)
  - bge-reranker-large: High-quality reranker (560MB)

Default download location: <data-dir>/models/<model-name>/
  (typically ~/.local/share/yams/models/ or $XDG_DATA_HOME/yams/models/)

Note: Models are stored in the unified YAMS data directory for consistency.
)";
    }

    // Build a Hugging Face resolve URL for a given repo and revision
    static std::string buildHfUrl(const std::string& repo, const std::string& revision) {
        const std::string rev = revision.empty() ? std::string("main") : revision;
        return std::string("https://huggingface.co/") + repo + "/resolve/" + rev +
               "/onnx/model.onnx";
    }

    // Find model.onnx for a repo/revision in the local HF cache (no network)
    static fs::path findInHfCache(const std::string& repo, const std::string& revision) {
        const char* home = std::getenv("HOME");
        const std::string homeDir = home ? home : "";
        const char* hfhome = std::getenv("HF_HOME");
        const char* ycache = std::getenv("YAMS_ONNX_HF_CACHE");

        std::vector<fs::path> roots;
        if (ycache)
            roots.emplace_back(ycache);
        if (hfhome) {
            roots.emplace_back(hfhome);
            roots.emplace_back(fs::path(hfhome) / "hub");
        }
        if (!homeDir.empty()) {
            roots.emplace_back(fs::path(homeDir) / ".cache" / "huggingface");
            roots.emplace_back(fs::path(homeDir) / ".cache" / "huggingface" / "hub");
        }

        // HF cache repo dir format: models--org--name
        std::string repo_dir = "models--";
        for (char c : repo)
            repo_dir += (c == '/') ? std::string("--") : std::string(1, c);
        const std::string rev = revision.empty() ? std::string("main") : revision;

        for (const auto& root : roots) {
            std::error_code ec;
            fs::path base = root / repo_dir;
            if (!fs::exists(base, ec))
                continue;

            // Prefer refs/<rev> to map to snapshot hash under snapshots/
            fs::path snapdir = base / "snapshots" / rev;
            fs::path ref = base / "refs" / rev;
            if (fs::exists(ref, ec)) {
                try {
                    std::ifstream in(ref);
                    std::string hash;
                    std::getline(in, hash);
                    if (!hash.empty())
                        snapdir = base / "snapshots" / hash;
                } catch (...) {
                }
            }
            if (fs::exists(snapdir, ec)) {
                for (auto& p : fs::recursive_directory_iterator(snapdir, ec)) {
                    if (!p.is_regular_file())
                        continue;
                    if (p.path().filename() == "model.onnx")
                        return p.path();
                }
            }
        }
        return {};
    }
};

// Factory function
std::unique_ptr<ICommand> createModelCommand() {
    return std::make_unique<ModelCommand>();
}

} // namespace yams::cli
