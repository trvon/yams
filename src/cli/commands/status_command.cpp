#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <vector>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>

namespace yams::cli {

namespace fs = std::filesystem;

class StatusCommand : public ICommand {
public:
    std::string getName() const override { return "status"; }

    std::string getDescription() const override {
        return "Show quick system status and health overview";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("status", getDescription());

        cmd->add_flag("--json", jsonOutput_, "Output in JSON format");
        cmd->add_flag("--verbose", verbose_, "Show detailed information");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Status command failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("StatusCommand::execute");

        try {
            // Ensure storage is initialized (will detect existing storage)
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                // Storage doesn't exist or can't be initialized
                if (jsonOutput_) {
                    std::cout
                        << R"({"status": "not_initialized", "message": "YAMS not initialized"})"
                        << std::endl;
                } else {
                    std::cout << "YAMS System Status\n";
                    std::cout << "==================\n";
                    std::cout << "✗ Not initialized\n";
                    std::cout << "  → Run 'yams init' to get started\n";
                }
                return Result<void>();
            }

            // Get system components
            auto store = cli_->getContentStore();
            auto metadataRepo = cli_->getMetadataRepository();

            if (!store || !metadataRepo) {
                return Error{ErrorCode::NotInitialized, "Failed to access storage components"};
            }

            // Gather status information
            StatusInfo status = gatherStatusInfo(store, metadataRepo);

            if (jsonOutput_) {
                outputJson(status);
            } else {
                outputText(status);
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    bool jsonOutput_ = false;
    bool verbose_ = false;

    struct StatusInfo {
        // Storage
        bool storageHealthy = false;
        uint64_t totalDocuments = 0;
        uint64_t totalSize = 0;
        std::string storagePath;

        // Models
        std::vector<std::string> availableModels;
        bool hasModels = false;

        // Embeddings
        bool autoGenerationEnabled = false;
        uint64_t embeddingCount = 0;
        bool vectorDbHealthy = false;
        std::string preferredModel;

        // Issues/Recommendations
        std::vector<std::string> warnings;
        std::vector<std::string> recommendations;
    };

    StatusInfo gatherStatusInfo(std::shared_ptr<api::IContentStore> store,
                                std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        StatusInfo info;

        // Storage information
        try {
            auto stats = store->getStats();
            info.storageHealthy = true;
            info.totalDocuments = stats.totalObjects;
            info.totalSize = stats.totalBytes;
            info.storagePath = cli_->getDataPath().string();
        } catch (const std::exception& e) {
            info.storageHealthy = false;
            info.warnings.push_back("Storage access failed: " + std::string(e.what()));
        }

        // Check available models
        const char* home = std::getenv("HOME");
        if (home) {
            fs::path modelsPath = fs::path(home) / ".yams" / "models";
            if (fs::exists(modelsPath)) {
                for (const auto& entry : fs::directory_iterator(modelsPath)) {
                    if (entry.is_directory()) {
                        fs::path modelFile = entry.path() / "model.onnx";
                        if (fs::exists(modelFile)) {
                            info.availableModels.push_back(entry.path().filename().string());
                        }
                    }
                }
            }
        }
        info.hasModels = !info.availableModels.empty();

        // Check embedding configuration
        std::unique_ptr<vector::EmbeddingService> embeddingService;

        if (store && metadataRepo) {
            embeddingService = std::make_unique<vector::EmbeddingService>(store, metadataRepo,
                                                                          cli_->getDataPath());
        }

        info.autoGenerationEnabled = embeddingService && embeddingService->isAvailable();
        info.preferredModel = info.hasModels ? info.availableModels[0] : "none";

        // Check vector database status
        try {
            vector::VectorDatabaseConfig vdbConfig;
            vdbConfig.database_path = (cli_->getDataPath() / "vectors.db").string();
            vdbConfig.embedding_dim = 384; // Default for status check

            auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
            if (vectorDb->initialize()) {
                info.vectorDbHealthy = true;
                if (vectorDb->tableExists()) {
                    info.embeddingCount = vectorDb->getVectorCount();
                }
            }
        } catch (const std::exception& e) {
            info.vectorDbHealthy = false;
            if (verbose_) {
                info.warnings.push_back("Vector database check failed: " + std::string(e.what()));
            }
        }

        // Generate recommendations
        generateRecommendations(info);

        return info;
    }

    void generateRecommendations(StatusInfo& info) {
        if (!info.hasModels) {
            info.recommendations.push_back(
                "Download an embedding model: yams model --download all-MiniLM-L6-v2");
        }

        if (info.hasModels && !info.autoGenerationEnabled) {
            info.recommendations.push_back("Enable auto-embedding: yams config embeddings enable");
        }

        if (info.totalDocuments > 0 && info.embeddingCount == 0) {
            info.recommendations.push_back(
                "Generate embeddings for existing documents: yams repair --embeddings");
        }

        if (info.totalDocuments > 100 && info.embeddingCount < info.totalDocuments / 2) {
            info.recommendations.push_back(
                "Many documents lack embeddings - consider running: yams repair --embeddings");
        }
    }

    void outputJson(const StatusInfo& info) {
        std::cout << "{\n";
        std::cout << "  \"storage\": {\n";
        std::cout << "    \"healthy\": " << (info.storageHealthy ? "true" : "false") << ",\n";
        std::cout << "    \"totalDocuments\": " << info.totalDocuments << ",\n";
        std::cout << "    \"totalSize\": " << info.totalSize << ",\n";
        std::cout << "    \"path\": \"" << info.storagePath << "\"\n";
        std::cout << "  },\n";

        std::cout << "  \"models\": {\n";
        std::cout << "    \"available\": [";
        for (size_t i = 0; i < info.availableModels.size(); ++i) {
            if (i > 0)
                std::cout << ", ";
            std::cout << "\"" << info.availableModels[i] << "\"";
        }
        std::cout << "],\n";
        std::cout << "    \"hasModels\": " << (info.hasModels ? "true" : "false") << "\n";
        std::cout << "  },\n";

        std::cout << "  \"embeddings\": {\n";
        std::cout << "    \"autoGenerationEnabled\": "
                  << (info.autoGenerationEnabled ? "true" : "false") << ",\n";
        std::cout << "    \"embeddingCount\": " << info.embeddingCount << ",\n";
        std::cout << "    \"vectorDbHealthy\": " << (info.vectorDbHealthy ? "true" : "false")
                  << ",\n";
        std::cout << "    \"preferredModel\": \"" << info.preferredModel << "\"\n";
        std::cout << "  },\n";

        std::cout << "  \"recommendations\": [";
        for (size_t i = 0; i < info.recommendations.size(); ++i) {
            if (i > 0)
                std::cout << ", ";
            std::cout << "\"" << info.recommendations[i] << "\"";
        }
        std::cout << "],\n";

        std::cout << "  \"warnings\": [";
        for (size_t i = 0; i < info.warnings.size(); ++i) {
            if (i > 0)
                std::cout << ", ";
            std::cout << "\"" << info.warnings[i] << "\"";
        }
        std::cout << "]\n";
        std::cout << "}\n";
    }

    void outputText(const StatusInfo& info) {
        std::cout << "YAMS System Status\n";
        std::cout << "==================\n\n";

        // Storage status
        std::cout << (info.storageHealthy ? "✓" : "✗") << " Storage: ";
        if (info.storageHealthy) {
            std::cout << "Healthy (" << formatSize(info.totalSize) << ", " << info.totalDocuments
                      << " documents)\n";
        } else {
            std::cout << "Issues detected\n";
        }

        // Models status
        std::cout << (info.hasModels ? "✓" : "⚠") << " Models: ";
        if (info.hasModels) {
            std::cout << info.availableModels.size() << " available (";
            for (size_t i = 0; i < info.availableModels.size(); ++i) {
                if (i > 0)
                    std::cout << ", ";
                std::cout << info.availableModels[i];
            }
            std::cout << ")\n";
        } else {
            std::cout << "No embedding models found\n";
        }

        // Embeddings status
        std::cout << (info.autoGenerationEnabled ? "✓" : "⚠") << " Embeddings: ";
        if (info.autoGenerationEnabled) {
            std::cout << "Auto-generation enabled";
        } else {
            std::cout << "Auto-generation disabled";
        }
        if (info.embeddingCount > 0) {
            std::cout << " (" << info.embeddingCount << " embeddings ready)";
        }
        std::cout << "\n";

        // Vector DB status
        std::cout << (info.vectorDbHealthy ? "✓" : "⚠") << " Vector DB: ";
        if (info.vectorDbHealthy) {
            std::cout << "Ready for semantic search\n";
        } else {
            std::cout << "Not available\n";
        }

        // Show recommendations
        if (!info.recommendations.empty()) {
            std::cout << "\nRecommendations:\n";
            for (const auto& rec : info.recommendations) {
                std::cout << "  → " << rec << "\n";
            }
        }

        // Show warnings if verbose or if there are critical issues
        if (!info.warnings.empty() && (verbose_ || !info.storageHealthy)) {
            std::cout << "\nWarnings:\n";
            for (const auto& warning : info.warnings) {
                std::cout << "  ⚠ " << warning << "\n";
            }
        }

        std::cout << "\nFor detailed statistics: yams stats\n";
        std::cout << "For configuration help: yams config --help\n";
    }

    std::string formatSize(uint64_t bytes) const {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);

        while (size >= 1024 && unitIndex < 4) {
            size /= 1024;
            unitIndex++;
        }

        std::ostringstream oss;
        if (unitIndex == 0) {
            oss << bytes << " B";
        } else {
            oss << std::fixed << std::setprecision(1) << size << " " << units[unitIndex];
        }
        return oss.str();
    }
};

// Factory function
std::unique_ptr<ICommand> createStatusCommand() {
    return std::make_unique<StatusCommand>();
}

} // namespace yams::cli