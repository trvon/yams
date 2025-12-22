#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <CLI/CLI.hpp>
#include <fmt/core.h>

#include <yams/cli/command.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::cli {

namespace {
std::string normalizePath(std::string path) {
    std::filesystem::path p(path);
    auto normalized = p.lexically_normal();
    std::string result = normalized.generic_string();
    bool originalAbsolute = !path.empty() && (path.front() == '/' || path.front() == '\\');
    if (originalAbsolute && (result.empty() || result.front() != '/'))
        result.insert(result.begin(), '/');
    if (result.size() > 1 && result.back() == '/')
        result.pop_back();
    return result;
}

struct TreeOptions {
    int depth{2};
    std::size_t limit{10};
    bool showCentroid{false};
};

Result<nlohmann::json> buildTreeJson(metadata::IMetadataRepository& repo, const std::string& path,
                                     const TreeOptions& opts, int depthRemaining) {
    nlohmann::json nodeJson;
    nodeJson["path"] = path;

    std::optional<metadata::PathTreeNode> nodeOpt;
    if (path != "/") {
        auto nodeRes = repo.findPathTreeNodeByFullPath(path);
        if (!nodeRes)
            return nodeRes.error();
        nodeOpt = nodeRes.value();
        if (!nodeOpt) {
            nodeJson["doc_count"] = 0;
            nodeJson["centroid_weight"] = 0;
            nodeJson["missing"] = true;
            nodeJson["children"] = nlohmann::json::array();
            return nodeJson;
        }
        nodeJson["doc_count"] = nodeOpt->docCount;
        nodeJson["centroid_weight"] = nodeOpt->centroidWeight;
        if (opts.showCentroid && !nodeOpt->centroid.empty())
            nodeJson["centroid"] = nodeOpt->centroid;
    }

    auto childrenRes = repo.listPathTreeChildren(path, opts.limit);
    if (!childrenRes)
        return childrenRes.error();
    auto children = std::move(childrenRes.value());

    if (path == "/") {
        int64_t total = 0;
        for (const auto& child : children)
            total += child.docCount;
        nodeJson["doc_count"] = total;
        nodeJson["centroid_weight"] = 0;
    }

    nlohmann::json childArray = nlohmann::json::array();
    int nextDepth = depthRemaining - 1;
    for (const auto& child : children) {
        if (nextDepth > 0) {
            auto childJsonRes = buildTreeJson(repo, child.fullPath, opts, nextDepth);
            if (!childJsonRes)
                return childJsonRes.error();
            auto childJson = std::move(childJsonRes.value());
            childJson["segment"] = child.pathSegment;
            childArray.push_back(std::move(childJson));
        } else {
            nlohmann::json summary = {{"path", child.fullPath},
                                      {"segment", child.pathSegment},
                                      {"doc_count", child.docCount},
                                      {"centroid_weight", child.centroidWeight}};
            if (opts.showCentroid && !child.centroid.empty())
                summary["centroid"] = child.centroid;
            summary["children"] = nlohmann::json::array();
            childArray.push_back(std::move(summary));
        }
    }
    nodeJson["children"] = std::move(childArray);
    return nodeJson;
}

void printTreeText(const nlohmann::json& node, int indent = 0) {
    const auto path = node.value("path", std::string{});
    const auto segment = node.value("segment", path);
    const auto docs = node.value("doc_count", 0);
    const auto weight = node.value("centroid_weight", 0);
    auto missing = node.value("missing", false);

    fmt::print("{}{:<} (path={}, docs={}, centroid_weight={}", std::string(indent * 2, ' '),
               segment, path, docs, weight);
    if (missing)
        fmt::print(" [missing]");
    fmt::print(")\n");

    if (!node.contains("children"))
        return;
    for (const auto& child : node["children"]) {
        printTreeText(child, indent + 1);
    }
}
} // namespace

class TreeCommand : public ICommand {
public:
    std::string getName() const override { return "tree"; }

    std::string getDescription() const override {
        return "Inspect aggregated path tree statistics";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand("tree", getDescription());
        cmd->add_option("--prefix", prefix_, "Path prefix to inspect")->default_val("/");
        cmd->add_option("--depth", depth_, "Traversal depth (levels)")->default_val(2);
        cmd->add_option("--limit", childLimit_, "Maximum children per level")->default_val(10);
        cmd->add_flag("--json", jsonOutput_, "Emit JSON instead of text");
        cmd->add_flag("--show-centroid", showCentroid_, "Include centroid vectors when available");

        cmd->callback([this]() {
            auto res = execute();
            if (!res) {
                spdlog::error("tree command failed: {}", res.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        // Ensure storage is initialized before accessing metadata
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }

        auto metadataRepo = cli_->getMetadataRepository();
        std::unique_ptr<metadata::ConnectionPool> fallbackPool;
        std::shared_ptr<metadata::MetadataRepository> fallbackRepo;

        if (!metadataRepo) {
            std::filesystem::path dbPath;
            const char* metadataEnv = std::getenv("YAMS_BENCH_METADATA_DB");
            if (metadataEnv && std::string_view(metadataEnv).empty() == false)
                dbPath = metadataEnv;

            if (dbPath.empty()) {
                const char* datasetEnv = std::getenv("YAMS_BENCH_DATASET_DIR");
                if (datasetEnv && std::string_view(datasetEnv).empty() == false) {
                    std::filesystem::path dir(datasetEnv);
                    std::filesystem::path candidate = dir / "metadata.db";
                    if (!std::filesystem::exists(candidate))
                        candidate = dir / "yams.db";
                    if (std::filesystem::exists(candidate))
                        dbPath = candidate;
                }
            }

            if (!dbPath.empty() && std::filesystem::exists(dbPath)) {
                metadata::ConnectionPoolConfig cfg;
                cfg.minConnections = 1;
                cfg.maxConnections = 2;
                fallbackPool = std::make_unique<metadata::ConnectionPool>(dbPath.string(), cfg);
                auto init = fallbackPool->initialize();
                if (!init)
                    return init.error();
                fallbackRepo = std::make_shared<metadata::MetadataRepository>(*fallbackPool);
                metadataRepo = fallbackRepo;
                spdlog::info("tree command: using metadata DB {}", dbPath.string());
            }
        }

        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository unavailable"};
        }

        auto normalized = normalizePath(prefix_);
        if (depth_ < 1)
            depth_ = 1;

        TreeOptions opts{depth_, childLimit_, showCentroid_};
        auto treeJsonRes = buildTreeJson(*metadataRepo, normalized, opts, depth_);
        if (!treeJsonRes)
            return treeJsonRes.error();

        auto treeJson = std::move(treeJsonRes.value());
        if (treeJson.value("missing", false)) {
            spdlog::warn("Prefix '{}' not present in path tree index", normalized);
        }

        if (jsonOutput_) {
            std::cout << treeJson.dump(2) << std::endl;
        } else {
            printTreeText(treeJson);
        }
        return Result<void>();
    }

private:
    YamsCLI* cli_{nullptr};
    std::string prefix_ = "/";
    int depth_{2};
    std::size_t childLimit_{10};
    bool jsonOutput_{false};
    bool showCentroid_{false};
};

std::unique_ptr<ICommand> createTreeCommand() {
    return std::make_unique<TreeCommand>();
}

} // namespace yams::cli
