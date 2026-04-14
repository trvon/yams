#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include <yams/compat/unistd.h>

#include "../common/test_helpers_catch2.h"

#include <nlohmann/json.hpp>
#include <numeric>
#include <unordered_map>
#include <unordered_set>

#include "../integration/daemon/test_daemon_harness.h"

#include <yams/app/services/document_ingestion_service.h>
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/model_provider.h>

#include <spdlog/spdlog.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using yams::app::services::AddOptions;
using yams::app::services::DocumentIngestionService;

namespace {

class BenchmarkMockModelProvider final : public yams::daemon::IModelProvider {
public:
    explicit BenchmarkMockModelProvider(std::size_t dim) : dim_(dim) {}

    void setProgressCallback(std::function<void(const yams::daemon::ModelLoadEvent&)> cb) override {
        progress_ = std::move(cb);
    }

    yams::Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        return generateEmbeddingFor(defaultModelName_, text);
    }

    yams::Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        return generateBatchEmbeddingsFor(defaultModelName_, texts);
    }

    yams::Result<std::vector<float>> generateEmbeddingFor(const std::string& modelName,
                                                          const std::string& text) override {
        if (!isModelLoaded(modelName)) {
            return yams::Error{yams::ErrorCode::NotFound, "model not loaded"};
        }
        std::vector<float> embedding(dim_, 0.0f);
        std::hash<std::string> hasher;
        const auto seed = hasher(text);
        for (std::size_t i = 0; i < embedding.size(); ++i) {
            embedding[i] = static_cast<float>((seed ^ (i * 131)) % 1000) / 1000.0f;
        }
        return embedding;
    }

    yams::Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string& modelName,
                               const std::vector<std::string>& texts) override {
        if (!isModelLoaded(modelName)) {
            return yams::Error{yams::ErrorCode::NotFound, "model not loaded"};
        }
        std::vector<std::vector<float>> batch;
        batch.reserve(texts.size());
        for (const auto& text : texts) {
            auto embedding = generateEmbeddingFor(modelName, text);
            if (!embedding) {
                return embedding.error();
            }
            batch.push_back(std::move(embedding.value()));
        }
        return batch;
    }

    yams::Result<void> loadModel(const std::string& modelName) override {
        if (progress_) {
            yams::daemon::ModelLoadEvent ev;
            ev.modelName = modelName;
            ev.phase = "loading";
            ev.message = "benchmark mock loading";
            progress_(ev);
        }
        loadedModels_.insert(modelName);
        defaultModelName_ = modelName;
        if (progress_) {
            yams::daemon::ModelLoadEvent ev;
            ev.modelName = modelName;
            ev.phase = "completed";
            ev.message = "benchmark mock ready";
            progress_(ev);
        }
        return yams::Result<void>();
    }

    yams::Result<void> unloadModel(const std::string& modelName) override {
        loadedModels_.erase(modelName);
        return yams::Result<void>();
    }

    bool isModelLoaded(const std::string& modelName) const override {
        return loadedModels_.contains(modelName);
    }

    std::vector<std::string> getLoadedModels() const override {
        return std::vector<std::string>(loadedModels_.begin(), loadedModels_.end());
    }

    size_t getLoadedModelCount() const override { return loadedModels_.size(); }

    yams::Result<yams::daemon::ModelInfo>
    getModelInfo(const std::string& modelName) const override {
        if (!isModelLoaded(modelName)) {
            return yams::Error{yams::ErrorCode::NotFound, "model not loaded"};
        }
        yams::daemon::ModelInfo info;
        info.name = modelName;
        info.embeddingDim = dim_;
        return info;
    }

    size_t getEmbeddingDim(const std::string&) const override { return dim_; }
    std::shared_ptr<yams::vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string& = "") override {
        return nullptr;
    }
    std::string getProviderName() const override { return "BenchmarkMockProvider"; }
    std::string getProviderVersion() const override { return "1.0"; }
    bool isAvailable() const override { return true; }
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override { loadedModels_.clear(); }

private:
    std::size_t dim_;
    std::string defaultModelName_;
    std::unordered_set<std::string> loadedModels_;
    std::function<void(const yams::daemon::ModelLoadEvent&)> progress_;
};

struct RunConfig {
    std::string label;
    int workers{1};
    int repeat{1};
    bool enableEmbeddings{false};
    std::vector<std::string> args;
};

struct BenchConfig {
    fs::path datasetPath;
    fs::path manifestPath;
    fs::path metricsPath;
    bool postRunCleanup{true};
    std::vector<RunConfig> runs;
};

json loadJsonFile(const fs::path& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("Failed to open config file: " + path.string());
    }
    json data;
    in >> data;
    return data;
}

fs::path normalizePath(const fs::path& base, const fs::path& value) {
    fs::path resolved = value;
    if (resolved.is_relative()) {
        resolved = base / resolved;
    }
    return fs::absolute(resolved).lexically_normal();
}

BenchConfig loadConfig(const fs::path& configPath) {
    const json root = loadJsonFile(configPath);
    BenchConfig cfg;

    const fs::path base =
        configPath.parent_path().empty() ? fs::current_path() : configPath.parent_path();

    cfg.datasetPath =
        normalizePath(base, root.value("dataset_path", std::string("tests/data/ingestion")));
    cfg.manifestPath = normalizePath(
        base, root.value("fixture_manifest", std::string("data/benchmarks/fixtures.json")));
    cfg.metricsPath =
        normalizePath(base, root.value("output_metrics",
                                       std::string("data/benchmarks/ingestion_baseline.jsonl")));
    cfg.postRunCleanup = root.value("post_run_cleanup", true);

    for (const auto& entry : root.at("runs")) {
        RunConfig run;
        run.label = entry.value("label", std::string("baseline"));
        run.workers = entry.value("workers", 1);
        run.repeat = std::max(1, entry.value("repeat", 1));
        run.enableEmbeddings = entry.value("enable_embeddings", false);
        if (entry.contains("args")) {
            for (const auto& arg : entry["args"]) {
                run.args.push_back(arg.get<std::string>());
            }
        }
        cfg.runs.push_back(std::move(run));
    }

    return cfg;
}

std::string isoTimestamp() {
    const auto now = std::chrono::system_clock::now();
    const auto tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &tt);
#else
    gmtime_r(&tt, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

std::vector<std::string> splitCsv(const std::string& csv) {
    std::vector<std::string> tokens;
    std::string current;
    std::istringstream iss(csv);
    while (std::getline(iss, current, ',')) {
        if (!current.empty()) {
            tokens.push_back(current);
        }
    }
    return tokens;
}

struct IngestOptions {
    bool recursive{true};
    bool verify{false};
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;
    std::string collection;
};

IngestOptions parseArgs(const std::vector<std::string>& args) {
    IngestOptions opts;
    for (const auto& arg : args) {
        if (arg == "--recursive") {
            opts.recursive = true;
        } else if (arg == "--no-recursive") {
            opts.recursive = false;
        } else if (arg.rfind("--include=", 0) == 0) {
            auto val = arg.substr(std::string("--include=").size());
            auto patterns = splitCsv(val);
            opts.includePatterns.insert(opts.includePatterns.end(), patterns.begin(),
                                        patterns.end());
        } else if (arg.rfind("--exclude=", 0) == 0) {
            auto val = arg.substr(std::string("--exclude=").size());
            auto patterns = splitCsv(val);
            opts.excludePatterns.insert(opts.excludePatterns.end(), patterns.begin(),
                                        patterns.end());
        } else if (arg.rfind("--tags=", 0) == 0) {
            auto val = arg.substr(std::string("--tags=").size());
            auto tags = splitCsv(val);
            opts.tags.insert(opts.tags.end(), tags.begin(), tags.end());
        } else if (arg.rfind("--collection=", 0) == 0) {
            opts.collection = arg.substr(std::string("--collection=").size());
        } else if (arg == "--verify") {
            opts.verify = true;
        } else if (arg == "--no-verify") {
            opts.verify = false;
        } else if (arg.rfind("--metadata=", 0) == 0) {
            auto kv = arg.substr(std::string("--metadata=").size());
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                auto key = kv.substr(0, pos);
                auto value = kv.substr(pos + 1);
                if (!key.empty()) {
                    opts.metadata.emplace(std::move(key), std::move(value));
                }
            }
        }
    }
    return opts;
}

size_t countFiles(const fs::path& root) {
    if (!fs::exists(root)) {
        throw std::runtime_error("Dataset directory does not exist: " + root.string());
    }
    size_t total = 0;
    for (const auto& entry : fs::recursive_directory_iterator(root)) {
        if (entry.is_regular_file()) {
            ++total;
        }
    }
    return total;
}

void ensureMetricsPath(const fs::path& path) {
    if (!path.parent_path().empty()) {
        fs::create_directories(path.parent_path());
    }
    std::ofstream touch(path, std::ios::app);
    if (!touch) {
        throw std::runtime_error("Failed to open metrics output: " + path.string());
    }
}

void deleteTree(const fs::path& root) {
    std::error_code ec;
    fs::remove_all(root, ec);
    if (ec) {
        std::cerr << "[bench] Warning: failed to remove " << root << ": " << ec.message() << "\n";
    }
}

using ScopedEnv = yams::test::ScopedEnvVar;

struct StatusSnapshot {
    uint64_t documentsTotal{0};
    uint64_t documentsIndexed{0};
    uint64_t workerQueued{0};
    uint64_t workerActive{0};
    uint64_t postIngestQueued{0};
    uint64_t postIngestInflight{0};
    uint64_t postIngestProcessed{0};
    uint64_t postIngestFailed{0};
    uint64_t topologyDirtyDocuments{0};
    uint64_t topologyLastSuccessAgeMs{0};
    uint64_t topologyRebuildLagMs{0};
    uint64_t topologyRebuildRunningAgeMs{0};
    uint64_t topologyLastDurationMs{0};
    uint64_t topologyRebuildsTotal{0};
    uint64_t topologyRebuildFailuresTotal{0};
    uint64_t topologyLastDocumentsRequested{0};
    uint64_t topologyLastDocumentsProcessed{0};
    uint64_t topologyLastClustersBuilt{0};
    uint64_t topologyLastMembershipsBuilt{0};
    uint64_t topologyLastDirtySeedCount{0};
    uint64_t topologyLastDirtyRegionDocs{0};
    uint64_t topologyLastCoalescedDirtySets{0};
    uint64_t topologyLastFallbackFullRebuilds{0};
    bool topologyArtifactsFresh{false};
    bool topologyRebuildRunning{false};

    [[nodiscard]] bool pipelineIdle(bool requireTopologyFresh) const {
        const bool topologyIdle =
            !requireTopologyFresh ||
            (topologyArtifactsFresh && !topologyRebuildRunning && topologyDirtyDocuments == 0);
        return workerQueued == 0 && workerActive == 0 && postIngestQueued == 0 &&
               postIngestInflight == 0 && topologyIdle;
    }

    static StatusSnapshot capture(yams::daemon::DaemonClient& client) {
        StatusSnapshot snapshot;
        auto statusResult = yams::cli::run_sync(client.status(), std::chrono::seconds(5));
        if (!statusResult) {
            return snapshot;
        }

        const auto& status = statusResult.value();
        auto getCount = [&](std::string_view key) -> uint64_t {
            auto it = status.requestCounts.find(std::string(key));
            return it != status.requestCounts.end() ? it->second : 0;
        };

        snapshot.documentsTotal = getCount(yams::daemon::metrics::kDocumentsTotal);
        snapshot.documentsIndexed = getCount(yams::daemon::metrics::kDocumentsIndexed);
        snapshot.workerQueued = getCount(yams::daemon::metrics::kWorkerQueued);
        snapshot.workerActive = getCount(yams::daemon::metrics::kWorkerActive);
        snapshot.postIngestQueued = getCount(yams::daemon::metrics::kPostIngestQueued);
        snapshot.postIngestInflight = getCount(yams::daemon::metrics::kPostIngestInflight);
        snapshot.postIngestProcessed = getCount(yams::daemon::metrics::kPostIngestProcessed);
        snapshot.postIngestFailed = getCount(yams::daemon::metrics::kPostIngestFailed);
        snapshot.topologyDirtyDocuments = getCount(yams::daemon::metrics::kTopologyDirtyDocuments);
        snapshot.topologyLastSuccessAgeMs =
            getCount(yams::daemon::metrics::kTopologyLastSuccessAgeMs);
        snapshot.topologyRebuildLagMs = getCount(yams::daemon::metrics::kTopologyRebuildLagMs);
        snapshot.topologyRebuildRunningAgeMs =
            getCount(yams::daemon::metrics::kTopologyRebuildRunningAgeMs);
        snapshot.topologyLastDurationMs = getCount(yams::daemon::metrics::kTopologyLastDurationMs);
        snapshot.topologyRebuildsTotal = getCount(yams::daemon::metrics::kTopologyRebuildsTotal);
        snapshot.topologyRebuildFailuresTotal =
            getCount(yams::daemon::metrics::kTopologyRebuildFailuresTotal);
        snapshot.topologyLastDocumentsRequested =
            getCount(yams::daemon::metrics::kTopologyLastDocumentsRequested);
        snapshot.topologyLastDocumentsProcessed =
            getCount(yams::daemon::metrics::kTopologyLastDocumentsProcessed);
        snapshot.topologyLastClustersBuilt =
            getCount(yams::daemon::metrics::kTopologyLastClustersBuilt);
        snapshot.topologyLastMembershipsBuilt =
            getCount(yams::daemon::metrics::kTopologyLastMembershipsBuilt);
        snapshot.topologyLastDirtySeedCount =
            getCount(yams::daemon::metrics::kTopologyLastDirtySeedCount);
        snapshot.topologyLastDirtyRegionDocs =
            getCount(yams::daemon::metrics::kTopologyLastDirtyRegionDocs);
        snapshot.topologyLastCoalescedDirtySets =
            getCount(yams::daemon::metrics::kTopologyLastCoalescedDirtySets);
        snapshot.topologyLastFallbackFullRebuilds =
            getCount(yams::daemon::metrics::kTopologyLastFallbackFullRebuilds);
        if (auto readyIt = status.readinessStates.find(
                std::string(yams::daemon::readiness::kTopologyArtifactsFresh));
            readyIt != status.readinessStates.end()) {
            snapshot.topologyArtifactsFresh = readyIt->second;
        }
        if (auto readyIt = status.readinessStates.find(
                std::string(yams::daemon::readiness::kTopologyRebuildRunning));
            readyIt != status.readinessStates.end()) {
            snapshot.topologyRebuildRunning = readyIt->second;
        }
        return snapshot;
    }
};

struct IdleWaitResult {
    bool completed{false};
    StatusSnapshot lastSnapshot{};
};

IdleWaitResult waitForPipelineIdle(yams::daemon::DaemonClient& client, std::chrono::seconds timeout,
                                   bool requireTopologyFresh) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    StatusSnapshot previous;
    StatusSnapshot last;
    bool havePrevious = false;
    int stableCount = 0;
    constexpr int kStableRequired = 5;

    while (std::chrono::steady_clock::now() < deadline) {
        StatusSnapshot current = StatusSnapshot::capture(client);
        last = current;
        bool stable = current.documentsTotal > 0 && current.pipelineIdle(requireTopologyFresh);
        if (stable && havePrevious) {
            stable = current.documentsTotal == previous.documentsTotal &&
                     current.documentsIndexed == previous.documentsIndexed &&
                     current.postIngestProcessed == previous.postIngestProcessed &&
                     current.postIngestFailed == previous.postIngestFailed &&
                     current.topologyRebuildsTotal == previous.topologyRebuildsTotal &&
                     current.topologyDirtyDocuments == previous.topologyDirtyDocuments;
        }

        if (stable) {
            ++stableCount;
            if (stableCount >= kStableRequired) {
                return IdleWaitResult{.completed = true, .lastSnapshot = current};
            }
        } else {
            stableCount = 0;
        }

        previous = current;
        havePrevious = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    return IdleWaitResult{.completed = false, .lastSnapshot = last};
}

bool useMockEmbeddingsForBench() {
    if (const char* env = std::getenv("YAMS_BENCH_FORCE_MOCK_EMBEDDINGS")) {
        return std::string(env) == "1";
    }
    return false;
}

std::optional<fs::path> writeMockEmbeddingConfig(const fs::path& root) {
    std::error_code ec;
    fs::create_directories(root, ec);
    const fs::path cfgPath = root / "mock_embeddings.toml";
    std::ofstream out(cfgPath);
    if (!out) {
        return std::nullopt;
    }
    out << "[embeddings]\n";
    out << "embedding_dim = 384\n";
    out << "preferred_model = \"all-MiniLM-L6-v2\"\n";
    return cfgPath;
}

void ensureBenchmarkEmbeddingsReady(yams::daemon::YamsDaemon* daemon, bool enableEmbeddings,
                                    bool useMockEmbeddings) {
    if (!enableEmbeddings || !daemon) {
        return;
    }

    auto serviceManager = daemon->getServiceManager();
    if (!serviceManager) {
        return;
    }

    if (useMockEmbeddings) {
        std::size_t dim = 384;
        if (auto vectorDb = serviceManager->getVectorDatabase()) {
            dim = vectorDb->getConfig().embedding_dim;
        }
        auto sharedProvider =
            std::make_shared<BenchmarkMockModelProvider>(std::max<std::size_t>(dim, 1));
        serviceManager->__test_setModelProvider(sharedProvider);
    }

    auto ready =
        serviceManager->ensureEmbeddingModelReadySync("all-MiniLM-L6-v2", {}, 10000, false, false);
    if (!ready) {
        spdlog::warn("[bench] Embedding model not ready for throughput run: {}",
                     ready.error().message);
    }
}

struct RunResult {
    int exitCode{0};
    double durationSeconds{0.0};
    double throughputFilesPerSecond{0.0};
    std::string command;
    fs::path dataDir;
    StatusSnapshot finalSnapshot{};
    bool drained{false};
    bool timedOut{false};
    bool topologyValidated{false};
    std::uint64_t addDocumentsAdded{0};
    std::string addMessage;
};

RunResult executeRun(const BenchConfig& cfg, const RunConfig& run, size_t datasetCount,
                     int iteration) {
    (void)datasetCount;

    fs::path runRoot = yams::test::make_temp_dir("yams_ingest_bench_") /
                       (run.label + "_" + std::to_string(iteration));
    fs::path runDataDir = runRoot / "data";
    fs::create_directories(runDataDir);

    std::string workerStr = std::to_string(std::max(1, run.workers));
    ScopedEnv envWorkers("YAMS_INDEXING_WORKERS", workerStr);
    ScopedEnv envLegacy("YAMS_BENCH_INDEX_WORKERS", workerStr);
    ScopedEnv envParallel("YAMS_ENABLE_PARALLEL_INGEST", "1");
    ScopedEnv envPoolSize("YAMS_STORAGE_POOL_SIZE", "8");
    ScopedEnv envSafeSingle("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1");
    const bool useMockEmbeddings = useMockEmbeddingsForBench();
    std::optional<ScopedEnv> envDisableVectors;
    if (!run.enableEmbeddings) {
        envDisableVectors.emplace("YAMS_DISABLE_VECTORS", "1");
    }
    ScopedEnv envDisableWatcher("YAMS_DISABLE_SESSION_WATCHER", "1");
    std::optional<ScopedEnv> envSkipModelLoading;
    if (!run.enableEmbeddings) {
        envSkipModelLoading.emplace("YAMS_SKIP_MODEL_LOADING", "1");
    }
    std::optional<ScopedEnv> envUseMockProvider;
    if (run.enableEmbeddings && useMockEmbeddings) {
        envUseMockProvider.emplace("YAMS_USE_MOCK_PROVIDER", "1");
    }
    std::optional<ScopedEnv> envMockEmbedDim;
    if (run.enableEmbeddings && useMockEmbeddings) {
        envMockEmbedDim.emplace("YAMS_EMBED_DIM", "384");
    }
    std::optional<fs::path> benchConfigPath;
    if (run.enableEmbeddings && useMockEmbeddings) {
        benchConfigPath = writeMockEmbeddingConfig(runRoot);
    }

    yams::daemon::TuneAdvisor::setEnableParallelIngest(true);
    yams::daemon::TuneAdvisor::setMaxIngestWorkers(static_cast<uint32_t>(std::max(1, run.workers)));
    yams::daemon::TuneAdvisor::setStoragePoolSize(8);

    const auto opts = parseArgs(run.args);

    yams::test::DaemonHarness::Options harnessOptions;
    harnessOptions.enableModelProvider = true;
    harnessOptions.useMockModelProvider = !run.enableEmbeddings || useMockEmbeddings;
    harnessOptions.autoLoadPlugins = run.enableEmbeddings && !useMockEmbeddings;
    harnessOptions.configureModelPool = run.enableEmbeddings && !useMockEmbeddings;
    harnessOptions.modelPoolLazyLoading = false;
    if (run.enableEmbeddings && !useMockEmbeddings) {
        harnessOptions.preloadModels = {"all-MiniLM-L6-v2"};
        if (const char* envPluginDir = std::getenv("YAMS_PLUGIN_DIR")) {
            harnessOptions.pluginDir = fs::path(envPluginDir);
        } else {
            harnessOptions.pluginDir = fs::current_path() / "builddir" / "plugins";
        }
    }
    harnessOptions.enableAutoRepair = false;
    harnessOptions.isolateState = true;
    harnessOptions.dataDir = runDataDir;
    if (benchConfigPath.has_value()) {
        harnessOptions.configPath = *benchConfigPath;
    }

    yams::test::DaemonHarness harness(harnessOptions);
    if (!harness.start(std::chrono::seconds(30), [](yams::daemon::YamsDaemon*) {})) {
        throw std::runtime_error("Failed to start daemon harness for ingestion benchmark");
    }
    ensureBenchmarkEmbeddingsReady(harness.daemon(), run.enableEmbeddings, useMockEmbeddings);

    yams::daemon::ClientConfig clientCfg;
    clientCfg.socketPath = harness.socketPath();
    clientCfg.autoStart = false;
    yams::daemon::DaemonClient client(clientCfg);

    DocumentIngestionService ingestion;
    AddOptions addOptions;
    addOptions.socketPath = harness.socketPath();
    addOptions.explicitDataDir = harness.dataDir();
    addOptions.path = cfg.datasetPath.string();
    addOptions.recursive = opts.recursive;
    addOptions.includePatterns = opts.includePatterns;
    addOptions.excludePatterns = opts.excludePatterns;
    addOptions.tags = opts.tags;
    addOptions.metadata = opts.metadata;
    addOptions.collection = opts.collection;
    addOptions.verify = opts.verify;
    addOptions.noEmbeddings = !run.enableEmbeddings;
    addOptions.timeoutMs = 30000;
    addOptions.retries = 2;

    int drainTimeoutSecs = 120;
    if (const char* envDrain = std::getenv("YAMS_BENCH_DRAIN_TIMEOUT_S"); envDrain && *envDrain) {
        try {
            drainTimeoutSecs = std::max(10, std::stoi(envDrain));
        } catch (...) {
        }
    }

    const auto start = std::chrono::steady_clock::now();
    auto addResult = ingestion.addViaDaemon(addOptions);
    if (!addResult) {
        throw std::runtime_error("Daemon ingestion failed: " + addResult.error().message);
    }
    auto waitResult =
        waitForPipelineIdle(client, std::chrono::seconds(drainTimeoutSecs), run.enableEmbeddings);
    const auto end = std::chrono::steady_clock::now();

    harness.stop();

    const double elapsed = std::chrono::duration<double>(end - start).count();
    const uint64_t documentsTotal = waitResult.lastSnapshot.documentsTotal;
    const double throughput =
        (documentsTotal > 0) ? static_cast<double>(documentsTotal) / elapsed : 0.0;

    std::ostringstream cmd;
    cmd << "daemon_ingest_full_pipeline label=" << run.label << " workers=" << run.workers
        << " embeddings=" << (run.enableEmbeddings ? 1 : 0);
    if (!opts.includePatterns.empty()) {
        cmd << " include=";
        for (size_t i = 0; i < opts.includePatterns.size(); ++i) {
            if (i)
                cmd << ',';
            cmd << opts.includePatterns[i];
        }
    }

    RunResult result;
    result.exitCode = 0;
    result.durationSeconds = elapsed;
    result.throughputFilesPerSecond = throughput;
    result.command = cmd.str();
    result.dataDir = runRoot;
    result.finalSnapshot = waitResult.lastSnapshot;
    result.drained = waitResult.completed;
    result.timedOut = !waitResult.completed;
    result.topologyValidated = run.enableEmbeddings;
    result.addDocumentsAdded = addResult.value().documentsAdded;
    result.addMessage = addResult.value().message;
    return result;
}

void appendMetrics(const fs::path& metricsPath, const RunConfig& run, const RunResult& result,
                   size_t datasetCount, int iteration) {
    std::ofstream out(metricsPath, std::ios::app);
    if (!out) {
        throw std::runtime_error("Failed to open metrics output: " + metricsPath.string());
    }

    const auto indexed = static_cast<std::uint64_t>(result.finalSnapshot.documentsTotal);
    const auto processed = static_cast<std::uint64_t>(datasetCount);
    const auto skipped =
        datasetCount > indexed ? static_cast<std::uint64_t>(datasetCount - indexed) : 0;
    const auto failed = static_cast<std::uint64_t>(0);

    const double throughput = result.throughputFilesPerSecond;

    json record{
        {"timestamp", isoTimestamp()},
        {"label", run.label},
        {"iteration", iteration},
        {"workers", run.workers},
        {"exit_code", result.exitCode},
        {"duration_seconds", result.durationSeconds},
        {"dataset_files", datasetCount},
        {"files_indexed", indexed},
        {"files_processed", processed},
        {"files_skipped", skipped},
        {"files_failed", failed},
        {"throughput_files_per_second", throughput},
        {"timed_out", result.timedOut},
        {"add_documents_added", result.addDocumentsAdded},
        {"add_message", result.addMessage},
        {"documents_total", result.finalSnapshot.documentsTotal},
        {"documents_indexed", result.finalSnapshot.documentsIndexed},
        {"worker_queued", result.finalSnapshot.workerQueued},
        {"worker_active", result.finalSnapshot.workerActive},
        {"post_ingest_queued", result.finalSnapshot.postIngestQueued},
        {"post_ingest_inflight", result.finalSnapshot.postIngestInflight},
        {"post_ingest_processed", result.finalSnapshot.postIngestProcessed},
        {"post_ingest_failed", result.finalSnapshot.postIngestFailed},
        {"topology_validated", result.topologyValidated},
        {"topology_artifacts_fresh", result.finalSnapshot.topologyArtifactsFresh},
        {"topology_rebuild_running", result.finalSnapshot.topologyRebuildRunning},
        {"topology_dirty_documents", result.finalSnapshot.topologyDirtyDocuments},
        {"topology_last_success_age_ms", result.finalSnapshot.topologyLastSuccessAgeMs},
        {"topology_rebuild_lag_ms", result.finalSnapshot.topologyRebuildLagMs},
        {"topology_rebuild_running_age_ms", result.finalSnapshot.topologyRebuildRunningAgeMs},
        {"topology_last_duration_ms", result.finalSnapshot.topologyLastDurationMs},
        {"topology_rebuilds_total", result.finalSnapshot.topologyRebuildsTotal},
        {"topology_rebuild_failures_total", result.finalSnapshot.topologyRebuildFailuresTotal},
        {"topology_last_documents_requested", result.finalSnapshot.topologyLastDocumentsRequested},
        {"topology_last_documents_processed", result.finalSnapshot.topologyLastDocumentsProcessed},
        {"topology_last_clusters_built", result.finalSnapshot.topologyLastClustersBuilt},
        {"topology_last_memberships_built", result.finalSnapshot.topologyLastMembershipsBuilt},
        {"topology_last_dirty_seed_count", result.finalSnapshot.topologyLastDirtySeedCount},
        {"topology_last_dirty_region_docs", result.finalSnapshot.topologyLastDirtyRegionDocs},
        {"topology_last_coalesced_dirty_sets", result.finalSnapshot.topologyLastCoalescedDirtySets},
        {"topology_last_fallback_full_rebuilds",
         result.finalSnapshot.topologyLastFallbackFullRebuilds},
        {"drained", result.drained},
        {"command", result.command},
        {"data_dir", result.dataDir.string()},
    };

    out << record.dump() << '\n';
}

} // namespace

std::unordered_map<std::string, double> loadBaselineThroughput(const fs::path& metricsPath) {
    std::unordered_map<std::string, std::pair<double, int>> accum;
    if (!fs::exists(metricsPath))
        return {};

    std::ifstream in(metricsPath);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty())
            continue;
        json record = json::parse(line, nullptr, false);
        if (!record.is_object())
            continue;
        if (!record.contains("label") || !record.contains("throughput_files_per_second"))
            continue;
        double throughput = record.value("throughput_files_per_second", 0.0);
        if (throughput <= 0.0)
            continue;
        std::string label = record.value("label", std::string{});
        if (label.empty())
            continue;
        auto& slot = accum[label];
        slot.first += throughput;
        slot.second += 1;
    }

    std::unordered_map<std::string, double> averages;
    for (auto& [label, data] : accum) {
        if (data.second > 0)
            averages[label] = data.first / static_cast<double>(data.second);
    }
    return averages;
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::warn);
    fs::path configPath;
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if ((arg == "--config" || arg == "-c") && i + 1 < argc) {
            configPath = fs::path(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " --config <path>\n";
            return 0;
        }
    }

    if (configPath.empty()) {
        std::cerr << "error: --config <path> is required\n";
        return 2;
    }

    try {
        BenchConfig cfg = loadConfig(configPath);
        const size_t datasetCount = countFiles(cfg.datasetPath);
        std::cout << "[bench] Dataset: " << cfg.datasetPath << " files=" << datasetCount
                  << std::endl;

        ensureMetricsPath(cfg.metricsPath);
        auto baseline = loadBaselineThroughput(cfg.metricsPath);
        std::unordered_map<std::string, std::vector<double>> newThroughputs;

        for (const auto& run : cfg.runs) {
            for (int iteration = 0; iteration < run.repeat; ++iteration) {
                RunResult result = executeRun(cfg, run, datasetCount, iteration);
                appendMetrics(cfg.metricsPath, run, result, datasetCount, iteration);
                newThroughputs[run.label].push_back(result.throughputFilesPerSecond);
                if (cfg.postRunCleanup) {
                    deleteTree(result.dataDir);
                }
                if (result.exitCode != 0) {
                    return result.exitCode;
                }
            }
        }

        bool regressionDetected = false;
        for (const auto& [label, samples] : newThroughputs) {
            if (samples.empty())
                continue;
            double newAvg = std::accumulate(samples.begin(), samples.end(), 0.0) /
                            static_cast<double>(samples.size());
            auto it = baseline.find(label);
            if (it != baseline.end() && it->second > 0.0) {
                double threshold = it->second * 0.9;
                if (newAvg < threshold) {
                    std::cerr << "[bench] throughput regression detected for '" << label
                              << "': baseline=" << it->second << " new=" << newAvg
                              << " (threshold=" << threshold << ")" << std::endl;
                    regressionDetected = true;
                }
            }
        }
        if (regressionDetected)
            return 3;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
