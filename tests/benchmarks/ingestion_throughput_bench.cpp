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

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <numeric>
#include <unordered_map>

#include <yams/api/content_store_builder.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using yams::app::services::AddDirectoryRequest;
using yams::app::services::AddDirectoryResponse;

namespace {

struct RunConfig {
    std::string label;
    int workers{1};
    int repeat{1};
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
    bool deferExtraction{true};
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
        } else if (arg == "--defer-extraction") {
            opts.deferExtraction = true;
        } else if (arg == "--no-defer-extraction") {
            opts.deferExtraction = false;
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

class ScopedEnv {
public:
    ScopedEnv(const std::string& key, std::string value) : key_(key) {
        const char* existing = std::getenv(key.c_str());
        if (existing)
            previous_ = std::string(existing);
        ::setenv(key.c_str(), value.c_str(), 1);
    }

    ~ScopedEnv() {
        if (previous_) {
            ::setenv(key_.c_str(), previous_->c_str(), 1);
        } else {
            ::unsetenv(key_.c_str());
        }
    }

private:
    std::string key_;
    std::optional<std::string> previous_{};
};

struct LocalIngestionSession {
    fs::path root;
    std::unique_ptr<yams::metadata::Database> database;
    std::unique_ptr<yams::metadata::ConnectionPool> pool;
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo;
    std::shared_ptr<yams::api::IContentStore> contentStore;
    yams::app::services::AppContext context;

    static LocalIngestionSession create(const fs::path& sessionRoot) {
        LocalIngestionSession session;
        session.root = sessionRoot;
        fs::create_directories(sessionRoot);
        fs::create_directories(sessionRoot / "storage");

        auto db = std::make_unique<yams::metadata::Database>();
        auto dbPath = (sessionRoot / "yams.db").string();
        auto openResult = db->open(dbPath, yams::metadata::ConnectionMode::Create);
        if (!openResult) {
            throw std::runtime_error("Failed to open DB: " + openResult.error().message);
        }

        yams::metadata::MigrationManager migrator(*db);
        auto initResult = migrator.initialize();
        if (!initResult) {
            throw std::runtime_error("Failed to initialize migrations: " +
                                     initResult.error().message);
        }
        migrator.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = migrator.migrate();
        if (!migrateResult) {
            throw std::runtime_error("Failed to run migrations: " + migrateResult.error().message);
        }

        yams::metadata::ConnectionPoolConfig poolCfg;
        poolCfg.minConnections = 1;
        poolCfg.maxConnections = 8;
        yams::daemon::TuneAdvisor::setEnableParallelIngest(true);
        yams::daemon::TuneAdvisor::setMaxIngestWorkers(poolCfg.maxConnections);
        yams::daemon::TuneAdvisor::setStoragePoolSize(poolCfg.maxConnections);
        auto pool = std::make_unique<yams::metadata::ConnectionPool>(dbPath, poolCfg);
        auto repo = std::make_shared<yams::metadata::MetadataRepository>(*pool);

        yams::api::ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(sessionRoot / "storage")
                               .withChunkSize(64 * 1024)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        if (!storeResult) {
            throw std::runtime_error("Failed to create content store: " +
                                     storeResult.error().message);
        }
        auto uniqueStore = std::move(storeResult.value());
        auto storeShared = std::shared_ptr<yams::api::IContentStore>(uniqueStore.release());

        session.database = std::move(db);
        session.pool = std::move(pool);
        session.metadataRepo = std::move(repo);
        session.contentStore = std::move(storeShared);

        session.context.store = session.contentStore;
        session.context.metadataRepo = session.metadataRepo;
        session.context.searchExecutor = nullptr;
        session.context.hybridEngine = nullptr;

        return session;
    }

    AddDirectoryResponse ingestDirectory(const fs::path& dataset, const IngestOptions& opts) {
        auto indexing = yams::app::services::makeIndexingService(context);
        if (!indexing) {
            throw std::runtime_error("Failed to create IndexingService (missing dependencies)");
        }

        AddDirectoryRequest req;
        req.directoryPath = dataset.string();
        req.collection = opts.collection;
        req.includePatterns = opts.includePatterns;
        req.excludePatterns = opts.excludePatterns;
        req.recursive = opts.recursive;
        req.verify = opts.verify;
        req.deferExtraction = opts.deferExtraction;
        req.tags = opts.tags;
        for (const auto& [key, value] : opts.metadata) {
            req.metadata[key] = value;
        }

        auto result = indexing->addDirectory(req);
        if (!result) {
            throw std::runtime_error("Ingestion failed: " + result.error().message);
        }
        return result.value();
    }
};

struct RunResult {
    int exitCode{0};
    double durationSeconds{0.0};
    double throughputFilesPerSecond{0.0};
    std::string command;
    fs::path dataDir;
    AddDirectoryResponse response{};
};

RunResult executeRun(const BenchConfig& cfg, const RunConfig& run, size_t datasetCount,
                     int iteration) {
    (void)datasetCount;

    fs::path baseDataDir = fs::temp_directory_path() / "yams_ingest_bench";
    fs::path runDataDir = baseDataDir / (run.label + "_" + std::to_string(iteration));
    fs::create_directories(runDataDir);

    std::string workerStr = std::to_string(std::max(1, run.workers));
    ScopedEnv envWorkers("YAMS_INDEXING_WORKERS", workerStr);
    ScopedEnv envLegacy("YAMS_BENCH_INDEX_WORKERS", workerStr);
    ScopedEnv envParallel("YAMS_ENABLE_PARALLEL_INGEST", "1");
    ScopedEnv envPoolSize("YAMS_STORAGE_POOL_SIZE", "8");

    LocalIngestionSession session = LocalIngestionSession::create(runDataDir);
    const auto opts = parseArgs(run.args);

    const auto start = std::chrono::steady_clock::now();
    AddDirectoryResponse response = session.ingestDirectory(cfg.datasetPath, opts);
    const auto end = std::chrono::steady_clock::now();

    const double elapsed = std::chrono::duration<double>(end - start).count();
    const double throughput =
        (response.filesIndexed > 0) ? static_cast<double>(response.filesIndexed) / elapsed : 0.0;

    std::ostringstream cmd;
    cmd << "local_ingest label=" << run.label << " workers=" << run.workers;
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
    result.dataDir = runDataDir;
    result.response = std::move(response);
    return result;
}

void appendMetrics(const fs::path& metricsPath, const RunConfig& run, const RunResult& result,
                   size_t datasetCount, int iteration) {
    std::ofstream out(metricsPath, std::ios::app);
    if (!out) {
        throw std::runtime_error("Failed to open metrics output: " + metricsPath.string());
    }

    const auto indexed = static_cast<std::uint64_t>(result.response.filesIndexed);
    const auto processed = static_cast<std::uint64_t>(result.response.filesProcessed);
    const auto skipped = static_cast<std::uint64_t>(result.response.filesSkipped);
    const auto failed = static_cast<std::uint64_t>(result.response.filesFailed);

    const double throughput = result.throughputFilesPerSecond;

    json record{
        {"timestamp", isoTimestamp()},   {"label", run.label},
        {"iteration", iteration},        {"workers", run.workers},
        {"exit_code", result.exitCode},  {"duration_seconds", result.durationSeconds},
        {"dataset_files", datasetCount}, {"files_indexed", indexed},
        {"files_processed", processed},  {"files_skipped", skipped},
        {"files_failed", failed},        {"throughput_files_per_second", throughput},
        {"command", result.command},     {"data_dir", result.dataDir.string()},
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
