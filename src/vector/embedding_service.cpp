#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <span>
#include <thread>
#include <tuple>
#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif
#ifndef _WIN32
#include <unistd.h>
#endif
#ifndef _WIN32
#include <pthread.h> // For setting thread priority
#endif
#if defined(__APPLE__)
#include <pthread/qos.h>
#elif defined(__linux__)
#include <sched.h>
#endif
#include <nlohmann/json.hpp>
#include <cerrno>
#include <fstream>
#ifndef _WIN32
#include <sys/resource.h>
#endif
#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/integrity/repair_utils.h>
#include <yams/metadata/query_helpers.h>
#include <yams/vector/dim_resolver.h>
#include <yams/vector/dynamic_batcher.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>

namespace yams::vector {

namespace fs = std::filesystem;

namespace {
// Simple RAII file lock helper stamped with PID/time
struct FileLock {
    int fd{-1};
    fs::path path;
#ifdef _WIN32
    HANDLE hFile = INVALID_HANDLE_VALUE;
#endif

    explicit FileLock(const fs::path& p) : path(p) {
#ifdef _WIN32
        // Windows implementation
        _sopen_s(&fd, path.string().c_str(), _O_CREAT | _O_RDWR | _O_BINARY, _SH_DENYNO,
                 _S_IREAD | _S_IWRITE);
        if (fd >= 0) {
            hFile = (HANDLE)_get_osfhandle(fd);
            if (hFile != INVALID_HANDLE_VALUE) {
                OVERLAPPED overlapped = {0};
                // LockFileEx is equivalent to fcntl locking
                if (!LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0,
                                MAXDWORD, MAXDWORD, &overlapped)) {
                    _close(fd);
                    fd = -1;
                    hFile = INVALID_HANDLE_VALUE;
                } else {
                    // Best-effort: stamp PID and time
                    try {
                        _chsize_s(fd, 0);
                        std::string stamp =
                            std::to_string(static_cast<long long>(_getpid())) + " " +
                            std::to_string(static_cast<long long>(::time(nullptr))) + "\n";
                        _write(fd, stamp.data(), static_cast<unsigned int>(stamp.size()));
                        _lseek(fd, 0, SEEK_SET);
                    } catch (...) {
                    }
                }
            } else {
                _close(fd);
                fd = -1;
            }
        }
#else
        // POSIX implementation
        fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd >= 0) {
            struct flock fl{};
            fl.l_type = F_WRLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = 0; // whole file
            if (fcntl(fd, F_SETLK, &fl) == -1) {
                ::close(fd);
                fd = -1;
            } else {
                // Best-effort: stamp PID and time for diagnostics
                try {
                    (void)ftruncate(fd, 0);
                    std::string stamp = std::to_string(static_cast<long long>(::getpid())) + " " +
                                        std::to_string(static_cast<long long>(::time(nullptr))) +
                                        "\n";
                    (void)::write(fd, stamp.data(), stamp.size());
                    (void)lseek(fd, 0, SEEK_SET);
                } catch (...) {
                }
            }
        }
#endif
    }

    bool locked() const { return fd >= 0; }

    ~FileLock() {
        if (fd >= 0) {
#ifdef _WIN32
            if (hFile != INVALID_HANDLE_VALUE) {
                OVERLAPPED overlapped = {0};
                UnlockFileEx(hFile, 0, MAXDWORD, MAXDWORD, &overlapped);
            }
            _close(fd);
#else
            struct flock fl{};
            fl.l_type = F_UNLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = 0;
            (void)fcntl(fd, F_SETLK, &fl);
            ::close(fd);
#endif
        }
    }
};
} // anonymous namespace

std::unique_ptr<EmbeddingService> EmbeddingService::create(cli::YamsCLI* cli) {
    (void)cli; // unused (interface parity; implementation handled elsewhere)
    // Forward declaration limitation - implementation moved to YamsCLI
    // This method is now implemented in yams_cli.cpp to avoid circular dependencies
    return nullptr;
}

EmbeddingService::EmbeddingService(std::shared_ptr<api::IContentStore> store,
                                   std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                                   std::filesystem::path dataPath)
    : store_(std::move(store)), metadataRepo_(std::move(metadataRepo)),
      dataPath_(std::move(dataPath)) {}

bool EmbeddingService::isAvailable() const {
    return !getAvailableModels().empty();
}

std::vector<std::string> EmbeddingService::getAvailableModels() const {
    std::vector<std::string> availableModels;

    fs::path modelsPath = dataPath_ / "models";
    if (!fs::exists(modelsPath)) {
        return availableModels;
    }

    for (const auto& entry : fs::directory_iterator(modelsPath)) {
        if (entry.is_directory()) {
            fs::path modelFile = entry.path() / "model.onnx";
            if (fs::exists(modelFile)) {
                availableModels.push_back(entry.path().filename().string());
            }
        }
    }

    return availableModels;
}

Result<void> EmbeddingService::generateEmbeddingForDocument(const std::string& documentHash) {
    return generateEmbeddingsForDocuments({documentHash});
}

bool EmbeddingService::startRepairAsync() {
    std::lock_guard<std::mutex> lock(workerMutex_);
    if (repairRunning_) {
        return false;
    }
    repairRunning_ = true;

    // Launch stop-aware background worker with std::jthread
    repairThread_ = yams::compat::jthread([this](yams::compat::stop_token stoken) {
#ifndef _WIN32
#if defined(__APPLE__)
        // Lower thread priority on macOS using QoS background class
        if (pthread_set_qos_class_self_np(QOS_CLASS_BACKGROUND, 0) != 0) {
            spdlog::warn("Failed to set repair thread QoS to background");
        }
#else
#if defined(SCHED_IDLE)
        bool priorityAdjusted = false;
        if (::geteuid() == 0) {
            // Lower thread priority on Linux using SCHED_IDLE when permitted
            struct sched_param sp{};
            sp.sched_priority = 0;
            if (int rc = pthread_setschedparam(pthread_self(), SCHED_IDLE, &sp); rc != 0) {
                spdlog::warn("Failed to set repair thread to idle priority (SCHED_IDLE), rc={} "
                             "(falling back to setpriority)",
                             rc);
            } else {
                priorityAdjusted = true;
            }
        }
        if (!priorityAdjusted) {
            // Fallback: lower niceness when SCHED_IDLE is unavailable/forbidden
            if (setpriority(PRIO_PROCESS, 0, 19) != 0) {
                spdlog::warn("Failed to lower repair thread priority via setpriority (errno={})",
                             errno);
            }
        }
#else
        // Fallback: lower niceness when SCHED_IDLE is unavailable
        if (setpriority(PRIO_PROCESS, 0, 19) != 0) {
            spdlog::warn("Failed to lower repair thread priority via setpriority (errno={})",
                         errno);
        }
#endif
#endif
#endif
        // Ensure running flag is reset when the thread exits
        struct Reset {
            bool& flag;
            std::mutex& mtx;
            ~Reset() {
                std::lock_guard<std::mutex> l(mtx);
                flag = false;
            }
        } reset{repairRunning_, workerMutex_};

        this->runRepair(stoken);
    });

    return true;
}

void EmbeddingService::stopRepair() {
    yams::compat::jthread local;
    {
        std::lock_guard<std::mutex> lock(workerMutex_);
        if (!repairThread_.joinable()) {
            return;
        }
        local = std::move(repairThread_);
    }
    // Request cooperative stop; jthread joins on destruction
    stopRequested_.store(true);
    local.request_stop();
    if (local.joinable()) {
        local.join();
    }
}

bool EmbeddingService::isRepairRunning() const {
    std::lock_guard<std::mutex> lock(workerMutex_);
    return repairRunning_;
}

void EmbeddingService::triggerRepairIfNeeded() {
    // Check if we have models available
    if (!isAvailable()) {
        spdlog::debug("No embedding models available, skipping repair");
        return;
    }

    // Quick check if there are missing embeddings (don't do full scan here)
    try {
        // Do not create vectors.db from health probe; let the daemon manage creation
        if (!std::filesystem::exists(dataPath_ / "vectors.db")) {
            spdlog::debug("triggerRepairIfNeeded: vectors.db not found; skipping probe");
            return;
        }
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath_ / "vectors.db").string();
        // Choose a model-aware default for dimension using centralized lookup
        auto models = getAvailableModels();
        std::string pick = models.empty() ? std::string() : models[0];
        if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL")) {
            std::string preferred(pref);
            for (const auto& m : models) {
                if (m == preferred) {
                    pick = m;
                    break;
                }
            }
        }
        // Use centralized dimension lookup - sentinel -> model name -> model config
        size_t modelAwareDim = 0;
        if (auto sentinelDim = dimres::read_dim_from_sentinel(dataPath_)) {
            modelAwareDim = *sentinelDim;
        } else if (!pick.empty()) {
            fs::path modelDir = dataPath_ / "models" / pick;
            if (auto cfgDim = dimres::dim_from_model_config(modelDir)) {
                modelAwareDim = *cfgDim;
            } else if (auto nameDim = dimres::dim_from_model_name(pick)) {
                modelAwareDim = *nameDim;
            }
        }
        if (modelAwareDim == 0) {
            spdlog::debug("triggerRepairIfNeeded: cannot determine dimension; skipping");
            return;
        }
        vdbConfig.embedding_dim = modelAwareDim;

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            spdlog::debug("Failed to initialize vector database for health check");
            return;
        }

        // Just check if we have any documents without embeddings (sample check)
        auto docsResult = metadata::queryDocumentsByPattern(*metadataRepo_, "%");
        if (!docsResult || docsResult.value().empty()) {
            spdlog::debug("No documents to process");
            return;
        }

        // Check a better sample of documents for missing embeddings
        size_t totalDocs = docsResult.value().size();
        size_t missingCount = 0;
        size_t checkedCount = 0;

        // For efficiency, check up to 50 documents spread across the collection
        size_t checkLimit = std::min(size_t(50), totalDocs);
        size_t step = std::max(size_t(1), totalDocs / checkLimit);

        for (size_t i = 0; i < totalDocs && checkedCount < checkLimit; i += step) {
            checkedCount++;
            if (!vectorDb->hasEmbedding(docsResult.value()[i].sha256Hash)) {
                missingCount++;
            }
        }

        // Extrapolate missing count if we only checked a sample
        if (checkedCount < totalDocs && missingCount > 0) {
            float missingRate = static_cast<float>(missingCount) / checkedCount;
            missingCount = static_cast<size_t>(missingRate * totalDocs);
        }

        if (missingCount == 0) {
            spdlog::debug("Health check found no missing embeddings (sampled {} of {} docs)",
                          checkedCount, totalDocs);
            return;
        }

        spdlog::info("Detected ~{} documents missing embeddings (sampled {} of {})", missingCount,
                     checkedCount, totalDocs);

        if (startRepairAsync()) {
            spdlog::info("Repair worker started");
        } else {
            spdlog::debug("Repair worker already running; skip.");
        }

    } catch (const std::exception& e) {
        spdlog::debug("Failed to check embedding health: {}", e.what());
    }
}

void EmbeddingService::runRepair(yams::compat::stop_token stopToken) {
    // Cross-process single-writer lock using a lockfile on vectors.db (declared file-scope)
    fs::path lockPath = dataPath_ / "vectors.db.lock";
    FileLock vlock(lockPath);

    spdlog::debug("Repair thread started");

    try {
        // Initialize vector DB (create on first use if missing)
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath_ / "vectors.db").string();
        // Determine embedding dimension using centralized lookup
        auto models = getAvailableModels();
        std::string pick = models.empty() ? std::string() : models[0];
        if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL")) {
            std::string preferred(pref);
            for (const auto& m : models) {
                if (m == preferred) {
                    pick = m;
                    break;
                }
            }
        }
        size_t heuristicDim = 0;
        if (!pick.empty()) {
            fs::path modelDir = dataPath_ / "models" / pick;
            if (auto cfgDim = dimres::dim_from_model_config(modelDir)) {
                heuristicDim = *cfgDim;
            } else if (auto nameDim = dimres::dim_from_model_name(pick)) {
                heuristicDim = *nameDim;
            }
        }
        // Prefer existing DB/sentinel dimension when present
        size_t repairDim = dimres::resolve_dim(dataPath_, heuristicDim, heuristicDim);
        try {
            vector::VectorDatabaseConfig probeCfg;
            probeCfg.database_path = (dataPath_ / "vectors.db").string();
            probeCfg.embedding_dim = repairDim;
            probeCfg.create_if_missing = false;
            auto probeDb = std::make_unique<vector::VectorDatabase>(probeCfg);
            if (probeDb->initialize()) {
                size_t existing = probeDb->getConfig().embedding_dim;
                if (existing > 0 && existing != repairDim) {
                    spdlog::warn("Repair thread: existing vector DB dim={} differs from target "
                                 "dim={} — aligning generator to {}",
                                 existing, repairDim, existing);
                    repairDim = existing;
                }
            }
        } catch (...) {
            // best-effort probe; continue with resolved dim
        }
        vdbConfig.embedding_dim = repairDim;
        // Align schema only when missing; avoid dropping existing tables during repair
        try {
            (void)yams::integrity::ensureVectorSchemaAligned(dataPath_, repairDim, false);
        } catch (...) {
            // best-effort; continue with DB init
        }
        vdbConfig.create_if_missing = true;

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            spdlog::error("Repair thread: Failed to initialize vector database");
            return;
        }

        if (stopToken.stop_requested() || stopRequested_.load()) {
            spdlog::debug("Repair thread: stop requested before scan");
            return;
        }

        auto docsResult = metadata::queryDocumentsByPattern(*metadataRepo_, "%");
        if (!docsResult) {
            spdlog::error("Repair thread: Failed to get documents");
            return;
        }

        std::vector<std::string> missingEmbeddings;
        missingEmbeddings.reserve(docsResult.value().size());
        for (const auto& doc : docsResult.value()) {
            if (stopToken.stop_requested() || stopRequested_.load()) {
                spdlog::debug("Repair thread: stop requested during scan");
                return;
            }
            if (!vectorDb->hasEmbedding(doc.sha256Hash)) {
                missingEmbeddings.push_back(doc.sha256Hash);
            }
        }

        if (missingEmbeddings.empty()) {
            spdlog::info("Repair thread: All embeddings already generated");
            return;
        }

        spdlog::info("Repair thread: Processing {} documents with missing embeddings",
                     missingEmbeddings.size());

        // Process in batches
        size_t batchSize = 32; // safe default
        if (const char* envBatch = std::getenv("YAMS_EMBED_BATCH")) {
            try {
                unsigned long v = std::stoul(std::string(envBatch));
                // Clamp to a reasonable range to avoid OOM or tiny batches
                if (v < 4UL)
                    v = 4UL;
                if (v > 128UL)
                    v = 128UL;
                batchSize = static_cast<size_t>(v);
            } catch (...) {
                spdlog::warn("Invalid YAMS_EMBED_BATCH='{}', using default {}", envBatch,
                             batchSize);
            }
        }
        spdlog::info("EmbeddingService: using batch size {}", batchSize);
        for (size_t i = 0; i < missingEmbeddings.size(); i += batchSize) {
            if (stopToken.stop_requested() || stopRequested_.load()) {
                spdlog::debug("Repair thread: stop requested during processing");
                break;
            }

            size_t end = std::min(i + batchSize, missingEmbeddings.size());
            std::vector<std::string> batch(missingEmbeddings.begin() + i,
                                           missingEmbeddings.begin() + end);

            auto result = generateEmbeddingsInternal(batch, false);
            if (!result) {
                spdlog::debug("Repair thread: Batch processing failed: {}", result.error().message);
            }

            // Small delay between batches to avoid hogging resources
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        spdlog::info("Repair thread: Completed processing");

    } catch (const std::exception& e) {
        spdlog::error("Repair thread exception: {}", e.what());
    }

    spdlog::debug("Repair thread stopped");
}

Result<void>
EmbeddingService::generateEmbeddingsForDocuments(const std::vector<std::string>& documentHashes) {
    return generateEmbeddingsInternal(documentHashes, false);
}

Result<void>
EmbeddingService::generateEmbeddingsInternal(const std::vector<std::string>& documentHashes,
                                             bool showProgress) {
    // Check for available models (extracted from repair command logic)
    auto availableModels = getAvailableModels();
    if (availableModels.empty()) {
        return Error{ErrorCode::NotFound, "No embedding models available"};
    }

    try {
        // 1. Configure embedding generation
        // Priority order:
        // - YAMS_PREFERRED_MODEL if present and available
        // - nomic-embed-text-v1 if available
        // - all-MiniLM-L6-v2 (default efficient)
        // - first available as last resort
        std::string selectedModel = availableModels[0];
        if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL")) {
            std::string preferred(pref);
            for (const auto& m : availableModels) {
                if (m == preferred) {
                    selectedModel = m;
                    break;
                }
            }
        } else {
            for (const auto& m : availableModels) {
                if (m == "nomic-embed-text-v1") {
                    selectedModel = m;
                    break;
                }
            }
            for (const auto& m : availableModels) {
                if (m == "all-MiniLM-L6-v2") {
                    selectedModel = m;
                    break;
                }
            }
        }

        fs::path modelsPath = dataPath_ / "models";

        // Get dimension from sentinel or probe existing DB
        size_t targetDbDim = 0;
        if (auto sentinelDim = dimres::read_dim_from_sentinel(dataPath_)) {
            targetDbDim = *sentinelDim;
        } else {
            try {
                vector::VectorDatabaseConfig probeCfg;
                probeCfg.database_path = (dataPath_ / "vectors.db").string();
                probeCfg.embedding_dim = 768; // provisional for probe only
                probeCfg.create_if_missing = false;
                auto probeDb = std::make_unique<vector::VectorDatabase>(probeCfg);
                if (probeDb->initialize()) {
                    targetDbDim = probeDb->getConfig().embedding_dim;
                }
            } catch (...) {
            }
        }

        // Get model dimension using centralized lookup
        auto modelDim = [&](const std::string& name) -> size_t {
            fs::path modelDir = modelsPath / name;
            if (auto cfgDim = dimres::dim_from_model_config(modelDir))
                return *cfgDim;
            if (auto nameDim = dimres::dim_from_model_name(name))
                return *nameDim;
            return 0; // Unknown
        };

        // If DB exists (targetDbDim>0), prefer a model that matches DB dim to avoid mismatches
        if (targetDbDim > 0) {
            auto hasModel = [&](const std::string& name) -> bool {
                std::error_code ec;
                return fs::exists(modelsPath / name / "model.onnx", ec);
            };
            size_t selDim = modelDim(selectedModel);
            if (selDim != targetDbDim && selDim > 0) {
                // Attempt to switch to a matching model if available
                bool switched = false;
                for (const auto& m : availableModels) {
                    if (modelDim(m) == targetDbDim && hasModel(m)) {
                        spdlog::warn(
                            "EmbeddingService: switching model '{}' -> '{}' to match DB dim {}",
                            selectedModel, m, targetDbDim);
                        selectedModel = m;
                        switched = true;
                        break;
                    }
                }
                if (!switched) {
                    spdlog::warn("EmbeddingService: selected model '{}' (dim={}) does not match DB "
                                 "dim {} and no matching model found; repair may fail",
                                 selectedModel, selDim, targetDbDim);
                }
            }
        }

        vector::EmbeddingConfig embConfig;
        embConfig.model_path = (modelsPath / selectedModel / "model.onnx").string();
        embConfig.model_name = selectedModel;

        // Set provisional dimensions using centralized lookup
        size_t provDim = modelDim(selectedModel);
        embConfig.embedding_dim = provDim > 0 ? provDim : targetDbDim;

        // Backend selection override via environment or special model name
        if (const char* be = std::getenv("YAMS_EMBED_BACKEND")) {
            std::string s(be);
            for (auto& c : s)
                c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
            if (s == "local")
                embConfig.backend = vector::EmbeddingConfig::Backend::Local;
            else if (s == "daemon")
                embConfig.backend = vector::EmbeddingConfig::Backend::Daemon;
            else
                embConfig.backend = vector::EmbeddingConfig::Backend::Hybrid;
        } else {
            // Heuristic: if selected model indicates plugin/daemon usage, prefer Daemon backend
            if (selectedModel == "onnx_plugin" || selectedModel == "daemon" ||
                selectedModel.find("plugin") != std::string::npos) {
                embConfig.backend = vector::EmbeddingConfig::Backend::Daemon;
            }
        }

        // 2. Initialize embedding generator
        auto embGenerator = std::make_unique<vector::EmbeddingGenerator>(embConfig);
        if (!embGenerator->initialize()) {
            return Error{ErrorCode::InternalError, "Failed to initialize embedding generator"};
        }

        // 3. Initialize vector database (using actual model-reported dim)
        vector::VectorDatabaseConfig vdbConfig;
        vdbConfig.database_path = (dataPath_ / "vectors.db").string();
        // Query generator for actual embedding dimension and max sequence length
        size_t actualDim = embGenerator->getEmbeddingDimension();
        size_t modelMaxSeq = embGenerator->getMaxSequenceLength();
        if (actualDim == 0)
            actualDim = embConfig.embedding_dim;
        if (modelMaxSeq == 0)
            modelMaxSeq = 512;
        vdbConfig.embedding_dim =
            yams::vector::dimres::resolve_dim(dataPath_, actualDim, actualDim);
        spdlog::info("EmbeddingService: model={} dim={} max_seq={} backend=Hybrid", selectedModel,
                     actualDim, modelMaxSeq);

        auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
        if (!vectorDb->initialize()) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to initialize vector database: " + vectorDb->getLastError()};
        }

        // Ensure existing DB schema matches the chosen dimension; do not drop tables here
        try {
            size_t cfgDim = 0;
            try {
                cfgDim = vectorDb->getConfig().embedding_dim;
            } catch (...) {
                cfgDim = actualDim;
            }
            if (cfgDim != vdbConfig.embedding_dim) {
                spdlog::warn("EmbeddingService: vector DB dim={} != target dim={}, attempting "
                             "non-destructive align",
                             cfgDim, vdbConfig.embedding_dim);
                (void)yams::integrity::ensureVectorSchemaAligned(dataPath_, vdbConfig.embedding_dim,
                                                                 false);
                // Recreate DB handle after alignment
                vector::VectorDatabaseConfig alignedCfg = vdbConfig;
                vectorDb = std::make_unique<vector::VectorDatabase>(alignedCfg);
                if (!vectorDb->initialize()) {
                    return Error{ErrorCode::DatabaseError,
                                 "Failed to reinitialize vector database after schema align: " +
                                     vectorDb->getLastError()};
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("EmbeddingService: schema alignment check failed: {}", e.what());
        }

        // Hard guard: if the runtime model/generator dimension still diverges from the DB schema,
        // abort early with a precise operator hint to avoid wasting cycles and failing at insert.
        try {
            size_t storedDim = vectorDb->getConfig().embedding_dim;
            if (storedDim > 0 && actualDim > 0 && storedDim != actualDim) {
                std::stringstream ss;
                ss << "Embedding dimension mismatch: DB expects " << storedDim
                   << ", but runtime model produces " << actualDim
                   << ". Install/select a model with dim=" << storedDim
                   << " (e.g., all-MiniLM-L6-v2 for 384) or recreate vectors.db at " << actualDim
                   << ".";
                return Error{ErrorCode::InvalidState, ss.str()};
            }
        } catch (...) {
        }

        // 4. Process documents using dynamic batching (use model-reported seq length)

        std::size_t advisoryDocCap = 0;
        if (const char* adv = std::getenv("YAMS_EMBED_BATCH_TARGET")) {
            try {
                advisoryDocCap = static_cast<std::size_t>(std::stoul(adv));
            } catch (...) {
            }
        }
        DynamicBatcherConfig bcfg;
        bcfg.maxSequenceLengthTokens = modelMaxSeq;
        // TuneAdvisor-provided safety factor (env can still override via TuneAdvisor setters)
        double safety = yams::daemon::TuneAdvisor::getEmbedSafety();
        bcfg.safetyFactor = safety;
        bcfg.advisoryDocCap = advisoryDocCap;
        // Optional advisory upper bound for docs per batch via env
        if (bcfg.advisoryDocCap == 0) {
            auto capTa = yams::daemon::TuneAdvisor::getEmbedDocCap();
            if (capTa > 0)
                bcfg.advisoryDocCap = capTa;
        }
        DynamicBatcher batcher{bcfg};

        size_t processed = 0;
        size_t skipped = 0;
        size_t failed = 0;

        // Allow configurable pause between batches to reduce sustained CPU pressure
        unsigned pause_ms = yams::daemon::TuneAdvisor::getEmbedPauseMs();

        for (size_t i = 0; i < documentHashes.size();) {
            std::vector<std::string> texts;
            std::vector<std::string> hashes;

            // Pack a batch by token budget using cheap estimator on content bytes
            auto getter = [&](std::size_t idx) {
                if (idx >= documentHashes.size())
                    return std::tuple<bool, std::string, std::string>{false, {}, {}};
                const auto& docHash = documentHashes[idx];
                if (vectorDb->hasEmbedding(docHash)) {
                    skipped++;
                    return std::tuple<bool, std::string, std::string>{false, {}, {}};
                }
                auto content = store_->retrieveBytes(docHash);
                if (!content) {
                    failed++;
                    return std::tuple<bool, std::string, std::string>{false, {}, {}};
                }
                std::string text(reinterpret_cast<const char*>(content.value().data()),
                                 content.value().size());
                return std::tuple<bool, std::string, std::string>{true, std::move(text), docHash};
            };
            std::size_t selected =
                batcher.packByTokens(i, documentHashes.size(), texts, hashes, getter);
            if (selected == 0) {
                ++i;
                continue;
            }
            i += selected; // advance window over input set

            if (!texts.empty()) {
                // Generate embeddings with adaptive split on failure
                std::function<std::vector<std::vector<float>>(const std::vector<std::string>&)>
                    generate_with_adapt;
                generate_with_adapt =
                    [&](const std::vector<std::string>& in) -> std::vector<std::vector<float>> {
                    auto out = embGenerator->generateEmbeddings(in);
                    if (!out.empty() || in.size() <= 1)
                        return out;
                    // Split and try halves to mitigate transient/OOM issues
                    size_t mid = in.size() / 2;
                    auto left =
                        generate_with_adapt(std::vector<std::string>(in.begin(), in.begin() + mid));
                    auto right =
                        generate_with_adapt(std::vector<std::string>(in.begin() + mid, in.end()));
                    if (left.empty() && right.empty())
                        return {};
                    std::vector<std::vector<float>> merged;
                    merged.reserve(left.size() + right.size());
                    merged.insert(merged.end(), left.begin(), left.end());
                    merged.insert(merged.end(), right.begin(), right.end());
                    return merged;
                };

                auto embeddings = generate_with_adapt(texts);
                if (embeddings.empty()) {
                    failed += texts.size();
                    batcher.onFailure();
                    continue;
                }

                // Store embeddings in vector database using batch insert for fewer transactions
                std::vector<vector::VectorRecord> records;
                records.reserve(std::min(embeddings.size(), hashes.size()));
                for (size_t k = 0; k < embeddings.size() && k < hashes.size(); ++k) {
                    // Get document metadata for record
                    auto docResult = metadataRepo_->getDocumentByHash(hashes[k]);

                    vector::VectorRecord record;
                    record.document_hash = hashes[k];
                    record.chunk_id = vector::utils::generateChunkId(hashes[k], 0);
                    record.embedding = embeddings[k];
                    record.content = texts[k].substr(0, 1000); // Store snippet

                    // Add metadata if document exists
                    if (docResult && docResult.value()) {
                        const auto& doc = docResult.value().value();
                        record.metadata["name"] = doc.fileName;
                        record.metadata["mime_type"] = doc.mimeType;
                        if (!doc.fileExtension.empty()) {
                            std::string ext = doc.fileExtension;
                            std::transform(
                                ext.begin(), ext.end(), ext.begin(),
                                [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                            if (!ext.empty() && ext.front() == '.')
                                ext.erase(ext.begin());
                            if (!ext.empty())
                                record.metadata["extension"] = std::move(ext);
                        }
                        auto centroidRes = metadataRepo_->upsertPathTreeForDocument(
                            doc, doc.id, false,
                            std::span<const float>(record.embedding.data(),
                                                   record.embedding.size()));
                        if (!centroidRes) {
                            spdlog::warn("EmbeddingService: failed to update path tree centroid "
                                         "for {}: {}",
                                         doc.filePath, centroidRes.error().message);
                        }
                    }
                    // Track model used for embedding
                    record.metadata["model"] = selectedModel;
                    records.push_back(std::move(record));
                }

                if (!records.empty()) {
                    // Resolve lock file path for batch-serialized writes
                    fs::path lockPath = dataPath_ / "vectors.db.lock";
                    // Acquire short-lived DB lock per batch with bounded wait/backoff
                    uint64_t timeout_ms = 10 * 60 * 1000ULL; // default 10 minutes
                    if (const char* env_ms = std::getenv("YAMS_REPAIR_LOCK_TIMEOUT_MS")) {
                        try {
                            timeout_ms = std::stoull(std::string(env_ms));
                        } catch (...) {
                        }
                    }
                    auto deadline =
                        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
                    uint64_t sleep_ms = 50;
                    bool inserted = false;
                    while (!inserted) {
                        FileLock batchLock(lockPath);
                        if (batchLock.locked()) {
                            // Insert while exclusively holding the lock, then release
                            if (vectorDb->insertVectorsBatch(records)) {
                                inserted = true;
                            } else {
                                // Failure while locked; give up on this batch
                                break;
                            }
                        } else {
                            spdlog::debug("Repair thread: vector DB busy; waiting for lock...");
                            if (std::chrono::steady_clock::now() >= deadline) {
                                spdlog::warn(
                                    "Repair thread: DB lock timeout; skipping batch of {} records",
                                    records.size());
                                break;
                            }
                            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                            sleep_ms = std::min<uint64_t>(sleep_ms * 2, 2000);
                        }
                    }

                    if (inserted) {
                        processed += records.size();
                        batcher.onSuccess(records.size());
                        // Visibility for operators: show DB path and inserted count
                        try {
                            const auto& cfg = vectorDb->getConfig();
                            spdlog::warn("[vectordb] persisted {} vectors to {} (EmbeddingService)",
                                         records.size(), cfg.database_path);
                        } catch (...) {
                            spdlog::warn("[vectordb] persisted {} vectors (EmbeddingService)",
                                         records.size());
                        }
                    } else {
                        failed += records.size();
                        batcher.onFailure();
                        try {
                            spdlog::error("[vectordb] batch insert failed: {}",
                                          vectorDb->getLastError());
                        } catch (...) {
                        }
                    }
                    // Optional pause between dynamic batches (cooperative throttling)
                    if (pause_ms > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
                    }
                }
            }
        }

        if (showProgress && processed > 0) {
            spdlog::info("Embedding generation completed: {} processed, {} skipped, {} failed",
                         processed, skipped, failed);
        }

        return Result<void>();

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Embedding generation failed: ") + e.what()};
    }
}

} // namespace yams::vector
