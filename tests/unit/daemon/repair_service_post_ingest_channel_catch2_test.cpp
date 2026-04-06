#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <cstdlib>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/resource/model_provider.h>

using namespace yams;
using namespace yams::daemon;

namespace {

template <typename T> void drainQueue(const std::shared_ptr<SpscQueue<T>>& q) {
    if (!q)
        return;
    T tmp{};
    while (q->try_pop(tmp)) {
    }
}

struct ServiceManagerFixture {
    DaemonConfig config_;
    StateComponent state_;
    DaemonLifecycleFsm lifecycleFsm_;
    std::filesystem::path testDir_;

    ServiceManagerFixture() {
        namespace fs = std::filesystem;
        testDir_ = fs::temp_directory_path() /
                   ("repair_service_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        config_.dataDir = testDir_ / "data";
        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";
        fs::create_directories(config_.dataDir);
    }

    ~ServiceManagerFixture() {
        namespace fs = std::filesystem;
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }
};

std::optional<RepairOperationResult> findOperationResult(const RepairResponse& response,
                                                         std::string_view operation) {
    for (const auto& result : response.operationResults) {
        if (result.operation == operation) {
            return result;
        }
    }
    return std::nullopt;
}

bool waitForCondition(std::chrono::milliseconds timeout, const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return predicate();
}

class SelectiveFailingModelProvider : public IModelProvider {
public:
    SelectiveFailingModelProvider(size_t dim, std::string poisonNeedle)
        : dim_(dim), poisonNeedle_(std::move(poisonNeedle)) {}

    void setProgressCallback(std::function<void(const ModelLoadEvent&)> cb) override {
        progress_ = std::move(cb);
    }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        return generateEmbeddingFor(defaultModelName_, text);
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        return generateBatchEmbeddingsFor(defaultModelName_, texts);
    }

    Result<std::vector<float>> generateEmbeddingFor(const std::string& modelName,
                                                    const std::string& text) override {
        ++singleCalls_;
        if (!isModelLoaded(modelName)) {
            return Error{ErrorCode::NotFound, "model not loaded"};
        }
        if (text.find(poisonNeedle_) != std::string::npos) {
            return Error{ErrorCode::InternalError, "poison single"};
        }
        return std::vector<float>(dim_, 0.5f);
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string& modelName,
                               const std::vector<std::string>& texts) override {
        ++batchCalls_;
        if (!isModelLoaded(modelName)) {
            return Error{ErrorCode::NotFound, "model not loaded"};
        }
        for (const auto& text : texts) {
            if (text.find(poisonNeedle_) != std::string::npos) {
                return Error{ErrorCode::InternalError, "poison batch"};
            }
        }
        return std::vector<std::vector<float>>(texts.size(), std::vector<float>(dim_, 0.25f));
    }

    Result<void> loadModel(const std::string& modelName) override {
        ++loadCalls_;
        if (progress_) {
            ModelLoadEvent ev;
            ev.modelName = modelName;
            ev.phase = "loading";
            ev.message = "mock loading";
            progress_(ev);
        }
        if (std::find(loadedModels_.begin(), loadedModels_.end(), modelName) ==
            loadedModels_.end()) {
            loadedModels_.push_back(modelName);
        }
        defaultModelName_ = modelName;
        if (progress_) {
            ModelLoadEvent ev;
            ev.modelName = modelName;
            ev.phase = "completed";
            ev.message = "mock ready";
            progress_(ev);
        }
        return Result<void>();
    }

    Result<void> unloadModel(const std::string& modelName) override {
        auto it = std::find(loadedModels_.begin(), loadedModels_.end(), modelName);
        if (it == loadedModels_.end()) {
            return ErrorCode::NotFound;
        }
        loadedModels_.erase(it);
        if (defaultModelName_ == modelName) {
            defaultModelName_.clear();
        }
        return Result<void>();
    }

    bool isModelLoaded(const std::string& modelName) const override {
        return std::find(loadedModels_.begin(), loadedModels_.end(), modelName) !=
               loadedModels_.end();
    }

    std::vector<std::string> getLoadedModels() const override { return loadedModels_; }
    size_t getLoadedModelCount() const override { return loadedModels_.size(); }

    Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
        if (!isModelLoaded(modelName)) {
            return Error{ErrorCode::NotFound, "model not loaded"};
        }
        ModelInfo info;
        info.name = modelName;
        info.embeddingDim = dim_;
        return info;
    }

    size_t getEmbeddingDim(const std::string&) const override { return dim_; }

    std::shared_ptr<vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string& = "") override {
        return nullptr;
    }

    std::string getProviderName() const override { return "SelectiveFailingModelProvider"; }
    std::string getProviderVersion() const override { return "vtest"; }
    bool isAvailable() const override { return true; }
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}

    std::size_t batchCalls() const { return batchCalls_; }
    std::size_t loadCalls() const { return loadCalls_; }
    std::size_t singleCalls() const { return singleCalls_; }

private:
    size_t dim_;
    std::string poisonNeedle_;
    std::string defaultModelName_;
    std::function<void(const ModelLoadEvent&)> progress_;
    std::vector<std::string> loadedModels_;
    std::size_t loadCalls_{0};
    std::size_t batchCalls_{0};
    std::size_t singleCalls_{0};
};

} // namespace

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: stuck-doc recovery enqueues to PostIngestQueue channel",
                 "[daemon][repair][stuck_docs][bus]") {
    // Keep this test isolated from other bus tests (singleton channels).
    auto postIngest =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", 64);
    auto postIngestRpc =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest_rpc", 64);
    auto postIngestTasks =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest_tasks", 64);
    drainQueue(postIngest);
    drainQueue(postIngestRpc);
    drainQueue(postIngestTasks);

    // Ensure optional subsystems don't trigger heavy init in this unit test.
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    // Wait for async initialization to settle before reading optional subsystem pointers.
    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    // Async init can race this test; wait briefly for repo availability.
    auto meta = sm->getMetadataRepo();
    for (int i = 0; i < 100 && meta == nullptr; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        meta = sm->getMetadataRepo();
    }
    REQUIRE(meta != nullptr);

    // PostIngestQueue is started asynchronously and can consume tasks quickly.
    // Pause it to make channel assertions deterministic.
    auto piq = sm->getPostIngestQueue();
    for (int i = 0; i < 200 && piq == nullptr; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        piq = sm->getPostIngestQueue();
    }
    REQUIRE(piq != nullptr);
    for (int i = 0; i < 200 && !piq->started(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(piq->started());
    piq->pauseAll();

    // Clear any background noise after pause.
    drainQueue(postIngest);
    drainQueue(postIngestRpc);
    drainQueue(postIngestTasks);

    metadata::DocumentInfo doc{};
    doc.fileName = "stuck.txt";
    doc.filePath = (config_.dataDir / "stuck.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = 1;
    doc.sha256Hash = std::string(64, 'a');
    doc.mimeType = "text/plain";
    doc.setCreatedTime(1);
    doc.setModifiedTime(1);
    doc.setIndexedTime(1);

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    // Mark as failed so it is always detected as "stuck".
    auto st = meta->updateDocumentExtractionStatus(docId, false, metadata::ExtractionStatus::Failed,
                                                   "test stuck");
    REQUIRE(st.has_value());

    auto rs =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(rs.has_value());

    RepairService::Config cfg;
    cfg.enable = false; // executeRepair() is synchronous; don't start the background loop here
    cfg.maxRetries = 3;

    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    RepairRequest req;
    req.repairStuckDocs = true;
    req.dryRun = false;
    req.maxRetries = 3;

    (void)repair.executeRepair(req, nullptr);

    // Correct behavior: tasks land on the high-priority channel PostIngestQueue consumes first
    // ("post_ingest_rpc").
    REQUIRE(postIngestRpc->size_approx() == 1);
    REQUIRE(postIngest->size_approx() == 0);
    REQUIRE(postIngestTasks->size_approx() == 0);

    drainQueue(postIngest);
    drainQueue(postIngestRpc);
    drainQueue(postIngestTasks);

    piq->resumeAll();

    sm->shutdown();
}

TEST_CASE_METHOD(
    ServiceManagerFixture,
    "RepairService: semantic dedupe removes duplicate members and marks groups applied",
    "[daemon][repair][dedupe]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    for (int i = 0; i < 100 && meta == nullptr; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        meta = sm->getMetadataRepo();
    }
    REQUIRE(meta != nullptr);

    metadata::DocumentInfo canonical{};
    canonical.fileName = "canonical.txt";
    canonical.filePath = (config_.dataDir / "canonical.txt").string();
    canonical.fileExtension = ".txt";
    canonical.fileSize = 1;
    canonical.sha256Hash = std::string(64, 'c');
    canonical.mimeType = "text/plain";
    canonical.setCreatedTime(1);
    canonical.setModifiedTime(1);
    canonical.setIndexedTime(1);

    metadata::DocumentInfo duplicate = canonical;
    duplicate.fileName = "duplicate.txt";
    duplicate.filePath = (config_.dataDir / "duplicate.txt").string();
    duplicate.sha256Hash = std::string(64, 'd');

    auto canonicalId = meta->insertDocument(canonical);
    auto duplicateId = meta->insertDocument(duplicate);
    REQUIRE(canonicalId.has_value());
    REQUIRE(duplicateId.has_value());

    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    metadata::SemanticDuplicateGroup group;
    group.groupKey = "semantic:repair-test";
    group.algorithmVersion = "semantic-dedupe-v1";
    group.status = "suggested";
    group.reviewState = "pending";
    group.canonicalDocumentId = canonicalId.value();
    group.memberCount = 2;
    group.maxPairScore = 0.97;
    group.threshold = 0.92;
    group.evidenceJson = R"({"source":"repair-test"})";
    group.createdAt = now;
    group.updatedAt = now;
    group.lastComputedAt = now;
    auto groupId = meta->upsertSemanticDuplicateGroup(group);
    REQUIRE(groupId.has_value());

    metadata::SemanticDuplicateGroupMember canonicalMember;
    canonicalMember.documentId = canonicalId.value();
    canonicalMember.role = "canonical";
    canonicalMember.decision = "keep";
    canonicalMember.createdAt = now;
    canonicalMember.updatedAt = now;

    metadata::SemanticDuplicateGroupMember duplicateMember;
    duplicateMember.documentId = duplicateId.value();
    duplicateMember.role = "duplicate";
    duplicateMember.decision = "unknown";
    duplicateMember.createdAt = now;
    duplicateMember.updatedAt = now;
    REQUIRE(meta->replaceSemanticDuplicateGroupMembers(groupId.value(),
                                                       {canonicalMember, duplicateMember})
                .has_value());

    RepairService::Config cfg;
    cfg.enable = false;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);

    RepairRequest req;
    req.repairDedupe = true;
    req.dryRun = false;

    auto resp = repair.executeRepair(req, nullptr);
    auto dedupeResult = findOperationResult(resp, "dedupe");
    REQUIRE(dedupeResult.has_value());
    CHECK(dedupeResult->succeeded >= 1);

    auto duplicateDoc = meta->getDocument(duplicateId.value());
    REQUIRE(duplicateDoc.has_value());
    CHECK_FALSE(duplicateDoc.value().has_value());

    auto updatedGroup = meta->getSemanticDuplicateGroupByKey("semantic:repair-test");
    REQUIRE(updatedGroup.has_value());
    REQUIRE(updatedGroup.value().has_value());
    CHECK(updatedGroup.value()->status == "applied");

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture, "RepairService: stop waits for in-flight executeRepair",
                 "[daemon][repair][shutdown][regression]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    RepairService::Config cfg;
    cfg.enable = true;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    repair.start();

    std::mutex progressMutex;
    std::condition_variable progressCv;
    bool progressEntered = false;
    bool allowProgress = false;
    std::atomic<bool> stopReturned{false};
    std::atomic<bool> repairCompleted{false};

    RepairRequest req;
    req.repairOrphans = true;
    req.dryRun = true;
    req.verbose = true;

    std::thread repairThread([&] {
        auto resp = repair.executeRepair(req, [&](const RepairEvent& event) {
            if (event.phase == "repairing" && event.operation == "orphans") {
                std::unique_lock<std::mutex> lk(progressMutex);
                progressEntered = true;
                progressCv.notify_all();
                progressCv.wait(lk, [&] { return allowProgress; });
            }
        });
        CHECK(resp.totalOperations == 1);
        CHECK(resp.operationResults.size() == 1);
        CHECK(resp.operationResults.front().operation == "orphans");
        repairCompleted.store(true, std::memory_order_release);
    });

    {
        std::unique_lock<std::mutex> lk(progressMutex);
        REQUIRE(progressCv.wait_for(lk, std::chrono::seconds(5), [&] { return progressEntered; }));
    }

    std::thread stopThread([&] {
        repair.stop();
        stopReturned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK_FALSE(stopReturned.load(std::memory_order_acquire));
    CHECK_FALSE(repairCompleted.load(std::memory_order_acquire));

    {
        std::lock_guard<std::mutex> lk(progressMutex);
        allowProgress = true;
    }
    progressCv.notify_all();

    repairThread.join();
    stopThread.join();

    CHECK(stopReturned.load(std::memory_order_acquire));
    CHECK(repairCompleted.load(std::memory_order_acquire));

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: post-ingest success should mark repair status completed",
                 "[daemon][repair][regression][post-ingest]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    auto store = sm->getContentStore();
    REQUIRE(meta != nullptr);
    REQUIRE(store != nullptr);

    auto piq = sm->getPostIngestQueue();
    for (int i = 0; i < 200 && piq == nullptr; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        piq = sm->getPostIngestQueue();
    }
    REQUIRE(piq != nullptr);
    for (int i = 0; i < 200 && !piq->started(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(piq->started());
    piq->resumeAll();

    auto postIngestRpc =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest_rpc", 64);
    REQUIRE(postIngestRpc != nullptr);
    drainQueue(postIngestRpc);

    const std::string text = "repair service regression test content";
    const auto textBytes = std::as_bytes(std::span<const char>(text.data(), text.size()));
    auto storeRes = store->storeBytes(textBytes);
    REQUIRE(storeRes.has_value());
    const auto hash = storeRes.value().contentHash;

    metadata::DocumentInfo doc{};
    doc.fileName = "regression.txt";
    doc.filePath = (config_.dataDir / "regression.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = text.size();
    doc.sha256Hash = hash;
    doc.mimeType = "text/plain";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    auto statusRes =
        meta->batchUpdateDocumentRepairStatuses({hash}, metadata::RepairStatus::Processing);
    REQUIRE(statusRes.has_value());

    InternalEventBus::PostIngestTask task;
    task.hash = hash;
    task.mime = "text/plain";
    REQUIRE(postIngestRpc->try_push(task));

    const bool completed = waitForCondition(std::chrono::seconds(5), [&]() {
        auto docRes = meta->getDocument(docId);
        if (!docRes || !docRes.value().has_value()) {
            return false;
        }
        const auto& current = docRes.value().value();
        return current.repairStatus == metadata::RepairStatus::Completed;
    });
    REQUIRE(completed);

    auto finalDocRes = meta->getDocument(docId);
    REQUIRE(finalDocRes.has_value());
    REQUIRE(finalDocRes.value().has_value());
    CHECK(finalDocRes.value()->repairStatus == metadata::RepairStatus::Completed);

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: foreground embedding repair uses unified queued path",
                 "[daemon][repair][regression][foreground-embeddings]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::nullopt);
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB", std::nullopt);
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::nullopt);
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    config_.enableAutoRepair = false;
    config_.autoLoadPlugins = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    auto vectorDb = sm->getVectorDatabase();
    REQUIRE(meta != nullptr);
    REQUIRE(vectorDb != nullptr);

    const std::string kModelName = "test-model";
    auto provider = std::make_shared<SelectiveFailingModelProvider>(
        vectorDb->getConfig().embedding_dim, "never-poison");
    sm->__test_setModelProvider(provider);

    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    std::vector<std::string> hashes;
    for (int i = 0; i < 2; ++i) {
        const std::string hash = std::string(63, static_cast<char>('a' + i)) + std::to_string(i);
        const std::string text = "foreground repair content " + std::to_string(i);

        metadata::DocumentInfo doc{};
        doc.fileName = "doc-" + std::to_string(i) + ".txt";
        doc.filePath = (config_.dataDir / doc.fileName).string();
        doc.fileExtension = "txt";
        doc.fileSize = static_cast<int64_t>(text.size());
        doc.sha256Hash = hash;
        doc.mimeType = "text/plain";
        doc.modifiedTime = now;
        doc.indexedTime = now;

        auto idRes = meta->insertDocument(doc);
        REQUIRE(idRes.has_value());

        metadata::DocumentContent content{};
        content.documentId = idRes.value();
        content.contentText = text;
        content.contentLength = static_cast<int64_t>(text.size());
        content.extractionMethod = "test";
        content.language = "en";
        REQUIRE(meta->insertContent(content).has_value());
        REQUIRE(meta->updateDocumentExtractionStatus(idRes.value(), true,
                                                     metadata::ExtractionStatus::Success)
                    .has_value());
        hashes.push_back(hash);
    }

    yams::vector::VectorRecord existing;
    existing.document_hash = hashes.front();
    existing.chunk_id = "existing-doc-vector";
    existing.embedding.assign(vectorDb->getConfig().embedding_dim, 0.2f);
    existing.content = "already embedded";
    existing.level = yams::vector::EmbeddingLevel::DOCUMENT;
    REQUIRE(vectorDb->insertVector(existing));
    REQUIRE(
        meta->updateDocumentEmbeddingStatusByHash(hashes.front(), true, kModelName).has_value());
    REQUIRE(meta->updateDocumentRepairStatus(hashes.front(), metadata::RepairStatus::Completed)
                .has_value());

    RepairService::Config cfg;
    cfg.enable = false;
    cfg.dataDir = config_.dataDir;
    cfg.maxBatch = 1;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);

    std::vector<RepairEvent> events;
    RepairRequest req;
    req.repairEmbeddings = true;
    req.foreground = true;
    req.embeddingModel = kModelName;

    auto response = repair.executeRepair(req, [&](const RepairEvent& ev) {
        if (ev.operation == "embeddings") {
            events.push_back(ev);
        }
    });

    REQUIRE(response.success);
    const auto op = findOperationResult(response, "embeddings");
    REQUIRE(op.has_value());
    CHECK(op->failed == 0);
    CHECK(op->processed == 1);
    CHECK(op->succeeded == 1);
    CHECK(provider->loadCalls() == 1);
    CHECK(provider->batchCalls() >= 2);
    CHECK_FALSE(events.empty());
    CHECK(std::any_of(events.begin(), events.end(), [](const RepairEvent& ev) {
        return ev.phase == "repairing" && ev.total > 0;
    }));
    CHECK(std::any_of(events.begin(), events.end(), [](const RepairEvent& ev) {
        return ev.phase == "repairing" &&
               ev.message.find("Loading embedding model") != std::string::npos;
    }));
    CHECK(std::any_of(events.begin(), events.end(), [](const RepairEvent& ev) {
        return ev.phase == "repairing" &&
               ev.message.find("Warming embedding model") != std::string::npos;
    }));
    CHECK(std::any_of(events.begin(), events.end(),
                      [](const RepairEvent& ev) { return ev.phase == "completed"; }));

    for (const auto& hash : hashes) {
        auto hasEmbedRes = meta->hasDocumentEmbeddingByHash(hash);
        REQUIRE(hasEmbedRes.has_value());
        CHECK(hasEmbedRes.value());
        CHECK_FALSE(vectorDb->getVectorsByDocument(hash).empty());
    }

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "EmbeddingService: empty content settles to skipped instead of processing",
                 "[daemon][repair][regression][embedding-service]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::nullopt);
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB", std::nullopt);
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::nullopt);
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    config_.enableAutoRepair = false;
    config_.autoLoadPlugins = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    auto vectorDb = sm->getVectorDatabase();
    REQUIRE(meta != nullptr);
    REQUIRE(vectorDb != nullptr);

    const std::string kModelName = "test-model";
    auto provider = std::make_shared<SelectiveFailingModelProvider>(
        vectorDb->getConfig().embedding_dim, "never-poison");
    REQUIRE(provider->loadModel(kModelName).has_value());
    sm->__test_setModelProvider(provider);

    const std::string hash = std::string(64, 'e');
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

    metadata::DocumentInfo doc{};
    doc.fileName = "empty.txt";
    doc.filePath = (config_.dataDir / doc.fileName).string();
    doc.fileExtension = "txt";
    doc.fileSize = 0;
    doc.sha256Hash = hash;
    doc.mimeType = "text/plain";
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());

    metadata::DocumentContent content{};
    content.documentId = idRes.value();
    content.contentText = "";
    content.contentLength = 0;
    content.extractionMethod = "test";
    content.language = "en";
    REQUIRE(meta->insertContent(content).has_value());
    REQUIRE(meta->updateDocumentExtractionStatus(idRes.value(), true,
                                                 metadata::ExtractionStatus::Success)
                .has_value());
    REQUIRE(meta->updateDocumentRepairStatus(hash, metadata::RepairStatus::Processing).has_value());

    auto embedChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>("embed_jobs",
                                                                                       128);
    REQUIRE(embedChannel != nullptr);
    InternalEventBus::EmbedJob drained;
    while (embedChannel->try_pop(drained)) {
    }

    InternalEventBus::EmbedJob job;
    job.hashes = {hash};
    job.batchSize = 1;
    job.skipExisting = false;
    job.modelName = kModelName;
    REQUIRE(embedChannel->try_push(std::move(job)));

    const bool settled = waitForCondition(std::chrono::seconds(10), [&]() {
        auto docRes = meta->getDocumentByHash(hash);
        return docRes && docRes.value().has_value() &&
               docRes.value()->repairStatus == metadata::RepairStatus::Skipped;
    });
    REQUIRE(settled);

    auto hasEmbedRes = meta->hasDocumentEmbeddingByHash(hash);
    REQUIRE(hasEmbedRes.has_value());
    CHECK_FALSE(hasEmbedRes.value());
    CHECK(vectorDb->getVectorsByDocument(hash).empty());

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "EmbeddingService: single bad document should not abort the rest of the job",
                 "[daemon][repair][regression][embedding-service]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::nullopt);
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB", std::nullopt);
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::nullopt);
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    config_.enableAutoRepair = false;
    config_.autoLoadPlugins = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    auto vectorDb = sm->getVectorDatabase();
    REQUIRE(meta != nullptr);
    REQUIRE(vectorDb != nullptr);

    const std::string kModelName = "test-model";
    auto provider = std::make_shared<SelectiveFailingModelProvider>(
        vectorDb->getConfig().embedding_dim, "poison-pill");
    REQUIRE(provider->loadModel(kModelName).has_value());
    sm->__test_setModelProvider(provider);

    auto embedChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>("embed_jobs",
                                                                                       128);
    REQUIRE(embedChannel != nullptr);
    InternalEventBus::EmbedJob drained;
    while (embedChannel->try_pop(drained)) {
    }

    struct SeedDoc {
        std::string hash;
        std::string text;
        metadata::RepairStatus expectedStatus;
        bool expectEmbedding;
    };

    std::vector<SeedDoc> docs = {
        {std::string(64, '1'), "good alpha text", metadata::RepairStatus::Completed, true},
        {std::string(64, '2'), "poison-pill content", metadata::RepairStatus::Failed, false},
        {std::string(64, '3'), "good beta text", metadata::RepairStatus::Completed, true},
    };

    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    std::vector<std::string> hashes;
    hashes.reserve(docs.size());
    for (std::size_t i = 0; i < docs.size(); ++i) {
        metadata::DocumentInfo doc{};
        doc.fileName = "doc-" + std::to_string(i) + ".txt";
        doc.filePath = (config_.dataDir / doc.fileName).string();
        doc.fileExtension = "txt";
        doc.fileSize = static_cast<int64_t>(docs[i].text.size());
        doc.sha256Hash = docs[i].hash;
        doc.mimeType = "text/plain";
        doc.modifiedTime = now;
        doc.indexedTime = now;

        auto idRes = meta->insertDocument(doc);
        REQUIRE(idRes.has_value());

        metadata::DocumentContent content{};
        content.documentId = idRes.value();
        content.contentText = docs[i].text;
        content.contentLength = static_cast<int64_t>(docs[i].text.size());
        content.extractionMethod = "test";
        content.language = "en";
        REQUIRE(meta->insertContent(content).has_value());
        REQUIRE(meta->updateDocumentExtractionStatus(idRes.value(), true,
                                                     metadata::ExtractionStatus::Success)
                    .has_value());
        REQUIRE(meta->updateDocumentRepairStatus(docs[i].hash, metadata::RepairStatus::Processing)
                    .has_value());
        hashes.push_back(docs[i].hash);
    }

    InternalEventBus::EmbedJob job;
    job.hashes = hashes;
    job.batchSize = static_cast<uint32_t>(hashes.size());
    job.skipExisting = false;
    job.modelName = kModelName;
    REQUIRE(embedChannel->try_push(std::move(job)));

    const bool settled = waitForCondition(std::chrono::seconds(10), [&]() {
        return provider->batchCalls() >= 4 && provider->singleCalls() >= 1;
    });
    REQUIRE(settled);

    for (const auto& seeded : docs) {
        auto docRes = meta->getDocumentByHash(seeded.hash);
        REQUIRE(docRes.has_value());
        REQUIRE(docRes.value().has_value());

        if (seeded.expectEmbedding) {
            const auto status = docRes.value()->repairStatus;
            CHECK((status == metadata::RepairStatus::Completed ||
                   status == metadata::RepairStatus::Processing));
        } else {
            CHECK(docRes.value()->repairStatus == metadata::RepairStatus::Failed);
        }

        const auto vectors = vectorDb->getVectorsByDocument(seeded.hash);
        if (seeded.expectEmbedding) {
            CHECK_FALSE(vectors.empty());
        } else {
            CHECK(vectors.empty());
        }
    }

    CHECK(provider->batchCalls() >= 2);
    CHECK(provider->singleCalls() >= 1);

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: startup scan should ignore extraction skipped docs",
                 "[daemon][repair][regression][startup-scan]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    REQUIRE(meta != nullptr);

    metadata::DocumentInfo doc{};
    doc.fileName = "binary.jpg";
    doc.filePath = (config_.dataDir / "binary.jpg").string();
    doc.fileExtension = "jpg";
    doc.fileSize = 123;
    doc.sha256Hash = std::string(64, 'b');
    doc.mimeType = "image/jpeg";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    auto extractionRes = meta->updateDocumentExtractionStatus(
        docId, false, metadata::ExtractionStatus::Skipped, "test skipped");
    REQUIRE(extractionRes.has_value());
    auto repairRes =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(repairRes.has_value());

    state_.stats.repairTotalBacklog.store(0, std::memory_order_relaxed);

    RepairService::Config cfg;
    cfg.enable = true;
    cfg.maxBatch = 16;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    repair.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    repair.stop();

    CHECK(state_.stats.repairTotalBacklog.load(std::memory_order_relaxed) == 0u);

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: stalled pending detection should use indexed age",
                 "[daemon][repair][regression][stuck-docs]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    REQUIRE(meta != nullptr);

    metadata::DocumentInfo doc{};
    doc.fileName = "pending.txt";
    doc.filePath = (config_.dataDir / "pending.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = 17;
    doc.sha256Hash = std::string(64, 'c');
    doc.mimeType = "text/plain";
    doc.setModifiedTime(1); // very old file mtime
    const auto recent =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.indexedTime = recent; // recently indexed, should not be stalled

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    auto extractionRes = meta->updateDocumentExtractionStatus(
        docId, false, metadata::ExtractionStatus::Pending, "pending test");
    REQUIRE(extractionRes.has_value());
    auto repairRes =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(repairRes.has_value());

    RepairService::Config cfg;
    cfg.enable = false;
    cfg.stalledThreshold = std::chrono::seconds(60);
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);

    RepairRequest req;
    req.repairStuckDocs = true;
    req.dryRun = true;
    req.maxRetries = 3;

    auto resp = repair.executeRepair(req, nullptr);
    auto stuckDocsResult = findOperationResult(resp, "stuck_docs");
    REQUIRE(stuckDocsResult.has_value());
    CHECK(stuckDocsResult->processed == 0u);

    sm->shutdown();
}

// ---------------------------------------------------------------------------
// FTS5 blind-spot regression tests
// ---------------------------------------------------------------------------
// These tests verify that documents marked as successfully extracted but
// missing their FTS5 index entries are correctly detected and repaired.
// Before the fix, all three code paths (rebuildFts5Index, spawnInitialScan,
// detectMissingWork) would silently skip these documents.

TEST_CASE_METHOD(
    ServiceManagerFixture,
    "RepairService: rebuildFts5Index catches extraction-success docs with missing FTS5",
    "[daemon][repair][fts5][regression]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    auto store = sm->getContentStore();
    REQUIRE(meta != nullptr);
    REQUIRE(store != nullptr);

    // Store real content so the extractor can find it.
    const std::string text = "hello world searchable content for fts5 repair test";
    const auto textBytes = std::as_bytes(std::span<const char>(text.data(), text.size()));
    auto storeRes = store->storeBytes(textBytes);
    REQUIRE(storeRes.has_value());
    const auto hash = storeRes.value().contentHash;

    // Insert document marked as successfully extracted.
    metadata::DocumentInfo doc{};
    doc.fileName = "fts5_missing.txt";
    doc.filePath = (config_.dataDir / "fts5_missing.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = static_cast<int64_t>(text.size());
    doc.sha256Hash = hash;
    doc.mimeType = "text/plain";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    // Mark extraction as successful at the DB level.
    auto statusRes =
        meta->updateDocumentExtractionStatus(docId, true, metadata::ExtractionStatus::Success);
    REQUIRE(statusRes.has_value());

    // Insert a content row so the ghost-success loop doesn't reset the status.
    metadata::DocumentContent content;
    content.documentId = docId;
    content.contentText = text;
    content.contentLength = static_cast<int64_t>(text.size());
    content.extractionMethod = "test";
    content.language = "en";
    auto contentRes = meta->insertContent(content);
    REQUIRE(contentRes.has_value());

    // Crucially: do NOT call indexDocumentContent(), so no FTS5 entry exists.
    auto hasFts = meta->hasFtsEntry(docId);
    REQUIRE(hasFts.has_value());
    REQUIRE_FALSE(hasFts.value());

    // Run repair with fts5 only.
    RepairService::Config cfg;
    cfg.enable = false;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    RepairRequest req;
    req.repairFts5 = true;
    req.dryRun = false;

    auto resp = repair.executeRepair(req, nullptr);

    // The document should have been rebuilt, not skipped.
    auto ftsResult = findOperationResult(resp, "fts5");
    REQUIRE(ftsResult.has_value());
    CHECK(ftsResult->succeeded >= 1u);

    // Verify an FTS5 entry now exists.
    auto hasFtsAfter = meta->hasFtsEntry(docId);
    REQUIRE(hasFtsAfter.has_value());
    CHECK(hasFtsAfter.value());

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: startup scan detects extraction-success docs with missing FTS5",
                 "[daemon][repair][fts5][startup-scan][regression]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    REQUIRE(meta != nullptr);

    // Insert document marked as successfully extracted but with no FTS5 entry.
    metadata::DocumentInfo doc{};
    doc.fileName = "fts5_scan.txt";
    doc.filePath = (config_.dataDir / "fts5_scan.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = 42;
    doc.sha256Hash = std::string(64, 'f');
    doc.mimeType = "text/plain";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    // Mark as successfully extracted.
    auto statusRes =
        meta->updateDocumentExtractionStatus(docId, true, metadata::ExtractionStatus::Success);
    REQUIRE(statusRes.has_value());

    // Ensure repair status is Pending (eligible for scan).
    auto repairRes =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(repairRes.has_value());

    // Verify NO FTS5 entry exists.
    auto hasFts = meta->hasFtsEntry(docId);
    REQUIRE(hasFts.has_value());
    REQUIRE_FALSE(hasFts.value());

    // Clear backlog counter.
    state_.stats.repairTotalBacklog.store(0, std::memory_order_relaxed);

    // Start RepairService (triggers spawnInitialScan which should detect the gap).
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.maxBatch = 16;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    repair.start();

    // spawnInitialScan is deferred by minDeferTicks (50 ticks * 100ms = ~5s).
    // Poll until the backlog counter is incremented or a generous timeout expires.
    const bool detected = waitForCondition(std::chrono::seconds(15), [&]() {
        return state_.stats.repairTotalBacklog.load(std::memory_order_relaxed) >= 1u;
    });
    repair.stop();

    // The document should have been enqueued (backlog > 0).
    CHECK(detected);

    sm->shutdown();
}
