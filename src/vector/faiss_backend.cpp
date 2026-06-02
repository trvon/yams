#ifdef YAMS_HAS_FAISS

#include <yams/vector/faiss_backend.h>

#include <spdlog/spdlog.h>

#include <faiss/Index.h>
#include <faiss/index_io.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexIDMap.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <utility>

namespace yams::vector {

struct FaissIndex {
    std::unique_ptr<faiss::Index> index;
};

namespace {

Error unsupported(const char* op) {
    return Error{ErrorCode::NotSupported,
                 std::string("FaissBackend::") + op + ": entity vectors not supported"};
}

Error internalError(const char* op, const std::string& detail) {
    return Error{ErrorCode::InternalError, std::string("FaissBackend::") + op + ": " + detail};
}

double l2ToCosine(float l2Sq, size_t dim) {
    // For L2-normalized vectors: cosine_sim ≈ 1.0 - l2_sq / 2.0
    (void)dim;
    return std::max(0.0, 1.0 - static_cast<double>(l2Sq) / 2.0);
}

} // namespace

FaissBackend::FaissBackend(const FaissBackendConfig& config) : config_(config) {}

FaissBackend::~FaissBackend() {
    close();
}

// ── Core lifecycle ──────────────────────────────────────────────────────────

Result<void> FaissBackend::initialize(const std::string& dbPath) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initialized_)
        return {};

    dbPath_ = dbPath;
    if (config_.maxElements == 0) {
        // Default capacity: reasonable for most use cases.  The HNSW index
        // internally rounds up, so this is pre-allocated only once.
        config_.maxElements = 2000000;
    }
    initialized_ = true;
    return {};
}

void FaissBackend::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_)
        return;
    if (index_) {
        auto idxFile = indexFilePath();
        faiss::write_index(index_->index.get(), idxFile.string().c_str());
    }
    index_.reset();
    records_.clear();
    chunkToIdx_.clear();
    docToIdxs_.clear();
    initialized_ = false;
}

bool FaissBackend::isInitialized() const {
    return initialized_;
}

// ── Schema ──────────────────────────────────────────────────────────────────

Result<void> FaissBackend::createTables(size_t embeddingDim) {
    config_.embeddingDim = embeddingDim;
    return {};
}

bool FaissBackend::tablesExist() const {
    // FAISS index is in-memory + file, not tables.  Always "exist" once
    // createTables has been called.
    return config_.embeddingDim > 0;
}

// ── Insert ──────────────────────────────────────────────────────────────────

void FaissBackend::ensureIndexReady() {
    if (index_)
        return;
    loadOrCreateIndex();
}

Result<void> FaissBackend::insertVector(const VectorRecord& record) {
    return insertVectorsBatch({record});
}

Result<void> FaissBackend::insertVectorsBatch(const std::vector<VectorRecord>& records) {
    if (records.empty())
        return {};
    std::lock_guard<std::mutex> lock(mutex_);
    ensureIndexReady();

    for (const auto& r : records) {
        if (r.embedding.empty())
            continue;

        size_t idx = records_.size();
        records_.push_back(r);
        if (!r.chunk_id.empty())
            chunkToIdx_[r.chunk_id] = idx;
        if (!r.document_hash.empty())
            docToIdxs_[r.document_hash].push_back(idx);

        auto idVal = static_cast<faiss::idx_t>(nextId());
        index_->index->add_with_ids(1, r.embedding.data(), &idVal);
        // FAISS stores its own copy; free the duplicate to halve vector RSS.
        records_.back().embedding.clear();
        records_.back().embedding.shrink_to_fit();
    }
    return {};
}

// ── Update / Delete ─────────────────────────────────────────────────────────

Result<void> FaissBackend::updateVector(const std::string& chunkId, const VectorRecord&) {
    // HNSW does not support in-place updates.  The caller should delete then
    // re-insert.  For now we treat this as a documentation-only requirement.
    (void)chunkId;
    return Error{ErrorCode::NotSupported,
                 "FaissBackend::updateVector: remove + re-insert recommended"};
}

Result<void> FaissBackend::deleteVector(const std::string& chunkId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = chunkToIdx_.find(chunkId);
    if (it == chunkToIdx_.end())
        return {};
    size_t idx = it->second;
    auto& r = records_[idx];

    // Remove from doc mapping
    if (!r.document_hash.empty()) {
        auto dit = docToIdxs_.find(r.document_hash);
        if (dit != docToIdxs_.end()) {
            auto& vec = dit->second;
            vec.erase(std::remove(vec.begin(), vec.end(), idx), vec.end());
        }
    }

    chunkToIdx_.erase(it);
    records_[idx] = VectorRecord{};
    return {};
}

Result<void> FaissBackend::deleteVectorsByDocument(const std::string& documentHash) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = docToIdxs_.find(documentHash);
    if (it == docToIdxs_.end())
        return {};

    for (auto idx : it->second) {
        auto& r = records_[idx];
        if (!r.chunk_id.empty())
            chunkToIdx_.erase(r.chunk_id);
        records_[idx] = VectorRecord{};
    }
    docToIdxs_.erase(it);
    return {};
}

// ── Search ──────────────────────────────────────────────────────────────────

Result<std::vector<VectorRecord>>
FaissBackend::searchSimilar(const std::vector<float>& queryEmbedding, size_t k,
                            float similarityThreshold,
                            const std::optional<std::string>& documentHash,
                            const std::unordered_set<std::string>& candidateHashes,
                            const std::map<std::string, std::string>& metadataFilters) {
    std::lock_guard<std::mutex> lock(mutex_);
    ensureIndexReady();

    if (records_.empty())
        return std::vector<VectorRecord>{};

    faiss::SearchParametersHNSW hnswParams;
    hnswParams.efSearch = static_cast<int>(config_.hnswEfSearch);
    faiss::idx_t nResults = static_cast<faiss::idx_t>(std::min(k, records_.size()));

    std::vector<float> distances(nResults);
    std::vector<faiss::idx_t> labels(nResults);

    try {
        index_->index->search(1, queryEmbedding.data(), nResults, distances.data(), labels.data(),
                              &hnswParams);
    } catch (const std::exception& e) {
        return internalError("searchSimilar", e.what());
    }

    std::vector<VectorRecord> results;
    results.reserve(nResults);

    for (faiss::idx_t i = 0; i < nResults; ++i) {
        if (labels[i] < 0)
            continue;

        double sim = l2ToCosine(distances[i], config_.embeddingDim);
        if (sim < static_cast<double>(similarityThreshold))
            continue;

        size_t recIdx = static_cast<size_t>(labels[i]) - 1; // ids are 1-based
        if (recIdx >= records_.size())
            continue;

        const auto& rec = records_[recIdx];
        if (rec.embedding_dim == 0)
            continue;

        if (documentHash && rec.document_hash != *documentHash)
            continue;
        if (!candidateHashes.empty() && !candidateHashes.count(rec.document_hash))
            continue;
        if (!metadataFilters.empty()) {
            bool allMatch = true;
            for (const auto& [k, v] : metadataFilters) {
                auto mit = rec.metadata.find(k);
                if (mit == rec.metadata.end() || mit->second != v) {
                    allMatch = false;
                    break;
                }
            }
            if (!allMatch)
                continue;
        }

        VectorRecord out = rec;
        out.relevance_score = static_cast<float>(sim);
        results.push_back(std::move(out));
    }

    return results;
}

Result<std::vector<std::vector<VectorRecord>>>
FaissBackend::searchSimilarBatch(const std::vector<std::vector<float>>& queryEmbeddings, size_t k,
                                 float similarityThreshold, size_t /*numThreads*/) {
    std::vector<std::vector<VectorRecord>> allResults;
    allResults.reserve(queryEmbeddings.size());
    for (const auto& qe : queryEmbeddings) {
        auto res = searchSimilar(qe, k, similarityThreshold);
        if (!res)
            return Error{res.error().code, res.error().message};
        allResults.push_back(std::move(res.value()));
    }
    return allResults;
}

// ── Lookup ──────────────────────────────────────────────────────────────────

Result<std::optional<VectorRecord>> FaissBackend::getVector(const std::string& chunkId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = chunkToIdx_.find(chunkId);
    if (it != chunkToIdx_.end()) {
        size_t idx = it->second;
        VectorRecord rec = records_[idx];
        reconstructEmbedding(rec, idx);
        return std::optional<VectorRecord>{std::move(rec)};
    }
    return std::optional<VectorRecord>{};
}

Result<std::map<std::string, VectorRecord>>
FaissBackend::getVectorsBatch(const std::vector<std::string>& chunkIds) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<std::string, VectorRecord> out;
    for (const auto& cid : chunkIds) {
        auto it = chunkToIdx_.find(cid);
        if (it != chunkToIdx_.end()) {
            size_t idx = it->second;
            VectorRecord rec = records_[idx];
            reconstructEmbedding(rec, idx);
            out.emplace(cid, std::move(rec));
        }
    }
    return out;
}

Result<std::vector<VectorRecord>>
FaissBackend::getVectorsByDocument(const std::string& documentHash) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = docToIdxs_.find(documentHash);
    if (it == docToIdxs_.end())
        return std::vector<VectorRecord>{};
    std::vector<VectorRecord> out;
    out.reserve(it->second.size());
    for (auto idx : it->second) {
        const auto& r = records_[idx];
        if (r.embedding_dim == 0)
            continue;
        VectorRecord rec = r;
        reconstructEmbedding(rec, idx);
        out.push_back(std::move(rec));
    }
    return out;
}

// ── Bulk / streaming ────────────────────────────────────────────────────────

Result<std::unordered_map<std::string, VectorRecord>> FaissBackend::getDocumentLevelVectorsAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, VectorRecord> out;
    for (const auto& [hash, idxs] : docToIdxs_) {
        if (!idxs.empty()) {
            const auto& r = records_[idxs[0]];
            if (r.embedding_dim == 0)
                continue;
            VectorRecord rec = r;
            reconstructEmbedding(rec, idxs[0]);
            out.emplace(hash, std::move(rec));
        }
    }
    return out;
}

Result<size_t>
FaissBackend::forEachDocumentLevelVector(const std::function<bool(VectorRecord&&)>& visitor) {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t delivered = 0;
    for (const auto& [hash, idxs] : docToIdxs_) {
        if (!idxs.empty()) {
            const auto& r = records_[idxs[0]];
            if (r.embedding_dim == 0)
                continue;
            VectorRecord rec(r);
            reconstructEmbedding(rec, idxs[0]);
            if (!visitor(std::move(rec)))
                break;
            ++delivered;
        }
    }
    return delivered;
}

// ── Count / stats ───────────────────────────────────────────────────────────

Result<size_t> FaissBackend::getVectorCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& r : records_) {
        if (r.embedding_dim > 0)
            ++count;
    }
    return count;
}

Result<VectorDatabaseStats> FaissBackend::getStats() {
    VectorDatabaseStats s;
    if (auto c = getVectorCount(); c)
        s.total_vectors = c.value();
    s.total_documents = docToIdxs_.size();
    return s;
}

// ── Embedding checks ────────────────────────────────────────────────────────

Result<bool> FaissBackend::hasEmbedding(const std::string& documentHash) {
    std::lock_guard<std::mutex> lock(mutex_);
    return docToIdxs_.count(documentHash) > 0;
}

Result<std::unordered_set<std::string>> FaissBackend::getEmbeddedDocumentHashes() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_set<std::string> out;
    for (const auto& [hash, idxs] : docToIdxs_) {
        if (!idxs.empty() && records_[idxs[0]].embedding_dim > 0)
            out.insert(hash);
    }
    return out;
}

// ── Index lifecycle ─────────────────────────────────────────────────────────

Result<void> FaissBackend::loadOrCreateIndex() {
    auto idxFile = indexFilePath();
    if (std::filesystem::exists(idxFile)) {
        try {
            std::unique_ptr<faiss::Index> faissIdx(faiss::read_index(idxFile.string().c_str()));
            if (faissIdx) {
                index_ = std::make_unique<FaissIndex>();
                index_->index = std::move(faissIdx);

                auto* idMap = dynamic_cast<faiss::IndexIDMap*>(index_->index.get());
                auto* hnsw = idMap ? dynamic_cast<faiss::IndexHNSW*>(idMap->index) : nullptr;
                if (hnsw)
                    hnsw->hnsw.efSearch = static_cast<int>(config_.hnswEfSearch);

                spdlog::info("FaissBackend: loaded persisted index from {} (ntotal={})",
                             idxFile.string(), index_->index->ntotal);
                return {};
            }
        } catch (const std::exception& e) {
            spdlog::warn("FaissBackend: failed to load persisted index ({}), creating new",
                         e.what());
        }
    }

    // Create fresh HNSW index wrapped for custom IDs
    index_ = std::make_unique<FaissIndex>();
    auto* flat = new faiss::IndexFlatL2(static_cast<int>(config_.embeddingDim));
    auto* hnsw = new faiss::IndexHNSWFlat(static_cast<int>(config_.embeddingDim),
                                          static_cast<int>(config_.hnswM));
    hnsw->hnsw.efConstruction = static_cast<int>(config_.hnswEfConstruction);
    hnsw->hnsw.efSearch = static_cast<int>(config_.hnswEfSearch);
    delete hnsw->storage;
    hnsw->storage = flat;
    auto* idMap = new faiss::IndexIDMap(hnsw);
    index_->index.reset(idMap);

    spdlog::info("FaissBackend: created new HNSW index (dim={}, M={}, efConstruction={})",
                 config_.embeddingDim, config_.hnswM, config_.hnswEfConstruction);
    return {};
}

Result<void> FaissBackend::buildIndex() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (index_)
        index_.reset();
    return loadOrCreateIndex();
}

Result<void> FaissBackend::prepareSearchIndex() {
    std::lock_guard<std::mutex> lock(mutex_);
    ensureIndexReady();
    return {};
}

Result<bool> FaissBackend::hasReusablePersistedSearchIndex() {
    auto idxFile = indexFilePath();
    return std::filesystem::exists(idxFile);
}

Result<void> FaissBackend::optimize() {
    // HNSW index is self-optimizing; explicit optimize is a no-op.
    return {};
}

Result<void> FaissBackend::persistIndex() {
    return saveIndex();
}

Result<void> FaissBackend::saveIndex() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!index_ || index_->index->ntotal == 0)
        return {};
    auto idxFile = indexFilePath();
    try {
        faiss::write_index(index_->index.get(), idxFile.string().c_str());
        spdlog::info("FaissBackend: persisted index to {} (ntotal={})", idxFile.string(),
                     index_->index->ntotal);
    } catch (const std::exception& e) {
        return Error{ErrorCode::IOError, std::string("FaissBackend::saveIndex: ") + e.what()};
    }
    return {};
}

// ── Helpers ─────────────────────────────────────────────────────────────────

size_t FaissBackend::nextId() {
    return nextId_++;
}

std::filesystem::path FaissBackend::indexFilePath() const {
    return dbPath_.parent_path() / (dbPath_.stem().string() + ".faiss_idx");
}

void FaissBackend::reconstructEmbedding(VectorRecord& rec, size_t recIdx) const {
    if (!rec.embedding.empty() || rec.embedding_dim == 0 || !index_)
        return;
    rec.embedding.resize(rec.embedding_dim);
    try {
        index_->index->reconstruct(static_cast<faiss::idx_t>(recIdx + 1), rec.embedding.data());
    } catch (...) {
        rec.embedding.clear();
    }
}

// ── Entity vectors (all unsupported) ────────────────────────────────────────

Result<void> FaissBackend::insertEntityVector(const EntityVectorRecord&) {
    return unsupported("insertEntityVector");
}
Result<void> FaissBackend::insertEntityVectorsBatch(const std::vector<EntityVectorRecord>&) {
    return unsupported("insertEntityVectorsBatch");
}
Result<void> FaissBackend::deleteEntityVectorsByNode(const std::string&) {
    return unsupported("deleteEntityVectorsByNode");
}
Result<void> FaissBackend::deleteEntityVectorsByDocument(const std::string&) {
    return unsupported("deleteEntityVectorsByDocument");
}
Result<std::vector<EntityVectorRecord>> FaissBackend::searchEntities(const std::vector<float>&,
                                                                     const EntitySearchParams&) {
    return unsupported("searchEntities");
}
Result<std::vector<EntityVectorRecord>> FaissBackend::getEntityVectorsByNode(const std::string&) {
    return unsupported("getEntityVectorsByNode");
}
Result<std::vector<EntityVectorRecord>>
FaissBackend::getEntityVectorsByDocument(const std::string&) {
    return unsupported("getEntityVectorsByDocument");
}
Result<bool> FaissBackend::hasEntityEmbedding(const std::string&) {
    return unsupported("hasEntityEmbedding");
}
Result<size_t> FaissBackend::getEntityVectorCount() {
    return unsupported("getEntityVectorCount");
}
Result<void> FaissBackend::markEntityAsStale(const std::string&) {
    return unsupported("markEntityAsStale");
}

// ── Transactions ────────────────────────────────────────────────────────────

Result<void> FaissBackend::beginTransaction() {
    return {};
}
Result<void> FaissBackend::commitTransaction() {
    return {};
}
Result<void> FaissBackend::rollbackTransaction() {
    return {};
}

// ── TurboQuant ──────────────────────────────────────────────────────────────

Result<void> FaissBackend::persistTurboQuantPerCoordScales(size_t, uint8_t, uint64_t,
                                                           const std::vector<float>&) {
    return unsupported("persistTurboQuantPerCoordScales");
}
Result<void> FaissBackend::persistTurboQuantFittedModel(size_t, uint8_t, uint64_t,
                                                        const std::vector<float>&,
                                                        const std::vector<float>&) {
    return unsupported("persistTurboQuantFittedModel");
}

} // namespace yams::vector

#endif // YAMS_HAS_FAISS
