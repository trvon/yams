#include <spdlog/spdlog.h>
#include <regex>
#include <yams/api/content_store.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/metadata_repository.h>

using yams::extraction::util::extractDocumentText;

namespace yams::daemon {

PostIngestQueue::PostIngestQueue(
    std::shared_ptr<api::IContentStore> store, std::shared_ptr<metadata::MetadataRepository> meta,
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
    std::shared_ptr<metadata::KnowledgeGraphStore> kg, std::size_t threads)
    : store_(std::move(store)), meta_(std::move(meta)), extractors_(std::move(extractors)),
      kg_(std::move(kg)) {
    if (threads == 0)
        threads = 2;
    threads_.reserve(threads);
    for (std::size_t i = 0; i < threads; ++i) {
        threads_.emplace_back([this] { workerLoop(); });
    }
}

PostIngestQueue::~PostIngestQueue() {
    stop_.store(true);
    cv_.notify_all();
    for (auto& t : threads_) {
        if (t.joinable())
            t.join();
    }
}

void PostIngestQueue::enqueue(Task t) {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (inflight_.find(t.hash) != inflight_.end()) {
            // Drop duplicate task while one is already queued or processing
            return;
        }
        inflight_.insert(t.hash);
        t.enqueuedAt = std::chrono::steady_clock::now();
        q_.push(std::move(t));
    }
    cv_.notify_one();
}

std::size_t PostIngestQueue::size() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return q_.size();
}

void PostIngestQueue::workerLoop() {
    while (!stop_.load()) {
        Task task;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [&] { return stop_.load() || !q_.empty(); });
            if (stop_.load())
                break;
            task = std::move(q_.front());
            q_.pop();
        }
        try {
            if (!store_ || !meta_) {
                spdlog::warn("PostIngest: store or metadata unavailable; dropping task {}",
                             task.hash);
                continue;
            }
            // Resolve document info first (id, name, mime, extension)
            auto infoRes = meta_->getDocumentByHash(task.hash);
            std::string fileName;
            std::string mime = task.mime;
            std::string extension;
            int64_t docId = -1;
            if (infoRes && infoRes.value().has_value()) {
                const auto& info = *infoRes.value();
                docId = info.id;
                if (!info.fileName.empty())
                    fileName = info.fileName;
                if (!info.mimeType.empty())
                    mime = info.mimeType;
                if (!info.fileExtension.empty())
                    extension = info.fileExtension;
            }
            // Extract text (heavy path allowed here)
            auto txt = extractDocumentText(store_, task.hash, mime, extension, extractors_);
            if (!txt || txt->empty()) {
                spdlog::debug("PostIngest: no text extracted for {} (mime={})", task.hash, mime);
                // Update extraction status to Skipped when doc exists
                if (docId >= 0) {
                    auto d = meta_->getDocument(docId);
                    if (d && d.value().has_value()) {
                        auto updated = d.value().value();
                        updated.contentExtracted = false;
                        updated.extractionStatus = metadata::ExtractionStatus::Skipped;
                        (void)meta_->updateDocument(updated);
                    }
                }
            } else {
                // Index into FTS5/fuzzy index
                if (docId >= 0) {
                    (void)meta_->indexDocumentContent(docId, fileName, *txt, mime);
                    (void)meta_->updateFuzzyIndex(docId);
                    // Mark extraction success on document row
                    auto d = meta_->getDocument(docId);
                    if (d && d.value().has_value()) {
                        auto updated = d.value().value();
                        updated.contentExtracted = true;
                        updated.extractionStatus = metadata::ExtractionStatus::Success;
                        (void)meta_->updateDocument(updated);
                    }
                } else {
                    spdlog::debug("PostIngest: document not found in metadata for hash {}",
                                  task.hash);
                }
            }
            // Knowledge Graph upsert from tags/metadata (best-effort)
            try {
                if (kg_ && meta_) {
                    auto infoRes2 = meta_->getDocumentByHash(task.hash);
                    if (infoRes2 && infoRes2.value().has_value()) {
                        const auto& doc = *infoRes2.value();
                        // Upsert document node and capture id directly
                        metadata::KGNode docNode;
                        docNode.nodeKey = std::string("doc:") + task.hash;
                        docNode.type = std::string("document");
                        docNode.label = !doc.fileName.empty()
                                            ? std::optional<std::string>(doc.fileName)
                                            : std::nullopt;
                        // Batch-upsert path (single item) for symmetry
                        std::vector<metadata::KGNode> initNodes;
                        initNodes.push_back(docNode);
                        std::int64_t docNodeId = -1;
                        if (yams::daemon::TuneAdvisor::kgBatchNodesEnabled()) {
                            auto ids = kg_->upsertNodes(initNodes);
                            if (ids && !ids.value().empty())
                                docNodeId = ids.value()[0];
                        } else {
                            auto r = kg_->upsertNode(initNodes[0]);
                            if (r)
                                docNodeId = r.value();
                        }
                        if (docNodeId >= 0) {
                            // Upsert tag nodes and then batch-add edges
                            auto tagsRes = meta_->getDocumentTags(doc.id);
                            std::vector<metadata::KGEdge> edges;
                            if (tagsRes && !tagsRes.value().empty()) {
                                edges.reserve(tagsRes.value().size());
                                std::vector<metadata::KGNode> tagNodes;
                                tagNodes.reserve(tagsRes.value().size());
                                for (const auto& t : tagsRes.value()) {
                                    metadata::KGNode tagNode;
                                    tagNode.nodeKey = std::string("tag:") + t;
                                    tagNode.type = std::string("tag");
                                    tagNodes.push_back(std::move(tagNode));
                                }
                                std::vector<std::int64_t> tagIds;
                                if (yams::daemon::TuneAdvisor::kgBatchNodesEnabled()) {
                                    auto rids = kg_->upsertNodes(tagNodes);
                                    if (rids)
                                        tagIds = std::move(rids.value());
                                } else {
                                    tagIds.reserve(tagNodes.size());
                                    for (const auto& n : tagNodes) {
                                        auto rr = kg_->upsertNode(n);
                                        if (!rr)
                                            continue;
                                        tagIds.push_back(rr.value());
                                    }
                                }
                                for (auto tid : tagIds) {
                                    metadata::KGEdge e;
                                    e.srcNodeId = docNodeId;
                                    e.dstNodeId = tid;
                                    e.relation = "HAS_TAG";
                                    edges.push_back(std::move(e));
                                }
                                if (!edges.empty()) {
                                    using TA = yams::daemon::TuneAdvisor;
                                    if (TA::kgBatchEdgesEnabled())
                                        (void)kg_->addEdgesUnique(edges);
                                    else
                                        for (const auto& e : edges)
                                            (void)kg_->addEdge(e);
                                }
                            }
                            // Lightweight entity enrichment from extracted text
                            try {
                                if (txt && !txt->empty()) {
                                    // URLs and emails (cap total entities to avoid blow-up)
                                    static const std::regex url_re(R"((https?:\/\/[^\s)]+))",
                                                                   std::regex::icase);
                                    static const std::regex email_re(
                                        R"(([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}))",
                                        std::regex::icase);
                                    static const std::regex path_re(R"((\/[^\s:]{2,}))");
                                    std::vector<metadata::KGEdge> entityEdges;
                                    std::vector<metadata::KGNode> nodeBuf;
                                    auto add_entity = [&](const std::string& key,
                                                          const std::string& type) {
                                        metadata::KGNode n;
                                        n.nodeKey = key;
                                        n.type = type;
                                        nodeBuf.push_back(std::move(n));
                                    };
                                    size_t added = 0;
                                    const size_t kMaxEntities =
                                        yams::daemon::TuneAdvisor::maxEntitiesPerDoc();
                                    std::smatch m;
                                    std::string s = *txt;
                                    auto it = s.cbegin();

                                    if (yams::daemon::TuneAdvisor::analyzerUrls()) {
                                        while (added < kMaxEntities &&
                                               std::regex_search(it, s.cend(), m, url_re)) {
                                            std::string url = m.str(1);
                                            add_entity(std::string("url:") + url, "url");
                                            it = m.suffix().first;
                                            ++added;
                                        }
                                    }
                                    it = s.cbegin();
                                    if (yams::daemon::TuneAdvisor::analyzerEmails()) {
                                        while (added < kMaxEntities &&
                                               std::regex_search(it, s.cend(), m, email_re)) {
                                            std::string email = m.str(1);
                                            add_entity(std::string("email:") + email, "email");
                                            it = m.suffix().first;
                                            ++added;
                                        }
                                    }
                                    it = s.cbegin();
                                    if (yams::daemon::TuneAdvisor::analyzerFilePaths()) {
                                        while (added < kMaxEntities &&
                                               std::regex_search(it, s.cend(), m, path_re)) {
                                            std::string path = m.str(1);
                                            add_entity(std::string("path:") + path, "path");
                                            it = m.suffix().first;
                                            ++added;
                                        }
                                    }
                                    if (!nodeBuf.empty()) {
                                        std::vector<std::int64_t> nids;
                                        if (yams::daemon::TuneAdvisor::kgBatchNodesEnabled()) {
                                            auto rids = kg_->upsertNodes(nodeBuf);
                                            if (rids)
                                                nids = std::move(rids.value());
                                        } else {
                                            nids.reserve(nodeBuf.size());
                                            for (const auto& n : nodeBuf) {
                                                auto rr = kg_->upsertNode(n);
                                                if (!rr)
                                                    continue;
                                                nids.push_back(rr.value());
                                            }
                                        }
                                        for (auto id : nids) {
                                            metadata::KGEdge e;
                                            e.srcNodeId = docNodeId;
                                            e.dstNodeId = id;
                                            e.relation = "MENTIONS";
                                            entityEdges.push_back(std::move(e));
                                        }
                                    }
                                    if (!entityEdges.empty()) {
                                        using TA = yams::daemon::TuneAdvisor;
                                        if (TA::kgBatchEdgesEnabled())
                                            (void)kg_->addEdgesUnique(entityEdges);
                                        else
                                            for (const auto& e : entityEdges)
                                                (void)kg_->addEdge(e);
                                    }
                                }
                            } catch (...) {
                            }
                        }
                    }
                }
            } catch (...) {
            }
        } catch (const std::exception& e) {
            spdlog::warn("PostIngest: exception: {}", e.what());
            failed_.fetch_add(1, std::memory_order_relaxed);
        } catch (...) {
            spdlog::warn("PostIngest: unknown exception");
            failed_.fetch_add(1, std::memory_order_relaxed);
        }
        processed_.fetch_add(1, std::memory_order_relaxed);
        // Mark task complete + update EMAs
        {
            std::lock_guard<std::mutex> lk(mtx_);
            inflight_.erase(task.hash);
            try {
                auto now = std::chrono::steady_clock::now();
                if (task.enqueuedAt.time_since_epoch().count() != 0) {
                    double ms = static_cast<double>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(now - task.enqueuedAt)
                            .count());
                    double prev = latencyMsEma_.load();
                    double ema = (prev == 0.0) ? ms : (kAlpha_ * ms + (1.0 - kAlpha_) * prev);
                    latencyMsEma_.store(ema);
                }
                if (lastCompleteTs_.time_since_epoch().count() != 0) {
                    double secs = std::chrono::duration<double>(now - lastCompleteTs_).count();
                    if (secs > 0.0) {
                        double inst = 1.0 / secs;
                        double prev = ratePerSecEma_.load();
                        double ema =
                            (prev == 0.0) ? inst : (kAlpha_ * inst + (1.0 - kAlpha_) * prev);
                        ratePerSecEma_.store(ema);
                    }
                }
                lastCompleteTs_ = now;
            } catch (...) {
            }
        }
    }
}

} // namespace yams::daemon
