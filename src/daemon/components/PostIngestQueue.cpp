#include <spdlog/spdlog.h>
#include <regex>
#include <yams/api/content_store.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/metadata/metadata_repository.h>

using yams::extraction::util::extractDocumentText;

namespace yams::daemon {

PostIngestQueue::PostIngestQueue(
    std::shared_ptr<api::IContentStore> store, std::shared_ptr<metadata::MetadataRepository> meta,
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
    std::shared_ptr<metadata::KnowledgeGraphStore> kg, std::size_t threads, std::size_t capacity)
    : store_(std::move(store)), meta_(std::move(meta)), extractors_(std::move(extractors)),
      kg_(std::move(kg)), capacity_(capacity ? capacity : 1000) {
    if (threads == 0)
        threads = 2;
    threads_.reserve(threads);
    for (std::size_t i = 0; i < threads; ++i) {
        auto flag = std::make_shared<std::atomic<bool>>(false);
        Worker w{std::thread([this, flag] {
                     while (!stop_.load(std::memory_order_relaxed) &&
                            !flag->load(std::memory_order_relaxed)) {
                         workerLoop();
                     }
                 }),
                 flag};
        threads_.emplace_back(std::move(w));
    }

#if !YAMS_INTERNAL_BUS_MPMC
    // When bus is SPSC, create a single dispatcher thread that drains the bus
    // and enqueues into the internal deques under lock, preserving SPSC semantics.
    if (TuneAdvisor::useInternalBusForPostIngest()) {
        busDispatcher_ = std::thread([this] {
            auto bus =
                InternalEventBus::instance()
                    .get_or_create_channel<InternalEventBus::PostIngestTask>("post_ingest", 4096);
            while (!stop_.load(std::memory_order_relaxed)) {
                InternalEventBus::PostIngestTask bt;
                if (bus && bus->try_pop(bt)) {
                    std::unique_lock<std::mutex> lk(mtx_);
                    if (inflight_.find(bt.hash) == inflight_.end()) {
                        inflight_.insert(bt.hash);
                        Task t{bt.hash, bt.mime, /*session*/ "", {}, Task::Stage::Metadata};
                        t.enqueuedAt = std::chrono::steady_clock::now();
                        qMeta_.push_back(std::move(t));
                        lk.unlock();
                        cv_.notify_one();
                        InternalEventBus::instance().incPostConsumed();
                    } else {
                        InternalEventBus::instance().incPostDropped();
                    }
                } else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
        });
    }
#endif
}

PostIngestQueue::~PostIngestQueue() {
    stop_.store(true);
    cv_.notify_all();
#if !YAMS_INTERNAL_BUS_MPMC
    if (busDispatcher_.joinable()) {
        busDispatcher_.join();
    }
#endif
    for (auto& w : threads_) {
        if (w.th.joinable())
            w.th.join();
    }
}

void PostIngestQueue::enqueue(Task t) {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        // Bounded queue with backpressure: wait while full
        // total queued used only for wait predicate below
        cv_.wait(lk, [&] {
            return stop_.load() || (qMeta_.size() + qKg_.size() + qEmb_.size()) < capacity_;
        });
        if (stop_.load())
            return; // shutdown requested
        if (inflight_.find(t.hash) != inflight_.end()) {
            // Drop duplicate task while one is already queued or processing
            return;
        }
        inflight_.insert(t.hash);
        t.enqueuedAt = std::chrono::steady_clock::now();
        if (t.session.empty())
            t.session = "default";
        switch (t.stage) {
            case Task::Stage::KnowledgeGraph:
                qKg_.push_back(std::move(t));
                break;
            case Task::Stage::Embeddings:
                qEmb_.push_back(std::move(t));
                break;
            default:
                qMeta_.push_back(std::move(t));
                break;
        }
    }
    cv_.notify_one();
}

bool PostIngestQueue::tryEnqueue(const Task& t) {
    std::unique_lock<std::mutex> lk(mtx_);
    if (stop_.load())
        return false;
    if ((qMeta_.size() + qKg_.size() + qEmb_.size()) >= capacity_)
        return false;
    if (inflight_.find(t.hash) != inflight_.end()) {
        return false; // duplicate suppressed
    }
    Task copy = t;
    copy.enqueuedAt = std::chrono::steady_clock::now();
    if (copy.session.empty())
        copy.session = "default";
    inflight_.insert(copy.hash);
    switch (copy.stage) {
        case Task::Stage::KnowledgeGraph:
            qKg_.push_back(std::move(copy));
            break;
        case Task::Stage::Embeddings:
            qEmb_.push_back(std::move(copy));
            break;
        default:
            qMeta_.push_back(std::move(copy));
            break;
    }
    lk.unlock();
    cv_.notify_one();
    return true;
}

void PostIngestQueue::notifyWorkers() {
    cv_.notify_one();
}

std::size_t PostIngestQueue::size() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return qMeta_.size() + qKg_.size() + qEmb_.size();
}

void PostIngestQueue::workerLoop() {
    Task task;
    bool haveTask = false;
    const bool busEnabled = TuneAdvisor::useInternalBusForPostIngest();
#if YAMS_INTERNAL_BUS_MPMC
    // Try InternalEventBus first when enabled (MPMC mode only). In SPSC mode, a single
    // dispatcher thread fills internal queues instead.
    if (busEnabled) {
        static std::shared_ptr<SpscQueue<InternalEventBus::PostIngestTask>> bus =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
                "post_ingest", 4096);
        InternalEventBus::PostIngestTask bt;
        if (bus && bus->try_pop(bt)) {
            // Deduplicate using inflight_ under lock
            {
                std::lock_guard<std::mutex> lk(mtx_);
                if (inflight_.find(bt.hash) != inflight_.end()) {
                    // Already queued/processing, skip
                    haveTask = false;
                } else {
                    inflight_.insert(bt.hash);
                    task.hash = std::move(bt.hash);
                    task.mime = std::move(bt.mime);
                    task.enqueuedAt = std::chrono::steady_clock::now();
                    haveTask = true;
                    InternalEventBus::instance().incPostConsumed();
                }
            }
        }
    }
#endif
    if (!haveTask) {
        std::unique_lock<std::mutex> lk(mtx_);
        auto predicate = [&] {
            return stop_.load() || (!qMeta_.empty() || !qKg_.empty() || !qEmb_.empty());
        };
        if (busEnabled) {
#if YAMS_INTERNAL_BUS_MPMC
            cv_.wait_for(lk, std::chrono::milliseconds(50), predicate);
#else
            cv_.wait(lk, predicate);
#endif
        } else {
            cv_.wait(lk, predicate);
        }
        if (stop_.load())
            return;
        if (!popNextTaskLocked(task)) {
            // Nothing eligible (token-starved), wait briefly to allow refill
            lk.unlock();
            if (busEnabled) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            return;
        }
        cv_.notify_one();
        haveTask = true;
    }

    bool processedOk = true;
    try {
        if (!store_ || !meta_) {
            spdlog::warn("PostIngest: store or metadata unavailable; dropping task {}", task.hash);
            processedOk = false; // skip heavy path
        }
        int64_t docId = -1;
        std::string fileName;
        std::string mime = task.mime;
        std::string extension;
        if (processedOk) {
            auto infoRes = meta_->getDocumentByHash(task.hash);
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
            auto txt = extractDocumentText(store_, task.hash, mime, extension, extractors_);
            if (!txt || txt->empty()) {
                spdlog::debug("PostIngest: no text extracted for {} (mime={})", task.hash, mime);
                if (docId >= 0) {
                    auto d = meta_->getDocument(docId);
                    if (d && d.value().has_value()) {
                        auto updated = d.value().value();
                        updated.contentExtracted = false;
                        updated.extractionStatus = metadata::ExtractionStatus::Skipped;
                        (void)meta_->updateDocument(updated);
                    }
                }
            } else if (docId >= 0) {
                {
                    metadata::DocumentContent contentRow;
                    contentRow.documentId = docId;
                    contentRow.contentText = *txt;
                    contentRow.contentLength = static_cast<int64_t>(contentRow.contentText.size());
                    contentRow.extractionMethod = "post_ingest";
                    double langConfidence = 0.0;
                    contentRow.language = yams::extraction::LanguageDetector::detectLanguage(
                        contentRow.contentText, &langConfidence);
                    auto contentUpsert = meta_->insertContent(contentRow);
                    if (!contentUpsert) {
                        spdlog::warn("PostIngest: failed to upsert content for {}: {}", task.hash,
                                     contentUpsert.error().message);
                    }
                }
                (void)meta_->indexDocumentContent(docId, fileName, *txt, mime);
                (void)meta_->updateFuzzyIndex(docId);
                auto d = meta_->getDocument(docId);
                if (d && d.value().has_value()) {
                    auto updated = d.value().value();
                    updated.contentExtracted = true;
                    updated.extractionStatus = metadata::ExtractionStatus::Success;
                    (void)meta_->updateDocument(updated);
                }
                // KG upsert (best-effort)
                try {
                    if (kg_) {
                        auto infoRes2 = meta_->getDocumentByHash(task.hash);
                        if (infoRes2 && infoRes2.value().has_value()) {
                            const auto& doc = *infoRes2.value();
                            metadata::KGNode docNode;
                            docNode.nodeKey = std::string("doc:") + task.hash;
                            docNode.type = std::string("document");
                            docNode.label = !doc.fileName.empty()
                                                ? std::optional<std::string>(doc.fileName)
                                                : std::nullopt;
                            std::int64_t docNodeId = -1;
                            if (TuneAdvisor::kgBatchNodesEnabled()) {
                                auto ids = kg_->upsertNodes({docNode});
                                if (ids && !ids.value().empty())
                                    docNodeId = ids.value()[0];
                            } else {
                                auto r = kg_->upsertNode(docNode);
                                if (r)
                                    docNodeId = r.value();
                            }
                            if (docNodeId >= 0) {
                                auto tagsRes = meta_->getDocumentTags(doc.id);
                                std::vector<metadata::KGEdge> edges;
                                if (tagsRes && !tagsRes.value().empty()) {
                                    std::vector<metadata::KGNode> tagNodes;
                                    tagNodes.reserve(tagsRes.value().size());
                                    for (const auto& t : tagsRes.value()) {
                                        metadata::KGNode tagNode;
                                        tagNode.nodeKey = std::string("tag:") + t;
                                        tagNode.type = std::string("tag");
                                        tagNodes.push_back(std::move(tagNode));
                                    }
                                    std::vector<std::int64_t> tagIds;
                                    if (TuneAdvisor::kgBatchNodesEnabled()) {
                                        auto rids = kg_->upsertNodes(tagNodes);
                                        if (rids)
                                            tagIds = std::move(rids.value());
                                    } else {
                                        for (const auto& n : tagNodes) {
                                            auto rr = kg_->upsertNode(n);
                                            if (rr)
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
                                        if (TuneAdvisor::kgBatchEdgesEnabled())
                                            (void)kg_->addEdgesUnique(edges);
                                        else
                                            for (const auto& e : edges)
                                                (void)kg_->addEdge(e);
                                    }
                                }
                                // Lightweight entity extraction
                                if (txt && !txt->empty()) {
                                    static const std::regex url_re(R"((https?:\/\/[^\s)]+))",
                                                                   std::regex::icase);
                                    static const std::regex email_re(
                                        R"(([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}))",
                                        std::regex::icase);
                                    static const std::regex path_re(R"((\/[^\s:]{2,}))");
                                    std::vector<metadata::KGNode> nodeBuf;
                                    auto add_entity = [&](const std::string& key,
                                                          const std::string& type) {
                                        metadata::KGNode n;
                                        n.nodeKey = key;
                                        n.type = type;
                                        nodeBuf.push_back(std::move(n));
                                    };
                                    size_t added = 0;
                                    const size_t kMax = TuneAdvisor::maxEntitiesPerDoc();
                                    std::smatch m;
                                    std::string s = *txt;
                                    auto it = s.cbegin();
                                    if (TuneAdvisor::analyzerUrls()) {
                                        while (added < kMax &&
                                               std::regex_search(it, s.cend(), m, url_re)) {
                                            add_entity(std::string("url:") + m.str(1), "url");
                                            it = m.suffix().first;
                                            ++added;
                                        }
                                    }
                                    it = s.cbegin();
                                    if (TuneAdvisor::analyzerEmails()) {
                                        while (added < kMax &&
                                               std::regex_search(it, s.cend(), m, email_re)) {
                                            add_entity(std::string("email:") + m.str(1), "email");
                                            it = m.suffix().first;
                                            ++added;
                                        }
                                    }
                                    it = s.cbegin();
                                    if (TuneAdvisor::analyzerFilePaths()) {
                                        while (added < kMax &&
                                               std::regex_search(it, s.cend(), m, path_re)) {
                                            add_entity(std::string("path:") + m.str(1), "path");
                                            it = m.suffix().first;
                                            ++added;
                                        }
                                    }
                                    if (!nodeBuf.empty()) {
                                        std::vector<std::int64_t> nids;
                                        if (TuneAdvisor::kgBatchNodesEnabled()) {
                                            auto rids = kg_->upsertNodes(nodeBuf);
                                            if (rids)
                                                nids = std::move(rids.value());
                                        } else {
                                            for (const auto& n : nodeBuf) {
                                                auto rr = kg_->upsertNode(n);
                                                if (rr)
                                                    nids.push_back(rr.value());
                                            }
                                        }
                                        std::vector<metadata::KGEdge> ents;
                                        ents.reserve(nids.size());
                                        for (auto id : nids) {
                                            metadata::KGEdge e;
                                            e.srcNodeId = docNodeId;
                                            e.dstNodeId = id;
                                            e.relation = "MENTIONS";
                                            ents.push_back(std::move(e));
                                        }
                                        if (!ents.empty()) {
                                            if (TuneAdvisor::kgBatchEdgesEnabled())
                                                (void)kg_->addEdgesUnique(ents);
                                            else
                                                for (const auto& e : ents)
                                                    (void)kg_->addEdge(e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (...) {
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("PostIngest: exception: {}", e.what());
        processedOk = false;
        failed_.fetch_add(1, std::memory_order_relaxed);
    } catch (...) {
        spdlog::warn("PostIngest: unknown exception");
        processedOk = false;
        failed_.fetch_add(1, std::memory_order_relaxed);
    }

    processed_.fetch_add(1, std::memory_order_relaxed);
    // Finalize: clear inflight and update EMAs
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
                    double ema = (prev == 0.0) ? inst : (kAlpha_ * inst + (1.0 - kAlpha_) * prev);
                    ratePerSecEma_.store(ema);
                }
            }
            lastCompleteTs_ = now;
        } catch (...) {
        }
    }
    // Optional tiny pause to reduce contention on very fast loops and give CPU back
    try {
        auto ms = TuneAdvisor::workerPollMs();
        if (ms > 0 && ms < 1000)
            std::this_thread::sleep_for(std::chrono::milliseconds(std::min<uint32_t>(ms, 25)));
    } catch (...) {
    }
}

bool PostIngestQueue::admitSessionLocked(const std::string& session) {
    auto now = std::chrono::steady_clock::now();
    auto it = buckets_.find(session);
    if (it == buckets_.end()) {
        buckets_[session] = Bucket{static_cast<double>(tokenBurst_), now};
        return true;
    }
    auto& b = it->second;
    // Refill tokens based on elapsed time
    double elapsed = std::chrono::duration<double>(now - b.last).count();
    b.tokens = std::min<double>(b.tokens + elapsed * tokenRatePerSec_, tokenBurst_);
    b.last = now;
    if (b.tokens >= 1.0) {
        b.tokens -= 1.0;
        return true;
    }
    return false;
}

bool PostIngestQueue::popNextTaskLocked(Task& out) {
    // Weighted-fair selection order cycles through a flattened schedule
    // Build a small static pattern based on weights: [M x wMeta][K x wKg][E x wEmb]
    auto nonEmpty = [&]() { return !qMeta_.empty() || !qKg_.empty() || !qEmb_.empty(); };
    if (!nonEmpty())
        return false;
    const uint32_t period = wMeta_ + wKg_ + wEmb_;
    for (uint32_t i = 0; i < period; ++i) {
        uint32_t idx = (schedCounter_ + i) % period;
        // map idx to a queue band
        if (idx < wMeta_) {
            if (!qMeta_.empty()) {
                const auto& cand = qMeta_.front();
                if (admitSessionLocked(cand.session)) {
                    out = cand;
                    qMeta_.pop_front();
                    schedCounter_ = (idx + 1) % period;
                    return true;
                }
            }
        } else if (idx < wMeta_ + wKg_) {
            if (!qKg_.empty()) {
                const auto& cand = qKg_.front();
                if (admitSessionLocked(cand.session)) {
                    out = cand;
                    qKg_.pop_front();
                    schedCounter_ = (idx + 1) % period;
                    return true;
                }
            }
        } else {
            if (!qEmb_.empty()) {
                const auto& cand = qEmb_.front();
                if (admitSessionLocked(cand.session)) {
                    out = cand;
                    qEmb_.pop_front();
                    schedCounter_ = (idx + 1) % period;
                    return true;
                }
            }
        }
    }
    // No eligible task due to token starvation
    return false;
}

bool PostIngestQueue::resize(std::size_t target) {
    if (target == 0)
        target = 1;
    std::lock_guard<std::mutex> lk(mtx_);
    std::size_t cur = threads_.size();
    if (target == cur)
        return false;
    if (target > cur) {
        std::size_t add = target - cur;
        for (std::size_t i = 0; i < add; ++i) {
            auto flag = std::make_shared<std::atomic<bool>>(false);
            Worker w{std::thread([this, flag] {
                         while (!stop_.load(std::memory_order_relaxed) &&
                                !flag->load(std::memory_order_relaxed)) {
                             workerLoop();
                         }
                     }),
                     flag};
            threads_.emplace_back(std::move(w));
        }
        spdlog::info("PostIngestQueue resized up to {} threads", threads_.size());
        return true;
    }
    // shrink: signal last N workers to exit; join outside lock
    std::size_t remove = cur - target;
    std::vector<std::thread> joiners;
    for (std::size_t i = 0; i < remove; ++i) {
        auto& w = threads_.back();
        if (w.exit)
            w.exit->store(true, std::memory_order_relaxed);
        joiners.emplace_back(std::move(w.th));
        threads_.pop_back();
    }
    // Unlock and join
    for (auto& t : joiners) {
        if (t.joinable())
            t.join();
    }
    spdlog::info("PostIngestQueue resized down to {} threads", threads_.size());
    return true;
}

} // namespace yams::daemon
