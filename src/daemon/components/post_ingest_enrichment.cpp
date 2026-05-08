// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
//
// PostIngestQueue enrichment stages: KG construction, symbol extraction,
// entity extraction, and title/NL entity processing.
//
// Extracted from PostIngestQueue.cpp to keep the coordinator file focused
// on lifecycle, queue management, and extraction dispatch.

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <filesystem>
#include <sstream>
#include <thread>
#include <unordered_set>

#include <simeon/corpus_adapter.hpp>

#include <yams/common/utf8_utils.h>
#include <yams/api/content_store.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/pressure_limited_poller.h>
#include <yams/daemon/resource/external_entity_provider_adapter.h>
#include <yams/extraction/title_util.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/query_text_utils.h>

namespace yams::daemon {

// Forward-declare shared helpers defined in PostIngestQueue.cpp
namespace {
extern bool isHighValueGraphType(std::string_view normalizedType);
} // namespace

namespace {

// ---------------------------------------------------------------------------
// Enrichment-stage utilities (enrichment-only, not shared with extraction)
// ---------------------------------------------------------------------------

constexpr float kMinNlEntityConfidence = 0.45f;
constexpr float kMinTitleConfidence = 0.55f;
constexpr size_t kMaxGlinerChars = 2000;

bool entityTextOverlapsTitle(std::string_view entityText, std::string_view titleText) {
    const std::string normEntity = search::normalizeEntityTextForKey(entityText);
    const std::string normTitle = search::normalizeEntityTextForKey(titleText);
    if (normEntity.empty() || normTitle.empty())
        return false;
    return normTitle.find(normEntity) != std::string::npos ||
           (normEntity.size() >= 4 && normEntity.find(normTitle) != std::string::npos);
}

struct TextSegmentWindow {
    std::string text;
    std::size_t startOffset = 0;
    std::size_t endOffset = 0;
};

std::vector<TextSegmentWindow> buildBodyClaimSegments(std::string_view textSnippet,
                                                      std::string_view titleText,
                                                      std::size_t maxSegments = 3) {
    std::vector<TextSegmentWindow> segments;
    if (textSnippet.empty() || maxSegments == 0)
        return segments;

    auto docSections = extraction::util::detectDocumentSections(textSnippet);
    auto flushSegment = [&](std::size_t segStart, std::size_t segEnd) {
        if (segEnd <= segStart)
            return;
        auto raw = std::string(textSnippet.substr(segStart, segEnd - segStart));
        auto cleaned = search::trimAndCollapseWhitespace(raw);
        if (cleaned.size() < 24)
            return;
        segments.push_back({std::move(cleaned), segStart, segEnd});
    };

    if (!docSections.sections.empty() && docSections.sections.size() >= 2) {
        std::size_t totalSecs = docSections.sections.size();
        std::size_t perSec = std::max<std::size_t>(1u, maxSegments / totalSecs);
        std::size_t remainder = maxSegments % totalSecs;
        for (std::size_t si = 0; si < totalSecs && segments.size() < maxSegments; ++si) {
            const auto& sec = docSections.sections[si];
            std::size_t secBudget = perSec + (si < remainder ? 1 : 0);
            if (secBudget == 0)
                continue;
            std::string_view secText =
                textSnippet.substr(sec.startOffset, sec.endOffset - sec.startOffset);
            std::size_t sentenceStart = 0, sentenceCount = 0, secSegments = 0;
            for (std::size_t i = 0; i < secText.size() && secSegments < secBudget; ++i) {
                const char c = secText[i];
                bool boundary = (c == '.' || c == '!' || c == '?' || c == '\n');
                if (boundary)
                    ++sentenceCount;
                if (!boundary)
                    continue;
                if (sentenceCount >= 2 || c == '\n') {
                    flushSegment(sec.startOffset + sentenceStart, sec.startOffset + i + 1);
                    sentenceStart = i + 1;
                    sentenceCount = 0;
                    ++secSegments;
                }
            }
            if (secSegments < secBudget && sentenceStart < secText.size())
                flushSegment(sec.startOffset + sentenceStart, sec.endOffset);
        }
    }

    if (segments.empty()) {
        std::size_t start = 0;
        if (!titleText.empty()) {
            const std::string normTitle = search::normalizeEntityTextForKey(titleText);
            const auto newline = textSnippet.find('\n');
            if (newline != std::string_view::npos) {
                if (search::normalizeEntityTextForKey(textSnippet.substr(0, newline)) == normTitle)
                    start = newline + 1;
            }
        }
        std::size_t sentenceStart = start, sentenceCount = 0;
        for (std::size_t i = start; i < textSnippet.size() && segments.size() < maxSegments; ++i) {
            const char c = textSnippet[i];
            bool boundary = (c == '.' || c == '!' || c == '?' || c == '\n');
            if (!boundary)
                continue;
            ++sentenceCount;
            if (sentenceCount >= 2 || c == '\n') {
                flushSegment(sentenceStart, i + 1);
                sentenceStart = i + 1;
                sentenceCount = 0;
            }
        }
        if (segments.size() < maxSegments)
            flushSegment(sentenceStart, textSnippet.size());
    }
    return segments;
}

std::string coOccurrenceRelation(std::string_view lhsType, std::string_view rhsType) {
    bool lhsBio = isHighValueGraphType(lhsType);
    bool rhsBio = isHighValueGraphType(rhsType);
    if (lhsBio && rhsBio)
        return "co_occurs_biomedical";
    if ((lhsType == "protein" && rhsType == "cell") || (lhsType == "cell" && rhsType == "protein"))
        return "protein_cell_association";
    if ((lhsType == "protein" && rhsType == "disease") ||
        (lhsType == "disease" && rhsType == "protein"))
        return "protein_disease_association";
    if ((lhsType == "drug" && rhsType == "disease") || (lhsType == "disease" && rhsType == "drug"))
        return "drug_disease_association";
    return "co_mentioned_with";
}

bool isUsefulNlEntity(const search::QueryConcept& qc) {
    if (qc.text.empty() || qc.confidence < kMinNlEntityConfidence)
        return false;
    if (qc.text.size() > 160)
        return false;
    bool hasAlphaNum = false;
    for (unsigned char c : qc.text)
        if (std::isalnum(c)) {
            hasAlphaNum = true;
            break;
        }
    if (!hasAlphaNum)
        return false;
    std::string nt = search::normalizeEntityTextForKey(qc.text);
    std::string ntype = search::canonicalizeEntityType(qc.type, qc.text);
    if (search::isLowValueEntityText(nt, ntype))
        return false;
    return true;
}

struct NlAliasVariant {
    std::string text;
    float confidence = 1.0f;
    std::string sourceTag;
};

std::vector<NlAliasVariant> buildNlAliasVariants(const std::string& entityText,
                                                 const std::string& entityType,
                                                 float baseConfidence) {
    std::vector<NlAliasVariant> variants;
    std::unordered_set<std::string> seen;
    const auto kind = search::surfaceVariantKindForEntityType(entityType);
    auto add = [&](const std::string& v, float cs, std::string st) {
        std::string n = search::normalizeEntityTextForKey(v);
        if (n.size() < 2 || !seen.insert(n).second)
            return;
        variants.push_back(
            {std::move(n), std::clamp(baseConfidence * cs, 0.05f, 1.0f), std::move(st)});
    };
    auto addGen = [&](const std::string& v, float ps, float ss, std::string pS, std::string sS) {
        auto g = search::generateSurfaceVariants(v, kind, 8);
        for (size_t i = 0; i < g.size() && variants.size() < 8; ++i)
            add(g[i], i == 0 ? ps : ss, i == 0 ? pS : sS);
    };
    addGen(entityText, 1.0f, 0.72f, "surface", "variant");
    if (!entityType.empty())
        addGen(entityType + " " + entityText, 0.95f, 0.68f, "type_qualified", "type_qualified");
    return variants;
}

std::string normalizeGraphPath(const std::string& path) {
    if (path.empty())
        return {};
    try {
        auto derived = metadata::computePathDerivedValues(path);
        if (!derived.normalizedPath.empty())
            return derived.normalizedPath;
    } catch (const std::exception&) {
        return path;
    }
    return path;
}
std::string makePathFileNodeKey(const std::string& path) {
    return "path:file:" + normalizeGraphPath(path);
}

} // namespace

// =========================================================================
// KG Stage
// =========================================================================

void PostIngestQueue::processKnowledgeGraphBatch(std::vector<InternalEventBus::KgJob>&& jobs) {
    if (jobs.empty())
        return;
    for (const auto& j : jobs) {
        (void)j;
        InternalEventBus::instance().incKgConsumed();
    }
    if (!graphComponent_) {
        if (!jobs.empty())
            spdlog::warn("[PostIngestQueue] KG batch skipped ({} jobs) - no graphComponent",
                         jobs.size());
        return;
    }
    auto t0 = std::chrono::steady_clock::now();
    std::vector<GraphComponent::DocumentGraphContext> ctxs;
    ctxs.reserve(jobs.size());
    for (auto& job : jobs)
        ctxs.push_back({.documentHash = std::move(job.hash),
                        .filePath = std::move(job.filePath),
                        .tags = std::move(job.tags),
                        .documentDbId = job.documentId,
                        .contentBytes = std::move(job.contentBytes),
                        .skipEntityExtraction = false});
    auto r = graphComponent_->onDocumentsIngestedBatch(ctxs);
    double ms =
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
    if (!r)
        spdlog::error("[PostIngestQueue] KG batch failed: {}", r.error().message);
}

void PostIngestQueue::dispatchToKgChannel(const std::string& hash, int64_t docId,
                                          const std::string& filePath,
                                          std::vector<std::string> tags,
                                          std::shared_ptr<std::vector<std::byte>> contentBytes) {
    bool active = graphComponent_ && !stagePaused_[1].load(std::memory_order_acquire) &&
                  maxKgConcurrent() > 0;
    if (!active) {
        InternalEventBus::instance().incKgDropped();
        return;
    }
    auto ch = kgChannel_;
    InternalEventBus::KgJob job{hash, docId, filePath, std::move(tags), std::move(contentBytes)};
    if (ch->try_push(std::move(job))) {
        InternalEventBus::instance().incKgQueued();
        TuningManager::notifyWakeup();
        return;
    }
    if (!ch->push_wait(std::move(job), std::chrono::milliseconds(10)))
        InternalEventBus::instance().incKgDropped();
    else {
        InternalEventBus::instance().incKgQueued();
        TuningManager::notifyWakeup();
    }
}

boost::asio::awaitable<void> PostIngestQueue::kgPoller() {
    auto ch = kgChannel_;
    PressureLimitedPollerConfig<InternalEventBus::KgJob> cfg;
    cfg.stageName = "KG";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[1];
    cfg.pauseFlag = &stagePaused_[1];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[1];
    cfg.getLimiterFn = [this] { return kgLimiter(); };
    cfg.maxConcurrentFn = &maxKgConcurrent;
    cfg.tryAcquireFn = [this](auto* l, auto& id, auto& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](auto& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this] { checkDrainAndSignal(); };
    cfg.notifyLifecycleFn = [this] { notifyLifecycle(); };
    cfg.executor = coordinator_->getExecutor();
    cfg.getHashFn = [](auto& j) -> std::string { return j.hash; };
    cfg.batchMode = true;
    cfg.batchSizeFn = [this] -> std::size_t {
        return adaptiveStageBatchSize(
            kgQueueDepth(), std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize()), 32u);
    };
    cfg.batchProcessFn = [this](auto&& jobs) { processKnowledgeGraphBatch(std::move(jobs)); };
    co_await pressureLimitedPoll(std::move(ch), std::move(cfg));
}

// =========================================================================
// Symbol Stage
// =========================================================================

void PostIngestQueue::processSymbolExtractionBatch(
    std::vector<InternalEventBus::SymbolExtractionJob>&& jobs) {
    for (const auto& j : jobs) {
        (void)j;
        InternalEventBus::instance().incSymbolConsumed();
    }
    if (!graphComponent_)
        return;
    std::unordered_set<std::string> seen;
    seen.reserve(jobs.size());
    for (auto& j : jobs) {
        if (j.hash.empty() || !seen.insert(j.hash).second)
            continue;
        try {
            const std::byte* dp = nullptr;
            std::size_t dl = 0;
            std::vector<std::byte> fb;
            if (j.contentBytes) {
                dp = j.contentBytes->data();
                dl = j.contentBytes->size();
            } else if (store_) {
                auto cr = store_->retrieveBytes(j.hash);
                if (cr) {
                    fb = std::move(cr.value());
                    dp = fb.data();
                    dl = fb.size();
                }
            }
            if (!dp)
                continue;
            GraphComponent::EntityExtractionJob ej{
                j.hash, j.filePath, std::move(j.language),
                std::string(reinterpret_cast<const char*>(dp), dl)};
            graphComponent_->submitEntityExtraction(std::move(ej));
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Symbol extraction failed: {} {}", j.hash, e.what());
        }
    }
}

void PostIngestQueue::dispatchToSymbolChannel(
    const std::string& hash, int64_t, const std::string& filePath, const std::string& language,
    std::shared_ptr<std::vector<std::byte>> contentBytes) {
    auto ch = symbolChannel_;
    InternalEventBus::SymbolExtractionJob job{hash, 0, filePath, language, std::move(contentBytes)};
    if (!ch->try_push(std::move(job))) {
        InternalEventBus::instance().incSymbolDropped();
        return;
    }
    InternalEventBus::instance().incSymbolQueued();
    TuningManager::notifyWakeup();
}

boost::asio::awaitable<void> PostIngestQueue::symbolPoller() {
    auto ch = symbolChannel_;
    PressureLimitedPollerConfig<InternalEventBus::SymbolExtractionJob> cfg;
    cfg.stageName = "symbol";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[2];
    cfg.pauseFlag = &stagePaused_[2];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[2];
    cfg.getLimiterFn = [this] { return symbolLimiter(); };
    cfg.maxConcurrentFn = &maxSymbolConcurrent;
    cfg.tryAcquireFn = [this](auto* l, auto& id, auto& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](auto& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this] { checkDrainAndSignal(); };
    cfg.notifyLifecycleFn = [this] { notifyLifecycle(); };
    cfg.executor = coordinator_->getExecutor();
    cfg.getHashFn = [](auto& j) -> std::string { return j.hash; };
    cfg.batchMode = true;
    cfg.batchSizeFn = [this] -> std::size_t {
        return adaptiveStageBatchSize(
            symbolQueueDepth(), std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize()), 16u);
    };
    cfg.batchProcessFn = [this](auto&& jobs) { processSymbolExtractionBatch(std::move(jobs)); };
    co_await pressureLimitedPoll(std::move(ch), std::move(cfg));
}

void PostIngestQueue::processSymbolExtractionStage(const std::string& hash, int64_t,
                                                   const std::string& filePath,
                                                   const std::string& language,
                                                   std::vector<std::byte>* contentBytes) {
    if (!graphComponent_)
        return;
    std::vector<std::byte> bytes;
    if (contentBytes)
        bytes = std::move(*contentBytes);
    else if (store_) {
        auto r = store_->retrieveBytes(hash);
        if (r)
            bytes = std::move(r.value());
        else
            return;
    } else
        return;
    GraphComponent::EntityExtractionJob ej{
        hash, filePath, language,
        std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size())};
    graphComponent_->submitEntityExtraction(std::move(ej));
}

// =========================================================================
// Entity Stage
// =========================================================================

void PostIngestQueue::dispatchToEntityChannel(
    const std::string& hash, int64_t, const std::string& filePath, const std::string& extension,
    std::shared_ptr<std::vector<std::byte>> contentBytes) {
    auto ch = entityChannel_;
    InternalEventBus::EntityExtractionJob job{hash, 0, filePath, extension,
                                              std::move(contentBytes)};
    if (!ch->try_push(std::move(job))) {
        InternalEventBus::instance().incEntityDropped();
        return;
    }
    InternalEventBus::instance().incEntityQueued();
    TuningManager::notifyWakeup();
    auto nth = entityDispatched_.fetch_add(1, std::memory_order_relaxed) + 1;
    if ((nth % 1000) == 0 || nth == 1)
        spdlog::info("[PIQ-entity] dispatched n={} ext={} file={}", nth, extension, filePath);
}

boost::asio::awaitable<void> PostIngestQueue::entityPoller() {
    auto ch = entityChannel_;
    PressureLimitedPollerConfig<InternalEventBus::EntityExtractionJob> cfg;
    cfg.stageName = "entity";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[3];
    cfg.pauseFlag = &stagePaused_[3];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[3];
    cfg.getLimiterFn = [this] { return entityLimiter(); };
    cfg.maxConcurrentFn = &maxEntityConcurrent;
    cfg.tryAcquireFn = [this](auto* l, auto& id, auto& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](auto& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this] { checkDrainAndSignal(); };
    cfg.notifyLifecycleFn = [this] { notifyLifecycle(); };
    cfg.executor =
        entityCoordinator_ ? entityCoordinator_->getExecutor() : coordinator_->getExecutor();
    cfg.getHashFn = [](auto& j) -> std::string { return j.hash; };
    cfg.batchMode = true;
    cfg.batchSizeFn = [this] -> std::size_t {
        return adaptiveStageBatchSize(
            entityQueueDepth(), std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize()), 16u);
    };
    cfg.batchProcessFn = [this](auto&& jobs) { processEntityExtractionBatch(std::move(jobs)); };
    co_await pressureLimitedPoll(std::move(ch), std::move(cfg));
}

void PostIngestQueue::processEntityExtractionBatch(
    std::vector<InternalEventBus::EntityExtractionJob>&& jobs) {
    if (jobs.empty())
        return;
    for (auto& j : jobs)
        processEntityExtractionStage(j.hash, j.documentId, j.filePath, j.extension,
                                     j.contentBytes.get());
}

void PostIngestQueue::processEntityExtractionStage(const std::string& hash, int64_t docId,
                                                   const std::string& filePath,
                                                   const std::string& extension,
                                                   std::vector<std::byte>* contentBytes) {
    std::shared_ptr<ExternalEntityProviderAdapter> prov;
    {
        std::lock_guard<std::mutex> lk(entityMutex_);
        for (auto& p : entityProviders_)
            if (p && p->supports(extension)) {
                prov = p;
                break;
            }
    }
    if (!prov) {
        InternalEventBus::instance().incEntityDropped();
        return;
    }

    std::vector<std::byte> content;
    if (contentBytes)
        content = std::move(*contentBytes);
    else if (store_) {
        auto r = store_->retrieveBytes(hash);
        if (r)
            content = std::move(r.value());
        else
            return;
    } else
        return;
    if (!kg_)
        return;

    size_t tn = 0, te = 0, ta = 0;
    std::unordered_map<std::string, size_t> etc;
    const std::string sid = hash;

    auto r = prov->extractEntitiesStreaming(
        content, filePath,
        [&](ExternalEntityProviderAdapter::EntityResult batch,
            const ExternalEntityProviderAdapter::ExtractionProgress& prog) -> bool {
            if (batch.nodes.empty())
                return true;
            bool hs = !sid.empty();
            auto wb = std::make_unique<WriteBatch>();
            wb->source = "PostIngestQueue::entityExtraction/" + hash.substr(0, 12) + "/batch/" +
                         std::to_string(prog.batchNumber);

            std::vector<metadata::KGNode> cn, vn;
            cn.reserve(batch.nodes.size());
            vn.reserve(batch.nodes.size());
            for (auto& node : batch.nodes) {
                cn.push_back(node);
                if (node.type && !node.type->empty())
                    etc[search::canonicalizeEntityType(*node.type, node.label.value_or(""))]++;
                else
                    etc["unknown"]++;
                if (hs) {
                    metadata::KGNode vn2 = node;
                    vn2.nodeKey = node.nodeKey + "@snap:" + sid;
                    vn2.type = node.type ? *node.type + "_version" : "entity_version";
                    nlohmann::json p = node.properties ? nlohmann::json::parse(*node.properties)
                                                       : nlohmann::json::object();
                    p["snapshot_id"] = sid;
                    p["document_hash"] = sid;
                    p["file_path"] = filePath;
                    p["canonical_key"] = node.nodeKey;
                    vn2.properties = p.dump();
                    vn.push_back(std::move(vn2));
                }
            }
            if (!cn.empty())
                wb->ops.emplace_back(UpsertNodesOp{std::move(cn)});
            if (!vn.empty())
                wb->ops.emplace_back(UpsertNodesOp{std::move(vn)});

            std::vector<DeferredEdge> des;
            des.reserve(batch.edges.size() + (hs ? batch.nodes.size() : 0));
            if (hs)
                for (auto& n : batch.nodes)
                    des.push_back(
                        {n.nodeKey, n.nodeKey + "@snap:" + sid, "observed_as", 1.0f,
                         nlohmann::json{{"snapshot_id", sid}, {"document_hash", sid}}.dump()});
            for (auto& e : batch.edges) {
                try {
                    if (!e.properties)
                        continue;
                    auto p = nlohmann::json::parse(*e.properties);
                    std::string sk = p.value("_src_key", ""), dk = p.value("_dst_key", "");
                    if (sk.empty() || dk.empty())
                        continue;
                    p.erase("_src_key");
                    p.erase("_dst_key");
                    des.push_back({hs ? sk + "@snap:" + sid : sk, hs ? dk + "@snap:" + sid : dk,
                                   e.relation, e.weight, p.dump()});
                } catch (...) {
                }
            }
            std::vector<metadata::KGAlias> das;
            das.reserve(batch.aliases.size());
            for (auto& a : batch.aliases)
                if (a.source && a.source->starts_with("_node_key:")) {
                    metadata::KGAlias al;
                    al.alias = a.alias;
                    al.source = std::string("ghidra|") + a.source->substr(10);
                    al.confidence = a.confidence;
                    das.push_back(std::move(al));
                }

            if (!des.empty())
                wb->ops.emplace_back(AddDeferredEdgesOp{std::move(des)});
            if (!das.empty())
                wb->ops.emplace_back(AddAliasesOp{std::move(das)});
            if (writeCoordinator_) {
                writeCoordinator_->enqueue(std::move(wb));
                tn += batch.nodes.size() * (hs ? 2 : 1);
                te += des.size();
                ta += das.size();
            }
            return true;
        });

    if (r) {
        std::string ts;
        for (auto& [t, c] : etc) {
            if (!ts.empty())
                ts += ", ";
            ts += t + "=" + std::to_string(c);
        }
        spdlog::info("[PIQ-entity] completed {} nodes={} edges={} aliases={} types=[{}]",
                     hash.substr(0, 12), tn, te, ta, ts);
    } else
        spdlog::warn("[PostIngestQueue] Entity extraction failed: {} {}", hash, r.error().message);
    InternalEventBus::instance().incEntityConsumed();
}

// =========================================================================
// Title Stage
// =========================================================================

void PostIngestQueue::dispatchToTitleChannel(const std::string& hash, int64_t docId,
                                             const std::string& textSnippet,
                                             const std::string& fallbackTitle,
                                             const std::string& filePath,
                                             const std::string& language,
                                             const std::string& mimeType) {
    auto ch = titleChannel_;
    InternalEventBus::TitleExtractionJob job{hash,     docId,    textSnippet, fallbackTitle,
                                             filePath, language, mimeType};
    auto cp = job;
    bool ok = ch->try_push(std::move(job));
    if (!ok) {
        TuningManager::notifyWakeup();
        for (int i = 0; i < 8 && !ok; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(4));
            ok = ch->try_push(std::move(cp));
        }
    }
    if (!ok)
        InternalEventBus::instance().incTitleDropped();
    else {
        InternalEventBus::instance().incTitleQueued();
        TuningManager::notifyWakeup();
    }
}

boost::asio::awaitable<void> PostIngestQueue::titlePoller() {
    auto ch = titleChannel_;
    PressureLimitedPollerConfig<InternalEventBus::TitleExtractionJob> cfg;
    cfg.stageName = "title";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[4];
    cfg.pauseFlag = &stagePaused_[4];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[4];
    cfg.getLimiterFn = [this] { return titleLimiter(); };
    cfg.maxConcurrentFn = &maxTitleConcurrent;
    cfg.tryAcquireFn = [this](auto* l, auto& id, auto& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](auto& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this] { checkDrainAndSignal(); };
    cfg.notifyLifecycleFn = [this] { notifyLifecycle(); };
    cfg.executor = coordinator_->getExecutor();
    cfg.getHashFn = [](auto& j) -> std::string { return j.hash; };
    cfg.isCapableFn = [this] -> bool { return hasTitleExtractor(); };
    cfg.batchMode = true;
    cfg.batchSizeFn = [this] -> std::size_t {
        return adaptiveStageBatchSize(
            titleQueueDepth(), std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize()), 32u);
    };
    cfg.batchProcessFn = [this](auto&& jobs) { processTitleExtractionBatch(std::move(jobs)); };
    co_await pressureLimitedPoll(std::move(ch), std::move(cfg));
}

void PostIngestQueue::processTitleExtractionBatch(
    std::vector<InternalEventBus::TitleExtractionJob>&& jobs) {
    if (jobs.empty())
        return;
    for (auto& j : jobs)
        processTitleExtractionStage(j.hash, j.documentId, j.textSnippet, j.fallbackTitle,
                                    j.filePath, j.language, j.mimeType);
}

void PostIngestQueue::processTitleExtractionStage(const std::string& hash, int64_t docId,
                                                  const std::string& textSnippet,
                                                  const std::string& fallbackTitle,
                                                  const std::string& filePath,
                                                  const std::string& language,
                                                  const std::string& /*mimeType*/) {
    titleNlDocsProcessed_.fetch_add(1, std::memory_order_relaxed);
    auto te = getTitleExtractor();
    if (!te)
        return;
    try {
        auto t0 = std::chrono::steady_clock::now();
        static const std::vector<std::string> kTypes = {
            "title",     "heading", "function",   "class",        "method",   "module",
            "file",      "symbol",  "person",     "organization", "location", "date",
            "event",     "product", "technology", "concept",      "protein",  "gene",
            "cell",      "disease", "chemical",   "drug",         "pathway",  "biological_process",
            "biomarker", "anatomy", "organism",
        };
        static const std::unordered_set<std::string> kTT = {
            "title", "heading", "function", "class", "method", "module", "file", "symbol"};
        auto r = te(textSnippet, kTypes);
        if (!r || !r.value().usedGliner || r.value().concepts.empty()) {
            InternalEventBus::instance().incTitleConsumed();
            return;
        }

        const search::QueryConcept* bt = nullptr;
        std::unordered_map<std::string, const search::QueryConcept*> ne;
        ne.reserve(r.value().concepts.size());
        for (auto& qc : r.value().concepts) {
            if (qc.text.empty())
                continue;
            if (kTT.count(qc.type)) {
                if (qc.confidence >= kMinTitleConfidence && (!bt || qc.confidence > bt->confidence))
                    bt = &qc;
            } else {
                if (!isUsefulNlEntity(qc))
                    continue;
                std::string k = search::canonicalizeEntityType(qc.type, qc.text) + ":" +
                                search::normalizeEntityTextForKey(qc.text);
                auto it = ne.find(k);
                if (it == ne.end() || qc.confidence > it->second->confidence)
                    ne[std::move(k)] = &qc;
            }
        }
        std::vector<const search::QueryConcept*> nl;
        nl.reserve(ne.size());
        for (auto& [_, p] : ne)
            nl.push_back(p);
        std::sort(nl.begin(), nl.end(),
                  [](auto a, auto b) { return a->confidence > b->confidence; });
        if (!nl.empty()) {
            titleNlDocsWithEntities_.fetch_add(1, std::memory_order_relaxed);
            titleNlEntitiesExtracted_.fetch_add(nl.size(), std::memory_order_relaxed);
        }

        if (bt) {
            auto nt = extraction::util::normalizeTitleCandidate(bt->text);
            if (!nt.empty() && nt != fallbackTitle && meta_ && docId >= 0 && writeCoordinator_) {
                auto wb = std::make_unique<WriteBatch>();
                wb->source = "PostIngestQueue::titleExtraction/title";
                wb->ops.emplace_back(
                    SetMetadataBatchOp{{{docId, "title", metadata::MetadataValue(nt)}}});
                writeCoordinator_->enqueue(std::move(wb));
            }
        }

        if (!nl.empty() && writeCoordinator_ && kg_) {
            auto batch = std::make_unique<DeferredKGBatch>();
            batch->nodes.reserve(nl.size() + 6);
            batch->deferredEdges.reserve(nl.size() * 4 + 8);
            batch->aliases.reserve(nl.size() * 3);
            std::string np = normalizeGraphPath(filePath);
            batch->sourceFile = np.empty() ? filePath : np;
            auto now = std::chrono::system_clock::now().time_since_epoch().count();
            batch->documentIdToDelete = docId;

            std::string dnk, fnk;
            if (!hash.empty()) {
                dnk = "doc:" + hash;
                metadata::KGNode dn;
                dn.nodeKey = dnk;
                dn.label = common::sanitizeUtf8(filePath);
                dn.type = "document";
                dn.properties = nlohmann::json{{"hash", hash},
                                               {"path", common::sanitizeUtf8(batch->sourceFile)},
                                               {"language", common::sanitizeUtf8(language)}}
                                    .dump();
                batch->nodes.push_back(std::move(dn));
            }
            if (!filePath.empty()) {
                fnk = makePathFileNodeKey(batch->sourceFile);
                metadata::KGNode fn;
                fn.nodeKey = fnk;
                fn.label = common::sanitizeUtf8(batch->sourceFile);
                fn.type = "file";
                nlohmann::json fp{{"path", common::sanitizeUtf8(batch->sourceFile)},
                                  {"language", common::sanitizeUtf8(language)}};
                if (!batch->sourceFile.empty())
                    fp["basename"] = common::sanitizeUtf8(
                        std::filesystem::path(batch->sourceFile).filename().string());
                if (!hash.empty())
                    fp["current_hash"] = hash;
                fn.properties = fp.dump();
                batch->nodes.push_back(std::move(fn));
            }

            std::string tnk = dnk.empty() ? fnk : dnk;
            std::string et = fallbackTitle.empty() ? filePath : fallbackTitle;
            std::string tsnk, ssnk;
            struct SR {
                std::string nk, r;
                std::size_t so = 0, eo = 0;
            };
            std::vector<SR> bss;

            auto as = [&](std::string sk, std::string lb, std::string st, std::string rg, float cf,
                          std::size_t so, std::size_t eo) {
                if (sk.empty() || lb.empty())
                    return;
                metadata::KGNode sn;
                sn.nodeKey = sk;
                sn.label = common::sanitizeUtf8(lb);
                sn.type = st;
                sn.properties =
                    nlohmann::json{
                        {"segment_type", st}, {"region", rg},
                        {"confidence", cf},   {"start_offset", so},
                        {"end_offset", eo},   {"path", common::sanitizeUtf8(batch->sourceFile)},
                        {"snapshot_id", hash}}
                        .dump();
                batch->nodes.push_back(std::move(sn));
                segmentNodesCreated_.fetch_add(1, std::memory_order_relaxed);
                if (rg == "body_claim")
                    bodySegmentNodesCreated_.fetch_add(1, std::memory_order_relaxed);
                if (!tnk.empty()) {
                    batch->deferredEdges.push_back(
                        {tnk, sk, "contains_segment", cf,
                         nlohmann::json{{"source", "gliner"}, {"region", rg}, {"confidence", cf}}
                             .dump()});
                    segmentEdgesCreated_.fetch_add(1, std::memory_order_relaxed);
                    batch->deferredEdges.push_back(
                        {std::move(sk), tnk, "segment_of", cf,
                         nlohmann::json{{"source", "gliner"}, {"region", rg}, {"confidence", cf}}
                             .dump()});
                    segmentEdgesCreated_.fetch_add(1, std::memory_order_relaxed);
                }
            };

            if (!hash.empty() && !et.empty()) {
                tsnk = "segment:title:" + hash;
                as(tsnk, et, "text_segment", "title",
                   bt ? std::clamp(bt->confidence, 0.5f, 1.0f) : 0.75f, 0, et.size());
            }
            if (!hash.empty() && !textSnippet.empty()) {
                ssnk = "segment:summary:" + hash;
                as(ssnk, textSnippet, "text_segment", "summary", 0.65f, 0, textSnippet.size());
                auto cs = buildBodyClaimSegments(textSnippet, et, 3);
                for (std::size_t si = 0; si < cs.size(); ++si) {
                    std::string sk = "segment:body:" + hash + ":" + std::to_string(si + 1);
                    as(sk, cs[si].text, "text_segment", "body_claim", 0.60f, cs[si].startOffset,
                       cs[si].endOffset);
                    bss.push_back({sk, "body_claim", cs[si].startOffset, cs[si].endOffset});
                }
            }

            struct ER {
                std::string nk, ty, tx;
                float cf = 0;
                bool to = false, hv = false;
                int si = -1;
            };
            std::vector<ER> ers;
            ers.reserve(nl.size());
            for (auto* qc : nl) {
                std::string tx = common::sanitizeUtf8(qc->text);
                std::string ty =
                    common::sanitizeUtf8(search::canonicalizeEntityType(qc->type, qc->text));
                std::string nk = "nl_entity:" + ty + ":" + search::normalizeEntityTextForKey(tx);
                metadata::KGNode n;
                n.nodeKey = nk;
                n.label = tx;
                n.type = ty;
                n.properties = nlohmann::json{
                    {"entity_text", tx},
                    {"entity_type", ty},
                    {"confidence", qc->confidence},
                    {"first_seen_file", common::sanitizeUtf8(filePath)},
                    {"last_seen", now},
                    {"first_seen_hash",
                     hash}}.dump();
                batch->nodes.push_back(std::move(n));
                bool to = entityTextOverlapsTitle(tx, et);
                bool hv = isHighValueGraphType(ty);
                int si = -1;
                if (!to)
                    for (std::size_t bi = 0; bi < bss.size(); ++bi)
                        if (qc->startOffset < bss[bi].eo && qc->endOffset > bss[bi].so) {
                            si = (int)bi;
                            break;
                        }
                ers.push_back({nk, ty, tx, qc->confidence, to, hv, si});
                for (auto& av : buildNlAliasVariants(tx, ty, qc->confidence)) {
                    metadata::KGAlias al;
                    al.alias = av.text;
                    al.source = "gliner." + av.sourceTag + "|" + nk;
                    al.confidence = av.confidence;
                    batch->aliases.push_back(std::move(al));
                }
                if (!tnk.empty()) {
                    batch->deferredEdges.push_back(
                        {nk, tnk, "mentioned_in", qc->confidence,
                         nlohmann::json{
                             {"source", "gliner"},
                             {"confidence", qc->confidence},
                             {"provenance",
                              nlohmann::json{{"source", "gliner"}, {"confidence", qc->confidence}}},
                             {"snapshot_id", hash}}
                             .dump()});
                    if (to)
                        batch->deferredEdges.push_back(
                            {nk, tnk, "title_mentions", std::min(1.0f, qc->confidence * 1.15f),
                             nlohmann::json{{"source", "gliner"},
                                            {"region", "title"},
                                            {"confidence", std::min(1.0f, qc->confidence * 1.15f)},
                                            {"snapshot_id", hash}}
                                 .dump()});
                }
                if (to && !tsnk.empty()) {
                    batch->deferredEdges.push_back(
                        {nk, tsnk, "mentioned_in_segment", std::min(1.0f, qc->confidence * 1.10f),
                         nlohmann::json{{"source", "gliner"},
                                        {"region", "title"},
                                        {"confidence", std::min(1.0f, qc->confidence * 1.10f)}}
                             .dump()});
                    entitySegmentEdgesCreated_.fetch_add(1, std::memory_order_relaxed);
                } else if (si >= 0 && (std::size_t)si < bss.size()) {
                    batch->deferredEdges.push_back(
                        {nk, bss[si].nk, "mentioned_in_segment",
                         std::min(1.0f, qc->confidence * 1.05f),
                         nlohmann::json{{"source", "gliner"},
                                        {"region", "body_claim"},
                                        {"confidence", std::min(1.0f, qc->confidence * 1.05f)},
                                        {"segment_index", si}}
                             .dump()});
                    entitySegmentEdgesCreated_.fetch_add(1, std::memory_order_relaxed);
                    bodyEntitySegmentEdgesCreated_.fetch_add(1, std::memory_order_relaxed);
                } else if (!ssnk.empty()) {
                    batch->deferredEdges.push_back({nk, ssnk, "mentioned_in_segment",
                                                    qc->confidence,
                                                    nlohmann::json{{"source", "gliner"},
                                                                   {"region", "summary"},
                                                                   {"confidence", qc->confidence}}
                                                        .dump()});
                    entitySegmentEdgesCreated_.fetch_add(1, std::memory_order_relaxed);
                }
                DeferredDocEntity de;
                de.documentId = docId;
                de.entityText = tx;
                de.nodeKey = nk;
                de.startOffset = qc->startOffset;
                de.endOffset = qc->endOffset;
                de.confidence = qc->confidence;
                de.extractor = "gliner_title_nl";
                batch->deferredDocEntities.push_back(std::move(de));
                deferredDocEntitiesQueued_.fetch_add(1, std::memory_order_relaxed);
            }

            std::stable_sort(ers.begin(), ers.end(), [](auto& a, auto& b) {
                if (a.to != b.to)
                    return a.to > b.to;
                if (a.hv != b.hv)
                    return a.hv > b.hv;
                return a.cf > b.cf;
            });
            constexpr std::size_t kMP = 3;
            std::size_t pc = 0;
            for (auto& r : ers) {
                if (pc >= kMP)
                    break;
                if (!r.hv || (!r.to && r.cf < 0.78f) || tnk.empty())
                    continue;
                batch->deferredEdges.push_back(
                    {r.nk, tnk, "primary_topic_of", std::min(1.0f, r.cf * (r.to ? 1.20f : 1.05f)),
                     nlohmann::json{{"source", "gliner"},
                                    {"confidence", std::min(1.0f, r.cf * (r.to ? 1.20f : 1.05f))},
                                    {"title_overlap", r.to},
                                    {"entity_type", r.ty},
                                    {"snapshot_id", hash}}
                         .dump()});
                ++pc;
            }

            constexpr std::size_t kME = 8, kMaxE = 16;
            std::size_t el = std::min(ers.size(), kME), cc = 0;
            for (std::size_t i = 0; i < el && cc < kMaxE; ++i)
                for (std::size_t j = i + 1; j < el && cc < kMaxE; ++j) {
                    if (!ers[i].hv && !ers[j].hv)
                        continue;
                    float cf = (ers[i].hv && ers[j].hv) ? 0.20f : 0.08f;
                    float w = std::max(cf, std::min(ers[i].cf, ers[j].cf) *
                                               ((ers[i].to || ers[j].to) ? 0.85f : 0.65f));
                    batch->deferredEdges.push_back({ers[i].nk, ers[j].nk,
                                                    coOccurrenceRelation(ers[i].ty, ers[j].ty), w,
                                                    nlohmann::json{{"source", "gliner"},
                                                                   {"confidence", w},
                                                                   {"lhs_type", ers[i].ty},
                                                                   {"rhs_type", ers[j].ty},
                                                                   {"snapshot_id", hash}}
                                                        .dump()});
                    ++cc;
                }

            {
                auto nth = gs_processed_.fetch_add(1, std::memory_order_relaxed) + 1;
                gs_totalEntities_ += nl.size();
                gs_highValueEntities_ +=
                    std::count_if(ers.begin(), ers.end(), [](auto& x) { return x.hv; });
                gs_totalEdges_ += cc;
                gs_totalPrimaryTopicEdges_ += pc;
            }

            if (writeCoordinator_) {
                // Update KG atomics immediately so overlay stats see data
                if (kg_)
                    kg_->updateEnqueueCounts(static_cast<std::int64_t>(nl.size()),
                                             static_cast<std::int64_t>(batch->deferredEdges.size()),
                                             static_cast<std::int64_t>(batch->aliases.size()));
                writeCoordinator_->enqueue(makeWriteBatchFromDeferredKGBatch(
                    std::move(batch), "PostIngestQueue::nlEntityKg/" + batch->sourceFile));
            }
        }
        InternalEventBus::instance().incTitleConsumed();
        auto d = std::chrono::steady_clock::now() - t0;
        spdlog::info("[PostIngestQueue] Title+NL for {} in {:.2f}ms (title={}, nl={})",
                     hash.substr(0, 12), std::chrono::duration<double, std::milli>(d).count(),
                     bt ? "yes" : "no", nl.size());
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Title+NL failed for {}: {}", hash, e.what());
    }
}

} // namespace yams::daemon
