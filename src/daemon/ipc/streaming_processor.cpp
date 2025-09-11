#include <yams/daemon/ipc/streaming_processor.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <variant>
#include <yams/daemon/ipc/mux_metrics_registry.h>

namespace yams::daemon {

std::size_t
StreamingRequestProcessor::compute_item_chunk_count(std::size_t approx_bytes_per_item) const {
    std::size_t target = cfg_.chunk_size > 0
                             ? (cfg_.chunk_size / std::max<std::size_t>(approx_bytes_per_item, 1))
                             : 256;
    auto snap = MuxMetricsRegistry::instance().snapshot();
    const int64_t q = snap.queuedBytes;
    if (q > static_cast<int64_t>(256ull * 1024ull * 1024ull)) {
        target /= 4; // heavy backlog: smaller pages
    } else if (q > static_cast<int64_t>(128ull * 1024ull * 1024ull)) {
        target /= 2;
    } else if (q < static_cast<int64_t>(8ull * 1024ull * 1024ull)) {
        target *= 3; // very light: triple page size
    } else if (q < static_cast<int64_t>(32ull * 1024ull * 1024ull)) {
        target *= 2; // light: double page size
    } else if (q < static_cast<int64_t>(64ull * 1024ull * 1024ull)) {
        target += (target / 2); // moderately light: +50%
    }
    // Widen clamps to allow much larger pages under light load and smaller under heavy load
    target = std::clamp<std::size_t>(target, 5, 50000);
    return target;
}

// NOTE:
// This is a simplified, deterministic implementation of StreamingRequestProcessor.
// Legacy staging / fast vs. full futures, adaptive keepalives, and environment
// overrides have been removed for clarity. The goals:
//  1. Always emit exactly one initial "heartbeat"/typed starter chunk after the handler
//     writes the header frame.
//  2. For Search/List/Grep: on the SECOND next_chunk() call, compute the full
//     response via delegate_->process(), store it, and begin paging deterministically.
//  3. For BatchEmbeddingRequest / EmbedDocumentsRequest: on the SECOND next_chunk(),
//     compute once and emit the final full response (is_last_chunk = true).
//  4. For AddDocumentRequest (and similar single-shot types we decide to stream):
//     same pattern: heartbeat first, final response second.
//  5. For all other request types: fall back to delegate (non‑streamed) unless
//     they were explicitly deferred in process_streaming().
//
// Paging logic uses compute_item_chunk_count() to size the next page, influenced
// by mux metrics (still dynamic but deterministic per call order).

// -------------------- process (non-streaming immediate) --------------------
boost::asio::awaitable<Response> StreamingRequestProcessor::process(const Request& request) {
    co_return co_await delegate_->process(request);
}

// -------------------- process_streaming (decide whether to defer) ----------
boost::asio::awaitable<std::optional<Response>>
StreamingRequestProcessor::process_streaming(const Request& request) {
    try {
        // Use MessageType classification first to avoid variant-dependent ambiguity.
        switch (getMessageType(request)) {
            case MessageType::BatchEmbeddingRequest:
                spdlog::debug("StreamingRequestProcessor: defer BatchEmbedding (deterministic)");
                mode_ = Mode::BatchEmbed;
                pending_request_ = request;
                co_return std::nullopt;
            case MessageType::EmbedDocumentsRequest:
                spdlog::debug("StreamingRequestProcessor: defer EmbedDocuments (deterministic)");
                mode_ = Mode::EmbedDocs;
                pending_request_ = request;
                co_return std::nullopt;
            default:
                break;
        }

        if (std::holds_alternative<SearchRequest>(request)) {
            spdlog::debug("StreamingRequestProcessor: defer Search for deterministic paging");
            mode_ = Mode::Search;
            pending_request_ = request;
            co_return std::nullopt;
        }
        if (std::holds_alternative<ListRequest>(request)) {
            spdlog::debug("StreamingRequestProcessor: defer List for deterministic paging");
            mode_ = Mode::List;
            pending_request_ = request;
            co_return std::nullopt;
        }
        if (std::holds_alternative<GrepRequest>(request)) {
            spdlog::debug("StreamingRequestProcessor: defer Grep for deterministic paging");
            mode_ = Mode::Grep;
            pending_request_ = request;
            co_return std::nullopt;
        }
        if (std::holds_alternative<AddDocumentRequest>(request)) {
            spdlog::debug("StreamingRequestProcessor: defer AddDocument for header-first");
            // Mode None: single final chunk after heartbeat
            pending_request_ = request;
            co_return std::nullopt;
        }
        // Fallback: defer GenerateEmbedding / LoadModel to provide typed start events.
        if (std::holds_alternative<GenerateEmbeddingRequest>(request) ||
            std::holds_alternative<LoadModelRequest>(request)) {
            spdlog::debug("StreamingRequestProcessor: defer single-step embedding/model load");
            pending_request_ = request;
            co_return std::nullopt;
        }

        // Not a streaming-recognized request: delegate immediately.
        co_return co_await delegate_->process(request);
    } catch (...) {
        // On unexpected failure; choose streaming path so caller can still progress.
        pending_request_ = request;
        co_return std::nullopt;
    }
}

// -------------------- supports_streaming -----------------------------------
bool StreamingRequestProcessor::supports_streaming(const Request& request) const {
    if (std::holds_alternative<SearchRequest>(request) ||
        std::holds_alternative<ListRequest>(request) ||
        std::holds_alternative<GrepRequest>(request) ||
        std::holds_alternative<AddDocumentRequest>(request) ||
        std::holds_alternative<BatchEmbeddingRequest>(request) ||
        std::holds_alternative<EmbedDocumentsRequest>(request) ||
        std::holds_alternative<GenerateEmbeddingRequest>(request) ||
        std::holds_alternative<LoadModelRequest>(request)) {
        return true;
    }
    return delegate_->supports_streaming(request);
}

// -------------------- next_chunk (deterministic) ---------------------------
boost::asio::awaitable<RequestProcessor::ResponseChunk> StreamingRequestProcessor::next_chunk() {
    try {
        // 1) If we have a pending request and have not yet sent the initial heartbeat,
        //    synthesize a typed starter frame (never last).
        if (pending_request_.has_value() && !heartbeat_sent_) {
            heartbeat_sent_ = true;
            auto mt = getMessageType(*pending_request_);

            switch (mt) {
                case MessageType::SearchRequest: {
                    SearchResponse r;
                    r.totalCount = 0;
                    r.elapsed = std::chrono::milliseconds(0);
                    co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
                }
                case MessageType::ListRequest: {
                    ListResponse r;
                    r.totalCount = 0;
                    co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
                }
                case MessageType::GrepRequest: {
                    GrepResponse r;
                    r.totalMatches = 0;
                    r.filesSearched = 0;
                    co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
                }
                case MessageType::BatchEmbeddingRequest:
                case MessageType::EmbedDocumentsRequest:
                case MessageType::GenerateEmbeddingRequest: {
                    EmbeddingEvent ev{};
                    if (auto* r = std::get_if<BatchEmbeddingRequest>(&*pending_request_))
                        ev.modelName = r->modelName;
                    if (auto* r = std::get_if<EmbedDocumentsRequest>(&*pending_request_))
                        ev.modelName = r->modelName;
                    if (auto* r = std::get_if<GenerateEmbeddingRequest>(&*pending_request_))
                        ev.modelName = r->modelName;
                    if (mt == MessageType::EmbedDocumentsRequest) {
                        ev.total = std::get<EmbedDocumentsRequest>(*pending_request_)
                                       .documentHashes.size();
                    } else if (mt == MessageType::BatchEmbeddingRequest) {
                        ev.total = std::get<BatchEmbeddingRequest>(*pending_request_).texts.size();
                    } else {
                        ev.total = 1;
                    }
                    ev.phase = "started";
                    ev.message = "embedding started";
                    co_return ResponseChunk{.data = Response{std::move(ev)},
                                            .is_last_chunk = false};
                }
                case MessageType::LoadModelRequest: {
                    ModelLoadEvent mev{};
                    mev.phase = "started";
                    mev.message = "load started";
                    mev.modelName = std::get<LoadModelRequest>(*pending_request_).modelName;
                    co_return ResponseChunk{.data = Response{std::move(mev)},
                                            .is_last_chunk = false};
                }
                default: {
                    SuccessResponse ok{"Streaming started"};
                    co_return ResponseChunk{.data = Response{std::move(ok)},
                                            .is_last_chunk = false};
                }
            }
        }

        // 2) After the heartbeat has been sent, act based on mode_ or message type.
        if (pending_request_.has_value()) {
            auto mt = getMessageType(*pending_request_);

            // Embedding & model load types: compute once now and finish.
            if (mt == MessageType::BatchEmbeddingRequest ||
                mt == MessageType::EmbedDocumentsRequest ||
                mt == MessageType::GenerateEmbeddingRequest ||
                mt == MessageType::LoadModelRequest) {
                auto final = co_await delegate_->process(*pending_request_);
                pending_request_.reset();
                co_return ResponseChunk{.data = std::move(final), .is_last_chunk = true};
            }

            // AddDocumentRequest: single final response after heartbeat.
            if (mt == MessageType::AddDocumentRequest) {
                auto final = co_await delegate_->process(*pending_request_);
                pending_request_.reset();
                co_return ResponseChunk{.data = std::move(final), .is_last_chunk = true};
            }

            // Search/List/Grep initial compute (first post-heartbeat chunk)
            if (mode_ == Mode::Search && !search_.has_value()) {
                auto r = co_await delegate_->process(*pending_request_);
                if (auto* s = std::get_if<SearchResponse>(&r)) {
                    search_ = SearchState{};
                    search_->results = std::move(s->results);
                    search_->totalCount = s->totalCount;
                    search_->elapsed = s->elapsed;
                    search_->pos = 0;
                } else {
                    pending_request_.reset();
                    co_return ResponseChunk{.data = std::move(r), .is_last_chunk = true};
                }
            } else if (mode_ == Mode::List && !list_.has_value()) {
                auto r = co_await delegate_->process(*pending_request_);
                if (auto* l = std::get_if<ListResponse>(&r)) {
                    list_ = ListState{};
                    list_->items = std::move(l->items);
                    list_->totalCount = l->totalCount;
                    list_->pos = 0;
                } else {
                    pending_request_.reset();
                    co_return ResponseChunk{.data = std::move(r), .is_last_chunk = true};
                }
            } else if (mode_ == Mode::Grep && !grep_.has_value()) {
                auto r = co_await delegate_->process(*pending_request_);
                if (auto* g = std::get_if<GrepResponse>(&r)) {
                    grep_ = GrepState{};
                    grep_->matches = std::move(g->matches);
                    grep_->totalMatches = g->totalMatches;
                    grep_->filesSearched = g->filesSearched;
                    grep_->pos = 0;
                } else {
                    pending_request_.reset();
                    co_return ResponseChunk{.data = std::move(r), .is_last_chunk = true};
                }
            }
        }

        // 3) Emit next page (Search)
        if (mode_ == Mode::Search) {
            if (!search_.has_value()) {
                // Still computing? Should not happen in deterministic path, fallback
                // keepalive-like.
                SearchResponse r;
                r.totalCount = 0;
                r.elapsed = std::chrono::milliseconds(0);
                co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
            auto& st = *search_;
            const std::size_t total = st.results.size();
            const std::size_t n = compute_item_chunk_count(512);
            const std::size_t start = st.pos;
            const std::size_t end = std::min(start + n, total);

            SearchResponse chunk;
            chunk.totalCount = st.totalCount;
            chunk.elapsed = st.elapsed;
            chunk.results.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                chunk.results.push_back(std::move(st.results[i]));
            }
            st.pos = end;
            bool last = (end >= total);
            if (last) {
                // clear pending only once we have emitted all pages
                pending_request_.reset();
            }
            co_return ResponseChunk{.data = Response{std::move(chunk)}, .is_last_chunk = last};
        }

        // 4) Emit next page (List)
        if (mode_ == Mode::List) {
            if (!list_.has_value()) {
                ListResponse r;
                r.totalCount = 0;
                co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
            auto& st = *list_;
            const std::size_t total = st.items.size();
            const std::size_t n = compute_item_chunk_count(2048);
            const std::size_t start = st.pos;
            const std::size_t end = std::min(start + n, total);

            ListResponse chunk;
            chunk.totalCount = st.totalCount;
            chunk.items.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                chunk.items.push_back(std::move(st.items[i]));
            }
            st.pos = end;
            bool last = (end >= total);
            if (last)
                pending_request_.reset();
            co_return ResponseChunk{.data = Response{std::move(chunk)}, .is_last_chunk = last};
        }

        // 5) Emit next page (Grep)
        if (mode_ == Mode::Grep) {
            if (!grep_.has_value()) {
                GrepResponse r;
                r.totalMatches = 0;
                r.filesSearched = 0;
                co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
            auto& st = *grep_;
            const std::size_t total = st.matches.size();
            std::size_t n = compute_item_chunk_count(1024);
            const std::size_t start = st.pos;
            const std::size_t end = std::min(start + n, total);

            GrepResponse chunk;
            chunk.totalMatches = st.totalMatches;
            chunk.filesSearched = st.filesSearched;
            chunk.matches.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                chunk.matches.push_back(std::move(st.matches[i]));
            }
            st.pos = end;
            bool last = (end >= total);
            if (last)
                pending_request_.reset();
            co_return ResponseChunk{.data = Response{std::move(chunk)}, .is_last_chunk = last};
        }

        // 6) Fallback: provide a graceful end-of-stream to avoid calling into
        // delegate_->next_chunk() (which is not used by the dispatcher adapter).
        SuccessResponse ok{"End of stream"};
        co_return ResponseChunk{.data = Response{std::move(ok)}, .is_last_chunk = true};
    } catch (const std::exception& e) {
        spdlog::error("StreamingRequestProcessor::next_chunk() exception: {}", e.what());
        ErrorResponse err{ErrorCode::InternalError, std::string("Streaming error: ") + e.what()};
        co_return ResponseChunk{.data = Response{std::move(err)}, .is_last_chunk = true};
    }
}

} // namespace yams::daemon
