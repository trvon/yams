#include <yams/daemon/ipc/streaming_processor.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <optional>
#include <string>
#include <variant>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/profiling.h>

namespace {
using yams::daemon::MessageType;

inline std::pair<std::size_t, std::size_t> page_bounds(std::size_t pos, std::size_t total,
                                                       std::size_t page) {
    const auto end = std::min(pos + page, total);
    return {pos, end};
}
} // namespace

namespace yams::daemon {

void StreamingRequestProcessor::reset_state() {
    pending_request_.reset();
    pending_final_.reset();
    search_.reset();
    list_.reset();
    grep_.reset();
    mode_ = Mode::None;
    heartbeat_sent_ = false;
    pending_total_ = 0;
}

std::size_t
StreamingRequestProcessor::compute_item_chunk_count(std::size_t approx_bytes_per_item) const {
    YAMS_ZONE_SCOPED_N("SRP::compute_item_chunk_count");
    std::size_t target = cfg_.chunk_size > 0
                             ? (cfg_.chunk_size / std::max<std::size_t>(approx_bytes_per_item, 1))
                             : 256;
    auto snap = MuxMetricsRegistry::instance().snapshot();
    const int64_t q = snap.queuedBytes;
    if (q > static_cast<int64_t>(TuneAdvisor::streamMuxVeryHighBytes())) {
        target = static_cast<std::size_t>(static_cast<double>(target) *
                                          TuneAdvisor::streamPageFactorVeryHighDiv());
    } else if (q > static_cast<int64_t>(TuneAdvisor::streamMuxHighBytes())) {
        target = static_cast<std::size_t>(static_cast<double>(target) *
                                          TuneAdvisor::streamPageFactorHighDiv());
    } else if (q < static_cast<int64_t>(TuneAdvisor::streamMuxLight1Bytes())) {
        target = static_cast<std::size_t>(static_cast<double>(target) *
                                          TuneAdvisor::streamPageFactorLight1Mul());
    } else if (q < static_cast<int64_t>(TuneAdvisor::streamMuxLight2Bytes())) {
        target = static_cast<std::size_t>(static_cast<double>(target) *
                                          TuneAdvisor::streamPageFactorLight2Mul());
    } else if (q < static_cast<int64_t>(TuneAdvisor::streamMuxLight3Bytes())) {
        target = static_cast<std::size_t>(static_cast<double>(target) *
                                          TuneAdvisor::streamPageFactorLight3Mul());
    }
    // Clamps
    target = std::clamp<std::size_t>(target, TuneAdvisor::streamPageClampMin(),
                                     TuneAdvisor::streamPageClampMax());
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
    // Directly process without copying
    co_return co_await delegate_->process(request);
}

// -------------------- process_streaming (decide whether to defer) ----------
boost::asio::awaitable<std::optional<Response>>
StreamingRequestProcessor::process_streaming_impl(Request request) {
    // Ensure previous stream state does not leak into this request.
    reset_state();
    Request& req_copy = request; // owned request in coroutine frame
    try {
        // NOTE: We use std::holds_alternative() for dispatch instead of getMessageType()
        // to avoid ODR/cross-TU variant index issues in tests.

        // BatchEmbeddingRequest: eager compute and store in pending_final_
        if (std::holds_alternative<BatchEmbeddingRequest>(req_copy)) {
            spdlog::debug("StreamingRequestProcessor: eager compute BatchEmbedding (no copy)");
            mode_ = Mode::BatchEmbed;
#ifdef YAMS_TESTING
            spdlog::debug("[SRP] defer BatchEmbedding -> streaming");
            if (auto* be0 = std::get_if<BatchEmbeddingRequest>(&req_copy)) {
                spdlog::debug("[SRP] original BE texts.size={}", be0->texts.size());
            }
#endif
            // Process directly - no need for protobuf round-trip in unit tests
            auto final = co_await delegate_->process(req_copy);
            if (auto* r = std::get_if<BatchEmbeddingResponse>(&final)) {
                pending_total_ = r->successCount;
            }
            pending_final_ = std::move(final);
            co_return std::nullopt;
        }

        // EmbedDocumentsRequest: eager compute and store in pending_final_
        if (std::holds_alternative<EmbedDocumentsRequest>(req_copy)) {
            spdlog::debug("StreamingRequestProcessor: eager compute EmbedDocuments (no copy)");
            mode_ = Mode::EmbedDocs;
#ifdef YAMS_TESTING
            spdlog::debug("[SRP] defer EmbedDocuments -> streaming");
            if (auto* ed0 = std::get_if<EmbedDocumentsRequest>(&req_copy)) {
                spdlog::debug("[SRP] original ED docs.size={}", ed0->documentHashes.size());
            }
#endif
            // Process directly - no need for protobuf round-trip in unit tests
            auto final = co_await delegate_->process(req_copy);
            if (auto* r = std::get_if<EmbedDocumentsResponse>(&final)) {
                pending_total_ = r->requested;
            }
            pending_final_ = std::move(final);
            co_return std::nullopt;
        }

        if (std::holds_alternative<SearchRequest>(req_copy)) {
            spdlog::debug("StreamingRequestProcessor: defer Search for deterministic paging");
            mode_ = Mode::Search;
            pending_request_.emplace(std::make_unique<Request>(std::move(request)));
            co_return std::nullopt;
        }
        if (std::holds_alternative<ListRequest>(req_copy)) {
            spdlog::info("[SRP] process_streaming: deferring ListRequest to streaming mode");
            mode_ = Mode::List;
            pending_request_.emplace(std::make_unique<Request>(std::move(request)));
            co_return std::nullopt;
        }
        if (std::holds_alternative<GrepRequest>(req_copy)) {
            spdlog::debug("StreamingRequestProcessor: defer Grep for deterministic paging");
            mode_ = Mode::Grep;
            pending_request_.emplace(std::make_unique<Request>(std::move(request)));
            co_return std::nullopt;
        }
        if (std::holds_alternative<AddDocumentRequest>(req_copy)) {
            spdlog::debug("StreamingRequestProcessor: defer AddDocument for header-first");
            // Mode None: single final chunk after heartbeat
            pending_request_.emplace(std::make_unique<Request>(std::move(request)));
            co_return std::nullopt;
        }
        // Fallback: defer GenerateEmbedding / LoadModel to provide typed start events.
        if (std::holds_alternative<GenerateEmbeddingRequest>(req_copy) ||
            std::holds_alternative<LoadModelRequest>(req_copy)) {
            spdlog::debug("StreamingRequestProcessor: defer single-step embedding/model load");
            pending_request_.emplace(std::make_unique<Request>(std::move(request)));
            co_return std::nullopt;
        }

        // Not a streaming-recognized request: delegate immediately.
        // Note: request is still valid here because all branches that move it
        // also co_return immediately, so we only reach here if it wasn't moved.
        co_return co_await delegate_->process(request);
    } catch (...) {
        // On unexpected failure; choose streaming path so caller can still progress.
        if (!pending_request_.has_value()) {
            pending_request_.emplace(std::make_unique<Request>(std::move(request)));
        }
        co_return std::nullopt;
    }
}

boost::asio::awaitable<std::optional<Response>>
StreamingRequestProcessor::process_streaming(const Request& request) {
    co_return co_await process_streaming_impl(Request{request});
}

boost::asio::awaitable<std::optional<Response>>
StreamingRequestProcessor::process_streaming(Request&& request) {
    co_return co_await process_streaming_impl(std::move(request));
}

// removed by-value overload to avoid ambiguity

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
    YAMS_ZONE_SCOPED_N("SRP::next_chunk");
#if defined(TRACY_ENABLE)
    // Mark this streaming step as its own fiber segment for clearer stacks
    YAMS_FIBER_ENTER("srp_chunk");
    struct FiberGuard {
        ~FiberGuard() { YAMS_FIBER_LEAVE(); }
    } _fg;
#endif
    try {
        // Unconditional debug: trace next_chunk entry and internal flags
        try {
#ifdef YAMS_TESTING
            bool has_req = pending_request_.has_value();
            spdlog::debug("[SRP] next_chunk enter has_pending={} heartbeat_sent={}",
                          has_req ? 1 : 0, heartbeat_sent_ ? 1 : 0);
#endif
        } catch (...) {
        }
        // 1) If we have a pending request and have not yet sent the initial heartbeat,
        //    synthesize a typed starter frame (never last).
        // NOTE: We use mode_ and std::holds_alternative() for dispatch instead of
        // getMessageType() to avoid ODR/cross-TU variant index issues in tests.
        if ((pending_request_.has_value() || pending_final_.has_value()) && !heartbeat_sent_) {
            heartbeat_sent_ = true;

            // For BatchEmbed/EmbedDocs, pending_final_ is set (not pending_request_)
            if (mode_ == Mode::BatchEmbed || mode_ == Mode::EmbedDocs) {
                SuccessResponse ok{"embedding started"};
#ifdef YAMS_TESTING
                spdlog::debug("[SRP] heartbeat mode={} total={}", static_cast<int>(mode_),
                              pending_total_);
#endif
                co_return ResponseChunk{.data = Response{std::move(ok)}, .is_last_chunk = false};
            }

            // For Search/List/Grep, use mode_
            if (mode_ == Mode::Search) {
                SearchResponse r;
                r.totalCount = 0;
                r.elapsed = std::chrono::milliseconds(0);
                co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
            if (mode_ == Mode::List) {
                spdlog::info("[SRP] next_chunk: sending List heartbeat");
                ListResponse r;
                r.totalCount = 0;
                co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
            if (mode_ == Mode::Grep) {
                GrepResponse r;
                r.totalMatches = 0;
                r.filesSearched = 0;
                co_return ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }

            // For other request types with pending_request_, use std::holds_alternative()
            if (pending_request_.has_value()) {
                const auto& req = **pending_request_;
                if (std::holds_alternative<GenerateEmbeddingRequest>(req)) {
                    SuccessResponse ok{"embedding started"};
                    co_return ResponseChunk{.data = Response{std::move(ok)},
                                            .is_last_chunk = false};
                }
                if (auto* lmr = std::get_if<LoadModelRequest>(&req)) {
                    ModelLoadEvent mev{};
                    mev.phase = "started";
                    mev.message = "load started";
                    mev.modelName = lmr->modelName;
                    co_return ResponseChunk{.data = Response{std::move(mev)},
                                            .is_last_chunk = false};
                }
            }

            // Default heartbeat for unknown types
            SuccessResponse ok{"Streaming started"};
            co_return ResponseChunk{.data = Response{std::move(ok)}, .is_last_chunk = false};
        }

        // 2) After the heartbeat has been sent, act based on mode_ or request type.
        // NOTE: We use mode_ and std::holds_alternative() for dispatch instead of
        // getMessageType() to avoid ODR/cross-TU variant index issues in tests.

        // BatchEmbed/EmbedDocs: emit precomputed final from pending_final_
        if (mode_ == Mode::BatchEmbed || mode_ == Mode::EmbedDocs) {
            if (pending_final_.has_value()) {
                auto final = std::move(pending_final_.value());
                reset_state();
                // Log counters for visibility
                if (std::holds_alternative<BatchEmbeddingResponse>(final)) {
                    [[maybe_unused]] const auto& r = std::get<BatchEmbeddingResponse>(final);
#ifdef YAMS_TESTING
                    spdlog::debug("[SRP] delegate final BatchEmbeddingResponse succ={} fail={}",
                                  r.successCount, r.failureCount);
#endif
                } else if (std::holds_alternative<EmbedDocumentsResponse>(final)) {
                    [[maybe_unused]] const auto& r = std::get<EmbedDocumentsResponse>(final);
#ifdef YAMS_TESTING
                    spdlog::debug("[SRP] delegate final EmbedDocumentsResponse req={} emb={}",
                                  r.requested, r.embedded);
#endif
                }
                co_return ResponseChunk{.data = std::move(final), .is_last_chunk = true};
            }
            // pending_final_ should always be set for BatchEmbed/EmbedDocs, but handle gracefully
            ErrorResponse err{ErrorCode::InternalError, "Missing final response for embedding"};
            reset_state();
            co_return ResponseChunk{.data = Response{std::move(err)}, .is_last_chunk = true};
        }

        // Handle other request types that have pending_request_
        if (pending_request_.has_value()) {
            const auto& req = **pending_request_;

            // GenerateEmbedding/LoadModel: call delegate once and finish
            if (std::holds_alternative<GenerateEmbeddingRequest>(req) ||
                std::holds_alternative<LoadModelRequest>(req)) {
                auto final = co_await delegate_->process(req);
                reset_state();
                co_return ResponseChunk{.data = std::move(final), .is_last_chunk = true};
            }

            // AddDocumentRequest: single final response after heartbeat
            if (std::holds_alternative<AddDocumentRequest>(req)) {
                try {
                    spdlog::debug("[SRP] AddDocument delegate processing start");
                } catch (...) {
                }
                auto final = co_await delegate_->process(req);
                try {
                    spdlog::debug("[SRP] AddDocument delegate processing done");
                } catch (...) {
                }
                reset_state();
                co_return ResponseChunk{.data = std::move(final), .is_last_chunk = true};
            }

            // Search/List/Grep initial compute (first post-heartbeat chunk)
            if (mode_ == Mode::Search && !search_.has_value()) {
                auto r = co_await delegate_->process(**pending_request_);
                if (auto* s = std::get_if<SearchResponse>(&r)) {
                    search_ = SearchState{};
                    search_->results = std::move(s->results);
                    search_->totalCount = s->totalCount;
                    search_->elapsed = s->elapsed;
                    search_->pos = 0;
                } else {
                    reset_state();
                    co_return ResponseChunk{.data = std::move(r), .is_last_chunk = true};
                }
            } else if (mode_ == Mode::List && !list_.has_value()) {
                spdlog::info("[SRP] next_chunk: calling delegate_->process() for ListRequest");
                auto r = co_await delegate_->process(**pending_request_);
                spdlog::info("[SRP] next_chunk: delegate_->process() returned for List");
                if (auto* l = std::get_if<ListResponse>(&r)) {
                    spdlog::info("[SRP] next_chunk: List got {} items", l->items.size());
                    list_ = ListState{};
                    list_->items = std::move(l->items);
                    list_->totalCount = l->totalCount;
                    list_->pos = 0;
                } else {
                    spdlog::info("[SRP] next_chunk: List got non-ListResponse, resetting");
                    reset_state();
                    co_return ResponseChunk{.data = std::move(r), .is_last_chunk = true};
                }
            } else if (mode_ == Mode::Grep && !grep_.has_value()) {
                auto r = co_await delegate_->process(**pending_request_);
                if (auto* g = std::get_if<GrepResponse>(&r)) {
                    grep_ = GrepState{};
                    grep_->matches = std::move(g->matches);
                    grep_->totalMatches = g->totalMatches;
                    grep_->filesSearched = g->filesSearched;
                    grep_->pos = 0;
                } else {
                    reset_state();
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
            const std::size_t start = st.pos;
            const auto [s, e] = page_bounds(start, total, compute_item_chunk_count(512));
            const std::size_t end = e;

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
                reset_state();
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
            const std::size_t start = st.pos;
            const auto [s, e] = page_bounds(start, total, compute_item_chunk_count(2048));
            const std::size_t end = e;

            ListResponse chunk;
            chunk.totalCount = st.totalCount;
            chunk.items.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                chunk.items.push_back(std::move(st.items[i]));
            }
            st.pos = end;
            bool last = (end >= total);
            if (last)
                reset_state();
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
            const std::size_t start = st.pos;
            const auto [s, e] = page_bounds(start, total, compute_item_chunk_count(1024));
            const std::size_t end = e;

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
                reset_state();
            co_return ResponseChunk{.data = Response{std::move(chunk)}, .is_last_chunk = last};
        }

        // 6) Fallback: provide a graceful end-of-stream to avoid calling into
        // delegate_->next_chunk() (which is not used by the dispatcher adapter).
        SuccessResponse ok{"End of stream"};
        reset_state();
        co_return ResponseChunk{.data = Response{std::move(ok)}, .is_last_chunk = true};
    } catch (const std::exception& e) {
        spdlog::error("StreamingRequestProcessor::next_chunk() exception: {}", e.what());
        ErrorResponse err{ErrorCode::InternalError, std::string("Streaming error: ") + e.what()};
        co_return ResponseChunk{.data = Response{std::move(err)}, .is_last_chunk = true};
    }
}

} // namespace yams::daemon
