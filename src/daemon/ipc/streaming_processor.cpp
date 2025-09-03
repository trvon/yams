#include <yams/daemon/ipc/streaming_processor.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <optional>
#include <variant>

namespace yams::daemon {

boost::asio::awaitable<Response> StreamingRequestProcessor::process(const Request& request) {
    co_return co_await delegate_->process(request);
}

boost::asio::awaitable<std::optional<Response>> StreamingRequestProcessor::process_streaming(const Request& request) {
    // Only intercept known stream-friendly types; otherwise delegate.
    if (std::holds_alternative<SearchRequest>(request)) {
        spdlog::debug(
            "StreamingRequestProcessor: deferring Search compute until first next_chunk()");
        mode_ = Mode::Search;
        pending_request_ = request;
        co_return std::optional<Response>{};
    }
    if (std::holds_alternative<ListRequest>(request)) {
        spdlog::debug("StreamingRequestProcessor: deferring List compute until first next_chunk()");
        mode_ = Mode::List;
        pending_request_ = request;
        co_return std::optional<Response>{};
    }
    if (std::holds_alternative<GrepRequest>(request)) {
        spdlog::debug("StreamingRequestProcessor: deferring Grep compute until first next_chunk()");
        mode_ = Mode::Grep;
        pending_request_ = request;
        co_return std::optional<Response>{};
    }
    if (std::holds_alternative<AddDocumentRequest>(request)) {
        // For AddDocument, send header immediately (handled by RequestHandler),
        // then compute full response on first next_chunk() and return as last chunk.
        spdlog::debug("StreamingRequestProcessor: deferring AddDocument compute until first "
                      "next_chunk() to send header-first");
        pending_request_ = request; // leave mode_ as None
        co_return std::optional<Response>{};
    }

    // Unknown type – pass through to delegate's streaming behavior
    co_return co_await delegate_->process_streaming(request);
}

bool StreamingRequestProcessor::supports_streaming(const Request& request) const {
    if (std::holds_alternative<SearchRequest>(request) ||
        std::holds_alternative<ListRequest>(request) ||
        std::holds_alternative<GrepRequest>(request) ||
        std::holds_alternative<AddDocumentRequest>(request)) {
        return true;
    }
    return delegate_->supports_streaming(request);
}

std::size_t
StreamingRequestProcessor::compute_item_chunk_count(std::size_t approx_bytes_per_item) const {
    std::size_t approx = cfg_.chunk_size > 0 ? cfg_.chunk_size / approx_bytes_per_item : 256;
    approx = std::clamp<std::size_t>(approx, 10, 5000);
    return approx;
}

boost::asio::awaitable<RequestProcessor::ResponseChunk> StreamingRequestProcessor::next_chunk() {
    try {
        // Emit a tiny heartbeat chunk immediately after header to keep client alive
        if (pending_request_.has_value() && !heartbeat_sent_) {
            heartbeat_sent_ = true;
            // Synthesize an empty chunk matching the selected mode
            switch (mode_) {
                case Mode::Search: {
                    SearchResponse r;
                    r.totalCount = 0;
                    r.elapsed = std::chrono::milliseconds(0);
                    co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)},
                                                              .is_last_chunk = false};
                }
                case Mode::List: {
                    ListResponse r;
                    r.totalCount = 0;
                    co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)},
                                                              .is_last_chunk = false};
                }
                case Mode::Grep: {
                    GrepResponse r;
                    r.totalMatches = 0;
                    r.filesSearched = 0;
                    co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)},
                                                              .is_last_chunk = false};
                }
                case Mode::None:
                default: {
                    // For non-chunkable types (e.g., AddDocument before compute), send a
                    // lightweight heartbeat so clients reset inactivity timers.
                    SuccessResponse ok{"Streaming started"};
                    co_return RequestProcessor::ResponseChunk{.data = Response{std::move(ok)},
                                                              .is_last_chunk = false};
                }
            }
        }

        // Lazy computation on first real chunk to ensure header already went out immediately
        if (pending_request_.has_value()) {
            auto req = *pending_request_;
            pending_request_.reset();
            auto t0 = std::chrono::steady_clock::now();
            
            Response full;
            try {
                full = co_await delegate_->process(req);
            } catch (const std::exception& e) {
                spdlog::error("StreamingRequestProcessor: delegate_->process() threw exception: {}", e.what());
                ErrorResponse err{ErrorCode::InternalError, 
                                std::string("Failed to process request: ") + e.what()};
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(err)},
                                                          .is_last_chunk = true};
            }
            
            auto t1 = std::chrono::steady_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

            if (auto* s = std::get_if<SearchResponse>(&full)) {
                search_ = SearchState{};
                search_->totalCount = s->totalCount;
                search_->elapsed = s->elapsed;
                search_->results = std::move(s->results);
                search_->pos = 0;
                
                // Add memory limit check
                constexpr size_t MAX_RESULTS = 100000; // Reasonable limit
                if (search_->results.size() > MAX_RESULTS) {
                    spdlog::warn("StreamingRequestProcessor(Search): result count {} exceeds limit {}, truncating",
                                search_->results.size(), MAX_RESULTS);
                    search_->results.resize(MAX_RESULTS);
                }
                
                spdlog::debug(
                    "StreamingRequestProcessor(Search): computed full response in {} ms ({} results)",
                    ms, search_->results.size());
            } else if (auto* l = std::get_if<ListResponse>(&full)) {
                list_ = ListState{};
                list_->totalCount = l->totalCount;
                list_->items = std::move(l->items);
                list_->pos = 0;
                
                // Add memory limit check
                constexpr size_t MAX_ITEMS = 100000; // Reasonable limit
                if (list_->items.size() > MAX_ITEMS) {
                    spdlog::warn("StreamingRequestProcessor(List): item count {} exceeds limit {}, truncating",
                                list_->items.size(), MAX_ITEMS);
                    list_->items.resize(MAX_ITEMS);
                }
                
                spdlog::debug(
                    "StreamingRequestProcessor(List): computed full response in {} ms ({} items)", ms,
                    list_->items.size());
            } else if (auto* g = std::get_if<GrepResponse>(&full)) {
                grep_ = GrepState{};
                grep_->totalMatches = g->totalMatches;
                grep_->filesSearched = g->filesSearched;
                grep_->matches = std::move(g->matches);
                grep_->pos = 0;
                
                // Add memory limit check
                constexpr size_t MAX_MATCHES = 100000; // Reasonable limit
                if (grep_->matches.size() > MAX_MATCHES) {
                    spdlog::warn("StreamingRequestProcessor(Grep): match count {} exceeds limit {}, truncating",
                                grep_->matches.size(), MAX_MATCHES);
                    grep_->matches.resize(MAX_MATCHES);
                }
                
                spdlog::debug("StreamingRequestProcessor(Grep): computed full response in {} ms ({} "
                              "matches across {} files)",
                              ms, grep_->matches.size(), grep_->filesSearched);
            } else {
                // Not a chunkable type; return as a single last chunk
                co_return RequestProcessor::ResponseChunk{.data = std::move(full),
                                                          .is_last_chunk = true};
            }
        }

    switch (mode_) {
        case Mode::Search: {
            auto& st = *search_;
            const std::size_t total = st.results.size();
            const std::size_t n = compute_item_chunk_count(512); // ~512B per result
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
            bool last = end >= total;
            spdlog::debug("StreamingRequestProcessor(Search): emit [{}..{})/{}, count={}, last={}",
                          start, end, total, chunk.results.size(), last);
            co_return RequestProcessor::ResponseChunk{.data = Response{std::move(chunk)},
                                                      .is_last_chunk = last};
        }
        case Mode::List: {
            auto& st = *list_;
            const std::size_t total = st.items.size();
            const std::size_t n =
                compute_item_chunk_count(2048); // ~2KB per item to reduce chunk churn
            const std::size_t start = st.pos;
            const std::size_t end = std::min(start + n, total);

            ListResponse chunk;
            chunk.totalCount = st.totalCount;
            chunk.items.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                chunk.items.push_back(std::move(st.items[i]));
            }
            st.pos = end;
            bool last = end >= total;
            spdlog::debug("StreamingRequestProcessor(List): emit [{}..{})/{}, count={}, last={}",
                          start, end, total, chunk.items.size(), last);
            co_return RequestProcessor::ResponseChunk{.data = Response{std::move(chunk)},
                                                      .is_last_chunk = last};
        }
        case Mode::Grep: {
            auto& st = *grep_;
            const std::size_t total = st.matches.size();
            const std::size_t n =
                compute_item_chunk_count(1024); // Reduced to ~1KB per match for faster emission
            const std::size_t start = st.pos;
            const std::size_t end = std::min(start + n, total);

            auto start_time = std::chrono::steady_clock::now();

            GrepResponse chunk;
            chunk.totalMatches = st.totalMatches;
            chunk.filesSearched = st.filesSearched;
            chunk.matches.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                chunk.matches.push_back(std::move(st.matches[i]));
            }
            st.pos = end;
            bool last = end >= total;

            auto end_time = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time)
                    .count();

            spdlog::debug("StreamingRequestProcessor(Grep): emit [{}..{})/{}, count={}, last={}, "
                          "duration={}ms",
                          start, end, total, chunk.matches.size(), last, duration);
            co_return RequestProcessor::ResponseChunk{.data = Response{std::move(chunk)},
                                                      .is_last_chunk = last};
        }
        case Mode::None:
        default:
            // Delegate as last resort
            co_return co_await delegate_->next_chunk();
    }
    } catch (const std::exception& e) {
        spdlog::error("StreamingRequestProcessor::next_chunk() exception: {}", e.what());
        ErrorResponse err{ErrorCode::InternalError, 
                        std::string("Streaming error: ") + e.what()};
        co_return RequestProcessor::ResponseChunk{.data = Response{std::move(err)},
                                                  .is_last_chunk = true};
    }
}

} // namespace yams::daemon
