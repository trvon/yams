#include <yams/daemon/ipc/streaming_processor.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <optional>
#include <cstdlib>
#include <string>
#include <variant>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <yams/app/services/grep_mode_tls.h>

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
        // Allow environment override for keepalive cadence (basic support)
        if (keepalive_interval_.count() == 500) {
            if (const char* s = std::getenv("YAMS_KEEPALIVE_MS")) {
                try {
                    auto v = static_cast<long>(std::stol(s));
                    if (v >= 50 && v <= 60000) {
                        keepalive_interval_ = std::chrono::milliseconds(v);
                    }
                } catch (...) {
                    // ignore invalid value
                }
            }
        }
        // Emit a tiny heartbeat chunk immediately after header to keep client alive
        if (pending_request_.has_value() && !heartbeat_sent_) {
            heartbeat_sent_ = true;
            last_keepalive_ = std::chrono::steady_clock::now();
            if (mode_ == Mode::Grep) {
                first_batch_start_ = last_keepalive_;
                if (!grep_env_applied_) {
                    if (const char* s = std::getenv("YAMS_GREP_FIRST_BATCH_MAX_WAIT_MS")) {
                        try { auto v = static_cast<long>(std::stol(s)); if (v >= 50 && v <= 60000) grep_first_batch_max_wait_ = std::chrono::milliseconds(v); } catch (...) {}
                    }
                    if (const char* s = std::getenv("YAMS_GREP_BATCH_SIZE")) {
                        try { auto v = static_cast<long>(std::stol(s)); if (v > 0) grep_batch_size_override_ = static_cast<std::size_t>(v); } catch (...) {}
                    }
                    grep_env_applied_ = true;
                }
            }
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

        // Start staged background compute after first heartbeat
        if (pending_request_.has_value()) {
            // Initialize first-burst limit from env once
            if (first_burst_limit_ == 0) first_burst_limit_ = 20;
            if (const char* env = std::getenv("YAMS_FIRST_BURST"); env && *env) {
                try { auto v = static_cast<std::size_t>(std::stoul(env)); if (v > 0) first_burst_limit_ = v; } catch (...) {}
            }

            if (!fast_future_.has_value() && !full_future_.has_value()) {
                auto req = *pending_request_;
                // Prepare a reduced-limit request for fast-first results when applicable
                Request fastReq = req;
                if (mode_ == Mode::Search) {
                    if (auto* s = std::get_if<SearchRequest>(&fastReq)) {
                        if (s->limit == 0 || s->limit > first_burst_limit_) s->limit = first_burst_limit_;
                    }
                } else if (mode_ == Mode::List) {
                    if (auto* l = std::get_if<ListRequest>(&fastReq)) {
                        if (l->limit == 0 || l->limit > first_burst_limit_) l->limit = first_burst_limit_;
                    }
                }

                // Spawn fast and full computes
                // For Grep, set thread-local execution mode to HotOnly (fast) and ColdOnly (full)
                auto fastCoro = [this, fastReq]() -> boost::asio::awaitable<Response> {
                    // TLS override for grep
                    yams::app::services::set_grep_mode_tls(yams::app::services::GrepExecMode::HotOnly);
                    auto r = co_await delegate_->process(fastReq);
                    yams::app::services::set_grep_mode_tls(yams::app::services::GrepExecMode::Unset);
                    co_return r;
                };
                auto fullCoro = [this, req]() -> boost::asio::awaitable<Response> {
                    yams::app::services::set_grep_mode_tls(yams::app::services::GrepExecMode::ColdOnly);
                    auto r = co_await delegate_->process(req);
                    yams::app::services::set_grep_mode_tls(yams::app::services::GrepExecMode::Unset);
                    co_return r;
                };
                if (mode_ == Mode::Grep) {
                    fast_future_.emplace(
                        boost::asio::co_spawn(
                            boost::asio::system_executor(),
                            fastCoro(),
                            boost::asio::use_future));
                    full_future_.emplace(
                        boost::asio::co_spawn(
                            boost::asio::system_executor(),
                            fullCoro(),
                            boost::asio::use_future));
                } else {
                    fast_future_.emplace(
                        boost::asio::co_spawn(
                            boost::asio::system_executor(),
                            delegate_->process(fastReq),
                            boost::asio::use_future));
                    full_future_.emplace(
                        boost::asio::co_spawn(
                            boost::asio::system_executor(),
                            delegate_->process(req),
                            boost::asio::use_future));
                }
                // Do not clear pending yet; we may need the request type
            }
            // If neither fast nor full compute is ready, emit periodic keepalive;
            // otherwise fall through to emit real data/completion.
            // Evaluate flag before touching the future to avoid no_state after get()
            bool fast_ready = !first_burst_emitted_ &&
                              fast_future_.has_value() &&
                              fast_future_->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
            bool full_ready = full_future_.has_value() &&
                              full_future_->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;

            if (!fast_ready && !full_ready) {
                // Throttle keepalives
                auto now = std::chrono::steady_clock::now();
                if (now - last_keepalive_ < keepalive_interval_) {
                    // Sleep until next keepalive interval
                    auto exec = co_await boost::asio::this_coro::executor;
                    boost::asio::steady_timer timer(exec);
                    auto delta = keepalive_interval_ - (now - last_keepalive_);
                    timer.expires_after(delta);
                    co_await timer.async_wait(boost::asio::use_awaitable);
                }
                last_keepalive_ = std::chrono::steady_clock::now();
                // Emit periodic keepalive matching mode
                switch (mode_) {
                    case Mode::Search: {
                        SearchResponse r; r.totalCount = 0; r.elapsed = std::chrono::milliseconds(0);
                        co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
                    }
                    case Mode::List: {
                        ListResponse r; r.totalCount = 0;
                        co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
                    }
                    case Mode::Grep: {
                        GrepResponse r; r.totalMatches = 0; r.filesSearched = 0;
                        co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
                    }
                    case Mode::None:
                    default: {
                        SuccessResponse ok{"Streaming in progress"};
                        co_return RequestProcessor::ResponseChunk{.data = Response{std::move(ok)}, .is_last_chunk = false};
                    }
                }
            }
        }

        // Fast compute completion: emit first-burst results
        if (!first_burst_emitted_ && fast_future_.has_value() && fast_future_->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            Response fast;
            try {
                fast = fast_future_->get();
            } catch (const std::exception& e) {
                spdlog::debug("StreamingRequestProcessor: fast compute failed: {}", e.what());
                fast_future_.reset();
            }
            if (auto* s = std::get_if<SearchResponse>(&fast)) {
                search_ = SearchState{};
                search_->totalCount = s->totalCount;
                search_->elapsed = s->elapsed;
                search_->results = std::move(s->results);
                search_->pos = 0;
                already_sent_ = search_->results.size();
                first_burst_emitted_ = true;
                // Emit the entire fast-burst as one chunk
                SearchResponse chunk;
                chunk.totalCount = search_->totalCount;
                chunk.elapsed = search_->elapsed;
                chunk.results = search_->results;
                // We consumed the future with get(); drop it to avoid no_state on later waits
                fast_future_.reset();
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(chunk)}, .is_last_chunk = false};
            } else if (auto* l = std::get_if<ListResponse>(&fast)) {
                list_ = ListState{};
                list_->totalCount = l->totalCount;
                list_->items = std::move(l->items);
                list_->pos = 0;
                already_sent_ = list_->items.size();
                first_burst_emitted_ = true;
                ListResponse chunk;
                chunk.totalCount = list_->totalCount;
                chunk.items = list_->items;
                fast_future_.reset();
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(chunk)}, .is_last_chunk = false};
            } else if (auto* g = std::get_if<GrepResponse>(&fast)) {
                grep_ = GrepState{};
                grep_->totalMatches = g->totalMatches;
                grep_->filesSearched = g->filesSearched;
                grep_->matches = std::move(g->matches);
                grep_->pos = 0;
                already_sent_ = grep_->matches.size();
                first_burst_emitted_ = true;
                if (cancel_cold_on_hot_) {
                    full_future_.reset();
                }
                GrepResponse chunk;
                chunk.totalMatches = grep_->totalMatches;
                chunk.filesSearched = grep_->filesSearched;
                chunk.matches = grep_->matches;
                fast_future_.reset();
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(chunk)}, .is_last_chunk = false};
            } else {
                // For non-chunkable types, ignore fast path
                fast_future_.reset();
            }
        }

        // Full compute completion: build full state when ready
        if (full_future_.has_value() && full_future_->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            Response full;
            try {
                full = full_future_->get();
            } catch (const std::exception& e) {
                spdlog::error("StreamingRequestProcessor: background compute failed: {}", e.what());
                ErrorResponse err{ErrorCode::InternalError, std::string("Failed to process request: ") + e.what()};
                full_future_.reset();
                pending_request_.reset();
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(err)}, .is_last_chunk = true};
            }
            full_future_.reset();
            pending_request_.reset();

            // Populate chunking state
            if (auto* s = std::get_if<SearchResponse>(&full)) {
                search_ = SearchState{};
                search_->totalCount = s->totalCount;
                search_->elapsed = s->elapsed;
                search_->results = std::move(s->results);
                search_->pos = std::min<std::size_t>(already_sent_, search_->results.size());
                
                // Add memory limit check
                constexpr size_t MAX_RESULTS = 100000; // Reasonable limit
                if (search_->results.size() > MAX_RESULTS) {
                    spdlog::warn("StreamingRequestProcessor(Search): result count {} exceeds limit {}, truncating",
                                search_->results.size(), MAX_RESULTS);
                    search_->results.resize(MAX_RESULTS);
                }
                
                spdlog::debug("StreamingRequestProcessor(Search): background compute completed ({} results)", search_->results.size());
            } else if (auto* l = std::get_if<ListResponse>(&full)) {
                list_ = ListState{};
                list_->totalCount = l->totalCount;
                list_->items = std::move(l->items);
                list_->pos = std::min<std::size_t>(already_sent_, list_->items.size());
                
                // Add memory limit check
                constexpr size_t MAX_ITEMS = 100000; // Reasonable limit
                if (list_->items.size() > MAX_ITEMS) {
                    spdlog::warn("StreamingRequestProcessor(List): item count {} exceeds limit {}, truncating",
                                list_->items.size(), MAX_ITEMS);
                    list_->items.resize(MAX_ITEMS);
                }
                
                spdlog::debug("StreamingRequestProcessor(List): background compute completed ({} items)", list_->items.size());
            } else if (auto* g = std::get_if<GrepResponse>(&full)) {
                // If grep full finishes before first-burst and timeout not elapsed, delay emitting
                if (!first_burst_emitted_ && first_batch_start_.time_since_epoch().count() != 0) {
                    auto now = std::chrono::steady_clock::now();
                    if (now - first_batch_start_ < grep_first_batch_max_wait_) {
                        // Re-arm full result by storing back into future-like state: stash as grep_
                        grep_ = GrepState{};
                        grep_->totalMatches = g->totalMatches;
                        grep_->filesSearched = g->filesSearched;
                        grep_->matches = std::move(g->matches);
                        grep_->pos = 0;
                        // Sleep until deadline and emit keepalive
                        auto exec = co_await boost::asio::this_coro::executor;
                        boost::asio::steady_timer timer(exec);
                        timer.expires_after(grep_first_batch_max_wait_ - (now - first_batch_start_));
                        co_await timer.async_wait(boost::asio::use_awaitable);
                        // Emit keepalive to maintain connection while waiting
                        GrepResponse r; r.totalMatches = 0; r.filesSearched = 0;
                        co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
                    }
                }
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
                
                spdlog::debug("StreamingRequestProcessor(Grep): background compute completed ({} matches across {} files)", grep_->matches.size(), grep_->filesSearched);
            } else {
                // Not a chunkable type; return as a single last chunk
                co_return RequestProcessor::ResponseChunk{.data = std::move(full),
                                                          .is_last_chunk = true};
            }
        }

    switch (mode_) {
        case Mode::Search: {
            if (!search_.has_value()) {
                // Compute still in progress; keepalive throttle handled above
                SearchResponse r; r.totalCount = 0; r.elapsed = std::chrono::milliseconds(0);
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
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
            if (!list_.has_value()) {
                ListResponse r; r.totalCount = 0;
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
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
            if (!grep_.has_value()) {
                GrepResponse r; r.totalMatches = 0; r.filesSearched = 0;
                co_return RequestProcessor::ResponseChunk{.data = Response{std::move(r)}, .is_last_chunk = false};
            }
            auto& st = *grep_;
            const std::size_t total = st.matches.size();
            std::size_t n = compute_item_chunk_count(1024); // ~1KB per match
            if (grep_batch_size_override_ > 0) n = std::min(n, grep_batch_size_override_);
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
            if (first_burst_emitted_ && cancel_cold_on_hot_) {
                // If hot already emitted and we consider cold cancelled, ensure no extra cold chunks linger
            }
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
