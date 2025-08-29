#include <yams/daemon/ipc/streaming_processor.h>

#include <algorithm>
#include <cstddef>
#include <optional>

namespace yams::daemon {

Task<Response> StreamingRequestProcessor::process(const Request& request) {
    co_return co_await delegate_->process(request);
}

Task<std::optional<Response>> StreamingRequestProcessor::process_streaming(const Request& request) {
    // Only intercept known stream-friendly types; otherwise delegate.
    if (!(std::holds_alternative<SearchRequest>(request) ||
          std::holds_alternative<ListRequest>(request) ||
          std::holds_alternative<GrepRequest>(request))) {
        co_return co_await delegate_->process_streaming(request);
    }

    Response full = co_await delegate_->process(request);

    if (auto* s = std::get_if<SearchResponse>(&full)) {
        mode_ = Mode::Search;
        search_.emplace();
        search_->totalCount = s->totalCount;
        search_->elapsed = s->elapsed;
        search_->results = std::move(s->results);
        search_->pos = 0;
        co_return std::optional<Response>{};
    }
    if (auto* l = std::get_if<ListResponse>(&full)) {
        mode_ = Mode::List;
        list_.emplace();
        list_->totalCount = l->totalCount;
        list_->items = std::move(l->items);
        list_->pos = 0;
        co_return std::optional<Response>{};
    }
    if (auto* g = std::get_if<GrepResponse>(&full)) {
        mode_ = Mode::Grep;
        grep_.emplace();
        grep_->totalMatches = g->totalMatches;
        grep_->filesSearched = g->filesSearched;
        grep_->matches = std::move(g->matches);
        grep_->pos = 0;
        co_return std::optional<Response>{};
    }

    // Unknown type – pass through as a single response
    co_return std::optional<Response>{std::move(full)};
}

bool StreamingRequestProcessor::supports_streaming(const Request& request) const {
    if (std::holds_alternative<SearchRequest>(request) ||
        std::holds_alternative<ListRequest>(request) ||
        std::holds_alternative<GrepRequest>(request)) {
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

Task<RequestProcessor::ResponseChunk> StreamingRequestProcessor::next_chunk() {
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
            co_return ResponseChunk{.data = Response{std::move(chunk)},
                                    .is_last_chunk = end >= total};
        }
        case Mode::List: {
            auto& st = *list_;
            const std::size_t total = st.items.size();
            const std::size_t n = compute_item_chunk_count(512); // ~512B per item
            const std::size_t start = st.pos;
            const std::size_t end = std::min(start + n, total);

            ListResponse chunk;
            chunk.totalCount = st.totalCount;
            chunk.items.reserve(end - start);
            for (std::size_t i = start; i < end; ++i) {
                chunk.items.push_back(std::move(st.items[i]));
            }
            st.pos = end;
            co_return ResponseChunk{.data = Response{std::move(chunk)},
                                    .is_last_chunk = end >= total};
        }
        case Mode::Grep: {
            auto& st = *grep_;
            const std::size_t total = st.matches.size();
            const std::size_t n = compute_item_chunk_count(256); // ~256B per match
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
            co_return ResponseChunk{.data = Response{std::move(chunk)},
                                    .is_last_chunk = end >= total};
        }
        case Mode::None:
        default:
            // Delegate as last resort
            co_return co_await delegate_->next_chunk();
    }
}

} // namespace yams::daemon
