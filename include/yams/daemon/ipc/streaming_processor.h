#pragma once

#include <memory>
#include <optional>
#include <variant>
#include <vector>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/request_handler.h>

namespace yams::daemon {

// StreamingRequestProcessor decorates another RequestProcessor and provides
// first-class streaming for common response types by chunking logical payloads
// (SearchResponse.results, ListResponse.items, GrepResponse.matches) rather
// than raw framed bytes.
class StreamingRequestProcessor : public RequestProcessor {
public:
    StreamingRequestProcessor(std::shared_ptr<RequestProcessor> delegate,
                              RequestHandler::Config cfg)
        : delegate_(std::move(delegate)), cfg_(cfg) {}

    [[nodiscard]] Task<Response> process(const Request& request) override;

    // Returns std::nullopt to signal chunked streaming; stores internal state
    // and serves chunks from next_chunk(). When not recognized, delegates.
    [[nodiscard]] Task<std::optional<Response>> process_streaming(const Request& request) override;

    [[nodiscard]] bool supports_streaming(const Request& request) const override;

    [[nodiscard]] Task<ResponseChunk> next_chunk() override;

private:
    struct SearchState {
        std::vector<SearchResult> results;
        std::size_t totalCount{0};
        std::size_t pos{0};
        std::chrono::milliseconds elapsed{0};
    };

    struct ListState {
        std::vector<ListEntry> items;
        std::size_t totalCount{0};
        std::size_t pos{0};
    };

    struct GrepState {
        std::vector<GrepMatch> matches;
        std::size_t totalMatches{0};
        std::size_t filesSearched{0};
        std::size_t pos{0};
    };

    enum class Mode { None, Search, List, Grep };

    std::size_t compute_item_chunk_count(std::size_t approx_bytes_per_item) const;

    std::shared_ptr<RequestProcessor> delegate_;
    RequestHandler::Config cfg_;

    Mode mode_{Mode::None};
    std::optional<SearchState> search_;
    std::optional<ListState> list_;
    std::optional<GrepState> grep_;
};

} // namespace yams::daemon
