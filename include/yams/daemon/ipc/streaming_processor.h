#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <variant>
#include <vector>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/request_handler.h>
#include <future>
#include <chrono>

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

    [[nodiscard]] boost::asio::awaitable<Response> process(const Request& request) override;

    // Returns std::nullopt to signal chunked streaming; stores internal state
    // and serves chunks from next_chunk(). When not recognized, delegates.
    [[nodiscard]] boost::asio::awaitable<std::optional<Response>> process_streaming(const Request& request) override;

    [[nodiscard]] bool supports_streaming(const Request& request) const override;

    [[nodiscard]] boost::asio::awaitable<RequestProcessor::ResponseChunk> next_chunk() override;

private:
    // Lazy compute: store request to compute on first next_chunk() to allow
    // header to be written immediately by RequestHandler before heavy work
    // (critical for Grep to avoid header-timeouts in clients/pools).
    std::optional<Request> pending_request_;
    bool heartbeat_sent_{false};

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

    // Background compute support and keepalive
    std::optional<std::future<Response>> compute_future_;
    // First-results-first staged compute
    std::optional<std::future<Response>> fast_future_;
    std::optional<std::future<Response>> full_future_;
    bool first_burst_emitted_{false};
    std::size_t already_sent_{0};
    std::size_t first_burst_limit_{20};
    std::chrono::steady_clock::time_point last_keepalive_{};
    std::chrono::milliseconds keepalive_interval_{500};

    // Grep race controls (PBI-008)
    std::chrono::steady_clock::time_point first_batch_start_{};
    std::chrono::milliseconds grep_first_batch_max_wait_{300};
    std::size_t grep_batch_size_override_{0};
    bool grep_env_applied_{false};
    bool cancel_cold_on_hot_{true};
};

} // namespace yams::daemon
