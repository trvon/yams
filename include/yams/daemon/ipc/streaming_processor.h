#pragma once

#include <chrono>
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

    [[nodiscard]] boost::asio::awaitable<Response> process(const Request& request) override;

    // Returns std::nullopt to signal chunked streaming; stores internal state
    // and serves chunks from next_chunk(). When not recognized, delegates.
    [[nodiscard]] boost::asio::awaitable<std::optional<Response>>
    process_streaming(const Request& request) override;

    // Prefer this overload for rvalues to take ownership into the coroutine
    // frame and avoid lifetime pitfalls when callers pass temporaries.
    [[nodiscard]] boost::asio::awaitable<std::optional<Response>>
    process_streaming(Request&& request);

    [[nodiscard]] bool supports_streaming(const Request& request) const override;

    [[nodiscard]] boost::asio::awaitable<RequestProcessor::ResponseChunk> next_chunk() override;

#ifdef YAMS_TESTING
    // Lightweight test counters to verify embed streaming paths are exercised
    static int _test_get_batch_embed_steps();
    static int _test_get_embed_docs_steps();
    static int _test_get_final_emits();
#endif

private:
    // Unified implementation that operates on an owned Request copy
    [[nodiscard]] boost::asio::awaitable<std::optional<Response>>
    process_streaming_impl(Request request);
    // Lazy compute: store request to compute on first next_chunk() to allow
    // header to be written immediately by RequestHandler before heavy work
    // (critical for Grep to avoid header-timeouts in clients/pools).
    std::optional<std::unique_ptr<Request>> pending_request_;
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

    enum class Mode { None, Search, List, Grep, BatchEmbed, EmbedDocs };

    std::size_t compute_item_chunk_count(std::size_t approx_bytes_per_item) const;

    std::shared_ptr<RequestProcessor> delegate_;
    RequestHandler::Config cfg_;
    Mode mode_{Mode::None};
    // When we want to avoid copying the original Request across translation
    // units (to dodge any ODR/ABI surprises), we precompute and cache the
    // final response and stream it after emitting a heartbeat.
    std::optional<Response> pending_final_;
    std::optional<SearchState> search_;
    std::optional<ListState> list_;
    std::optional<GrepState> grep_;
    // Cached total for embedding requests to avoid any ABI/ODR surprises when copying variants
    std::size_t pending_total_{0};

    // Clear all per-request state so each streaming request starts from a known baseline.
    void reset_state();
};

} // namespace yams::daemon
