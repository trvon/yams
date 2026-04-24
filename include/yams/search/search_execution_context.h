#pragma once

#include <boost/asio/any_io_executor.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace yams::search {

struct IndexFreshnessSnapshot {
    std::uint32_t ingestQueued{0};
    std::uint32_t ingestInFlight{0};
    std::uint32_t postIngestQueued{0};
    std::uint32_t postIngestInFlight{0};
    std::uint64_t lexicalDeltaQueuedEpoch{0};
    std::uint64_t lexicalDeltaPublishedEpoch{0};
    std::uint32_t lexicalDeltaPendingDocs{0};
    std::uint64_t lexicalDeltaPublishedDocs{0};
    std::uint32_t lexicalDeltaRecentDocs{0};
    std::uint64_t topologyEpoch{0};
    bool lexicalReady{false};
    bool vectorReady{false};
    bool kgReady{false};
    bool topologyReady{false};
    bool awaitingDrain{false};
    bool simeonLexicalConfigured{false};
    bool simeonLexicalReady{false};
    bool simeonLexicalBuilding{false};
    bool simeonFragmentGeometryReady{false};

    [[nodiscard]] bool corpusWarming() const noexcept {
        return ingestQueued > 0 || ingestInFlight > 0 || postIngestQueued > 0 ||
               postIngestInFlight > 0 || lexicalDeltaPendingDocs > 0 || awaitingDrain ||
               !lexicalReady;
    }
};

struct SearchExecutionContext {
    std::uint32_t activeRequests{1};
    std::uint32_t queuedRequests{0};
    std::uint32_t concurrencyLimit{1};
    std::uint32_t recommendedWorkers{1};
    bool allowApproximateFacets{false};
    bool shortQueryBudgeted{false};
    bool pressureBudgeted{false};
    IndexFreshnessSnapshot freshness{};
    std::vector<std::string> topologyOverlayHashes;
    std::optional<boost::asio::any_io_executor> workerExecutor;
};

[[nodiscard]] SearchExecutionContext defaultSearchExecutionContext() noexcept;
[[nodiscard]] const SearchExecutionContext* getSearchExecutionContext() noexcept;
[[nodiscard]] SearchExecutionContext currentSearchExecutionContext() noexcept;

class SearchExecutionContextGuard {
public:
    explicit SearchExecutionContextGuard(SearchExecutionContext context) noexcept;
    SearchExecutionContextGuard(const SearchExecutionContextGuard&) = delete;
    SearchExecutionContextGuard& operator=(const SearchExecutionContextGuard&) = delete;
    SearchExecutionContextGuard(SearchExecutionContextGuard&&) = delete;
    SearchExecutionContextGuard& operator=(SearchExecutionContextGuard&&) = delete;
    ~SearchExecutionContextGuard();

private:
    std::optional<SearchExecutionContext> previous_;
};

} // namespace yams::search
