#pragma once

#include <boost/asio/any_io_executor.hpp>

#include <cstdint>
#include <optional>

namespace yams::search {

struct SearchExecutionContext {
    std::uint32_t activeRequests{1};
    std::uint32_t queuedRequests{0};
    std::uint32_t concurrencyLimit{1};
    std::uint32_t recommendedWorkers{1};
    bool allowApproximateFacets{false};
    bool shortQueryBudgeted{false};
    bool pressureBudgeted{false};
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
