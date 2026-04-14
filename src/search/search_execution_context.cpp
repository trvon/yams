#include <yams/search/search_execution_context.h>

#include <algorithm>
#include <thread>

namespace yams::search {
namespace {
thread_local std::optional<SearchExecutionContext> t_context;
}

SearchExecutionContext defaultSearchExecutionContext() noexcept {
    SearchExecutionContext context;
    const auto hw = std::max(1u, std::thread::hardware_concurrency());
    const auto conservativeWorkers = hw > 1 ? hw - 1 : 1;
    context.activeRequests = 1;
    context.queuedRequests = 0;
    context.concurrencyLimit = static_cast<std::uint32_t>(conservativeWorkers);
    context.recommendedWorkers = static_cast<std::uint32_t>(std::min(4u, conservativeWorkers));
    context.postIngestQueued = 0;
    context.postIngestInFlight = 0;
    context.allowApproximateFacets = false;
    context.shortQueryBudgeted = false;
    context.pressureBudgeted = false;
    context.searchEngineReady = false;
    context.searchEngineAwaitingDrain = false;
    context.corpusWarming = false;
    context.workerExecutor.reset();
    return context;
}

const SearchExecutionContext* getSearchExecutionContext() noexcept {
    return t_context ? &*t_context : nullptr;
}

SearchExecutionContext currentSearchExecutionContext() noexcept {
    return t_context.value_or(defaultSearchExecutionContext());
}

SearchExecutionContextGuard::SearchExecutionContextGuard(SearchExecutionContext context) noexcept
    : previous_(t_context) {
    t_context = std::move(context);
}

SearchExecutionContextGuard::~SearchExecutionContextGuard() {
    t_context = std::move(previous_);
}

} // namespace yams::search
