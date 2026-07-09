// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>
#include <yams/search/search_tracing.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace yams::search::detail {

template <typename Work>
auto postWork(Work work, const std::optional<boost::asio::any_io_executor>& executor)
    -> std::future<decltype(work())> {
    using WorkResult = decltype(work());
    std::packaged_task<WorkResult()> task(std::move(work));
    auto future = task.get_future();
    if (executor) {
        boost::asio::post(*executor, [task = std::move(task)]() mutable { task(); });
    } else {
        task();
    }
    return future;
}

enum class ComponentStatus : std::uint8_t { Success, Failed, TimedOut };

struct SearchCandidateFanout {
    using Future = std::future<Result<std::vector<ComponentResult>>>;

    Future text;
    Future kg;
    Future path;
    Future vector;
    Future topologyVector;
    Future entityVector;
    Future tag;
    Future metadata;
};

template <typename Fn>
SearchCandidateFanout::Future
scheduleComponent(float weight, const std::optional<boost::asio::any_io_executor>& executor,
                  Fn&& fn) {
    if (weight <= 0.0f) {
        return {};
    }
    return postWork(std::forward<Fn>(fn), executor);
}

struct ComponentFanoutSinks {
    std::vector<ComponentResult>& allComponentResults;
    std::map<std::string, int64_t>& componentTiming;
    std::vector<std::string>& contributing;
    std::vector<std::string>& failed;
    std::vector<std::string>& timedOut;
    std::atomic<uint64_t>& timedOutQueries;
};

class ComponentFanoutCollector {
public:
    using Future = SearchCandidateFanout::Future;

    ComponentFanoutCollector(const SearchEngineConfig& config, SearchTraceCollector& trace,
                             ComponentFanoutSinks sinks)
        : config_(config), trace_(trace), sinks_(sinks) {}

    ComponentStatus collect(Future& future, const char* name, std::atomic<uint64_t>& queryCount,
                            std::atomic<uint64_t>& avgTime);

    void collectAndRecord(Future& future, const char* name, std::atomic<uint64_t>& queryCount,
                          std::atomic<uint64_t>& avgTime);

private:
    const SearchEngineConfig& config_;
    SearchTraceCollector& trace_;
    ComponentFanoutSinks sinks_;
};

} // namespace yams::search::detail
