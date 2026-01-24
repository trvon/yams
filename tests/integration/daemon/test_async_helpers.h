#pragma once

#include <chrono>
#include <filesystem>
#include <future>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/core/types.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::test_async {

using namespace std::chrono_literals;

namespace detail {
template <typename T>
inline yams::Result<T> run_with_timeout(boost::asio::awaitable<yams::Result<T>> aw,
                                        std::chrono::milliseconds timeout) {
    // Use shared_ptr to keep promise alive even if this function returns early (timeout)
    // Fixes race where coroutine completes after timeout and tries to access destroyed promise
    auto prom = std::make_shared<std::promise<yams::Result<T>>>();
    auto fut = prom->get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [a = std::move(aw), prom]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await std::move(a);
            prom->set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (fut.wait_for(timeout) != std::future_status::ready) {
        return yams::Error{yams::ErrorCode::Timeout, "test await timeout"};
    }
    return fut.get();
}
} // namespace detail

// Test convenience wrappers
template <typename T>
inline yams::Result<T> res(boost::asio::awaitable<yams::Result<T>> aw,
                           std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    return detail::run_with_timeout<T>(std::move(aw), timeout);
}

template <typename T>
inline bool ok(boost::asio::awaitable<yams::Result<T>> aw,
               std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    return static_cast<bool>(detail::run_with_timeout<T>(std::move(aw), timeout));
}

template <typename T>
inline T val(boost::asio::awaitable<yams::Result<T>> aw,
             std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    auto r = detail::run_with_timeout<T>(std::move(aw), timeout);
    if (!r)
        throw std::runtime_error("Async task failed");
    return std::move(r.value());
}

} // namespace yams::test_async

// Backward-compat: provide yams::cli::run_sync used by tests (global yams::cli)
namespace yams::cli {
template <typename T>
inline yams::Result<T> run_sync(boost::asio::awaitable<yams::Result<T>> aw,
                                std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    return yams::test_async::detail::run_with_timeout<T>(std::move(aw), timeout);
}
} // namespace yams::cli

// Metadata visibility helper for integration tests
// Waits until a document with the given hash is visible in the metadata repository
namespace yams::test {

template <typename MetadataRepo>
inline bool
waitForDocumentMetadata(MetadataRepo& repo, const std::string& hash,
                        std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto doc = repo.getDocumentByHash(hash);
        if (doc && doc.value().has_value()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

// Helper that takes a shared_ptr to MetadataRepository
template <typename MetadataRepo>
inline bool
waitForDocumentMetadata(std::shared_ptr<MetadataRepo> repo, const std::string& hash,
                        std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
    if (!repo)
        return false;
    return waitForDocumentMetadata(*repo, hash, timeout);
}

// Wait for at least `minCount` documents to be visible under a given path pattern.
// Useful for directory ingestion where the response hash is empty.
template <typename MetadataRepo>
inline bool
waitForDocumentsByPath(MetadataRepo& repo, const std::string& pathPattern, size_t minCount = 1,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    // Canonicalize path to handle macOS symlinks (/var -> /private/var)
    std::string canonicalPattern = pathPattern;
    try {
        if (!pathPattern.empty() && std::filesystem::exists(pathPattern)) {
            canonicalPattern = std::filesystem::canonical(pathPattern).string();
        }
    } catch (...) {
        // Ignore errors, use original pattern
    }

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        // Use queryDocuments with a LIKE pattern
        yams::metadata::DocumentQueryOptions opts;
        opts.likePattern = canonicalPattern + "%"; // SQL LIKE pattern
        opts.limit = static_cast<int>(minCount + 1);
        auto docs = repo.queryDocuments(opts);
        if (docs && docs.value().size() >= minCount) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

// Helper that takes a shared_ptr
template <typename MetadataRepo>
inline bool
waitForDocumentsByPath(std::shared_ptr<MetadataRepo> repo, const std::string& pathPattern,
                       size_t minCount = 1,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    if (!repo)
        return false;
    return waitForDocumentsByPath(*repo, pathPattern, minCount, timeout);
}

} // namespace yams::test
