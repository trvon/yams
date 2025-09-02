#pragma once

// Non-spin CLI bridge utilities for running coroutine Tasks to completion
// using Boost.Asio co_spawn + use_future without busy loops.
//
// This header provides small helpers to synchronously wait on yams::Task<T>
// (C++20 coroutine) results from CLI code paths that remain synchronous,
// while the underlying I/O continues to be fully asynchronous.
//
// Design notes:
// - We use boost::asio::co_spawn with system_executor and use_future. The
//   coroutine body (awaitables) is scheduled/resumed by the project's
//   io_context and does not depend on the executor provided to co_spawn.
// - No spin-wait loops are used. The wait happens on a std::future with an
//   optional timeout.
// - On timeout, we return a Timeout error to the caller. Cancellation of the
//   underlying coroutine is not forced here (that requires explicit cancel
//   wiring in the async pipeline).
//
// Usage examples:
//
//   // Task<Result<Response>> path
//   yams::Result<MyResponse> r = yams::cli::run_sync(
//       my_async_call_returning_Task_Result_MyResponse(), std::chrono::seconds(5));
//
//   // Task<void> but wrapped as Task<Result<void>>
//   yams::Result<void> ok = yams::cli::run_sync(
//       my_async_task_void_wrapped_as_result(), std::chrono::seconds(2));
//
//   // Task<T> where T is not Result<...> (we convert exceptions to Error)
//   auto plain = yams::cli::run_sync_plain(
//       my_async_plain_task_returning_T(), std::chrono::seconds(1));
//
// Implementation limitations:
// - Timeout does not attempt to cancel in-flight I/O. It only stops waiting and
//   returns a Timeout error. The in-flight coroutine should be designed to
//   handle cancellation signals from higher layers, if desired.

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <type_traits>

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/asio/use_future.hpp>

#include <yams/core/task.h>
#include <yams/core/types.h>
#include <yams/daemon/client/global_io_context.h>

namespace yams {
namespace cli {

namespace detail {

// Map exceptions to a generic InternalError for plain Task<T> bridges.
// For Task<Result<T>> bridges, we rely on the Result surface returned by the task.
inline Error to_internal_error_from_exception(const char* what) {
    return Error{ErrorCode::InternalError, what ? std::string(what) : std::string("Internal error")};
}

} // namespace detail

// Bridge for Task<Result<T>>: returns Result<T> to the caller.
// If timeout is non-zero and the future is not ready within the duration,
// returns Error{Timeout, "..."} without forcing cancellation of the coroutine.
// Primary template for Task<Result<T>> pattern (our most common use case).
template <typename T>
inline Result<T> run_sync(Task<Result<T>> task,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
    struct TaskRunner {
        static Task<void> run(std::shared_ptr<Task<Result<T>>> task, std::shared_ptr<std::promise<Result<T>>> promise) {
            try {
                auto result = co_await std::move(*task);
                promise->set_value(std::move(result));
            } catch (const std::exception& e) {
                promise->set_value(Error{ErrorCode::InternalError, e.what()});
            } catch (...) {
                promise->set_value(Error{ErrorCode::InternalError, "Unknown error"});
            }
            co_return;
        }
    };
    
    auto promise = std::make_shared<std::promise<Result<T>>>();
    auto future = promise->get_future();
    auto task_ptr = std::make_shared<Task<Result<T>>>(std::move(task));

    // Schedule task execution on global IO context
    yams::daemon::GlobalIOContext::instance().get_io_context().post([task_ptr, promise]() {
        auto coro = TaskRunner::run(task_ptr, promise);
        (void)coro; // fire and forget
    });

    if (timeout.count() > 0) {
        auto st = future.wait_for(timeout);
        if (st != std::future_status::ready) {
            return Error{ErrorCode::Timeout, "Operation timed out"};
        }
    }

    try {
        return future.get();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    } catch (...) {
        return Error{ErrorCode::InternalError, "Unknown error"};
    }
}

// Bridge specialization for Task<Result<void>> to make call sites simpler.
template <>
inline Result<void> run_sync<void>(Task<Result<void>> task,
                                   std::chrono::milliseconds timeout) {
    struct TaskRunner {
        static Task<void> run(std::shared_ptr<Task<Result<void>>> task, std::shared_ptr<std::promise<Result<void>>> promise) {
            try {
                auto result = co_await std::move(*task);
                promise->set_value(std::move(result));
            } catch (const std::exception& e) {
                promise->set_value(Error{ErrorCode::InternalError, e.what()});
            } catch (...) {
                promise->set_value(Error{ErrorCode::InternalError, "Unknown error"});
            }
            co_return;
        }
    };
    
    auto promise = std::make_shared<std::promise<Result<void>>>();
    auto future = promise->get_future();
    auto task_ptr = std::make_shared<Task<Result<void>>>(std::move(task));

    // Schedule task execution on global IO context
    yams::daemon::GlobalIOContext::instance().get_io_context().post([task_ptr, promise]() {
        auto coro = TaskRunner::run(task_ptr, promise);
        (void)coro; // fire and forget
    });

    if (timeout.count() > 0) {
        auto st = future.wait_for(timeout);
        if (st != std::future_status::ready) {
            return Error{ErrorCode::Timeout, "Operation timed out"};
        }
    }

    try {
        return future.get();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    } catch (...) {
        return Error{ErrorCode::InternalError, "Unknown error"};
    }
}

// Bridge for plain Task<T> (non-Result returns).
// Converts thrown exceptions into Error{InternalError,...} and returns Result<T>.
template <typename T>
inline Result<T> run_sync_plain(Task<T> task,
                                std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
    using boost::asio::co_spawn;
    using boost::asio::system_executor;
    using boost::asio::use_future;

    std::future<T> fut = co_spawn(system_executor{}, std::move(task), use_future);

    if (timeout.count() > 0) {
        auto st = fut.wait_for(timeout);
        if (st != std::future_status::ready) {
            return Error{ErrorCode::Timeout, "Operation timed out"};
        }
    }

    try {
        return fut.get();
    } catch (const std::exception& e) {
        return detail::to_internal_error_from_exception(e.what());
    } catch (...) {
        return Error{ErrorCode::InternalError, "Unknown error"};
    }
}

} // namespace cli
} // namespace yams