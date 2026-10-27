#pragma once

/**
 * @file profiling.h
 * @brief Profiling support using Tracy profiler
 *
 * This header provides wrapper macros for Tracy profiling that are
 * automatically disabled when TRACY_ENABLE is not defined.
 */

#ifdef TRACY_ENABLE
#include <tracy/Tracy.hpp>

#include <cstdint>
#include <optional>
#include <type_traits>

namespace yams::profiling_detail {
// Tracy's PlotData is overloaded on int64_t/float/double only; an int, size_t or bool argument is
// ambiguous. Normalize arithmetic values so YAMS_PLOT accepts any numeric type.
template <typename T> constexpr auto plotValue(T v) {
    if constexpr (std::is_floating_point_v<T>)
        return static_cast<double>(v);
    else
        return static_cast<int64_t>(v);
}
} // namespace yams::profiling_detail

// Zone macros. Use a line-unique zone variable so multiple zones can live in
// the same scope — Tracy's ZoneScoped* all reuse one fixed variable name
// (___tracy_scoped_zone), which is a redefinition error when two appear in one
// block.
#define YAMS_ZONE_CONCAT_(a, b) a##b
#define YAMS_ZONE_CONCAT(a, b) YAMS_ZONE_CONCAT_(a, b)
#define YAMS_ZONE_SCOPED() ZoneNamed(YAMS_ZONE_CONCAT(__yamsZone_, __LINE__), true)
#define YAMS_ZONE_SCOPED_N(name) ZoneNamedN(YAMS_ZONE_CONCAT(__yamsZone_, __LINE__), name, true)
#define YAMS_ZONE_SCOPED_C(color) ZoneNamedC(YAMS_ZONE_CONCAT(__yamsZone_, __LINE__), color, true)
#define YAMS_ZONE_SCOPED_NC(name, color)                                                           \
    ZoneNamedNC(YAMS_ZONE_CONCAT(__yamsZone_, __LINE__), name, color, true)
// Dynamic (runtime) zone name — ptr/len need not be compile-time constants.
#define YAMS_ZONE_SCOPED_DYN(ptr, len)                                                             \
    ZoneNamed(YAMS_ZONE_CONCAT(__yamsZoneDyn_, __LINE__), true);                                   \
    YAMS_ZONE_CONCAT(__yamsZoneDyn_, __LINE__).Name(ptr, len)

// A zone must begin and end on the same thread, in stack order. In a coroutine, a
// scoped zone must not stay open across co_await/co_yield: the coroutine can resume
// on another thread and the profiler drops the capture ("Zone is ended twice").
// Scope zones to synchronous code. YAMS_ZONE_SYNC_N opens a zone that
// YAMS_ZONE_END closes early, at the same scope, before the first suspension point.
// tests/scripts/check_tracy_zones.py enforces this.
#define YAMS_ZONE_SYNC_N(var, name)                                                                \
    static constexpr ::tracy::SourceLocationData YAMS_ZONE_CONCAT(__yamsSyncLoc_, var){            \
        name, TracyFunction, TracyFile, (uint32_t)TracyLine, 0};                                   \
    std::optional<::tracy::ScopedZone> var;                                                        \
    var.emplace(&YAMS_ZONE_CONCAT(__yamsSyncLoc_, var), TRACY_CALLSTACK, true)
#define YAMS_ZONE_END(var) var.reset()
// Zone with a runtime name (a C string). The profiler groups zones by name, so this attributes
// time per runtime key (for example a caller). It allocates per call; keep it off hot paths.
#define YAMS_ZONE_TRANSIENT_N(var, name) ZoneTransientN(var, name, true)

// Frame marking
#define YAMS_FRAME_MARK() FrameMark
#define YAMS_FRAME_MARK_NAMED(name) FrameMarkNamed(name)
#define YAMS_FRAME_MARK_START(name) FrameMarkStart(name)
#define YAMS_FRAME_MARK_END(name) FrameMarkEnd(name)

// Thread naming
#define YAMS_SET_THREAD_NAME(name) tracy::SetThreadName(name)

// Memory tracking
#define YAMS_ALLOC(ptr, size) TracyAlloc(ptr, size)
#define YAMS_FREE(ptr) TracyFree(ptr)

// Message logging
#define YAMS_MESSAGE(txt, size) TracyMessage(txt, size)
#define YAMS_MESSAGE_L(txt) TracyMessageL(txt)

// Plot values
#define YAMS_PLOT(name, val) TracyPlot(name, ::yams::profiling_detail::plotValue(val))
#define YAMS_PLOT_F(name, val) TracyPlot(name, static_cast<float>(val))

// Locks
#define YAMS_LOCKABLE(type, name) TracyLockable(type, name)
#define YAMS_SHARED_LOCKABLE(type, name) TracySharedLockable(type, name)

// Custom YAMS-specific profiling macros
#define YAMS_EMBEDDING_ZONE_BATCH(size)                                                            \
    YAMS_ZONE_SCOPED_N("Embedding::Batch");                                                        \
    YAMS_PLOT("BatchSize", static_cast<int64_t>(size))

#define YAMS_VECTOR_SEARCH_ZONE(k, threshold)                                                      \
    YAMS_ZONE_SCOPED_N("Vector::Search");                                                          \
    YAMS_PLOT("SearchK", static_cast<int64_t>(k));                                                 \
    YAMS_PLOT_F("SearchThreshold", threshold)

#define YAMS_WORKER_QUEUE_PLOT(size) YAMS_PLOT("WorkerQueue", size)

#define YAMS_ONNX_ZONE(operation) YAMS_ZONE_SCOPED_NC("ONNX::" operation, 0x00FF00)

#define YAMS_STORAGE_ZONE(operation) YAMS_ZONE_SCOPED_NC("Storage::" operation, 0x0080FF)

// Fibers/coroutines (optional). If Tracy provides fiber APIs, use them; else no-op.
#ifdef TracyFiberEnter
#define YAMS_FIBER_ENTER(name) TracyFiberEnter(name)
#define YAMS_FIBER_LEAVE() TracyFiberLeave
#else
#define YAMS_FIBER_ENTER(name)
#define YAMS_FIBER_LEAVE()
#endif

#else
// No-op macros when profiling is disabled
#define YAMS_ZONE_SCOPED()
#define YAMS_ZONE_SCOPED_N(name)
#define YAMS_ZONE_SCOPED_C(color)
#define YAMS_ZONE_SCOPED_NC(name, color)
#define YAMS_ZONE_SCOPED_DYN(ptr, len)
#define YAMS_ZONE_SYNC_N(var, name)
#define YAMS_ZONE_END(var)
#define YAMS_ZONE_TRANSIENT_N(var, name)

#define YAMS_FRAME_MARK()
#define YAMS_FRAME_MARK_NAMED(name)
#define YAMS_FRAME_MARK_START(name)
#define YAMS_FRAME_MARK_END(name)

// Thread naming
#define YAMS_SET_THREAD_NAME(name)

#define YAMS_ALLOC(ptr, size)
#define YAMS_FREE(ptr)

#define YAMS_MESSAGE(txt, size)
#define YAMS_MESSAGE_L(txt)

#define YAMS_PLOT(name, val)
#define YAMS_PLOT_F(name, val)

#define YAMS_LOCKABLE(type, name) type name
#define YAMS_SHARED_LOCKABLE(type, name) type name

// No-op versions of custom YAMS macros
#define YAMS_EMBEDDING_ZONE_BATCH(size)
#define YAMS_VECTOR_SEARCH_ZONE(k, threshold)
#define YAMS_WORKER_QUEUE_PLOT(size)
#define YAMS_ONNX_ZONE(operation)
#define YAMS_STORAGE_ZONE(operation)

// Fibers no-op when profiling disabled
#define YAMS_FIBER_ENTER(name)
#define YAMS_FIBER_LEAVE()

#endif // TRACY_ENABLE
