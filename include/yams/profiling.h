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

// Zone macros
#define YAMS_ZONE_SCOPED() ZoneScoped
#define YAMS_ZONE_SCOPED_N(name) ZoneScopedN(name)
#define YAMS_ZONE_SCOPED_C(color) ZoneScopedC(color)
#define YAMS_ZONE_SCOPED_NC(name, color) ZoneScopedNC(name, color)

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
#define YAMS_PLOT(name, val) TracyPlot(name, val)
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

#else
// No-op macros when profiling is disabled
#define YAMS_ZONE_SCOPED()
#define YAMS_ZONE_SCOPED_N(name)
#define YAMS_ZONE_SCOPED_C(color)
#define YAMS_ZONE_SCOPED_NC(name, color)

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

#endif // TRACY_ENABLE