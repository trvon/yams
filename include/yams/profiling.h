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
    
    // Memory tracking
    #define YAMS_ALLOC(ptr, size) TracyAlloc(ptr, size)
    #define YAMS_FREE(ptr) TracyFree(ptr)
    
    // Message logging
    #define YAMS_MESSAGE(txt, size) TracyMessage(txt, size)
    #define YAMS_MESSAGE_L(txt) TracyMessageL(txt)
    
    // Plot values
    #define YAMS_PLOT(name, val) TracyPlot(name, val)
    #define YAMS_PLOT_F(name, val) TracyPlotF(name, val)
    
    // Locks
    #define YAMS_LOCKABLE(type, name) TracyLockable(type, name)
    #define YAMS_SHARED_LOCKABLE(type, name) TracySharedLockable(type, name)
    
#else
    // No-op macros when profiling is disabled
    #define YAMS_ZONE_SCOPED()
    #define YAMS_ZONE_SCOPED_N(name)
    #define YAMS_ZONE_SCOPED_C(color)
    #define YAMS_ZONE_SCOPED_NC(name, color)
    
    #define YAMS_FRAME_MARK()
    #define YAMS_FRAME_MARK_NAMED(name)
    
    #define YAMS_ALLOC(ptr, size)
    #define YAMS_FREE(ptr)
    
    #define YAMS_MESSAGE(txt, size)
    #define YAMS_MESSAGE_L(txt)
    
    #define YAMS_PLOT(name, val)
    #define YAMS_PLOT_F(name, val)
    
    #define YAMS_LOCKABLE(type, name) type name
    #define YAMS_SHARED_LOCKABLE(type, name) type name
    
#endif // TRACY_ENABLE