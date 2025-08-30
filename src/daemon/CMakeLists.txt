# ============================================================================
# Daemon Client Library (lightweight, no circular dependencies)
# ============================================================================
add_library(yams_daemon_client STATIC
    client/daemon_client.cpp
    client/circuit_breaker.cpp
    ipc/ipc_protocol.cpp
    ipc/message_serializer.cpp
    ipc/message_framing.cpp
)

target_include_directories(yams_daemon_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(yams_daemon_client
    PUBLIC
        yams::core
    PRIVATE
        spdlog::spdlog
)

# tl-expected is header-only; link to whichever target exists to propagate include dirs
if(TARGET tl-expected::tl-expected)
    target_link_libraries(yams_daemon_client PUBLIC tl-expected::tl-expected)
elseif(TARGET tl::expected)
    target_link_libraries(yams_daemon_client PUBLIC tl::expected)
endif()

target_compile_features(yams_daemon_client PUBLIC cxx_std_20)
set_target_properties(yams_daemon_client PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Create alias for daemon client
add_library(yams::daemon_client ALIAS yams_daemon_client)

# ============================================================================
# Daemon IPC Library (shared by daemon and MCP)
# ============================================================================
add_library(yams_daemon_ipc STATIC
    ipc/async_socket.cpp
    ipc/async_ipc_server.cpp
    ipc/request_handler.cpp
    ipc/streaming_processor.cpp
    ipc/connection_fsm.cpp
    ipc/retrieval_session.cpp
    ipc/thread_pool.cpp
)

# Install daemon IPC library and export with YamsTargets so dependents are satisfied
install(TARGETS yams_daemon_ipc
    EXPORT YamsTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

target_include_directories(yams_daemon_ipc
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(yams_daemon_ipc
    PUBLIC
        yams::core
        ZLIB::ZLIB          # For CRC32 in framing
    PRIVATE
        spdlog::spdlog
)

# tl-expected is header-only; link interface target to propagate include dirs
if(TARGET tl-expected::tl-expected)
    target_link_libraries(yams_daemon_ipc PUBLIC tl-expected::tl-expected)
elseif(TARGET tl::expected)
    target_link_libraries(yams_daemon_ipc PUBLIC tl::expected)
endif()

target_compile_features(yams_daemon_ipc PUBLIC cxx_std_20)
set_target_properties(yams_daemon_ipc PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Create alias for daemon ipc
add_library(yams::daemon_ipc ALIAS yams_daemon_ipc)

# Platform-specific async I/O defines for the IPC library (to match implementation)
option(YAMS_USE_KQUEUE "Use kqueue backend for async IO on Apple/BSD" OFF)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_package(PkgConfig)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(LIBURING liburing)
        if(LIBURING_FOUND)
            target_compile_definitions(yams_daemon_ipc PRIVATE HAVE_IO_URING=1)
            target_link_libraries(yams_daemon_ipc PRIVATE ${LIBURING_LIBRARIES})
            target_include_directories(yams_daemon_ipc PRIVATE ${LIBURING_INCLUDE_DIRS})
            message(STATUS "io_uring support enabled for yams_daemon_ipc")
        else()
            message(STATUS "yams_daemon_ipc: liburing not found, using select/epoll fallback")
        endif()
    endif()
elseif(APPLE)
    if(YAMS_USE_KQUEUE)
        target_compile_definitions(yams_daemon_ipc PRIVATE HAVE_KQUEUE=1)
        message(STATUS "kqueue support enabled for async I/O (yams_daemon_ipc)")
    else()
        message(STATUS "kqueue disabled (YAMS_USE_KQUEUE=OFF); using select()-based backend for yams_daemon_ipc")
    endif()
endif()

# ============================================================================
# Main Daemon Library
# ============================================================================
add_library(yams_daemon STATIC
    daemon.cpp
    components/RequestDispatcher.cpp
    components/LifecycleComponent.cpp
    components/ServiceManager.cpp
    components/RepairCoordinator.cpp
    resource/mock_model_provider.cpp
    resource/plugin_loader.cpp
)

target_include_directories(yams_daemon
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(yams_daemon
    PUBLIC
        yams::core
        yams::search
        yams::vector
        yams::api
        yams_downloader
        yams_repair
        yams_app_services
        yams::daemon_ipc
        yams_daemon_client
    PRIVATE
        spdlog::spdlog
        pthread
        ${CMAKE_DL_LIBS}  # For dlopen/dlsym
        # Removed: onnxruntime::onnxruntime (moved to plugin)
)

# tl-expected is header-only; link interface target so tl headers are visible during yams_daemon compile
if(TARGET tl-expected::tl-expected)
    target_link_libraries(yams_daemon PUBLIC tl-expected::tl-expected)
elseif(TARGET tl::expected)
    target_link_libraries(yams_daemon PUBLIC tl::expected)
endif()

# Note: ONNX plugin is now loaded dynamically at runtime, not linked statically

# Platform-specific async I/O dependencies
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Check for io_uring support
    find_package(PkgConfig)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(LIBURING liburing)
        if(LIBURING_FOUND)
            target_compile_definitions(yams_daemon PRIVATE HAVE_IO_URING=1)
            target_link_libraries(yams_daemon PRIVATE ${LIBURING_LIBRARIES})
            target_include_directories(yams_daemon PRIVATE ${LIBURING_INCLUDE_DIRS})
            message(STATUS "io_uring support enabled for async I/O")
        else()
            message(WARNING "liburing not found - async I/O will use fallback implementation")
            message(STATUS "Install liburing-dev for better async I/O performance on Linux")
        endif()
    endif()
elseif(APPLE)
    if(YAMS_USE_KQUEUE)
        # macOS uses kqueue when explicitly enabled
        target_compile_definitions(yams_daemon PRIVATE HAVE_KQUEUE=1)
        message(STATUS "kqueue support enabled for async I/O (yams_daemon)")
    else()
        message(STATUS "kqueue disabled (YAMS_USE_KQUEUE=OFF); using select()-based backend for yams_daemon")
    endif()
endif()

target_compile_features(yams_daemon PUBLIC cxx_std_20)
set_target_properties(yams_daemon PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Daemon executable
add_executable(yams-daemon
    daemon_main.cpp
)

target_link_libraries(yams-daemon
    PRIVATE
        yams::daemon
        yams_config
        CLI11::CLI11
        spdlog::spdlog
)

# Export and install daemon client library
install(TARGETS yams_daemon_client
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Export and install daemon library
install(TARGETS yams_daemon
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install daemon executable
install(TARGETS yams-daemon
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Install headers
install(
    DIRECTORY ${CMAKE_SOURCE_DIR}/include/yams/daemon
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/yams
    FILES_MATCHING PATTERN "*.h"
)

# Add alias for consistency
add_library(yams::daemon ALIAS yams_daemon)

# ============================================================================
# Optional ONNX Model Provider Plugin
# ============================================================================

# Check if ONNX Runtime is available
find_package(onnxruntime QUIET)

if(onnxruntime_FOUND AND NOT YAMS_DISABLE_ONNX)
    message(STATUS "Building ONNX model provider plugin")

    # Create ONNX plugin as a separate library
    add_library(yams_onnx_plugin SHARED
        resource/onnx_model_pool.cpp
        resource/onnx_model_provider.cpp
    )

    target_include_directories(yams_onnx_plugin
        PRIVATE
            $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
            ${CMAKE_CURRENT_SOURCE_DIR}
    )

    target_link_libraries(yams_onnx_plugin
        PRIVATE
            yams::core
            yams::vector
            onnxruntime::onnxruntime
            spdlog::spdlog
    )

    target_compile_features(yams_onnx_plugin PUBLIC cxx_std_20)

    # Install ONNX plugin to plugins directory
    install(TARGETS yams_onnx_plugin
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins
    )

    # Create plugins directory during installation
    install(DIRECTORY DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins)

    # Make plugin available for tests
    add_library(yams::onnx_plugin ALIAS yams_onnx_plugin)
else()
    message(STATUS "ONNX Runtime not found or disabled - ONNX plugin will not be built")
    message(STATUS "  Embedding features will use mock provider")
endif()

# Link profiling where appropriate when profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET yams_profiling)
    if(TARGET yams_daemon_client)
        target_link_libraries(yams_daemon_client PRIVATE $<BUILD_INTERFACE:yams_profiling>)
    endif()
    if(TARGET yams_daemon_ipc)
        target_link_libraries(yams_daemon_ipc PRIVATE $<BUILD_INTERFACE:yams_profiling>)
    endif()
    if(TARGET yams_daemon)
        target_link_libraries(yams_daemon PRIVATE $<BUILD_INTERFACE:yams_profiling>)
    endif()
    if(TARGET yams-daemon)
        target_link_libraries(yams-daemon PRIVATE $<BUILD_INTERFACE:yams_profiling>)
    endif()
endif()
