# ============================================================================
# Daemon Client Library (lightweight, no circular dependencies)
# ============================================================================
add_library(yams_daemon_client STATIC
    client/daemon_client.cpp
    client/asio_transport.cpp
    ipc/ipc_protocol.cpp
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
        yams::daemon_ipc
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

# Ensure Boost.Asio headers are available for implementation without exporting Boost targets
if(TARGET Boost::headers)
    get_target_property(_boost_hdrs_inc Boost::headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_hdrs_inc)
        target_include_directories(yams_daemon_ipc PRIVATE ${_boost_hdrs_inc})
    endif()
elseif(TARGET Boost::boost)
    get_target_property(_boost_boost_inc Boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_inc)
        target_include_directories(yams_daemon_ipc PRIVATE ${_boost_boost_inc})
    endif()
elseif(TARGET boost::boost)
    get_target_property(_boost_boost_lc_inc boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_lc_inc)
        target_include_directories(yams_daemon_ipc PRIVATE ${_boost_boost_lc_inc})
    endif()
endif()

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

# Default: use Boost.Asio-backed IO context
option(YAMS_IPC_USE_BOOST_ASIO "Use Boost.Asio backend for AsyncIOContext (default ON)" ON)
if(YAMS_IPC_USE_BOOST_ASIO)
    target_compile_definitions(yams_daemon_ipc PRIVATE YAMS_USE_BOOST_ASIO=1)
    # Ensure Boost headers are visible to this target without creating a link dependency
    # Prefer Conan-provided include variables if available
    if(DEFINED Boost_INCLUDE_DIRS)
        target_include_directories(yams_daemon_ipc PRIVATE ${Boost_INCLUDE_DIRS})
    elseif(DEFINED boost_INCLUDE_DIRS)
        target_include_directories(yams_daemon_ipc PRIVATE ${boost_INCLUDE_DIRS})
    elseif(TARGET Boost::headers)
        get_target_property(_bh_inc Boost::headers INTERFACE_INCLUDE_DIRECTORIES)
        if(_bh_inc)
            target_include_directories(yams_daemon_ipc PRIVATE ${_bh_inc})
        endif()
    elseif(TARGET Boost::boost)
        get_target_property(_bb_inc Boost::boost INTERFACE_INCLUDE_DIRECTORIES)
        if(_bb_inc)
            target_include_directories(yams_daemon_ipc PRIVATE ${_bb_inc})
        endif()
    elseif(TARGET boost::boost)
        get_target_property(_blc_inc boost::boost INTERFACE_INCLUDE_DIRECTORIES)
        if(_blc_inc)
            target_include_directories(yams_daemon_ipc PRIVATE ${_blc_inc})
        endif()
    else()
        # Fallback: fetch Boost headers locally (header-only) to satisfy Conan builds lacking Boost
        include(FetchContent)
        FetchContent_Declare(
            Boost
            URL https://github.com/boostorg/boost/releases/download/boost-1.86.0/boost-1.86.0.tar.xz
            URL_HASH SHA256=9a2fd4f2e1a16ad8c9f4c5b09d0a717d67868d8ad4dc4d79da808b6b8c97e469
            DOWNLOAD_EXTRACT_TIMESTAMP ON
        )
        FetchContent_MakeAvailable(Boost)
        if(DEFINED boost_SOURCE_DIR)
            # Add common boost include roots to cover asio headers
            target_include_directories(yams_daemon_ipc PRIVATE
                ${boost_SOURCE_DIR}
                ${boost_SOURCE_DIR}/libs/headers/include
                ${boost_SOURCE_DIR}/libs/asio/include
            )
        endif()
    endif()
    # If project created a yams_boost_headers interface target, use it privately
    if(TARGET yams_boost_headers)
        target_link_libraries(yams_daemon_ipc PRIVATE yams_boost_headers)
    endif()
    message(STATUS "yams_daemon_ipc: Boost.Asio IO backend enabled (YAMS_USE_BOOST_ASIO)")
endif()

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

# =========================================================================
# Optional: Protobuf payload support for IPC (generated + serializer)
# =========================================================================
if(Protobuf_FOUND)
    message(STATUS "IPC: Protobuf FOUND - enabling protobuf payload support")

    set(YAMS_IPC_PROTO_DIR ${CMAKE_SOURCE_DIR}/include)
    set(YAMS_IPC_PROTO_REL yams/daemon/ipc/proto/ipc_envelope.proto)
    set(YAMS_IPC_PROTO ${YAMS_IPC_PROTO_DIR}/${YAMS_IPC_PROTO_REL})
    set(YAMS_IPC_PROTO_OUTDIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${YAMS_IPC_PROTO_OUTDIR})

    set(YAMS_IPC_PROTO_GEN_SRCS
        ${YAMS_IPC_PROTO_OUTDIR}/yams/daemon/ipc/proto/ipc_envelope.pb.cc
        ${YAMS_IPC_PROTO_OUTDIR}/yams/daemon/ipc/proto/ipc_envelope.pb.h)

    # Prefer Conan/CMake imported protoc targets; fallback to variables or system protoc with a warning.
    set(YAMS_PROTOC_EXECUTABLE "")
    set(YAMS_PROTOC_GENEX "")

    # Prefer Conan package folder variables from CMakeDeps (config-aware)
    if(NOT YAMS_PROTOC_EXECUTABLE)
        if(DEFINED protobuf_PACKAGE_FOLDER_DEBUG)
            set(_PROTOC_CAND ${protobuf_PACKAGE_FOLDER_DEBUG}/bin/protoc)
            if(EXISTS ${_PROTOC_CAND})
                set(YAMS_PROTOC_EXECUTABLE ${_PROTOC_CAND})
                message(STATUS "IPC: Using Conan protoc (Debug) -> ${YAMS_PROTOC_EXECUTABLE}")
            endif()
            unset(_PROTOC_CAND)
        endif()
        if(NOT YAMS_PROTOC_EXECUTABLE AND DEFINED protobuf_PACKAGE_FOLDER)
            set(_PROTOC_CAND ${protobuf_PACKAGE_FOLDER}/bin/protoc)
            if(EXISTS ${_PROTOC_CAND})
                set(YAMS_PROTOC_EXECUTABLE ${_PROTOC_CAND})
                message(STATUS "IPC: Using Conan protoc -> ${YAMS_PROTOC_EXECUTABLE}")
            endif()
            unset(_PROTOC_CAND)
        endif()
        if(NOT YAMS_PROTOC_EXECUTABLE AND DEFINED protobuf_PACKAGE_FOLDER_RELEASE)
            set(_PROTOC_CAND ${protobuf_PACKAGE_FOLDER_RELEASE}/bin/protoc)
            if(EXISTS ${_PROTOC_CAND})
                set(YAMS_PROTOC_EXECUTABLE ${_PROTOC_CAND})
                message(STATUS "IPC: Using Conan protoc (Release) -> ${YAMS_PROTOC_EXECUTABLE}")
            endif()
            unset(_PROTOC_CAND)
        endif()
    endif()

    # Try deriving protoc path from Conan include dir first
    if(NOT YAMS_PROTOC_EXECUTABLE)
        get_target_property(_PB_IFACES protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
        if(_PB_IFACES)
            foreach(_idir IN LISTS _PB_IFACES)
                if(_idir MATCHES ".*/\\.conan2/.*/p/include$")
                    get_filename_component(_pkgroot ${_idir} DIRECTORY)
                    set(_protoc ${_pkgroot}/bin/protoc)
                    if(EXISTS ${_protoc})
                        set(YAMS_PROTOC_EXECUTABLE ${_protoc})
                        message(STATUS "IPC: Using Conan protoc from package -> ${YAMS_PROTOC_EXECUTABLE}")
                        break()
                    endif()
                endif()
            endforeach()
        endif()
        unset(_PB_IFACES)
    endif()

    if(NOT YAMS_PROTOC_EXECUTABLE)
        if(TARGET protobuf::conan_protoc)
            set(YAMS_PROTOC_GENEX $<TARGET_FILE:protobuf::conan_protoc>)
            message(STATUS "IPC: Using Conan protoc target 'protobuf::conan_protoc'")
        elseif(TARGET protobuf::protoc)
            set(YAMS_PROTOC_GENEX $<TARGET_FILE:protobuf::protoc>)
            message(STATUS "IPC: Using protoc target 'protobuf::protoc'")
        elseif(DEFINED PROTOBUF_PROTOC_EXECUTABLE AND PROTOBUF_PROTOC_EXECUTABLE)
            set(YAMS_PROTOC_EXECUTABLE ${PROTOBUF_PROTOC_EXECUTABLE})
            message(STATUS "IPC: Using PROTOBUF_PROTOC_EXECUTABLE -> ${YAMS_PROTOC_EXECUTABLE}")
        elseif(DEFINED Protobuf_PROTOC_EXECUTABLE AND Protobuf_PROTOC_EXECUTABLE)
            set(YAMS_PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
            message(STATUS "IPC: Using Protobuf_PROTOC_EXECUTABLE -> ${YAMS_PROTOC_EXECUTABLE}")
        else()
            find_program(_PROTOC_EXE NAMES protoc)
            if(_PROTOC_EXE)
                set(YAMS_PROTOC_EXECUTABLE ${_PROTOC_EXE})
                if(NOT YAMS_PROTOC_EXECUTABLE MATCHES ".*/\\.conan2/.*/p/bin/protoc$")
                    message(WARNING "IPC: Falling back to system protoc at ${YAMS_PROTOC_EXECUTABLE}. Ensure it matches the protobuf headers (3.21.12).")
                endif()
            else()
                message(FATAL_ERROR "IPC: protoc not found. Install protobuf compiler or provide it via Conan.")
            endif()
            unset(_PROTOC_EXE)
        endif()
    endif()

    add_custom_command(
        OUTPUT ${YAMS_IPC_PROTO_GEN_SRCS}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${YAMS_IPC_PROTO_OUTDIR}
        COMMAND $<$<BOOL:${YAMS_PROTOC_GENEX}>:${YAMS_PROTOC_GENEX}>$<$<NOT:$<BOOL:${YAMS_PROTOC_GENEX}>>:${YAMS_PROTOC_EXECUTABLE}>
                --proto_path=${YAMS_IPC_PROTO_DIR}
                --cpp_out=${YAMS_IPC_PROTO_OUTDIR}
                ${YAMS_IPC_PROTO_REL}
        DEPENDS ${YAMS_IPC_PROTO}
        VERBATIM
        COMMENT "Generating IPC protobuf sources"
    )

    add_library(yams_ipc_proto STATIC
        ipc/ipc_protocol.cpp
        ipc/proto_serializer.cpp
        ${YAMS_IPC_PROTO_GEN_SRCS}
    )

    target_include_directories(yams_ipc_proto
        PUBLIC
            $<BUILD_INTERFACE:${YAMS_IPC_PROTO_OUTDIR}>
            $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    )

    target_link_libraries(yams_ipc_proto
        PUBLIC
            yams::core
            protobuf::libprotobuf
        PRIVATE
            spdlog::spdlog
    )

    target_compile_features(yams_ipc_proto PUBLIC cxx_std_20)
    set_target_properties(yams_ipc_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)

    # Install and export so dependents in export set are satisfied
    install(TARGETS yams_ipc_proto
        EXPORT YamsTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )

    # Link into client and ipc libs so protobuf payloads are available
    target_link_libraries(yams_daemon_client PRIVATE yams_ipc_proto)
    target_link_libraries(yams_daemon_ipc PRIVATE yams_ipc_proto)
else()
    message(STATUS "IPC: Protobuf NOT found - protobuf payload support disabled")
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
