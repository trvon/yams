# ============================================================================
# Helper function to add Boost headers to a target
# ============================================================================
function(yams_add_boost_headers TARGET_NAME)
    # First try to use the yams_boost_headers target if available
    if(TARGET yams_boost_headers)
        target_link_libraries(${TARGET_NAME} PUBLIC yams_boost_headers)
    elseif(DEFINED Boost_INCLUDE_DIRS)
        target_include_directories(${TARGET_NAME} PRIVATE ${Boost_INCLUDE_DIRS})
    elseif(DEFINED boost_INCLUDE_DIRS)
        target_include_directories(${TARGET_NAME} PRIVATE ${boost_INCLUDE_DIRS})
    elseif(TARGET Boost::headers)
        target_link_libraries(${TARGET_NAME} PRIVATE Boost::headers)
    elseif(TARGET Boost::boost)
        target_link_libraries(${TARGET_NAME} PRIVATE Boost::boost)
    elseif(TARGET boost::boost)
        target_link_libraries(${TARGET_NAME} PRIVATE boost::boost)
    else()
        # Try to find Boost package if not already found
        find_package(Boost CONFIG QUIET)
        if(Boost_FOUND AND DEFINED Boost_INCLUDE_DIRS)
            target_include_directories(${TARGET_NAME} PRIVATE ${Boost_INCLUDE_DIRS})
        else()
            message(WARNING "Boost headers not found for ${TARGET_NAME}. Boost.Asio functionality may not compile.")
        endif()
    endif()
endfunction()

# ============================================================================
# Daemon Client Library (lightweight, no circular dependencies)
# ============================================================================
add_library(yams_daemon_client STATIC
    client/asio_connection.cpp
    client/asio_connection_pool.cpp
    client/daemon_client.cpp
    client/asio_transport.cpp
    client/global_io_context.cpp
    components/DaemonLifecycleFsm.cpp
    ipc/ipc_protocol.cpp
    ipc/message_framing.cpp
)
set_target_properties(yams_daemon_client PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

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

# Add Boost headers for Boost.Asio support
yams_add_boost_headers(yams_daemon_client)

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
# Build as OBJECTs for test ABI parity + a STATIC lib for production linking
# ============================================================================
add_library(yams_daemon_ipc_obj OBJECT
    ipc/request_handler.cpp
    ipc/streaming_processor.cpp
    ipc/connection_fsm.cpp
    ipc/retrieval_session.cpp
    ipc/thread_pool.cpp
    ipc/socket_utils.cpp
    ipc/resource_tuner.cpp
    components/PoolManager.cpp
)

add_library(yams_daemon_ipc STATIC
    $<TARGET_OBJECTS:yams_daemon_ipc_obj>
)
set_target_properties(yams_daemon_ipc PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

if(BUILD_TESTING)
    # Build the IPC OBJECTs with YAMS_TESTING for ABI parity with unit tests,
    # without leaking to dependents.
    target_compile_definitions(yams_daemon_ipc_obj PRIVATE YAMS_TESTING=1)
endif()

# Install daemon IPC static library and export with YamsTargets so dependents are satisfied
install(TARGETS yams_daemon_ipc
    EXPORT YamsTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

target_include_directories(yams_daemon_ipc_obj
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_include_directories(yams_daemon_ipc
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(yams_daemon_ipc_obj
    PUBLIC
        yams::core
        ZLIB::ZLIB          # For CRC32 in framing
    PRIVATE
        spdlog::spdlog
)

target_link_libraries(yams_daemon_ipc
    PUBLIC
        yams::core
        ZLIB::ZLIB
    PRIVATE
        spdlog::spdlog
)



# tl-expected is header-only; link interface target to propagate include dirs
if(TARGET tl-expected::tl-expected)
    target_link_libraries(yams_daemon_ipc_obj PUBLIC tl-expected::tl-expected)
elseif(TARGET tl::expected)
    target_link_libraries(yams_daemon_ipc_obj PUBLIC tl::expected)
endif()

target_compile_features(yams_daemon_ipc_obj PUBLIC cxx_std_20)
target_compile_features(yams_daemon_ipc PUBLIC cxx_std_20)
set_target_properties(yams_daemon_ipc PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(yams_daemon_ipc_obj PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Create alias for daemon ipc
add_library(yams::daemon_ipc ALIAS yams_daemon_ipc)

# Platform-specific async I/O defines for the IPC library (to match implementation)
option(YAMS_USE_KQUEUE "Use kqueue backend for async IO on Apple/BSD" OFF)

# Native Boost.Asio is now the only implementation
# Add Boost headers for Boost.Asio support
yams_add_boost_headers(yams_daemon_ipc_obj)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_package(PkgConfig)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(LIBURING liburing)
        if(LIBURING_FOUND)
            target_compile_definitions(yams_daemon_ipc_obj PRIVATE HAVE_IO_URING=1)
            target_link_libraries(yams_daemon_ipc_obj PRIVATE ${LIBURING_LIBRARIES})
            target_include_directories(yams_daemon_ipc_obj PRIVATE ${LIBURING_INCLUDE_DIRS})
            message(STATUS "io_uring support enabled for yams_daemon_ipc")
        else()
            message(STATUS "yams_daemon_ipc: liburing not found, using select/epoll fallback")
        endif()
    endif()
elseif(APPLE)
    if(YAMS_USE_KQUEUE)
        target_compile_definitions(yams_daemon_ipc_obj PRIVATE HAVE_KQUEUE=1)
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
    set_target_properties(yams_ipc_proto PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

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
            nlohmann_json::nlohmann_json
        PRIVATE
            spdlog::spdlog
    )

    target_compile_features(yams_ipc_proto PUBLIC cxx_std_20)
    set_target_properties(yams_ipc_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)
    # Disable clang-tidy for generated protobuf sources to avoid false positives
    set_target_properties(yams_ipc_proto PROPERTIES CXX_CLANG_TIDY "")

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
    components/DaemonMetrics.cpp
    components/RequestDispatcher.cpp
    components/dispatcher/request_dispatcher_plugins.cpp
    components/dispatcher/request_dispatcher_models.cpp
    components/dispatcher/request_dispatcher_embeddings.cpp
    components/dispatcher/request_dispatcher_documents.cpp
    components/dispatcher/request_dispatcher_status.cpp
    components/dispatcher/request_dispatcher_search.cpp
    components/dispatcher/request_dispatcher_misc.cpp
    components/LifecycleComponent.cpp
    components/ServiceManager.cpp
    components/PostIngestQueue.cpp
    components/PoolManager.cpp
    components/WorkerPool.cpp
    components/RepairCoordinator.cpp
    components/SocketServer.cpp
    components/WalMetricsProvider.cpp
    resource/mock_model_provider.cpp
    resource/abi_model_provider_adapter.cpp
    resource/abi_plugin_loader.cpp
    resource/plugin_host.cpp
    resource/plugin_loader.cpp
    resource/wasm_runtime.cpp
    resource/plugin_host_services_shim.cpp
)

# Avoid LTO bitcode in the static daemon lib to prevent link failures when executables disable IPO
set_target_properties(yams_daemon PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

# Statically include IPC OBJECT files in the daemon library itself so lld can
# resolve RequestHandler/ConnectionFsm/ResourceTuner symbols during grouped
# static library scans on Linux.
target_sources(yams_daemon PRIVATE $<TARGET_OBJECTS:yams_daemon_ipc_obj>)

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
        yams::wal
        yams_downloader
        yams_repair
        yams_app_services
        yams::daemon_ipc
        yams_daemon_client
    PRIVATE
        $<$<AND:$<VERSION_GREATER_EQUAL:${CMAKE_VERSION},3.24>,$<PLATFORM_ID:Linux>>:$<LINK_GROUP:RESCAN,yams_wal>>
        spdlog::spdlog
        pthread
        ${CMAKE_DL_LIBS}  # For dlopen/dlsym
        # Removed: onnxruntime::onnxruntime (moved to plugin)
)

# Ensure WAL symbols are retained when linking with lld and section GC
if(NOT APPLE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_options(yams_daemon PRIVATE -Wl,--no-gc-sections)
    # Additionally, force-resolve yams_wal to avoid missing symbols under some link orders
    target_link_libraries(yams_daemon PRIVATE $<$<LINK_LANGUAGE:CXX>:-Wl,--whole-archive> yams_wal $<$<LINK_LANGUAGE:CXX>:-Wl,--no-whole-archive>)
endif()

# Optional: enable WASM runtime (wasmtime-cpp) integration when available
# Set -DYAMS_HAVE_WASMTIME=ON and provide include/link flags externally or via a toolchain.
option(YAMS_HAVE_WASMTIME "Enable wasmtime-cpp integration for WASM plugins" OFF)
if(YAMS_HAVE_WASMTIME)
    target_compile_definitions(yams_daemon PRIVATE YAMS_HAVE_WASMTIME=1)
    find_package(wasmtime QUIET)
    if(wasmtime_FOUND)
        target_link_libraries(yams_daemon PRIVATE wasmtime::wasmtime)
        message(STATUS "WASM: wasmtime found; linking wasmtime::wasmtime")
    else()
        message(WARNING "WASM: YAMS_HAVE_WASMTIME=ON but wasmtime not found; building stubs")
    endif()
endif()

# Propagate install prefix to code so PluginLoader can discover installed plugins at runtime
if(CMAKE_INSTALL_PREFIX)
    # Define as a string literal macro for plugin_loader.cpp
    target_compile_definitions(yams_daemon PRIVATE YAMS_INSTALL_PREFIX="${CMAKE_INSTALL_PREFIX}")
endif()

# Add Boost headers for Boost.Asio support
yams_add_boost_headers(yams_daemon)

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
# IPC OBJECTS are linked into yams_daemon (static lib) above. No need to add to the executable.
if(APPLE)
    target_link_libraries(yams-daemon PRIVATE
        yams::daemon
        yams::daemon_ipc
        yams::daemon_client
        yams_ipc_proto
        yams_config
        yams::repair
        yams_app_services
        yams_downloader
        yams_search
        yams_vector
        yams_api
        yams_metadata
        yams_indexing
        yams_content
        yams_detection
        yams::storage_engine
        yams::reference_counter
        # Public alias for consistent propagation
        yams::compression
        yams_manifest
        yams_chunking
        yams_crypto
        yams_extraction
        CLI11::CLI11
        spdlog::spdlog
    )
    # Prefer plugin discovery under @loader_path for macOS; allow plugins/ sibling dir
    set_target_properties(yams-daemon PROPERTIES
        BUILD_RPATH "@loader_path;@loader_path/../plugins"
        INSTALL_RPATH "@loader_path;@loader_path/../plugins"
    )
    target_link_options(yams-daemon PRIVATE
        "-Wl,-force_load,$<TARGET_FILE:yams_compression>"
        "-Wl,-force_load,$<TARGET_FILE:yams_wal>"
    )
    # Defensive: strip any lingering GNU ld group/whole-archive flags if injected elsewhere
    get_target_property(_yamsd_libs yams-daemon LINK_LIBRARIES)
    if(_yamsd_libs)
        list(FILTER _yamsd_libs EXCLUDE REGEX "^-Wl,--start-group$|^-Wl,--end-group$|^-Wl,--whole-archive$|^-Wl,--no-whole-archive$")
        set_target_properties(yams-daemon PROPERTIES LINK_LIBRARIES "${_yamsd_libs}")
    endif()
else()
    target_link_libraries(yams-daemon PRIVATE
        "-Wl,--start-group"
            yams::daemon
            # Force-resolve IPC objects on Linux to satisfy references from both
            # yams::daemon and yams::daemon_client under lld/--gc-sections
            "-Wl,--whole-archive" $<TARGET_FILE:yams_daemon_ipc> "-Wl,--no-whole-archive"
            yams::daemon_ipc
            yams::daemon_client
            yams_ipc_proto
            yams_config
            yams::repair
            yams_app_services
            yams_downloader
            yams_search
            yams_vector
            yams_api
            yams_metadata
            yams_indexing
            yams_content
            yams_detection
            yams::storage_engine
            yams::reference_counter
            "-Wl,--whole-archive" $<TARGET_FILE:yams_compression> "-Wl,--no-whole-archive"
            yams::compression
            yams_manifest
            yams_chunking
            yams_crypto
            yams_extraction
        "-Wl,--end-group"
        CLI11::CLI11
        spdlog::spdlog
    )
endif()

# Nudge lld to retain required LZMA entry points on ELF platforms
if(NOT APPLE)
    target_link_options(yams-daemon PRIVATE
        "-Wl,--no-as-needed"
        "-Wl,--no-gc-sections"
        "-Wl,-u,Lzma2Enc_Create"
        "-Wl,-u,Lzma2EncProps_Init"
        "-Wl,-u,Lzma2Enc_SetProps"
        "-Wl,-u,Lzma2Enc_Destroy"
        "-Wl,-u,Lzma2Enc_WriteProperties"
        "-Wl,-u,Lzma2Enc_Encode2"
        "-Wl,-u,LzmaEncProps_Init"
        "-Wl,-u,LzmaEncode"
        "-Wl,-u,LzmaDec_Allocate"
        "-Wl,-u,LzmaDec_Init"
        "-Wl,-u,LzmaDec_DecodeToBuf"
        "-Wl,-u,Lzma2Dec_Allocate"
        "-Wl,-u,Lzma2Dec_Init"
        "-Wl,-u,Lzma2Dec_DecodeToBuf"
    )
endif()




# Work around rare LTO toolchain issues that can drop 'main' in some environments
set_target_properties(yams-daemon PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

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
    # Explicit permissions to guard against environments/tooling that may strip
    # execute bits (observed macOS case where installed file became 0644).
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
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

if(onnxruntime_FOUND AND NOT YAMS_DISABLE_ONNX AND OFF)
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

    # Ensure the plugin isn't built with IPO/LTO and that C symbols remain visible
    set_target_properties(yams_onnx_plugin PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION FALSE
    C_VISIBILITY_PRESET default
    CXX_VISIBILITY_PRESET default
    VISIBILITY_INLINES_HIDDEN OFF
    )

    # Optional: fail link if there are unresolved symbols (Linux/ELF)
    if(UNIX AND NOT APPLE)
        target_link_options(yams_onnx_plugin PRIVATE "-Wl,-z,defs")
    endif()

    # Install ONNX plugin to plugins directory
    install(TARGETS yams_onnx_plugin
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins
    )

    # Create plugins directory during installation
    install(DIRECTORY DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins)

    # Make plugin available for tests
    add_library(yams::onnx_plugin ALIAS yams_onnx_plugin)
else()
    message(STATUS "Skipping legacy in-tree ONNX plugin build (using plugins/onnx)")
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
