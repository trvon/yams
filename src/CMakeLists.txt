# Core components
add_subdirectory(core)
add_subdirectory(crypto)
add_subdirectory(chunking)
add_subdirectory(compression)
add_subdirectory(storage)
add_subdirectory(manifest)
add_subdirectory(wal)
add_subdirectory(integrity)
add_subdirectory(repair)
add_subdirectory(api)
add_subdirectory(metadata)
add_subdirectory(detection)
add_subdirectory(extraction)
add_subdirectory(indexing)
add_subdirectory(search)

# Daemon component (core component for high-performance embedding generation)
add_subdirectory(daemon)

# Reusable Asio client pool (needed by vector, CLI, MCP)
add_library(yams_asio_client STATIC
    cli/asio_client_pool.cpp
)

target_include_directories(yams_asio_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_asio_client
    PUBLIC
        yams_boost_headers
        yams_daemon_client
    PRIVATE
        spdlog::spdlog
)

target_compile_features(yams_asio_client PUBLIC cxx_std_20)
set_target_properties(yams_asio_client PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Create alias for consistent naming
add_library(yams::asio_client ALIAS yams_asio_client)

# Export/install the Asio client so dependents exported to YamsTargets (e.g., yams_vector)
# have all their PUBLIC dependencies present in the export set.
install(TARGETS yams_asio_client
    EXPORT YamsTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Ensure Boost headers are visible even with Conan target quirks
if(TARGET yams_boost_headers)
    get_target_property(_yams_boost_inc yams_boost_headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_yams_boost_inc)
        target_include_directories(yams_asio_client SYSTEM PRIVATE ${_yams_boost_inc})
    endif()
endif()
if(DEFINED Boost_INCLUDE_DIRS)
    target_include_directories(yams_asio_client SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
endif()
target_include_directories(yams_asio_client SYSTEM PRIVATE
    $<$<TARGET_EXISTS:boost::_libboost>:$<TARGET_PROPERTY:boost::_libboost,INTERFACE_INCLUDE_DIRECTORIES>>
)
if(TARGET Boost::headers)
    get_target_property(_boost_headers_inc_asio Boost::headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_headers_inc_asio)
        target_include_directories(yams_asio_client SYSTEM PRIVATE ${_boost_headers_inc_asio})
    endif()
elseif(TARGET Boost::boost)
    get_target_property(_boost_boost_inc_asio Boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_inc_asio)
        target_include_directories(yams_asio_client SYSTEM PRIVATE ${_boost_boost_inc_asio})
    endif()
elseif(TARGET boost::boost)
    get_target_property(_boost_boost_lc_inc_asio boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_lc_inc_asio)
        target_include_directories(yams_asio_client SYSTEM PRIVATE ${_boost_boost_lc_inc_asio})
    endif()
endif()

add_subdirectory(vector)
add_subdirectory(benchmarks)
add_subdirectory(config)
add_subdirectory(content)
add_subdirectory(downloader)

# Application services (for MCP and CLI)
add_subdirectory(app)

# CLI and MCP components (only if tools are being built)
if(YAMS_BUILD_TOOLS)
    # MCP library only if MCP server is enabled
    if(YAMS_BUILD_MCP_SERVER)
        add_subdirectory(mcp)
    endif()
    add_subdirectory(cli)
endif()

# Main storage library that combines all components
add_library(yams_storage INTERFACE)

target_link_libraries(yams_storage
    INTERFACE
        yams::core
        yams::crypto
        yams::chunking
        yams::compression
        yams::storage_engine
        yams::manifest
        yams::wal
        yams::integrity
        yams::api
        yams::metadata
        yams::detection
        yams::extraction
        yams::indexing
        yams::search
        yams::vector
)

target_include_directories(yams_storage
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Export target
install(TARGETS yams_storage
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Create alias for consistent naming
add_library(YAMS::Storage ALIAS yams_storage)
add_library(yams::storage ALIAS yams_storage)
