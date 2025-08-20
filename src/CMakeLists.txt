# Core components
add_subdirectory(core)
add_subdirectory(crypto)
add_subdirectory(chunking)
add_subdirectory(compression)
add_subdirectory(storage)
add_subdirectory(manifest)
add_subdirectory(wal)
add_subdirectory(integrity)
add_subdirectory(api)
add_subdirectory(metadata)
add_subdirectory(detection)
add_subdirectory(extraction)
add_subdirectory(indexing)
add_subdirectory(search)

# Daemon component (core component for high-performance embedding generation)
add_subdirectory(daemon)

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
