# Content API library
add_library(yams_api
    content_store_impl.cpp
    async_content_store.cpp
    content_metadata.cpp
    content_store_builder.cpp
    progress_reporter.cpp
    metadata_api.cpp
    semantic_search_api.cpp
    raw_text_storage.cpp
)
set_target_properties(yams_api PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

target_include_directories(yams_api
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_api
    PUBLIC
        yams::core
        yams::storage_engine
        yams::reference_counter
        yams::chunking
        yams::crypto
        yams::manifest
        yams::metadata
        yams::indexing
        yams::search
        yams::vector
    PRIVATE
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        Threads::Threads
)

# Set properties
set_target_properties(yams_api PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    POSITION_INDEPENDENT_CODE ON
)

# Export target
install(TARGETS yams_api
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Create alias
add_library(yams::api ALIAS yams_api)

# HTTP API removed - use MCP server instead
