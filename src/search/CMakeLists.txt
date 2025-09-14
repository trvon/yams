# Search library
add_library(yams_search
    query_tokenizer.cpp
    query_parser.cpp
    search_results.cpp
    search_filters.cpp
    result_ranker.cpp
    search_executor.cpp
    cache_key.cpp
    search_cache.cpp
    hybrid_search_engine.cpp
    entity_linker.cpp
    kg_scorer_simple.cpp
    hybrid_search_factory.cpp
    search_engine_builder.cpp
    bk_tree.cpp
    hotzone_manager.cpp
)
set_target_properties(yams_search PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

target_include_directories(yams_search
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_search
    PUBLIC
        yams_core
        yams_metadata
        yams_vector
        spdlog::spdlog
)

# Set library properties
set_target_properties(yams_search PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)

# Create alias for consistent naming
add_library(yams::search ALIAS yams_search)

# Benchmarks have been moved to tests/benchmarks for centralization


# Install targets
install(TARGETS yams_search
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
