# Indexing library
add_library(yams_indexing STATIC
    document_indexer.cpp
    indexing_pipeline.cpp
)

target_include_directories(yams_indexing PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_indexing PUBLIC
    yams_core
    yams_metadata
    yams_extraction
    spdlog::spdlog
    Threads::Threads
)

# Set compile features
target_compile_features(yams_indexing PUBLIC cxx_std_20)

# Create alias for consistent naming
add_library(yams::indexing ALIAS yams_indexing)