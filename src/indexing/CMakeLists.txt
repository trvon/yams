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
    yams_content
    yams_detection
    spdlog::spdlog
    Threads::Threads
)

# Set compile features
target_compile_features(yams_indexing PUBLIC cxx_std_20)

# Enable position-independent code for linking into shared libraries
set_target_properties(yams_indexing PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET Tracy::TracyClient)
    target_link_libraries(yams_indexing PRIVATE Tracy::TracyClient)
endif()

# Create alias for consistent naming
add_library(yams::indexing ALIAS yams_indexing)