# Vector database library

# Create the vector library
add_library(yams_vector
    vector_database.cpp
    embedding_generator.cpp
    document_chunker.cpp
    vector_index_manager.cpp
    vector_index_optimizer.cpp
    model_registry.cpp
    model_cache.cpp
    model_loader.cpp
)

# Set properties
set_target_properties(yams_vector PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    POSITION_INDEPENDENT_CODE ON
)

# Include directories
target_include_directories(yams_vector
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link dependencies
target_link_libraries(yams_vector
    PUBLIC
        yams::core
    PRIVATE
        spdlog::spdlog
        # TODO: Add LanceDB and Arrow dependencies when available
        # LanceDB::lancedb
        # Arrow::arrow
        # Arrow::parquet
)

# Compiler flags
target_compile_definitions(yams_vector
    PRIVATE
        # Enable debug logging in debug builds
        $<$<CONFIG:Debug>:YAMS_VECTOR_DEBUG>
)

# TODO: Find and link LanceDB package
# find_package(LanceDB REQUIRED)
# find_package(Arrow REQUIRED)
# 
# target_link_libraries(yams_vector
#     PRIVATE
#         LanceDB::lancedb
#         Arrow::arrow
#         Arrow::parquet
# )

# Export target
install(TARGETS yams_vector
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install headers
install(
    DIRECTORY ${CMAKE_SOURCE_DIR}/include/yams/vector
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/yams
    FILES_MATCHING PATTERN "*.h"
)

# Create alias for consistent naming
add_library(yams::vector ALIAS yams_vector)