# Metadata module
add_library(yams_metadata STATIC
    database.cpp
    connection_pool.cpp
    migration.cpp
    metadata_repository.cpp
    knowledge_graph_store_sqlite.cpp
)

target_include_directories(yams_metadata PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_metadata
    PUBLIC
        yams::core
        sqlite3
    PRIVATE
        yams::search
        spdlog::spdlog
)

# Enable position-independent code for linking into shared libraries
set_target_properties(yams_metadata PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Install library
install(TARGETS yams_metadata
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install headers
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../../include/yams/metadata
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/yams
)

# Create alias
add_library(yams::metadata ALIAS yams_metadata)
