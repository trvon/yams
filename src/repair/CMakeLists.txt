# Repair utilities library
add_library(yams_repair STATIC
    embedding_repair_util.cpp
)

target_include_directories(yams_repair PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_repair PUBLIC
    yams_core
    yams_api
    yams_metadata
    yams_vector
    spdlog::spdlog
)

# Set compile features
target_compile_features(yams_repair PUBLIC cxx_std_20)

# Export and install
install(TARGETS yams_repair
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Create alias for consistent naming
add_library(yams::repair ALIAS yams_repair)