# Detection module for file type detection using magic numbers

# Check for libmagic
find_path(LIBMAGIC_INCLUDE_DIR magic.h)
find_library(LIBMAGIC_LIBRARY magic)

if(LIBMAGIC_INCLUDE_DIR AND LIBMAGIC_LIBRARY)
    message(STATUS "Found libmagic: ${LIBMAGIC_LIBRARY}")
    set(YAMS_HAS_LIBMAGIC TRUE)
else()
    message(STATUS "libmagic not found, using built-in patterns only")
    set(YAMS_HAS_LIBMAGIC FALSE)
endif()

# Create detection library
add_library(yams_detection STATIC
    file_type_detector.cpp
)

target_include_directories(yams_detection
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

if(YAMS_HAS_LIBMAGIC)
    target_include_directories(yams_detection PRIVATE ${LIBMAGIC_INCLUDE_DIR})
    target_link_libraries(yams_detection PRIVATE ${LIBMAGIC_LIBRARY})
    target_compile_definitions(yams_detection PRIVATE YAMS_HAS_LIBMAGIC)
endif()

target_link_libraries(yams_detection
    PUBLIC
        yams::core
        yams::metadata
    PRIVATE
        nlohmann_json::nlohmann_json
        spdlog::spdlog
)

target_compile_features(yams_detection PUBLIC cxx_std_20)

# Create alias
add_library(yams::detection ALIAS yams_detection)

# Install targets
install(TARGETS yams_detection
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install headers
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/yams/detection
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/yams
    FILES_MATCHING PATTERN "*.h"
)