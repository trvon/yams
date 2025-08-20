# Optional media processing dependencies
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(TAGLIB taglib)
    pkg_check_modules(LIBARCHIVE libarchive)
endif()

# Check for command-line media tools
find_program(FFPROBE_EXECUTABLE ffprobe)
find_program(MEDIAINFO_EXECUTABLE mediainfo)

# Content handler library
set(CONTENT_SOURCES
    content_handler_registry.cpp
    text_content_handler.cpp
    pdf_content_handler.cpp
    binary_content_handler.cpp
    image_content_handler.cpp
    audio_content_handler.cpp
)

# Add video handler if video processing is available
if(FFPROBE_EXECUTABLE OR MEDIAINFO_EXECUTABLE)
    list(APPEND CONTENT_SOURCES video_content_handler.cpp)
endif()

# Add archive handler
list(APPEND CONTENT_SOURCES archive_content_handler.cpp)

add_library(yams_content STATIC ${CONTENT_SOURCES})

target_include_directories(yams_content
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Configure optional dependencies
if(TAGLIB_FOUND)
    target_compile_definitions(yams_content PRIVATE YAMS_HAVE_TAGLIB)
    target_link_libraries(yams_content PRIVATE ${TAGLIB_LIBRARIES})
    target_include_directories(yams_content PRIVATE ${TAGLIB_INCLUDE_DIRS})
endif()

if(LIBARCHIVE_FOUND)
    target_compile_definitions(yams_content PRIVATE YAMS_HAVE_LIBARCHIVE)
    target_link_libraries(yams_content PRIVATE ${LIBARCHIVE_LIBRARIES})
    target_include_directories(yams_content PRIVATE ${LIBARCHIVE_INCLUDE_DIRS})
endif()

if(FFPROBE_EXECUTABLE)
    target_compile_definitions(yams_content PRIVATE YAMS_HAVE_FFPROBE)
endif()

if(MEDIAINFO_EXECUTABLE)
    target_compile_definitions(yams_content PRIVATE YAMS_HAVE_MEDIAINFO)
endif()

target_link_libraries(yams_content
    PUBLIC
        yams::core
        yams::detection
        yams::extraction
        yams::crypto
        spdlog::spdlog
        Threads::Threads
)

# Set compile features
target_compile_features(yams_content PUBLIC cxx_std_20)

# Create alias
add_library(yams::content ALIAS yams_content)

# Installation
install(TARGETS yams_content
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install headers
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/yams/content
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/yams
    FILES_MATCHING PATTERN "*.h"
)