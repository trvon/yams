# Extraction library
set(EXTRACTION_SOURCES
    text_extractor_factory.cpp
    text_extractor_utils.cpp
    plain_text_extractor.cpp
    html_text_extractor.cpp
    format_handlers/format_handler.cpp
    ../extraction/extraction_util.cpp
)

add_library(yams_extraction STATIC
    ${EXTRACTION_SOURCES}
)
set_target_properties(yams_extraction PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

target_include_directories(yams_extraction
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_extraction
    PUBLIC
        yams::core
        spdlog::spdlog
)

# Enable position-independent code for linking into shared libraries
set_target_properties(yams_extraction PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Create alias
add_library(yams::extraction ALIAS yams_extraction)

# Installation
install(TARGETS yams_extraction
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/yams/extraction
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/yams
)
