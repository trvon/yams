# Extraction library
add_library(yams_extraction STATIC
    text_extractor_factory.cpp
    plain_text_extractor.cpp
)

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
