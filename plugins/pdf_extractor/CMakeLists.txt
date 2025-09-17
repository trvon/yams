# CMakeLists.txt for the PDF Extractor Plugin

cmake_minimum_required(VERSION 3.16)

project(YamsPdfExtractorPlugin)

# Find the main YAMS project to get its targets and definitions
# This assumes the plugin is being built as part of the main YAMS build tree.


# Plugin sources
set(PLUGIN_SOURCES
    pdf_extractor_plugin.cpp
    pdf_extractor.cpp
    pdf_content_handler.cpp
)

add_library(yams_pdf_extractor SHARED ${PLUGIN_SOURCES})

# Link against YAMS libraries and PDFium
target_link_libraries(yams_pdf_extractor
    PRIVATE
        yams_daemon
        yams_app_services
        pdfium::pdfium
)

# Include directories
target_include_directories(yams_pdf_extractor
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# Set standard C++ properties
set_target_properties(yams_pdf_extractor PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

# macOS: ensure the plugin can locate libpdfium.dylib installed into ${CMAKE_INSTALL_LIBDIR}
# Plugin installs to ${CMAKE_INSTALL_LIBDIR}/yams/plugins, so two levels up is the lib dir.
if(APPLE)
    set_target_properties(yams_pdf_extractor PROPERTIES
        MACOSX_RPATH ON
        BUILD_WITH_INSTALL_RPATH ON
        INSTALL_RPATH "@loader_path/../.."
    )
    # Also keep link-time rpath for build-tree testing where applicable
    set_property(TARGET yams_pdf_extractor APPEND PROPERTY BUILD_RPATH "@loader_path/../..")
endif()

# Install the plugin
install(TARGETS yams_pdf_extractor
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins
)

# Optionally install the plugin manifest alongside sources for reference (not required at runtime)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/plugin_manifest.json
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins)
