# Extraction library
set(EXTRACTION_SOURCES
    text_extractor_factory.cpp
    plain_text_extractor.cpp
    html_text_extractor.cpp
    format_handlers/format_handler.cpp
)

# Add PDF extractor if enabled
if(YAMS_ENABLE_PDF AND TARGET pdfium::pdfium)
    list(APPEND EXTRACTION_SOURCES pdf_extractor.cpp)
    set(YAMS_HAS_PDF_SUPPORT ON)
endif()

add_library(yams_extraction STATIC
    ${EXTRACTION_SOURCES}
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

# Link PDFium if available
if(YAMS_HAS_PDF_SUPPORT)
    target_link_libraries(yams_extraction
        PUBLIC
            pdfium::pdfium
    )

    target_compile_definitions(yams_extraction
        PUBLIC
            YAMS_HAS_PDF_SUPPORT=1
    )

    message(STATUS "PDF text extraction support enabled in yams_extraction")
endif()

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
