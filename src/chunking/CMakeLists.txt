add_library(yams_chunking
    rabin_chunker.cpp
    streaming_chunker.cpp
)

target_link_libraries(yams_chunking
    PUBLIC
        yams::core
        yams::crypto
    PRIVATE
        spdlog::spdlog
)

target_include_directories(yams_chunking
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Export target
install(TARGETS yams_chunking
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_library(yams::chunking ALIAS yams_chunking)