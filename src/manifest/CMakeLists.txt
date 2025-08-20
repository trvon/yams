# Generate protobuf files
set(PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/../../include/yams/manifest/manifest.proto)
set(PROTO_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

# Force disable Protocol Buffers - use fallback serialization only
# Temporarily disabled due to Abseil dependency issues
add_library(yams_manifest
    manifest_manager.cpp
)

target_link_libraries(yams_manifest
    PUBLIC
        yams::core
        yams::crypto
        yams::chunking
    PRIVATE
        spdlog::spdlog
)

# Protocol Buffers is temporarily disabled - using fallback serialization only

target_include_directories(yams_manifest
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(yams_manifest PUBLIC cxx_std_20)

# Export target
install(TARGETS yams_manifest
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_library(yams::manifest ALIAS yams_manifest)