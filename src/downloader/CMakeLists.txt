# yams/src/downloader/CMakeLists.txt
#
# Downloader component: provides a library target that aggregates dependencies
# (libcurl, OpenSSL, JSON, logging, threads) and exposes a stable alias.
# Implementation sources will be added in subsequent commits.

option(YAMS_ENABLE_DOWNLOADER "Enable downloader component" ON)

if(NOT YAMS_ENABLE_DOWNLOADER)
    message(STATUS "Downloader component disabled (YAMS_ENABLE_DOWNLOADER=OFF)")
    return()
endif()

# Create an interface library for now (no sources yet). This allows downstream
# targets (CLI/MCP) to link against yams::downloader and inherit required deps.
set(DOWNLOADER_SOURCES
    http_adapter_curl.cpp
    disk_writer.cpp
    integrity_verifier.cpp
    resume_store.cpp
    rate_limiter.cpp
    download_manager.cpp
)
add_library(yams_downloader STATIC ${DOWNLOADER_SOURCES})
set_target_properties(yams_downloader PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

# Public include directories (future headers under include/ will be visible)
target_include_directories(yams_downloader
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# C++20 for all consumers
target_compile_features(yams_downloader PUBLIC cxx_std_20)

# Link required dependencies for the downloader
# - yams::curl (defined at top-level; wraps CURL::libcurl)
# - OpenSSL (for TLS/crypto; CURL may already bring it but we link explicitly)
# - Threads (for concurrency in the downloader)
# - spdlog and nlohmann_json for logging and JSON I/O
target_link_libraries(yams_downloader
    PUBLIC
        yams::curl
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        spdlog::spdlog
        nlohmann_json::nlohmann_json
)

# Provide a stable alias for consumers
add_library(yams::downloader ALIAS yams_downloader)

# Install/export the interface target for downstream use
install(TARGETS yams_downloader
    EXPORT YamsTargets
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
