# Compression module
add_library(yams_compression STATIC
    zstandard_compressor.cpp
    lzma_compressor.cpp
    compression_registry.cpp
    compression_utils.cpp
    compression_header.cpp
    compression_policy.cpp
    compression_scheduler.cpp
    compression_monitor.cpp
    error_handler.cpp
    integrity_validator.cpp
    recovery_manager.cpp
    transaction_manager.cpp
)

target_include_directories(yams_compression
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_compression
    PUBLIC
        yams::core
        libzstd_static
        lzma_sdk
        OpenSSL::Crypto  # For CRC32
    PRIVATE
        spdlog::spdlog
)

target_compile_features(yams_compression PUBLIC cxx_std_20)

# Create alias
add_library(yams::compression ALIAS yams_compression)

# Installation
install(TARGETS yams_compression
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)