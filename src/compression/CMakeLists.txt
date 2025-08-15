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

if(YAMS_USE_CONAN)
    target_link_libraries(yams_compression
        PUBLIC
            yams::core
            zstd::libzstd_static
            lzma_sdk  # Always use our custom LZMA SDK build
            OpenSSL::Crypto  # For CRC32
        PRIVATE
            spdlog::spdlog
    )
else()
    target_link_libraries(yams_compression
        PUBLIC
            yams::core
            libzstd_static
            lzma_sdk
            OpenSSL::Crypto  # For CRC32
        PRIVATE
            spdlog::spdlog
    )
endif()

target_compile_features(yams_compression PUBLIC cxx_std_20)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET Tracy::TracyClient)
    target_link_libraries(yams_compression PRIVATE Tracy::TracyClient)
endif()

# Create alias
add_library(yams::compression ALIAS yams_compression)

# Installation
install(TARGETS yams_compression
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)