# Compression module
option(YAMS_ENABLE_LZMA "Enable LZMA compression via 7-Zip SDK" ON)
option(YAMS_EMBED_LZMA_IN_COMPRESSION "Embed LZMA SDK objects directly into yams_compression (avoids archive ordering issues)" ON)

set(YAMS_COMPRESSION_SRCS
    zstandard_compressor.cpp
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

if(YAMS_ENABLE_LZMA)
    list(APPEND YAMS_COMPRESSION_SRCS lzma_compressor.cpp)
    # Embed LZMA SDK C sources directly into yams_compression to ensure
    # symbol resolution under lld + gc-sections without relying on archive order.
    if(DEFINED lzma_SOURCE_DIR AND YAMS_EMBED_LZMA_IN_COMPRESSION)
        list(APPEND YAMS_COMPRESSION_SRCS
            ${lzma_SOURCE_DIR}/C/LzmaEnc.c
            ${lzma_SOURCE_DIR}/C/LzmaDec.c
            ${lzma_SOURCE_DIR}/C/Lzma2Enc.c
            ${lzma_SOURCE_DIR}/C/Lzma2Dec.c
            ${lzma_SOURCE_DIR}/C/Lzma86Enc.c
            ${lzma_SOURCE_DIR}/C/Lzma86Dec.c
            ${lzma_SOURCE_DIR}/C/Bra.c
            ${lzma_SOURCE_DIR}/C/Bra86.c
            ${lzma_SOURCE_DIR}/C/LzFind.c
            ${lzma_SOURCE_DIR}/C/LzFindMt.c
            ${lzma_SOURCE_DIR}/C/LzFindOpt.c
            ${lzma_SOURCE_DIR}/C/CpuArch.c
            ${lzma_SOURCE_DIR}/C/Alloc.c
            ${lzma_SOURCE_DIR}/C/Threads.c
            ${lzma_SOURCE_DIR}/C/MtCoder.c
            ${lzma_SOURCE_DIR}/C/MtDec.c
            ${lzma_SOURCE_DIR}/C/Lzma2DecMt.c
            ${lzma_SOURCE_DIR}/C/7zStream.c
        )
    endif()
endif()

add_library(yams_compression STATIC ${YAMS_COMPRESSION_SRCS})
set_target_properties(yams_compression PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

target_include_directories(yams_compression
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        $<$<AND:$<BOOL:${YAMS_ENABLE_LZMA}>,$<BOOL:${YAMS_EMBED_LZMA_IN_COMPRESSION}>>:${lzma_SOURCE_DIR}/C>
)


if(YAMS_USE_CONAN)
    target_link_libraries(yams_compression
        PUBLIC
            yams::core
            zstd::libzstd_static
            $<$<AND:$<BOOL:${YAMS_ENABLE_LZMA}>,$<NOT:$<BOOL:${YAMS_EMBED_LZMA_IN_COMPRESSION}>>>:lzma_sdk>
            OpenSSL::Crypto  # For CRC32
        PRIVATE
            spdlog::spdlog
    )
else()
    target_link_libraries(yams_compression
        PUBLIC
            yams::core
            libzstd_static
            $<$<AND:$<BOOL:${YAMS_ENABLE_LZMA}>,$<NOT:$<BOOL:${YAMS_EMBED_LZMA_IN_COMPRESSION}>>>:lzma_sdk>
            OpenSSL::Crypto  # For CRC32
        PRIVATE
            spdlog::spdlog
    )
endif()

# Silence warnings for embedded third-party C sources
if(YAMS_ENABLE_LZMA AND YAMS_EMBED_LZMA_IN_COMPRESSION)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(yams_compression PRIVATE $<$<COMPILE_LANGUAGE:C>:-w>)
    elseif(MSVC)
        target_compile_options(yams_compression PRIVATE $<$<COMPILE_LANGUAGE:C>:/w>)
    endif()
endif()
if(UNIX)
    target_link_libraries(yams_compression PUBLIC pthread)
endif()

target_compile_features(yams_compression PUBLIC cxx_std_20)

# Enable position-independent code for linking into shared libraries
set_target_properties(yams_compression PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET yams_profiling)
    target_link_libraries(yams_compression PRIVATE $<BUILD_INTERFACE:yams_profiling>)
endif()

# Create alias
add_library(yams::compression ALIAS yams_compression)

# Installation
install(TARGETS yams_compression
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
