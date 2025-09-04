# YAMS MCP Server executable

add_executable(yams-mcp-server
    main.cpp
)

target_link_libraries(yams-mcp-server
    PRIVATE
        "-Wl,--start-group"
            # Retain the MCP static lib fully to avoid GC of reachable symbols
            "-Wl,--whole-archive" yams_mcp "-Wl,--no-whole-archive"
            # Retain only our compression archive (avoid linking lzma_sdk twice)
            $<$<PLATFORM_ID:Darwin>:-Wl,-force_load,$<TARGET_FILE:yams_compression>>
            $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
            $<$<NOT:$<PLATFORM_ID:Darwin>>:$<TARGET_FILE:yams_compression>>
            $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
            yams::compression
        "-Wl,--end-group"
        CLI11::CLI11
        spdlog::spdlog
        Threads::Threads
)

# Nudge lld to retain required LZMA entry points on ELF platforms
if(NOT APPLE)
    target_link_options(yams-mcp-server PRIVATE
        "-Wl,--no-as-needed"
        "-Wl,--no-gc-sections"
        "-Wl,-u,Lzma2Enc_Create"
        "-Wl,-u,Lzma2EncProps_Init"
        "-Wl,-u,Lzma2Enc_SetProps"
        "-Wl,-u,Lzma2Enc_Destroy"
        "-Wl,-u,Lzma2Enc_WriteProperties"
        "-Wl,-u,Lzma2Enc_Encode2"
        "-Wl,-u,LzmaEncProps_Init"
        "-Wl,-u,LzmaEncode"
        "-Wl,-u,LzmaDec_Allocate"
        "-Wl,-u,LzmaDec_Init"
        "-Wl,-u,LzmaDec_DecodeToBuf"
        "-Wl,-u,Lzma2Dec_Allocate"
        "-Wl,-u,Lzma2Dec_Init"
        "-Wl,-u,Lzma2Dec_DecodeToBuf"
    )
endif()

target_include_directories(yams-mcp-server
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(yams-mcp-server PRIVATE cxx_std_20)

# Avoid rare LTO + GC issues where 'main' can be dropped by the toolchain
set_target_properties(yams-mcp-server PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)


# Installation
install(TARGETS yams-mcp-server
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT runtime
)
