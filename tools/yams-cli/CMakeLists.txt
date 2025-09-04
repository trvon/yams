# YAMS CLI executable

add_executable(yams
    main.cpp
)

target_link_libraries(yams
    PRIVATE
        "-Wl,--start-group"
            "-Wl,--whole-archive" yams_cli "-Wl,--no-whole-archive"
            # Retain our compression archive; avoid linking lzma_sdk twice
            $<$<PLATFORM_ID:Darwin>:-Wl,-force_load,$<TARGET_FILE:yams_compression>>
            $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
            $<$<NOT:$<PLATFORM_ID:Darwin>>:$<TARGET_FILE:yams_compression>>
            $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
            yams::compression
        "-Wl,--end-group"
)

# Nudge lld to retain required LZMA entry points on ELF platforms
if(NOT APPLE)
    target_link_options(yams PRIVATE
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


# Link MCP library only if enabled
if(YAMS_BUILD_MCP_SERVER)
    target_link_libraries(yams PRIVATE yams_mcp)
endif()

target_include_directories(yams
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(yams PRIVATE cxx_std_20)
set_target_properties(yams PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)


# Avoid rare LTO/GC interactions that can drop 'main' during release linking
set_target_properties(yams PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)


install(TARGETS yams
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT runtime
)
