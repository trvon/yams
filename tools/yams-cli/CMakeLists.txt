# YAMS CLI executable

add_executable(yams
    main.cpp
)

if(APPLE)
    # macOS: link normally, but force-load archives that must be kept
    target_link_libraries(yams PRIVATE
        yams_cli
        yams::compression
    )
    target_link_options(yams PRIVATE
        "-Wl,-force_load,$<TARGET_FILE:yams_cli>"
        "-Wl,-force_load,$<TARGET_FILE:yams_compression>"
    )
    # Defensive: strip any lingering GNU ld group/whole-archive flags if injected elsewhere
    get_target_property(_yams_libs yams LINK_LIBRARIES)
    if(_yams_libs)
        list(FILTER _yams_libs EXCLUDE REGEX "^-Wl,--start-group$|^-Wl,--end-group$|^-Wl,--whole-archive$|^-Wl,--no-whole-archive$")
        set_target_properties(yams PROPERTIES LINK_LIBRARIES "${_yams_libs}")
    endif()
else()
    # Linux and others: use linker groups and whole-archive
    target_link_libraries(yams PRIVATE
        "-Wl,--start-group"
            "-Wl,--whole-archive" yams_cli "-Wl,--no-whole-archive"
            "-Wl,--whole-archive" $<TARGET_FILE:yams_compression> "-Wl,--no-whole-archive"
            yams::compression
        "-Wl,--end-group"
    )
endif()

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
