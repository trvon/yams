# YAMS MCP Server executable

add_executable(yams-mcp-server
    main.cpp
)

# Linux/lld needs IPC OBJECTs for symbol resolution; avoid on macOS to prevent
# pulling server-only objects (RequestHandler) that depend on RequestDispatcher.
if(UNIX AND NOT APPLE)
    target_sources(yams-mcp-server PRIVATE $<TARGET_OBJECTS:yams_daemon_ipc_obj>)
endif()

if(APPLE)
    target_link_libraries(yams-mcp-server PRIVATE
        yams_mcp
        yams::daemon_ipc
        yams::daemon_client
        yams_ipc_proto
        yams::compression
        CLI11::CLI11
        spdlog::spdlog
        Threads::Threads
    )
    target_link_options(yams-mcp-server PRIVATE
        "-Wl,-force_load,$<TARGET_FILE:yams_mcp>"
        "-Wl,-force_load,$<TARGET_FILE:yams_compression>"
    )
    # Defensive: strip any lingering GNU ld group/whole-archive flags if injected elsewhere
    get_target_property(_yams_mcp_libs yams-mcp-server LINK_LIBRARIES)
    if(_yams_mcp_libs)
        list(FILTER _yams_mcp_libs EXCLUDE REGEX "^-Wl,--start-group$|^-Wl,--end-group$|^-Wl,--whole-archive$|^-Wl,--no-whole-archive$")
        set_target_properties(yams-mcp-server PROPERTIES LINK_LIBRARIES "${_yams_mcp_libs}")
    endif()
else()
    target_link_libraries(yams-mcp-server PRIVATE
        "-Wl,--start-group"
            "-Wl,--whole-archive" yams_mcp "-Wl,--no-whole-archive"
            yams::daemon_ipc
            yams::daemon_client
            yams_ipc_proto
            "-Wl,--whole-archive" $<TARGET_FILE:yams_compression> "-Wl,--no-whole-archive"
            yams::compression
        "-Wl,--end-group"
        CLI11::CLI11
        spdlog::spdlog
        Threads::Threads
    )
endif()

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
