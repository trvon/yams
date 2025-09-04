# YAMS CLI executable

add_executable(yams
    main.cpp
)

target_link_libraries(yams
    PRIVATE
        # Group static archives to satisfy lld’s archive resolution
        "-Wl,--start-group"
            # Force inclusion of symbols from static CLI archive to avoid order-related stripping
            "-Wl,--whole-archive" yams_cli "-Wl,--no-whole-archive"
            # Ensure LZMA SDK is pulled for compression references reachable via transitive deps
            "-Wl,--whole-archive" lzma_sdk "-Wl,--no-whole-archive"
        "-Wl,--end-group"
)

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
target_link_options(yams PRIVATE -Wl,--no-gc-sections)
target_link_options(yams PRIVATE -fuse-ld=bfd)

# Avoid rare LTO/GC interactions that can drop 'main' during release linking
set_target_properties(yams PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)
target_link_options(yams PRIVATE -Wl,--no-gc-sections)

install(TARGETS yams
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT runtime
)
