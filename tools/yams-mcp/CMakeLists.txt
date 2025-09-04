# YAMS MCP Server executable

add_executable(yams-mcp-server
    main.cpp
)

target_link_libraries(yams-mcp-server
    PRIVATE
        yams_mcp
        CLI11::CLI11
        spdlog::spdlog
        Threads::Threads
)

target_include_directories(yams-mcp-server
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(yams-mcp-server PRIVATE cxx_std_20)

# Avoid rare LTO + GC issues where 'main' can be dropped by the toolchain
set_target_properties(yams-mcp-server PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)
target_link_options(yams-mcp-server PRIVATE -Wl,--no-gc-sections)

# Installation
install(TARGETS yams-mcp-server
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT runtime
)
