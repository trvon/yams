# YAMS CLI library

# Collect CLI source files
set(CLI_SOURCES
    yams_cli.cpp
    command_registry.cpp
    time_parser.cpp
    commands/add_command.cpp
    commands/get_command.cpp
    commands/restore_command.cpp
    commands/cat_command.cpp
    commands/init_command.cpp
    commands/search_command.cpp
    commands/config_command.cpp
    commands/auth_command.cpp
    commands/uninstall_command.cpp
    commands/migrate_command.cpp
    commands/delete_command.cpp
    commands/list_command.cpp
    commands/stats_command.cpp
    commands/update_command.cpp
    commands/browse_command.cpp
    commands/completion_command.cpp
    tui/browse_state.cpp
    tui/browse_services.cpp
    tui/browse_commands.cpp
)

# Add serve command only if MCP is enabled
if(YAMS_BUILD_MCP_SERVER)
    list(APPEND CLI_SOURCES commands/serve_command.cpp)
endif()

# Create CLI library
add_library(yams_cli STATIC ${CLI_SOURCES})

# Link dependencies
target_link_libraries(yams_cli
    PUBLIC
        yams_api
        yams_search
        yams_metadata
        yams_detection
        CLI11::CLI11
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        OpenSSL::Crypto
        imtui-ncurses
)

# Link MCP library only if enabled
if(YAMS_BUILD_MCP_SERVER)
    target_link_libraries(yams_cli PUBLIC yams_mcp)
    target_compile_definitions(yams_cli PRIVATE YAMS_BUILD_MCP_SERVER)
endif()

# Include directories
target_include_directories(yams_cli
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/generated>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    PRIVATE
        ${CMAKE_BINARY_DIR}/_deps/imtui-src/third-party/imgui/imgui
        $<INSTALL_INTERFACE:include>
)

# Ensure generated help/man assets are built before CLI when docs target exists
if(TARGET docs)
    add_dependencies(yams_cli docs)
endif()

# Set C++ standard
target_compile_features(yams_cli PUBLIC cxx_std_20)
