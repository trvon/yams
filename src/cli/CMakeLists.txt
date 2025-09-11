# YAMS CLI library

# Collect CLI source files
set(CLI_SOURCES
    yams_cli.cpp
    command_registry.cpp
    session_store.cpp
    time_parser.cpp
    progress_indicator.cpp
    commands/add_command.cpp
    commands/get_command.cpp
    commands/restore_command.cpp
    commands/cat_command.cpp
    commands/init_command.cpp
    commands/search_command.cpp
    commands/grep_command.cpp
    commands/config_command.cpp
    commands/auth_command.cpp
    commands/uninstall_command.cpp
    commands/migrate_command.cpp
    commands/delete_command.cpp
    commands/list_command.cpp
    commands/stats_command.cpp
    commands/status_command.cpp
    commands/update_command.cpp
    commands/session_command.cpp
    commands/completion_command.cpp
    commands/repair_mime_command.cpp
    commands/repair_command.cpp
    commands/model_command.cpp
    commands/daemon_command.cpp
    commands/download_command.cpp
    commands/plugin_command.cpp
    commands/doctor_command.cpp
    commands/dr_command.cpp
    commands/graph_command.cpp
)

# Add TUI sources only if enabled
if(YAMS_ENABLE_TUI)
    list(APPEND CLI_SOURCES
        commands/browse_command.cpp
        tui/browse_state.cpp
        tui/browse_services.cpp
        tui/browse_commands.cpp
    )
endif()

# Add serve command only if MCP is enabled
if(YAMS_BUILD_MCP_SERVER)
    list(APPEND CLI_SOURCES commands/serve_command.cpp)
endif()

# Create CLI library
add_library(yams_cli STATIC ${CLI_SOURCES})

# Avoid LTO bitcode in this static lib to prevent link-time unresolveds in final executables
set_target_properties(yams_cli PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

# Link dependencies
target_link_libraries(yams_cli
    PUBLIC
        yams_api
        yams::vector
        yams_search
        yams_metadata
        yams_detection
        yams_config
        yams_daemon
        yams::daemon_client
        yams_boost_headers
    $<$<TARGET_EXISTS:boost::_libboost>:boost::_libboost>
    $<$<TARGET_EXISTS:Boost::headers>:Boost::headers>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<TARGET_EXISTS:Boost::boost>>:Boost::boost>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<NOT:$<TARGET_EXISTS:Boost::boost>>,$<TARGET_EXISTS:boost::boost>>:boost::boost>
        CLI11::CLI11
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        OpenSSL::Crypto
        $<$<BOOL:${YAMS_ENABLE_TUI}>:imtui-ncurses>
        sqlite3
        yams_downloader
        $<$<PLATFORM_ID:Linux>:dl>
)

# Boost.Asio headers are provided via yams_boost_headers

# Avoid linking against aggregate boost::boost to prevent pulling Boost.Test libs transitively

# Link MCP library only if enabled
if(YAMS_BUILD_MCP_SERVER)
    target_link_libraries(yams_cli PUBLIC yams_mcp)
    target_compile_definitions(yams_cli PRIVATE YAMS_BUILD_MCP_SERVER)
endif()

# Add TUI compile definition if enabled
if(YAMS_ENABLE_TUI)
    target_compile_definitions(yams_cli PRIVATE YAMS_ENABLE_TUI)
endif()

# Include directories
target_include_directories(yams_cli
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/generated>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/cli
        ${CMAKE_BINARY_DIR}/_deps/imtui-src/third-party/imgui/imgui
        $<INSTALL_INTERFACE:include>
)

# Ensure Boost headers (Asio) are visible to this target even if generator-provided
# targets don't propagate include dirs as expected in some Conan profiles.
if(TARGET yams_boost_headers)
    get_target_property(_yams_boost_inc yams_boost_headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_yams_boost_inc)
        target_include_directories(yams_cli SYSTEM PRIVATE ${_yams_boost_inc})
    endif()
endif()

# Fallback safety net if Boost provides a variable
if(DEFINED Boost_INCLUDE_DIRS)
    target_include_directories(yams_cli SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
endif()

# When using Conan's Boost, the aggregate internal target often carries the include dirs.
# Pull them in explicitly to avoid missing Asio headers in some profiles.
target_include_directories(yams_cli SYSTEM PRIVATE
    $<$<TARGET_EXISTS:boost::_libboost>:$<TARGET_PROPERTY:boost::_libboost,INTERFACE_INCLUDE_DIRECTORIES>>
)

# As an extra hardening step, pull include dirs off Boost targets directly when available
if(TARGET Boost::headers)
    get_target_property(_boost_headers_inc Boost::headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_headers_inc)
        target_include_directories(yams_cli SYSTEM PRIVATE ${_boost_headers_inc})
    endif()
elseif(TARGET Boost::boost)
    get_target_property(_boost_boost_inc Boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_inc)
        target_include_directories(yams_cli SYSTEM PRIVATE ${_boost_boost_inc})
    endif()
elseif(TARGET boost::boost)
    get_target_property(_boost_boost_lc_inc boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_lc_inc)
        target_include_directories(yams_cli SYSTEM PRIVATE ${_boost_boost_lc_inc})
    endif()
endif()

# Ensure generated help/man assets are built before CLI when docs target exists
if(TARGET docs)
    add_dependencies(yams_cli docs)
endif()

# Set C++ standard
target_compile_features(yams_cli PUBLIC cxx_std_20)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET yams_profiling)
    target_link_libraries(yams_cli PRIVATE $<BUILD_INTERFACE:yams_profiling>)
endif()

# Link the reusable Asio client into the CLI
