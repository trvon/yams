# YAMS MCP Server executable

add_executable(yams-mcp-server
    main.cpp
)

target_link_libraries(yams-mcp-server
    PRIVATE
        yams_mcp
        yams_app_services
        yams_api
        yams_storage
        yams_search
        yams_metadata
        CLI11::CLI11
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        Threads::Threads
)

target_include_directories(yams-mcp-server
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(yams-mcp-server PRIVATE cxx_std_20)

# Installation
install(TARGETS yams-mcp-server
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT runtime
)