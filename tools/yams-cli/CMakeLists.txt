# YAMS CLI executable

add_executable(yams
    main.cpp
)

target_link_libraries(yams
    PRIVATE
        yams_cli
        yams_api
        yams_search
        yams_metadata
        yams_storage
        CLI11::CLI11
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        Threads::Threads
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

install(TARGETS yams
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT runtime
)