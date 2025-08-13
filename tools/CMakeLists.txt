# Core user tools
if(YAMS_BUILD_CLI)
    add_subdirectory(yams-cli)
endif()

if(YAMS_BUILD_MCP_SERVER)
    add_subdirectory(yams-mcp)
endif()

# Maintenance tools
if(YAMS_BUILD_MAINTENANCE_TOOLS)
    # Simple stats tool
    add_executable(yams-stats yams-tools/src/simple_stats.cpp)
    target_link_libraries(yams-stats
        PRIVATE
            yams::storage_engine
            yams::reference_counter
            yams::crypto
            yams::core
            spdlog::spdlog
    )
    target_compile_features(yams-stats PRIVATE cxx_std_20)

    # Simple GC tool
    add_executable(yams-gc yams-tools/src/simple_gc.cpp)
    target_link_libraries(yams-gc
        PRIVATE
            yams::storage_engine
            yams::reference_counter
            yams::crypto
            yams::core
            spdlog::spdlog
    )
    target_compile_features(yams-gc PRIVATE cxx_std_20)

    # Retrieval evaluation tool
    add_executable(yams-eval eval/retrieval_eval.cpp)
    # No special libraries required; pure C++20
    target_compile_features(yams-eval PRIVATE cxx_std_20)

    # Installation for maintenance tools
    install(TARGETS yams-stats yams-gc yams-eval
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT maintenance
    )
endif()
