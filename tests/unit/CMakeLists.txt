# Unit test subdirectories
add_subdirectory(crypto)
add_subdirectory(chunking)
add_subdirectory(compression)
add_subdirectory(storage)
add_subdirectory(wal)
add_subdirectory(manifest)
add_subdirectory(api)
add_subdirectory(integrity)
add_subdirectory(metadata)
add_subdirectory(extraction)
add_subdirectory(indexing)
add_subdirectory(search)
add_subdirectory(vector)

# Add MCP tests if tools are being built
if(YAMS_BUILD_TOOLS)
    add_subdirectory(mcp)
endif()

# Combined unit test executable (optional)
add_executable(all_unit_tests
    main.cpp
)

target_link_libraries(all_unit_tests
    PRIVATE
        test_utils
        yams::crypto
        yams::chunking
        yams::storage_engine
)

# Discover tests
gtest_discover_tests(all_unit_tests
    PROPERTIES
        LABELS "unit"
        TIMEOUT 30
)