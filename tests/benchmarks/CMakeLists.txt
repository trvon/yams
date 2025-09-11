# Performance benchmarks
find_package(benchmark QUIET)
if(benchmark_FOUND OR TARGET benchmark::benchmark)
    # API Benchmarks
    add_executable(api_benchmarks api_benchmarks.cpp)
    target_link_libraries(api_benchmarks
        PRIVATE
            test_utils
            yams::api
            yams::core
            nlohmann_json::nlohmann_json
    )
    target_compile_features(api_benchmarks PRIVATE cxx_std_20)

    # Core Benchmarks
    add_executable(core_benchmarks core_benchmarks.cpp)
    target_link_libraries(core_benchmarks
        PRIVATE
            test_utils
            yams::core
            yams::crypto
            yams::chunking
            yams::storage_engine
            yams::compression
            yams::manifest
            nlohmann_json::nlohmann_json
    )
    target_compile_features(core_benchmarks PRIVATE cxx_std_20)

    # Search Benchmarks
    add_executable(search_benchmarks search_benchmarks.cpp)
    target_link_libraries(search_benchmarks
        PRIVATE
            test_utils
            yams::search
            yams::api
            yams::metadata
            nlohmann_json::nlohmann_json
    )
    target_compile_features(search_benchmarks PRIVATE cxx_std_20)

    # Custom target to run all benchmarks
    add_custom_target(run_all_benchmarks
        COMMAND api_benchmarks
        COMMAND core_benchmarks
        COMMAND search_benchmarks
        DEPENDS api_benchmarks core_benchmarks search_benchmarks
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running all performance benchmarks"
    )
else()
    message(STATUS "Google Benchmark not found, skipping performance benchmarks")
endif()
