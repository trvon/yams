# Performance benchmarks
find_package(benchmark QUIET)

if(benchmark_FOUND)
    add_executable(performance_benchmarks
        performance_benchmarks.cpp
    )
    
    add_executable(compression_benchmark
        compression_benchmark.cpp
    )
    
    target_link_libraries(performance_benchmarks
        PRIVATE
            yams::crypto
            yams::chunking
            yams::storage_engine
            yams::reference_counter
            yams::manifest
            benchmark::benchmark
            benchmark::benchmark_main
    )
    
    target_link_libraries(compression_benchmark
        PRIVATE
            yams::compression
            yams::core
            benchmark::benchmark
    )
    
    target_compile_features(performance_benchmarks PRIVATE cxx_std_20)
    target_compile_features(compression_benchmark PRIVATE cxx_std_20)
    
    # Add custom target to run benchmarks
    add_custom_target(run_benchmarks
        COMMAND performance_benchmarks
        DEPENDS performance_benchmarks
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running performance benchmarks"
    )
else()
    message(STATUS "Google Benchmark not found, skipping performance benchmarks")
endif()