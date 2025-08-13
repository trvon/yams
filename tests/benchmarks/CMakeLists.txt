# Performance benchmarks
# Check if benchmark target exists (either from find_package or FetchContent)
find_package(benchmark QUIET)
if(benchmark_FOUND OR TARGET benchmark::benchmark)
    # Original benchmarks
    add_executable(performance_benchmarks
        performance_benchmarks.cpp
    )
    
    add_executable(compression_benchmark
        compression_benchmark.cpp
    )
    
    # New comprehensive benchmarks
    add_executable(ingestion_benchmark
        ingestion_benchmark.cpp
    )
    
    add_executable(search_benchmark
        search_benchmark.cpp
    )
    
    add_executable(metadata_benchmark
        metadata_benchmark.cpp
    )
    
    # Search-specific benchmarks (moved from src/search/benchmarks)
    add_executable(query_parser_bench
        query_parser_bench.cpp
    )
    
    add_executable(bk_tree_bench
        bk_tree_bench.cpp
    )
    
    add_executable(search_executor_bench
        search_executor_bench.cpp
    )
    
    add_executable(query_tokenizer_bench
        query_tokenizer_bench.cpp
    )
    
    add_executable(result_ranker_bench
        result_ranker_bench.cpp
    )
    
    # Link original benchmarks
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
    
    # Link new benchmarks
    target_link_libraries(ingestion_benchmark
        PRIVATE
            yams::api
            yams::storage_engine
            yams::chunking
            yams::crypto
            test_utils
            benchmark::benchmark
    )
    
    target_link_libraries(search_benchmark
        PRIVATE
            yams::api
            yams::search
            test_utils
            benchmark::benchmark
    )
    
    target_link_libraries(metadata_benchmark
        PRIVATE
            yams::metadata
            yams::api
            test_utils
            benchmark::benchmark
    )
    
    # Link search benchmarks
    target_link_libraries(query_parser_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::benchmarks
    )
    
    target_link_libraries(bk_tree_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::benchmarks
    )
    
    target_link_libraries(search_executor_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::metadata
            yams::benchmarks
    )
    
    target_link_libraries(query_tokenizer_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::benchmarks
    )
    
    target_link_libraries(result_ranker_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::benchmarks
    )
    
    # Set C++20 for all benchmarks
    target_compile_features(performance_benchmarks PRIVATE cxx_std_20)
    target_compile_features(compression_benchmark PRIVATE cxx_std_20)
    target_compile_features(ingestion_benchmark PRIVATE cxx_std_20)
    target_compile_features(search_benchmark PRIVATE cxx_std_20)
    target_compile_features(metadata_benchmark PRIVATE cxx_std_20)
    target_compile_features(query_parser_bench PRIVATE cxx_std_20)
    target_compile_features(bk_tree_bench PRIVATE cxx_std_20)
    target_compile_features(search_executor_bench PRIVATE cxx_std_20)
    target_compile_features(query_tokenizer_bench PRIVATE cxx_std_20)
    target_compile_features(result_ranker_bench PRIVATE cxx_std_20)
    
    # Include directories for test utilities
    target_include_directories(ingestion_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    target_include_directories(search_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    target_include_directories(metadata_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    target_include_directories(query_parser_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    target_include_directories(bk_tree_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    target_include_directories(search_executor_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    target_include_directories(query_tokenizer_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    target_include_directories(result_ranker_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
    
    # Add custom target to run all benchmarks
    add_custom_target(run_benchmarks
        COMMAND performance_benchmarks
        COMMAND compression_benchmark
        COMMAND ingestion_benchmark
        COMMAND search_benchmark
        COMMAND metadata_benchmark
        COMMAND query_parser_bench
        COMMAND bk_tree_bench
        COMMAND search_executor_bench
        COMMAND query_tokenizer_bench
        COMMAND result_ranker_bench
        DEPENDS performance_benchmarks compression_benchmark 
                ingestion_benchmark search_benchmark metadata_benchmark
                query_parser_bench bk_tree_bench search_executor_bench
                query_tokenizer_bench result_ranker_bench
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running all performance benchmarks"
    )
    
    # Add individual benchmark targets
    add_custom_target(run_ingestion_benchmark
        COMMAND ingestion_benchmark
        DEPENDS ingestion_benchmark
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running ingestion benchmarks"
    )
    
    add_custom_target(run_search_benchmark
        COMMAND search_benchmark
        DEPENDS search_benchmark
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running search benchmarks"
    )
    
    add_custom_target(run_metadata_benchmark
        COMMAND metadata_benchmark
        DEPENDS metadata_benchmark
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running metadata benchmarks"
    )
    
    # Search benchmark targets
    add_custom_target(run_search_benchmarks
        COMMAND query_parser_bench
        COMMAND bk_tree_bench
        COMMAND search_executor_bench
        COMMAND query_tokenizer_bench
        COMMAND result_ranker_bench
        DEPENDS query_parser_bench bk_tree_bench search_executor_bench
                query_tokenizer_bench result_ranker_bench
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running all search-specific benchmarks"
    )
else()
    message(STATUS "Google Benchmark not found, skipping performance benchmarks")
endif()