# Search module benchmarks (Google Benchmark executables)
# This directory is added only when YAMS_BUILD_BENCHMARKS=ON from the parent CMake.
# We still guard on the benchmark target to ensure dependencies are present.

if(TARGET benchmark::benchmark)
    # Query Parser Benchmark
    add_executable(query_parser_bench
        query_parser_bench.cpp
    )
    target_link_libraries(query_parser_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::benchmarks
    )

    # BK-tree Benchmark
    add_executable(bk_tree_bench
        bk_tree_bench.cpp
    )
    target_link_libraries(bk_tree_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::benchmarks
    )

    # Search Executor Benchmark
    add_executable(search_executor_bench
        search_executor_bench.cpp
    )
    target_link_libraries(search_executor_bench
        PRIVATE
            benchmark::benchmark
            yams::search
            yams::metadata
            yams::benchmarks
    )

    # Additional search benchmarks (built only if sources exist)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/query_tokenizer_bench.cpp")
        add_executable(query_tokenizer_bench
            query_tokenizer_bench.cpp
        )
        target_link_libraries(query_tokenizer_bench
            PRIVATE
                benchmark::benchmark
                yams::search
                yams::benchmarks
        )
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/result_ranker_bench.cpp")
        add_executable(result_ranker_bench
            result_ranker_bench.cpp
        )
        target_link_libraries(result_ranker_bench
            PRIVATE
                benchmark::benchmark
                yams::search
                yams::benchmarks
        )
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/hybrid_search_bench.cpp")
        add_executable(hybrid_search_bench
            hybrid_search_bench.cpp
        )
        target_link_libraries(hybrid_search_bench
            PRIVATE
                benchmark::benchmark
                yams::search
                yams::benchmarks
        )
    endif()

    # Consistent C++ standard settings
    foreach(_bench_target IN ITEMS
        query_parser_bench
        bk_tree_bench
        search_executor_bench
        query_tokenizer_bench
        result_ranker_bench
        hybrid_search_bench
    )
        if(TARGET ${_bench_target})
            set_target_properties(${_bench_target} PROPERTIES
                CXX_STANDARD 20
                CXX_STANDARD_REQUIRED ON
                POSITION_INDEPENDENT_CODE ON
            )
        endif()
    endforeach()
else()
    message(STATUS "benchmark::benchmark target not found; skipping search benchmarks")
endif()
