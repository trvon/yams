# CMakeLists for src/benchmarks utility library (yams_benchmarks)

# Create the reusable benchmarks utility library that provides:
# - VectorBenchmark / BenchmarkSuite framework
# - MemoryProfiler
# This library does NOT depend on Google Benchmark. It is meant to be used by
# benchmark executables under src/**/benchmarks as well as other internal tools.

add_library(yams_benchmarks STATIC
    vector_benchmark.cpp
)

# Public headers live under include/yams/**
target_include_directories(yams_benchmarks
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Link minimal dependencies required by the framework
target_link_libraries(yams_benchmarks
    PUBLIC
        yams::core
        spdlog::spdlog
)

# Target properties
set_target_properties(yams_benchmarks PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME "yams_benchmarks"
)

# Provide a namespaced alias for downstream consumers
add_library(yams::benchmarks ALIAS yams_benchmarks)

# Install rules (headers are installed by top-level; this installs the library)
install(TARGETS yams_benchmarks
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
