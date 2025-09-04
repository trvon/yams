# YAMS Config library

# Collect config source files
set(CONFIG_SOURCES
    config_migration.cpp
)

# Create config library
add_library(yams_config STATIC ${CONFIG_SOURCES})

# Avoid LTO bitcode in this static lib to keep downstream links stable without IPO
set_target_properties(yams_config PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

# Link dependencies
target_link_libraries(yams_config
    PUBLIC
        yams_core
        spdlog::spdlog
    PRIVATE
        nlohmann_json::nlohmann_json
)

# Include directories
target_include_directories(yams_config
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Set C++ standard
target_compile_features(yams_config PUBLIC cxx_std_20)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET yams_profiling)
    target_link_libraries(yams_config PRIVATE $<BUILD_INTERFACE:yams_profiling>)
endif()
