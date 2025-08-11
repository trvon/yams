# YAMS Storage Maintenance Tools

# Find required packages
find_package(CLI11 REQUIRED)
find_package(nlohmann_json REQUIRED)

# Collect source files
set(TOOL_SOURCES
    src/main.cpp
    src/commands/gc_command.cpp
    src/commands/stats_command.cpp
    src/commands/verify_command.cpp
)

# Create executable
add_executable(yams-tools ${TOOL_SOURCES})

# Link libraries
target_link_libraries(yams-tools
    PRIVATE
        yams::storage_engine
        yams::reference_counter
        yams::manifest
        yams::integrity
        yams::crypto
        yams::core
        CLI11::CLI11
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        Threads::Threads
)

# Include directories
target_include_directories(yams-tools
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include
)

# Set C++ standard
target_compile_features(yams-tools PRIVATE cxx_std_20)

# Installation
install(TARGETS yams-tools
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Create symlinks for individual commands (optional)
set(COMMANDS gc stats verify)
foreach(cmd ${COMMANDS})
    install(CODE "
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E create_symlink
                yams-tools
                \$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/yams-${cmd}
        )
    ")
endforeach()