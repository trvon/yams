# YAMS MCP server library

# Collect MCP source files
set(MCP_SOURCES
    mcp_server.cpp
)

# Create MCP library
add_library(yams_mcp STATIC ${MCP_SOURCES})

# Link dependencies
target_link_libraries(yams_mcp
    PUBLIC
        yams_api
        yams_search
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
)

# Include directories
target_include_directories(yams_mcp
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    # Boost.Beast is header-only and included with Boost
)

# Set C++ standard
target_compile_features(yams_mcp PUBLIC cxx_std_20)