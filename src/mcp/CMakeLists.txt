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

# Link Boost if using Conan
if(YAMS_USE_CONAN AND TARGET boost::boost)
    target_link_libraries(yams_mcp PUBLIC boost::boost)
elseif(YAMS_USE_CONAN)
    # Fallback to manual include if target doesn't exist
    message(STATUS "Using manual Boost include path")
    target_include_directories(yams_mcp PUBLIC /Users/trevon/.conan2/p/b/boostf44bdc852762a/p/include)
endif()

# Include directories
target_include_directories(yams_mcp
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    # Boost.Beast is header-only and included with Boost
)

# Set C++ standard
target_compile_features(yams_mcp PUBLIC cxx_std_20)