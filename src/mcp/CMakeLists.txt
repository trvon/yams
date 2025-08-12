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

# Find and link Boost
if(YAMS_USE_CONAN AND TARGET boost::boost)
    target_link_libraries(yams_mcp PUBLIC boost::boost)
elseif(YAMS_USE_CONAN)
    # Fallback to manual include if target doesn't exist
    message(STATUS "Using manual Boost include path")
    target_include_directories(yams_mcp PUBLIC /Users/trevon/.conan2/p/b/boostf44bdc852762a/p/include)
else()
    # Traditional build - use FetchContent Boost or system Boost
    if(TARGET Boost::headers)
        # Using FetchContent Boost from main CMakeLists.txt
        message(STATUS "Using FetchContent Boost headers for MCP server")
        target_link_libraries(yams_mcp PUBLIC Boost::headers)
    else()
        # Try to find system Boost as fallback
        find_package(Boost QUIET)
        if(Boost_FOUND)
            target_include_directories(yams_mcp PUBLIC ${Boost_INCLUDE_DIRS})
            if(Boost_LIBRARIES)
                target_link_libraries(yams_mcp PUBLIC ${Boost_LIBRARIES})
            endif()
            message(STATUS "Found system Boost: ${Boost_VERSION}")
        else()
            message(FATAL_ERROR "Boost is required for MCP server but was not found. Please ensure Boost is available via FetchContent or system installation.")
        endif()
    endif()
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