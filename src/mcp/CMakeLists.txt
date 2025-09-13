# YAMS MCP server library

# Collect MCP source files
set(MCP_SOURCES
    mcp_server.cpp
    tool_registry.cpp
    http_server.cpp
)

# Create MCP library
add_library(yams_mcp STATIC ${MCP_SOURCES})

# Avoid LTO bitcode in this static lib to keep downstream linkers happy
set_target_properties(yams_mcp PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

# Link dependencies
target_link_libraries(yams_mcp
    PUBLIC
        yams_api
        yams_search
        yams_app_services
        yams_downloader
        yams::daemon_ipc
        yams_config
    yams_boost_headers
    $<$<TARGET_EXISTS:boost::_libboost>:boost::_libboost>
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        $<$<TARGET_EXISTS:tl::expected>:tl::expected>
)

# Boost.Asio headers are provided via yams_boost_headers

# Include directories
target_include_directories(yams_mcp
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    # Boost.Beast is header-only and included with Boost
)

# Ensure Boost headers (Asio) are visible to this target
if(TARGET yams_boost_headers)
    get_target_property(_yams_boost_inc yams_boost_headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_yams_boost_inc)
        target_include_directories(yams_mcp SYSTEM PRIVATE ${_yams_boost_inc})
    endif()
endif()

if(DEFINED Boost_INCLUDE_DIRS)
    target_include_directories(yams_mcp SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
endif()

# When Conan defines a monolithic internal Boost target, also import its include dirs.
target_include_directories(yams_mcp SYSTEM PRIVATE
    $<$<TARGET_EXISTS:boost::_libboost>:$<TARGET_PROPERTY:boost::_libboost,INTERFACE_INCLUDE_DIRECTORIES>>
    $<$<TARGET_EXISTS:boost::_libboost>:$<TARGET_PROPERTY:boost::_libboost,INTERFACE_SYSTEM_INCLUDE_DIRECTORIES>>
)

# Propagate include dirs from Boost targets using generator expressions as a safety net
target_include_directories(yams_mcp SYSTEM PRIVATE
    $<$<TARGET_EXISTS:Boost::headers>:$<TARGET_PROPERTY:Boost::headers,INTERFACE_INCLUDE_DIRECTORIES>>
    $<$<TARGET_EXISTS:Boost::headers>:$<TARGET_PROPERTY:Boost::headers,INTERFACE_SYSTEM_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<TARGET_EXISTS:Boost::boost>>:$<TARGET_PROPERTY:Boost::boost,INTERFACE_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<TARGET_EXISTS:Boost::boost>>:$<TARGET_PROPERTY:Boost::boost,INTERFACE_SYSTEM_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<NOT:$<TARGET_EXISTS:Boost::boost>>,$<TARGET_EXISTS:boost::boost>>:$<TARGET_PROPERTY:boost::boost,INTERFACE_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<NOT:$<TARGET_EXISTS:Boost::boost>>,$<TARGET_EXISTS:boost::boost>>:$<TARGET_PROPERTY:boost::boost,INTERFACE_SYSTEM_INCLUDE_DIRECTORIES>>
)

# Debug: print resolved Boost include directories
get_target_property(_mcp_includes yams_mcp INCLUDE_DIRECTORIES)
message(STATUS "[yams_mcp] INCLUDE_DIRECTORIES=${_mcp_includes}")
get_target_property(_mcp_sys_includes yams_mcp INTERFACE_SYSTEM_INCLUDE_DIRECTORIES)
message(STATUS "[yams_mcp] INTERFACE_SYSTEM_INCLUDE_DIRECTORIES=${_mcp_sys_includes}")
if(TARGET Boost::headers)
    get_target_property(_dbg_bhdr_inc Boost::headers INTERFACE_INCLUDE_DIRECTORIES)
    message(STATUS "[Boost::headers] INTERFACE_INCLUDE_DIRECTORIES=${_dbg_bhdr_inc}")
endif()
if(TARGET boost::_libboost)
    get_target_property(_dbg_blib_inc boost::_libboost INTERFACE_INCLUDE_DIRECTORIES)
    message(STATUS "[boost::_libboost] INTERFACE_INCLUDE_DIRECTORIES=${_dbg_blib_inc}")
endif()

# Also link the Boost headers target explicitly when available to ensure
# include directories are attached as usage requirements for this target.
if(TARGET Boost::headers)
    target_link_libraries(yams_mcp PUBLIC Boost::headers)
elseif(TARGET Boost::boost)
    target_link_libraries(yams_mcp PUBLIC Boost::boost)
elseif(TARGET boost::boost)
    target_link_libraries(yams_mcp PUBLIC boost::boost)
endif()

if(TARGET Boost::headers)
    get_target_property(_boost_headers_inc Boost::headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_headers_inc)
        target_include_directories(yams_mcp SYSTEM PRIVATE ${_boost_headers_inc})
    endif()
elseif(TARGET Boost::boost)
    get_target_property(_boost_boost_inc Boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_inc)
        target_include_directories(yams_mcp SYSTEM PRIVATE ${_boost_boost_inc})
    endif()
elseif(TARGET boost::boost)
    get_target_property(_boost_boost_lc_inc boost::boost INTERFACE_INCLUDE_DIRECTORIES)
    if(_boost_boost_lc_inc)
        target_include_directories(yams_mcp SYSTEM PRIVATE ${_boost_boost_lc_inc})
    endif()
endif()

# Set C++ standard
target_compile_features(yams_mcp PUBLIC cxx_std_20)

# Ensure zlib is available for CRC32 in message_framing.cpp
find_package(ZLIB REQUIRED)
