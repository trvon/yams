# YAMS Application Services Library

set(APP_SERVICES_SOURCES
    document_service.cpp
    search_service.cpp
    grep_service.cpp
    grep_mode_tls.cpp
    download_service.cpp
    indexing_service.cpp
    stats_service.cpp
    utils.cpp
    factory.cpp
    session_service.cpp
)

add_library(yams_app_services STATIC ${APP_SERVICES_SOURCES})
set_target_properties(yams_app_services PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

target_link_libraries(yams_app_services
    PUBLIC
        yams_api
        yams_metadata
        yams_search
        yams_vector
        yams_downloader
        yams_boost_headers
    $<$<TARGET_EXISTS:boost::_libboost>:boost::_libboost>
    $<$<TARGET_EXISTS:Boost::headers>:Boost::headers>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<TARGET_EXISTS:Boost::boost>>:Boost::boost>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<NOT:$<TARGET_EXISTS:Boost::boost>>,$<TARGET_EXISTS:boost::boost>>:boost::boost>
        nlohmann_json::nlohmann_json
        spdlog::spdlog
)

target_include_directories(yams_app_services
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/cli
)

target_compile_features(yams_app_services PUBLIC cxx_std_20)

# Ensure Boost headers (Asio) are visible to this target and consumers
if(TARGET yams_boost_headers)
    get_target_property(_yams_boost_inc yams_boost_headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_yams_boost_inc)
        target_include_directories(yams_app_services SYSTEM PUBLIC ${_yams_boost_inc})
    endif()
endif()

# Propagate include dirs from Boost targets using generator expressions as a safety net
target_include_directories(yams_app_services SYSTEM PUBLIC
    $<$<TARGET_EXISTS:Boost::headers>:$<TARGET_PROPERTY:Boost::headers,INTERFACE_INCLUDE_DIRECTORIES>>
    $<$<TARGET_EXISTS:Boost::headers>:$<TARGET_PROPERTY:Boost::headers,INTERFACE_SYSTEM_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<TARGET_EXISTS:Boost::boost>>:$<TARGET_PROPERTY:Boost::boost,INTERFACE_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<TARGET_EXISTS:Boost::boost>>:$<TARGET_PROPERTY:Boost::boost,INTERFACE_SYSTEM_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<NOT:$<TARGET_EXISTS:Boost::boost>>,$<TARGET_EXISTS:boost::boost>>:$<TARGET_PROPERTY:boost::boost,INTERFACE_INCLUDE_DIRECTORIES>>
    $<$<AND:$<NOT:$<TARGET_EXISTS:Boost::headers>>,$<NOT:$<TARGET_EXISTS:Boost::boost>>,$<TARGET_EXISTS:boost::boost>>:$<TARGET_PROPERTY:boost::boost,INTERFACE_SYSTEM_INCLUDE_DIRECTORIES>>
)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET yams_profiling)
    target_link_libraries(yams_app_services PRIVATE $<BUILD_INTERFACE:yams_profiling>)
endif()

# Export and install app services library
install(TARGETS yams_app_services
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Create alias for namespaced usage
add_library(yams::app_services ALIAS yams_app_services)
