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
        nlohmann_json::nlohmann_json
        spdlog::spdlog
)

target_include_directories(yams_app_services
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(yams_app_services PUBLIC cxx_std_20)

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
