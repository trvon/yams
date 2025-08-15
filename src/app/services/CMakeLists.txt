# YAMS Application Services Library

set(APP_SERVICES_SOURCES
    document_service.cpp
    search_service.cpp
    grep_service.cpp
    factory.cpp
)

add_library(yams_app_services STATIC ${APP_SERVICES_SOURCES})

target_link_libraries(yams_app_services
    PUBLIC
        yams_api
        yams_metadata
        yams_search
        yams_vector
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
if(YAMS_ENABLE_PROFILING AND TARGET Tracy::TracyClient)
    target_link_libraries(yams_app_services PRIVATE Tracy::TracyClient)
endif()