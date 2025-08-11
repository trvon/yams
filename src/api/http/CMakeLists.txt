# Find Drogon
find_package(Drogon CONFIG REQUIRED)
find_package(Jsoncpp REQUIRED)

# HTTP API library
add_library(yams_http_api
    content_controller.cpp
    auth_filter.cpp
    server.cpp
)

target_include_directories(yams_http_api
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(yams_http_api
    PUBLIC
        yams::api
        Drogon::Drogon
    PRIVATE
        spdlog::spdlog
        jsoncpp_lib
)

# Set properties
set_target_properties(yams_http_api PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Export target
install(TARGETS yams_http_api
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Create alias
add_library(yams::http_api ALIAS yams_http_api)