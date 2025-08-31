cmake_minimum_required(VERSION 3.20)

project(yams_object_storage_s3 LANGUAGES C CXX)

add_library(yams_object_storage_s3 SHARED
    s3_plugin.cpp
)

set_target_properties(yams_object_storage_s3 PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
    OUTPUT_NAME "yams_object_storage_s3"
)

# Public include directories
target_include_directories(yams_object_storage_s3
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

# Link against core YAMS components and curl/openssl as needed
target_link_libraries(yams_object_storage_s3
    PRIVATE
        yams::core
        yams::storage_engine
        OpenSSL::Crypto
        ZLIB::ZLIB
)

install(TARGETS yams_object_storage_s3
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
