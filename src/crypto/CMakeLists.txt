add_library(yams_crypto
    sha256_hasher.cpp
)

target_link_libraries(yams_crypto
    PUBLIC
        yams::core
        OpenSSL::Crypto
    PRIVATE
        spdlog::spdlog
)

target_include_directories(yams_crypto
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Export target
install(TARGETS yams_crypto
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_library(yams::crypto ALIAS yams_crypto)