add_library(yams_integrity
    integrity_verifier.cpp
    verification_scheduler.cpp
    verification_monitor.cpp
    repair_manager.cpp
    chunk_validator.cpp
)

target_link_libraries(yams_integrity
    PUBLIC
        yams::core
        yams::storage_engine
        yams::crypto
        yams::manifest
        yams::chunking
    PRIVATE
        spdlog::spdlog
        Threads::Threads
)

target_include_directories(yams_integrity
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Export target
install(TARGETS yams_integrity
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_library(yams::integrity ALIAS yams_integrity)