add_library(yams_wal
    wal_entry.cpp
    wal_manager.cpp
    wal_file.cpp
    wal_recovery.cpp
)

target_link_libraries(yams_wal
    PUBLIC
        yams::core
    PRIVATE
        spdlog::spdlog
        Threads::Threads
)

target_include_directories(yams_wal
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Export target
install(TARGETS yams_wal
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_library(yams::wal ALIAS yams_wal)