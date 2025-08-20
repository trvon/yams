# Find SQLite3
find_package(SQLite3 REQUIRED)

# Platform-specific sources
if(UNIX)
    set(PLATFORM_SOURCES platform/posix_storage.cpp)
elseif(WIN32)
    set(PLATFORM_SOURCES platform/windows_storage.cpp)
endif()

# Storage engine library
add_library(yams_storage_engine
    storage_engine.cpp
    compressed_storage_engine.cpp
    ${PLATFORM_SOURCES}
)

target_link_libraries(yams_storage_engine
    PUBLIC
        yams::core
        yams::crypto
        yams::compression
    PRIVATE
        spdlog::spdlog
        Threads::Threads
)

target_include_directories(yams_storage_engine
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(yams_storage_engine PUBLIC cxx_std_20)

# Platform-specific definitions
if(UNIX)
    target_compile_definitions(yams_storage_engine PRIVATE YAMS_PLATFORM_POSIX)
elseif(WIN32)
    target_compile_definitions(yams_storage_engine PRIVATE YAMS_PLATFORM_WINDOWS)
endif()

# Reference counting library
add_library(yams_reference_counter
    reference_counter.cpp
    garbage_collector.cpp
)

target_link_libraries(yams_reference_counter
    PUBLIC
        yams::core
        yams::storage_engine
    PRIVATE
        sqlite3
        spdlog::spdlog
        Threads::Threads
)

target_include_directories(yams_reference_counter
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(yams_reference_counter PUBLIC cxx_std_20)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET Tracy::TracyClient)
    target_link_libraries(yams_storage_engine PRIVATE Tracy::TracyClient)
    target_link_libraries(yams_reference_counter PRIVATE Tracy::TracyClient)
endif()

# Export targets
install(TARGETS yams_storage_engine yams_reference_counter
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_library(yams::storage_engine ALIAS yams_storage_engine)
add_library(yams::reference_counter ALIAS yams_reference_counter)