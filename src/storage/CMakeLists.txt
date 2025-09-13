# Find SQLite3 (Conan or system). Prefer imported targets that propagate include dirs.
find_package(SQLite3 REQUIRED)

# Resolve an appropriate sqlite3 target that ensures include directories are usage requirements.
set(YAMS_SQLITE3_TARGET "")
if(TARGET sqlite3::sqlite3)
    set(YAMS_SQLITE3_TARGET sqlite3::sqlite3)
elseif(TARGET SQLite::SQLite3) # Some distributions use this namespace
    set(YAMS_SQLITE3_TARGET SQLite::SQLite3)
elseif(TARGET sqlite3) # Fallback imported target
    set(YAMS_SQLITE3_TARGET sqlite3)
endif()

if(NOT YAMS_SQLITE3_TARGET)
    # Fallback to variables (older FindSQLite3) — add include dir manually
    if(SQLite3_INCLUDE_DIRS)
        add_library(yams_sqlite3 INTERFACE)
        target_include_directories(yams_sqlite3 INTERFACE ${SQLite3_INCLUDE_DIRS})
        if(SQLite3_LIBRARIES)
            target_link_libraries(yams_sqlite3 INTERFACE ${SQLite3_LIBRARIES})
        endif()
        set(YAMS_SQLITE3_TARGET yams_sqlite3)
        message(STATUS "Created interface target yams_sqlite3 from SQLite3 variables")
    else()
        message(FATAL_ERROR "Could not locate an sqlite3 CMake target or variables; ensure Conan/system sqlite3 package is available")
    endif()
endif()

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
    storage_backend_factory.cpp
    url_backend.cpp
    s3_signer.cpp
    object_storage_plugin_loader.cpp
    ../storage/object_storage_adapter.cpp
    ${PLATFORM_SOURCES}
)
set_target_properties(yams_storage_engine PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

target_link_libraries(yams_storage_engine
    PUBLIC
        yams::core
        yams::crypto
        yams::compression
    PRIVATE
        spdlog::spdlog
        Threads::Threads
        CURL::libcurl
        $<$<PLATFORM_ID:Linux>:dl>
)

target_include_directories(yams_storage_engine
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(yams_storage_engine PUBLIC cxx_std_20)

# Enable position-independent code for linking into shared libraries
set_target_properties(yams_storage_engine PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

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
set_target_properties(yams_reference_counter PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

target_link_libraries(yams_reference_counter
    PUBLIC
        yams::core
        yams::storage_engine
        ${YAMS_SQLITE3_TARGET}
    PRIVATE
        spdlog::spdlog
        Threads::Threads
)

target_include_directories(yams_reference_counter
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(yams_reference_counter PUBLIC cxx_std_20)

# Enable position-independent code for linking into shared libraries
set_target_properties(yams_reference_counter PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET yams_profiling)
    target_link_libraries(yams_storage_engine PRIVATE $<BUILD_INTERFACE:yams_profiling>)
    target_link_libraries(yams_reference_counter PRIVATE $<BUILD_INTERFACE:yams_profiling>)
endif()

# Export targets
install(TARGETS yams_storage_engine yams_reference_counter
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_library(yams::storage_engine ALIAS yams_storage_engine)
add_library(yams::reference_counter ALIAS yams_reference_counter)
