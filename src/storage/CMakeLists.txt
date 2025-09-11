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
