# Vector database library

# Get sqlite-vec source if available
if(TARGET sqlite_vec)
    get_target_property(SQLITE_VEC_SOURCE_DIR sqlite_vec SOURCE_DIR)
else()
    # If sqlite_vec target doesn't exist, we need the source path
    if(DEFINED sqlite_vec_SOURCE_DIR)
        set(SQLITE_VEC_SOURCE_DIR ${sqlite_vec_SOURCE_DIR})
    endif()
endif()

# Create the vector library
add_library(yams_vector
    vector_database.cpp
    embedding_generator.cpp
    embedding_service.cpp    # Simple embedding service (replaces complex worker)
    document_chunker.cpp
    vector_index_manager.cpp
    vector_index_optimizer.cpp
    model_registry.cpp
    model_cache.cpp
    model_loader.cpp
    sqlite_vec_backend.cpp  # SQLite-vec backend implementation
    mock_embedding_provider.cpp  # Mock provider for testing
    # Compile sqlite-vec directly into the library
    ${SQLITE_VEC_SOURCE_DIR}/sqlite-vec.c
)
set_target_properties(yams_vector PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)

# Disable clang-tidy on this target to avoid third-party C lint errors
set_target_properties(yams_vector PROPERTIES C_CLANG_TIDY "")

# Suppress warnings for sqlite-vec.c (third-party code)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set_source_files_properties(${SQLITE_VEC_SOURCE_DIR}/sqlite-vec.c
        PROPERTIES
        COMPILE_FLAGS "-w"  # Suppress all warnings
        C_CLANG_TIDY ""     # Disable clang-tidy on third-party C
    )
elseif(MSVC)
    set_source_files_properties(${SQLITE_VEC_SOURCE_DIR}/sqlite-vec.c
        PROPERTIES
        COMPILE_FLAGS "/w"  # Suppress all warnings
    )
endif()

# Set properties
set_target_properties(yams_vector PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    POSITION_INDEPENDENT_CODE ON
)

# Include directories
target_include_directories(yams_vector
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# Mark third-party headers as SYSTEM to relax warnings from external code
target_include_directories(yams_vector
    SYSTEM PRIVATE
        ${CMAKE_SOURCE_DIR}/third_party/hnswlib  # HNSWlib headers
)

# Link dependencies
# Make ONNX Runtime selectable; when enabled, require it to be present
option(YAMS_ENABLE_ONNX "Enable ONNX Runtime support in vector library" ON)

if(YAMS_ENABLE_ONNX)
    # When ONNX is enabled, require the package instead of silently disabling
    if(NOT TARGET onnxruntime::onnxruntime)
        find_package(onnxruntime REQUIRED)
    endif()
    message(STATUS "ONNX Runtime found - enabling local embedding generation")
else()
    message(STATUS "ONNX Runtime disabled - using mock/daemon embedding providers")
endif()

target_link_libraries(yams_vector
    PUBLIC
        yams::core
        yams::metadata  # Need access to database
        yams::api       # Need IContentStore for worker
    PRIVATE
        spdlog::spdlog
        sqlite3                 # Direct SQLite access
)

# Link fmt only if provided (fallback when std::format is unavailable)
if(DEFINED YAMS_HAS_STD_FORMAT AND NOT YAMS_HAS_STD_FORMAT)
    if(TARGET fmt::fmt)
        target_link_libraries(yams_vector PRIVATE fmt::fmt)
    endif()
endif()

# Link daemon client if available (avoids circular dependency)
if(TARGET yams::daemon_client)
    target_link_libraries(yams_vector PRIVATE yams::daemon_client)
    target_compile_definitions(yams_vector PRIVATE YAMS_HAS_DAEMON_CLIENT)
endif()

# Asio client pool no longer used directly; DaemonClient provides transport

# Conditionally link ONNX Runtime
if(YAMS_ENABLE_ONNX AND TARGET onnxruntime::onnxruntime)
    target_link_libraries(yams_vector PRIVATE onnxruntime::onnxruntime)
endif()

# Compiler flags
target_compile_definitions(yams_vector
    PRIVATE
        # Enable debug logging in debug builds
        $<$<CONFIG:Debug>:YAMS_VECTOR_DEBUG>
        # SQLite-vec flags
        SQLITE_CORE=1
        SQLITE_ENABLE_LOAD_EXTENSION=1
        SQLITE_VEC_STATIC=1
)

# Conditionally enable ONNX Runtime support
if(YAMS_ENABLE_ONNX AND TARGET onnxruntime::onnxruntime)
    target_compile_definitions(yams_vector PRIVATE YAMS_USE_ONNX_RUNTIME)
endif()

# Create sqlite-vec.h manually since we're compiling as SQLITE_CORE
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/sqlite-vec.h
"#ifndef SQLITE_VEC_H
#define SQLITE_VEC_H

#define SQLITE_CORE 1
#include <sqlite3.h>

#define SQLITE_VEC_API
#define SQLITE_VEC_VERSION \"v0.1.3\"
#define SQLITE_VEC_DATE \"2024-08-15\"
#define SQLITE_VEC_SOURCE \"yams-embedded\"

#ifdef __cplusplus
extern \"C\" {
#endif

int sqlite3_vec_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);

#ifdef __cplusplus
}
#endif

#endif /* SQLITE_VEC_H */
")

# Add include path for the generated header
target_include_directories(yams_vector
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET yams_profiling)
    target_link_libraries(yams_vector PRIVATE $<BUILD_INTERFACE:yams_profiling>)
endif()

# TODO: Find and link LanceDB package
# find_package(LanceDB REQUIRED)
# find_package(Arrow REQUIRED)
#
# target_link_libraries(yams_vector
#     PRIVATE
#         LanceDB::lancedb
#         Arrow::arrow
#         Arrow::parquet
# )

# Export target
install(TARGETS yams_vector
    EXPORT YamsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install headers
install(
    DIRECTORY ${CMAKE_SOURCE_DIR}/include/yams/vector
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/yams
    FILES_MATCHING PATTERN "*.h"
)

# Create alias for consistent naming
add_library(yams::vector ALIAS yams_vector)
