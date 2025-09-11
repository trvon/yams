# Try to find wasmtime-cpp headers and wasmtime library
# Defines:
#  wasmtime_FOUND
#  wasmtime_INCLUDE_DIRS
#  wasmtime_LIBRARIES
#  Target: wasmtime::wasmtime

set(_WASMTIME_HINTS
    $ENV{WASMTIME_ROOT}
    ${WASMTIME_ROOT}
    $ENV{WASMTIME_DIR}
    ${WASMTIME_DIR}
    /usr/local
    /usr
)

find_path(WASMTIME_INCLUDE_DIR
    NAMES wasmtime.hh
    HINTS ${_WASMTIME_HINTS}
    PATH_SUFFIXES include include/wasmtime
)

find_library(WASMTIME_LIBRARY
    NAMES wasmtime
    HINTS ${_WASMTIME_HINTS}
    PATH_SUFFIXES lib lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(wasmtime DEFAULT_MSG WASMTIME_INCLUDE_DIR WASMTIME_LIBRARY)

if(wasmtime_FOUND)
    set(wasmtime_INCLUDE_DIRS ${WASMTIME_INCLUDE_DIR})
    set(wasmtime_LIBRARIES ${WASMTIME_LIBRARY})
    if(NOT TARGET wasmtime::wasmtime)
        add_library(wasmtime::wasmtime UNKNOWN IMPORTED)
        set_target_properties(wasmtime::wasmtime PROPERTIES
            IMPORTED_LOCATION "${WASMTIME_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${WASMTIME_INCLUDE_DIR}")
    endif()
endif()

