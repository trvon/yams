cmake_minimum_required(VERSION 3.20)

project(yams-bootstrap LANGUAGES NONE)

set(YAMS_PROFILE "Release" CACHE STRING "Build profile forwarded to setup script")
set_property(CACHE YAMS_PROFILE PROPERTY STRINGS Debug Release Profiling Fuzzing)

set(YAMS_BUILD_DIR "" CACHE PATH "Optional YAMS build directory override")
option(YAMS_OFFLINE "Disable network dependency resolution" OFF)
option(YAMS_USE_SYSTEM_DEPS "Skip Conan and use system dependencies" OFF)

find_program(MESON_EXECUTABLE meson REQUIRED)

if(WIN32)
  find_program(YAMS_POWERSHELL_EXECUTABLE NAMES pwsh powershell REQUIRED)
  set(_setup_cmd
    "${YAMS_POWERSHELL_EXECUTABLE}" -NoProfile -ExecutionPolicy Bypass
    -File "${CMAKE_SOURCE_DIR}/setup.ps1"
    -BuildType "${YAMS_PROFILE}")
  if(YAMS_OFFLINE)
    list(APPEND _setup_cmd -Offline)
  endif()
  if(YAMS_USE_SYSTEM_DEPS)
    list(APPEND _setup_cmd -SystemDeps)
  endif()
else()
  find_program(YAMS_BASH_EXECUTABLE bash REQUIRED)
  set(_setup_cmd "${YAMS_BASH_EXECUTABLE}" "${CMAKE_SOURCE_DIR}/setup.sh" "${YAMS_PROFILE}")
  if(YAMS_OFFLINE)
    list(APPEND _setup_cmd --offline)
  endif()
  if(YAMS_USE_SYSTEM_DEPS)
    list(APPEND _setup_cmd --system-deps)
  endif()
endif()

if(YAMS_BUILD_DIR)
  set(_meson_build_dir "${YAMS_BUILD_DIR}")
else()
  if(YAMS_PROFILE STREQUAL "Debug")
    set(_meson_build_dir "builddir")
  else()
    string(TOLOWER "${YAMS_PROFILE}" _profile_lower)
    set(_meson_build_dir "build/${_profile_lower}")
  endif()
endif()

set(_env_cmd "${CMAKE_COMMAND}" -E env)
if(YAMS_BUILD_DIR)
  list(APPEND _env_cmd "YAMS_BUILD_DIR=${YAMS_BUILD_DIR}")
endif()

add_custom_target(yams-configure
  COMMAND ${_env_cmd} ${_setup_cmd}
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  USES_TERMINAL
  COMMENT "Configuring YAMS via setup script"
)

add_custom_target(yams ALL
  COMMAND "${MESON_EXECUTABLE}" compile -C "${_meson_build_dir}"
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  DEPENDS yams-configure
  USES_TERMINAL
  COMMENT "Building YAMS via Meson"
)

add_custom_target(yams-install
  COMMAND "${MESON_EXECUTABLE}" install -C "${_meson_build_dir}"
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  DEPENDS yams
  USES_TERMINAL
  COMMENT "Installing YAMS via Meson"
)
