// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

/**
 * @file windows_init.h
 * @brief Windows-specific initialization utilities
 *
 * This header provides platform-specific initialization that must happen
 * early in program startup, before any DLLs are loaded. Include this header
 * in main.cpp of any YAMS executable that needs to find DLLs in the
 * application directory.
 *
 * Usage:
 *   #include <yams/platform/windows_init.h>
 *   // The static initializer will run automatically before main()
 */

#ifndef YAMS_PLATFORM_WINDOWS_INIT_H
#define YAMS_PLATFORM_WINDOWS_INIT_H

#ifdef _WIN32

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <filesystem>

namespace yams::platform {

/**
 * @brief Set up DLL search paths for Windows
 *
 * Configures Windows to search for DLLs in the application's directory.
 * This is necessary because:
 * 1. Conan deploys DLLs alongside the executable
 * 2. Windows doesn't automatically search the executable's directory for DLLs
 * 3. Without this, delay-loaded DLLs will fail to load
 *
 * @return true if setup succeeded, false otherwise
 */
inline bool setup_dll_search_path() {
    wchar_t exe_path[MAX_PATH];
    DWORD len = GetModuleFileNameW(nullptr, exe_path, MAX_PATH);
    if (len == 0 || len >= MAX_PATH) {
        return false;
    }

    std::filesystem::path exe_dir = std::filesystem::path(exe_path).parent_path();

    // Set primary DLL directory to application folder
    if (!SetDllDirectoryW(exe_dir.c_str())) {
        return false;
    }

    // Also add to the DLL search path list for delay-loaded DLLs
    // This uses the secure DLL search mode introduced in Windows 8
    AddDllDirectory(exe_dir.c_str());

    return true;
}

namespace detail {

/**
 * @brief Static initializer that runs before main()
 *
 * This struct's constructor is called during static initialization,
 * ensuring DLL paths are configured before any code runs that might
 * trigger DLL loading.
 */
struct DllPathInitializer {
    DllPathInitializer() {
        setup_dll_search_path();
    }
};

// Global instance - constructor runs during static init
[[maybe_unused]] static DllPathInitializer dll_path_initializer;

} // namespace detail

} // namespace yams::platform

#else // !_WIN32

namespace yams::platform {

// No-op on non-Windows platforms
inline bool setup_dll_search_path() {
    return true;
}

} // namespace yams::platform

#endif // _WIN32

#endif // YAMS_PLATFORM_WINDOWS_INIT_H
