#pragma once

#ifdef _WIN32
#include <string>
#include <windows.h>

#define RTLD_LAZY 0
#define RTLD_NOW 0
#define RTLD_GLOBAL 0
#define RTLD_LOCAL 0

inline void* dlopen(const char* filename, int flags) {
    if (!filename)
        return GetModuleHandle(NULL);
    std::string name(filename);
    // Convert to wide string
    int size_needed = MultiByteToWideChar(CP_UTF8, 0, &name[0], (int)name.size(), NULL, 0);
    std::wstring wname(size_needed, 0);
    MultiByteToWideChar(CP_UTF8, 0, &name[0], (int)name.size(), &wname[0], size_needed);
    return (void*)LoadLibraryW(wname.c_str());
}

inline void* dlsym(void* handle, const char* symbol) {
    return (void*)GetProcAddress((HMODULE)handle, symbol);
}

inline int dlclose(void* handle) {
    return FreeLibrary((HMODULE)handle) ? 0 : -1;
}

inline char* dlerror() {
    static char msg[256];
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, GetLastError(),
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), msg, sizeof(msg), NULL);
    return msg;
}
#else
#include <dlfcn.h>
#endif
