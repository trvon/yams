#pragma once

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <process.h> // for _getpid
#include <stdlib.h>

#define getpid _getpid
#define access _access
#define F_OK 0

// setenv/unsetenv shims
inline int setenv(const char* name, const char* value, int overwrite) {
    if (!overwrite && getenv(name))
        return 0;
    return _putenv_s(name, value);
}

inline int unsetenv(const char* name) {
    return _putenv_s(name, "");
}

#else
#include <unistd.h>
#endif
