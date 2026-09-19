// Compile-only cross-compile portability probe.
//
// Compiled with -fsyntax-only by scripts/local-ci/cross-compile-smoke.sh when a
// Windows/Linux cross compiler is installed. Exercises the defect classes that
// only fail under a cross toolchain, not the macOS host sanitizer lane:
//
//   * std::filesystem::path::c_str() returns wchar_t on Windows; passing it to a
//     narrow const char* API is a compile error. The correct form is .string().c_str().
//   * std::jthread is absent from libc++/older libstdc++; yams::compat::jthread
//     is the portable spelling.
//   * libstdc++ system_clock::from_time_t wraps past ~2262-04-11; keep a
//     representable epoch so the smoke fails only on genuinely broken runtimes.

#include <chrono>
#include <filesystem>
#include <string>

#include <yams/compat/thread_stop_compat.h>

namespace {

// Narrow C API taking const char*: the boundary where path.c_str() (wchar_t on
// Windows) would fail to compile.
int narrow_open(const char* path) {
    return path != nullptr ? 0 : 1;
}

} // namespace

int main() {
    const std::filesystem::path path{"/tmp/yams-portability-probe"};
    const std::string text = path.string();

    // Both forms must compile: std::string::c_str() and the .string().c_str()
    // conversion of a filesystem::path.
    (void)narrow_open(text.c_str());
    (void)narrow_open(path.string().c_str());

    // Portable jthread spelling (falls back to std::thread on libc++ without jthread).
    yams::compat::jthread worker{[] {}};
    worker.request_stop();
    worker.join();

    // A representable epoch, far from the libstdc++ from_time_t overflow ceiling.
    (void)std::chrono::system_clock::from_time_t(0);
    return 0;
}
