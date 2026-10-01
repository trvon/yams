#include <catch2/catch_session.hpp>

#include <string>
#include <string_view>
#include <vector>

#if defined(_MSC_VER)
#include <crtdbg.h>
#include <cstdio>
#include <cstdlib>
#include <stdlib.h>
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace {

#if defined(_MSC_VER)
// A Debug CRT assertion (debug-iterator check, _ASSERTE, invalid parameter) or abort() opens a
// modal dialog by default. In a non-interactive test run nobody can dismiss it, so the case hangs
// until the harness timeout with no output. Route every such report to stderr and suppress the
// Windows Error Reporting dialogs so the case fails fast with the message instead.
void reportCrtFailuresToStderr() {
    ::SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);
    _set_error_mode(_OUT_TO_STDERR);
    _set_abort_behavior(0, _CALL_REPORTFAULT);
    for (int reportType : {_CRT_WARN, _CRT_ERROR, _CRT_ASSERT}) {
        _CrtSetReportMode(reportType, _CRTDBG_MODE_FILE | _CRTDBG_MODE_DEBUG);
        _CrtSetReportFile(reportType, _CRTDBG_FILE_STDERR);
    }
    std::setvbuf(stderr, nullptr, _IONBF, 0);
}
#endif

bool isOption(std::string_view arg) {
    return !arg.empty() && arg.front() == '-';
}

std::vector<std::string> normalizeArgs(int argc, char* argv[]) {
    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(argc));

    bool hasOption = false;
    for (int i = 1; i < argc; ++i) {
        if (argv[i] && isOption(argv[i])) {
            hasOption = true;
            break;
        }
    }

    if (argc <= 2 || hasOption) {
        for (int i = 0; i < argc; ++i) {
            args.emplace_back(argv[i] ? argv[i] : "");
        }
        return args;
    }

    // Meson --test-args splits a test name like "Foo bar" into multiple argv entries.
    // Keep Catch2's parser on the single test-spec path; Catch2 3.12 + libc++ ASAN can
    // otherwise trip a container-overflow while collecting multiple positional specs.
    args.emplace_back(argv[0] ? argv[0] : "");
    std::string testSpec;
    for (int i = 1; i < argc; ++i) {
        if (!testSpec.empty()) {
            testSpec.push_back(' ');
        }
        testSpec += argv[i] ? argv[i] : "";
    }
    args.push_back(std::move(testSpec));
    return args;
}

} // namespace

int main(int argc, char* argv[]) {
#if defined(_MSC_VER)
    reportCrtFailuresToStderr();
#endif
    auto args = normalizeArgs(argc, argv);
    std::vector<char*> normalizedArgv;
    normalizedArgv.reserve(args.size());
    for (auto& arg : args) {
        normalizedArgv.push_back(arg.data());
    }

    return Catch::Session().run(static_cast<int>(normalizedArgv.size()), normalizedArgv.data());
}
