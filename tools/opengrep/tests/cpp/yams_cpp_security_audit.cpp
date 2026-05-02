#include <cstdio>
#include <cstdlib>
#include <exception>

struct CURL;
extern void curl_easy_setopt(CURL*, int, long);
extern void curl_easy_setopt(CURL*, int, bool);
constexpr int CURLOPT_SSL_VERIFYPEER = 64;
constexpr int CURLOPT_SSL_VERIFYHOST = 81;

void insecureCurl(CURL* curl) {
    // ruleid: yams.cpp.curl-tls-verification-disabled
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    // ruleid: yams.cpp.curl-tls-verification-disabled
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
}

void secureCurl(CURL* curl) {
    // ok: yams.cpp.curl-tls-verification-disabled
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    // ok: yams.cpp.curl-tls-verification-disabled
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    bool insecure = false;
    // ok: yams.cpp.curl-tls-verification-disabled
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, insecure ? 0L : 1L);
    // ok: yams.cpp.curl-tls-verification-disabled
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, insecure ? 0L : 2L);
}

void shellExecution(const char* cmd) {
    // ruleid: yams.cpp.shell-command-execution
    std::system(cmd);
    // ruleid: yams.cpp.shell-command-execution
    popen(cmd, "r");
}

void noShellExecution() {
    const char* fixed = "yams";
    // ok: yams.cpp.shell-command-execution
    (void)fixed;
    // ok: yams.cpp.shell-command-execution
    popen("system_profiler SPDisplaysDataType", "r");
}

void insecureTempName() {
    char templ[] = "/tmp/yamsXXXXXX";
    // ruleid: yams.cpp.insecure-temp-name
    mktemp(templ);
    // ruleid: yams.cpp.insecure-temp-name
    std::tmpnam(nullptr);
}

void safeTempName() {
    char templ[] = "/tmp/yamsXXXXXX";
    // ok: yams.cpp.insecure-temp-name
    mkstemp(templ);
}

void swallowedException() {
    // ruleid: yams.cpp.empty-catch-block
    try {
        throw 1;
    } catch (...) {
    }

    // ruleid: yams.cpp.empty-catch-block
    try {
        throw std::exception{};
    } catch (const std::exception& e) {
    }
}

void documentedException() {
    try {
        throw 1;
    }
    // ok: yams.cpp.empty-catch-block
    catch (...) {
        // Best effort cleanup; nothing actionable here.
    }
}

namespace spdlog {
void debug(const char*, ...);
}

void loggerGuard() {
    // ok: yams.cpp.empty-catch-block
    try {
        spdlog::debug("cleanup");
    } catch (...) {
    }
}

#include <filesystem>
namespace fs = std::filesystem;

void recursiveDelete(std::filesystem::path path, std::error_code& ec) {
    // ruleid: yams.cpp.recursive-delete-remove-all
    std::filesystem::remove_all(path);
    // ruleid: yams.cpp.recursive-delete-remove-all
    fs::remove_all(path, ec);
}

void removeAllScoped(std::filesystem::path path, std::filesystem::path root, std::error_code& ec) {
    if (path.string().find(root.string()) == 0) {
        // ok: yams.cpp.recursive-delete-remove-all
        fs::remove_all(path, ec);
    }
}

void nonRecursiveDelete(std::filesystem::path path) {
    // ok: yams.cpp.recursive-delete-remove-all
    std::filesystem::remove(path);
}
