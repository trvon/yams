// Custom Catch2 main for smoke tests that exits without running static destructors.
#include <catch2/catch_session.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>

#include <yams/config/config_helpers.h>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

namespace {

// Session state (the current session, watch settings) lives under XDG_STATE_HOME. Without a
// private one these cases created and closed sessions in the developer's own state directory:
// they left `mcp-grep-hot-*` session files behind and cleared the developer's current session.
// A caller that already set XDG_STATE_HOME keeps it. Mirrors tests/support/catch2_main.cpp.
std::filesystem::path isolateStateHome() {
    if (yams::config::getenv_nonempty("XDG_STATE_HOME")) {
        return {};
    }
    std::error_code ec;
    const auto base = std::filesystem::temp_directory_path(ec);
    if (ec) {
        return {};
    }
#if defined(_WIN32)
    const auto pid = static_cast<unsigned long long>(::_getpid());
#else
    const auto pid = static_cast<unsigned long long>(::getpid());
#endif
    auto dir = base / ("yams-smoke-state-" + std::to_string(pid));
    std::filesystem::create_directories(dir, ec);
    if (ec || !yams::config::set_environment("XDG_STATE_HOME", dir.string().c_str())) {
        return {};
    }
    return dir;
}

} // namespace

int main(int argc, char** argv) {
    const auto stateHome = isolateStateHome();
    int rc = Catch::Session().run(argc, argv);
    if (!stateHome.empty()) {
        std::error_code ec;
        std::filesystem::remove_all(stateHome, ec);
    }
#if defined(_WIN32)
    _exit(rc == 0 ? 0 : 1);
#else
    _Exit(rc == 0 ? 0 : 1);
#endif
}
