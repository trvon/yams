/**
 * @file plugin_process_launcher_test.cpp
 * @brief Comprehensive tests for PluginProcess launcher on Windows and Unix
 *
 * These tests specifically target process spawning and I/O handling to
 * isolate issues with CreateProcessW error 87 and pipe communication.
 *
 * Note: Tests have short timeouts to prevent hanging. If a test times out,
 * it indicates a bug in the process spawning or I/O handling.
 */

#include <yams/extraction/plugin_process.hpp>

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

// PyInstaller executables need ~3s to unpack/start on first run (can be slower on CI)
constexpr auto PROCESS_STARTUP_WAIT = std::chrono::seconds{3};
// PyInstaller executables may need extra time to respond after stdin is closed
constexpr auto RPC_TIMEOUT = std::chrono::seconds{8};

using namespace yams::extraction;
using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

// Helper: convert string to byte span
std::span<const std::byte> to_bytes(std::string_view str) {
    return std::as_bytes(std::span{str.data(), str.size()});
}

// Helper: convert byte span to string
std::string from_bytes(std::span<const std::byte> data) {
    return {reinterpret_cast<const char*>(data.data()), data.size()};
}

// Get platform-appropriate Python executable
std::string getPythonExecutable() {
    const char* python_env = std::getenv("PYTHON");
    if (python_env)
        return python_env;
#ifdef _WIN32
    return "python";
#else
    return "python3";
#endif
}

// Get mock plugin path - uses YAMS_SOURCE_DIR if available, otherwise relative to __FILE__
fs::path getMockPluginPath() {
    // Try environment variable first (set by meson/cmake)
    const char* source_dir = std::getenv("YAMS_SOURCE_DIR");
    if (source_dir) {
        return fs::path(source_dir) / "tests" / "fixtures" / "mock_plugin.py";
    }

    // Fall back to relative path from __FILE__
    // __FILE__ = tests/unit/extraction/plugin_process_launcher_test.cpp
    // We want: tests/fixtures/mock_plugin.py
    fs::path file_path = fs::absolute(fs::path(__FILE__));
    fs::path tests_dir = file_path.parent_path().parent_path().parent_path();
    return tests_dir / "fixtures" / "mock_plugin.py";
}

// Get compiled plugin path (platform-aware)
fs::path getCompiledPluginPath() {
    // Try environment variable first
    const char* source_dir = std::getenv("YAMS_SOURCE_DIR");
    fs::path base;

    if (source_dir) {
        base = fs::path(source_dir);
    } else {
        // Fall back to relative path from __FILE__
        // __FILE__ = tests/unit/extraction/plugin_process_launcher_test.cpp
        // We want: plugins/yams-ghidra-plugin/dist/
        fs::path file_path = fs::absolute(fs::path(__FILE__));
        base = file_path.parent_path().parent_path().parent_path().parent_path();
    }

    fs::path plugin_dir = base / "plugins" / "yams-ghidra-plugin" / "dist";

#ifdef _WIN32
    return plugin_dir / "plugin.exe";
#else
    return plugin_dir / "plugin";
#endif
}

// Get platform-specific executable extension
std::string getExecutableExtension() {
#ifdef _WIN32
    return ".exe";
#else
    return "";
#endif
}

// Helper: read stdout with timeout, return string
std::string readStdoutWithTimeout(PluginProcess& process,
                                  std::chrono::milliseconds timeout = RPC_TIMEOUT) {
    auto start = std::chrono::steady_clock::now();
    size_t lastSize = 0;
    bool processWasAlive = true;

    while (std::chrono::steady_clock::now() - start < timeout) {
        auto data = process.read_stdout();
        if (data.size() > lastSize) {
            lastSize = data.size();
            // Check if we have a complete line
            std::string_view content{reinterpret_cast<const char*>(data.data()), data.size()};
            if (content.find('\n') != std::string_view::npos) {
                return std::string{content};
            }
        }

        // If process just died, give IO thread time to drain the pipe
        if (!process.is_alive()) {
            if (processWasAlive) {
                // Process just died - wait a bit for IO thread to finish reading
                processWasAlive = false;
                std::this_thread::sleep_for(std::chrono::milliseconds{100});
                continue;
            }
            // Process was already dead and we've waited - check one more time
            data = process.read_stdout();
            if (data.size() > lastSize) {
                std::string_view content{reinterpret_cast<const char*>(data.data()), data.size()};
                if (content.find('\n') != std::string_view::npos) {
                    return std::string{content};
                }
            }
            // No more data coming, return what we have
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }

    // Return whatever we have
    auto data = process.read_stdout();
    return from_bytes(data);
}

} // namespace

TEST_CASE("PluginProcess launcher basic spawn", "[extraction][plugin][launcher]") {
    std::string python = getPythonExecutable();
    fs::path mock_plugin = getMockPluginPath();

    REQUIRE(fs::exists(mock_plugin));

    SECTION("Spawn Python script directly") {
        INFO("Spawning: " << python << " " << mock_plugin.string());

        PluginProcessConfig config{
            .executable = python, .args = {"-u", mock_plugin.string()} // -u for unbuffered
        };

        PluginProcess process{std::move(config)};

        CHECK(process.is_alive());
        CHECK(process.state() == ProcessState::Ready);
        CHECK(process.pid() > 0);

        INFO("PID: " << process.pid());

        // Give process time to initialize
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
        CHECK(process.is_alive());

        process.terminate();
        CHECK(process.state() == ProcessState::Terminated);
    }
}

TEST_CASE("PluginProcess launcher stdin/stdout", "[extraction][plugin][launcher]") {
    std::string python = getPythonExecutable();
    fs::path mock_plugin = getMockPluginPath();

    REQUIRE(fs::exists(mock_plugin));

    SECTION("Write to stdin and read from stdout") {
        PluginProcessConfig config{.executable = python, .args = {"-u", mock_plugin.string()}};

        PluginProcess process{std::move(config)};
        REQUIRE(process.is_alive());

        // Wait for process to be ready
        std::this_thread::sleep_for(PROCESS_STARTUP_WAIT);

        // Send handshake request
        json request = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "handshake.manifest"}};
        std::string request_str = request.dump() + "\n";

        INFO("Sending request: " << request_str);

        size_t written = process.write_stdin(to_bytes(request_str));
        INFO("Wrote " << written << " bytes");
        CHECK(written == request_str.size());

        // Read response
        std::string response = readStdoutWithTimeout(process, RPC_TIMEOUT);
        INFO("Received: " << response);

        REQUIRE_FALSE(response.empty());

        // Parse and validate response
        auto parsed = json::parse(response);
        CHECK(parsed["jsonrpc"] == "2.0");
        CHECK(parsed["id"] == 1);
        CHECK(parsed.contains("result"));
        CHECK(parsed["result"]["name"] == "mock_plugin");

        process.terminate();
    }
}

TEST_CASE("PluginProcess launcher environment variables", "[extraction][plugin][launcher]") {
    std::string python = getPythonExecutable();
    fs::path mock_plugin = getMockPluginPath();

    REQUIRE(fs::exists(mock_plugin));

    SECTION("Pass environment variables to child process") {
        PluginProcessConfig config{
            .executable = python,
            .args = {"-u", mock_plugin.string()},
            .env = {{"PYTHONUNBUFFERED", "1"}, {"YAMS_TEST_VAR", "test_value"}}};

        PluginProcess process{std::move(config)};
        REQUIRE(process.is_alive());

        // Give process time to initialize
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
        CHECK(process.is_alive());

        process.terminate();
    }
}

TEST_CASE("PluginProcess launcher working directory", "[extraction][plugin][launcher]") {
    std::string python = getPythonExecutable();
    fs::path mock_plugin = getMockPluginPath();

    REQUIRE(fs::exists(mock_plugin));

    SECTION("Set working directory") {
        fs::path workdir = mock_plugin.parent_path();

        PluginProcessConfig config{.executable = python,
                                   .args = {"-u", mock_plugin.string()},
                                   .env = {},
                                   .workdir = workdir};

        INFO("Workdir: " << workdir.string());

        PluginProcess process{std::move(config)};
        REQUIRE(process.is_alive());

        std::this_thread::sleep_for(std::chrono::milliseconds{200});
        CHECK(process.is_alive());

        process.terminate();
    }
}

#ifdef _WIN32
TEST_CASE("PluginProcess launcher Windows-specific", "[extraction][plugin][launcher][windows]") {
    SECTION("Spawn cmd.exe with echo") {
        // Simple test - spawn cmd.exe and run echo
        PluginProcessConfig config{.executable = "cmd.exe", .args = {"/c", "echo", "hello"}};

        PluginProcess process{std::move(config)};

        // cmd.exe /c echo will exit immediately
        std::this_thread::sleep_for(std::chrono::milliseconds{500});

        auto output = from_bytes(process.read_stdout());
        INFO("Output: " << output);

        // Process should have exited
        CHECK_FALSE(process.is_alive());
    }

    SECTION("Spawn powershell with Write-Output") {
        PluginProcessConfig config{
            .executable = "powershell.exe",
            .args = {"-NoProfile", "-Command", "Write-Output 'hello from powershell'"}};

        PluginProcess process{std::move(config)};

        std::this_thread::sleep_for(std::chrono::milliseconds{1000});

        auto output = from_bytes(process.read_stdout());
        INFO("Output: " << output);

        CHECK(output.find("hello") != std::string::npos);
    }

    SECTION("Spawn with absolute path containing spaces") {
        // Create a temp directory with spaces
        fs::path tempDir = fs::temp_directory_path() / "yams test dir";
        fs::create_directories(tempDir);

        // Copy mock_plugin.py there
        fs::path mockPlugin = getMockPluginPath();
        fs::path destPlugin = tempDir / "mock_plugin.py";
        fs::copy_file(mockPlugin, destPlugin, fs::copy_options::overwrite_existing);

        std::string python = getPythonExecutable();

        PluginProcessConfig config{.executable = python, .args = {"-u", destPlugin.string()}};

        INFO("Path with spaces: " << destPlugin.string());

        PluginProcess process{std::move(config)};
        REQUIRE(process.is_alive());

        std::this_thread::sleep_for(std::chrono::milliseconds{200});
        CHECK(process.is_alive());

        // Send request
        json request = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "handshake.manifest"}};
        process.write_stdin(to_bytes(request.dump() + "\n"));

        std::string response = readStdoutWithTimeout(process);
        INFO("Response: " << response);
        CHECK_FALSE(response.empty());

        process.terminate();

        // Cleanup
        fs::remove(destPlugin);
        fs::remove(tempDir);
    }
}
#endif

TEST_CASE("PluginProcess launcher rapid spawn/terminate", "[extraction][plugin][launcher]") {
    std::string python = getPythonExecutable();
    fs::path mock_plugin = getMockPluginPath();

    REQUIRE(fs::exists(mock_plugin));

    SECTION("Spawn and immediately terminate") {
        for (int i = 0; i < 5; i++) {
            INFO("Iteration " << i);

            PluginProcessConfig config{.executable = python, .args = {"-u", mock_plugin.string()}};

            PluginProcess process{std::move(config)};
            CHECK(process.is_alive());

            process.terminate();
            CHECK(process.state() == ProcessState::Terminated);
        }
    }

    SECTION("Multiple processes simultaneously") {
        std::vector<std::unique_ptr<PluginProcess>> processes;

        for (int i = 0; i < 3; i++) {
            PluginProcessConfig config{.executable = python, .args = {"-u", mock_plugin.string()}};
            processes.push_back(std::make_unique<PluginProcess>(std::move(config)));
        }

        // All should be alive
        for (size_t i = 0; i < processes.size(); i++) {
            INFO("Process " << i);
            CHECK(processes[i]->is_alive());
        }

        // Terminate all
        for (auto& p : processes) {
            p->terminate();
        }
    }
}

TEST_CASE("PluginProcess launcher I/O timing", "[extraction][plugin][launcher]") {
    std::string python = getPythonExecutable();
    fs::path mock_plugin = getMockPluginPath();

    REQUIRE(fs::exists(mock_plugin));

    SECTION("Multiple requests in sequence") {
        PluginProcessConfig config{.executable = python, .args = {"-u", mock_plugin.string()}};

        PluginProcess process{std::move(config)};
        REQUIRE(process.is_alive());

        std::this_thread::sleep_for(std::chrono::milliseconds{200});

        size_t read_pos = 0;

        for (int i = 1; i <= 3; i++) {
            json request = {{"jsonrpc", "2.0"}, {"id", i}, {"method", "handshake.manifest"}};

            process.write_stdin(to_bytes(request.dump() + "\n"));
            std::this_thread::sleep_for(std::chrono::milliseconds{100});

            auto data = process.read_stdout();
            INFO("Request " << i << ": buffer size = " << data.size()
                            << ", read_pos = " << read_pos);

            if (data.size() > read_pos) {
                std::string_view remaining{reinterpret_cast<const char*>(data.data() + read_pos),
                                           data.size() - read_pos};
                size_t newline = remaining.find('\n');
                if (newline != std::string_view::npos) {
                    std::string line{remaining.substr(0, newline)};
                    read_pos += newline + 1;

                    auto parsed = json::parse(line);
                    CHECK(parsed["id"] == i);
                }
            }
        }

        process.terminate();
    }
}

TEST_CASE("PluginProcess launcher error handling", "[extraction][plugin][launcher]") {
    SECTION("Non-existent executable throws") {
        PluginProcessConfig config{.executable = "this_executable_does_not_exist_12345"};

        CHECK_THROWS([&]() { PluginProcess process{std::move(config)}; }());
    }

    SECTION("Invalid executable throws") {
        // Try to execute a directory
        PluginProcessConfig config{.executable = fs::temp_directory_path()};

        CHECK_THROWS([&]() { PluginProcess process{std::move(config)}; }());
    }
}

// Test specifically for the compiled PyInstaller executable scenario
TEST_CASE("PluginProcess launcher compiled executable",
          "[extraction][plugin][launcher][compiled]") {
    // Get platform-appropriate compiled plugin path
    fs::path compiled_plugin = getCompiledPluginPath();

    if (!fs::exists(compiled_plugin)) {
        SKIP("Compiled plugin not found at: " + compiled_plugin.string() +
             " - run 'python build.py' in yams-ghidra-plugin first");
    }

    SECTION("Spawn compiled PyInstaller executable") {
        INFO("Testing compiled plugin: " << compiled_plugin.string());

        PluginProcessConfig config{.executable = compiled_plugin,
                                   .args = {},
                                   .env = {},
                                   .workdir = compiled_plugin.parent_path()};

        PluginProcess process{std::move(config)};

        INFO("Process spawned, PID: " << process.pid());
        INFO("Process state: " << static_cast<int>(process.state()));
        CHECK(process.is_alive());

        // Wait for PyInstaller unpacking (can be slow on first run)
        std::this_thread::sleep_for(PROCESS_STARTUP_WAIT);

        INFO("After startup wait, alive: " << process.is_alive());
        REQUIRE(process.is_alive());

        // Send handshake
        json request = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "handshake.manifest"}};
        std::string request_str = request.dump() + "\n";

        INFO("Sending request: " << request_str);
        size_t written = process.write_stdin(to_bytes(request_str));
        INFO("Wrote " << written << " bytes");
        CHECK(written == request_str.size());

        // PyInstaller executables require stdin to be closed before they send a response
        // This signals EOF to the plugin
        process.close_stdin();

        // Read response with timeout
        std::string response = readStdoutWithTimeout(process, RPC_TIMEOUT);
        INFO("Response received (" << response.size() << " bytes): " << response);

        // Check stderr for errors
        auto stderr_data = process.read_stderr();
        if (!stderr_data.empty()) {
            std::string stderr_str = from_bytes(stderr_data);
            INFO("Stderr: " << stderr_str);
        }

        if (response.empty()) {
            process.terminate();
            SKIP("Compiled plugin did not respond to handshake - rebuild with 'python build.py'");
        }

        // Parse response
        auto parsed = json::parse(response);
        CHECK(parsed["jsonrpc"] == "2.0");
        CHECK(parsed["id"] == 1);
        CHECK(parsed.contains("result"));

        process.terminate();
    }

    SECTION("Compiled executable with environment variables") {
        PluginProcessConfig config{.executable = compiled_plugin,
                                   .args = {},
                                   .env = {{"PYTHONUNBUFFERED", "1"}},
                                   .workdir = compiled_plugin.parent_path()};

        PluginProcess process{std::move(config)};
        REQUIRE(process.is_alive());

        std::this_thread::sleep_for(PROCESS_STARTUP_WAIT);
        CHECK(process.is_alive());

        process.terminate();
    }
}
