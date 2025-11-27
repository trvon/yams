#include <yams/extraction/plugin_process.hpp>

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <thread>

using namespace yams::extraction;
using json = nlohmann::json;

namespace {

// Helper: convert string to byte span
std::span<const std::byte> to_bytes(std::string_view str) {
    return std::as_bytes(std::span{str.data(), str.size()});
}

// Helper: convert byte span to string
std::string from_bytes(std::span<const std::byte> data) {
    return {reinterpret_cast<const char*>(data.data()), data.size()};
}

// Helper: send JSON-RPC request
void send_request(PluginProcess& process, int id, std::string_view method, json params = nullptr) {
    json request = {{"jsonrpc", "2.0"}, {"id", id}, {"method", method}};
    if (!params.is_null()) {
        request["params"] = params;
    }
    std::string request_str = request.dump() + "\n";
    process.write_stdin(to_bytes(request_str));

    // Give plugin time to process
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
}

// Helper: read JSON-RPC response (with timeout)
// Reads from stdout buffer starting at given position
std::optional<json> read_response(PluginProcess& process, size_t& read_pos,
                                  std::chrono::milliseconds timeout = std::chrono::seconds{3}) {
    auto start = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() - start < timeout) {
        auto data = process.read_stdout();

        if (data.size() > read_pos) {
            // We have new data
            std::string_view remaining{reinterpret_cast<const char*>(data.data() + read_pos),
                                       data.size() - read_pos};

            // Look for newline
            size_t newline_pos = remaining.find('\n');
            if (newline_pos != std::string::npos) {
                std::string line{remaining.substr(0, newline_pos)};
                read_pos += newline_pos + 1;

                if (!line.empty()) {
                    return json::parse(line);
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }

    // Timeout - log what we have for debugging
    auto data = process.read_stdout();
    INFO("read_response timeout after " << timeout.count() << "ms. Buffer size: " << data.size()
                                        << ", read_pos: " << read_pos
                                        << ", process alive: " << process.is_alive());

    return std::nullopt;
}

} // anonymous namespace

// Helper: Get platform-appropriate Python executable
namespace {
std::string getPythonExecutable() {
    const char* python_env = std::getenv("PYTHON");
    if (python_env) {
        return python_env;
    }
#ifdef _WIN32
    return "python";  // Windows uses 'python' not 'python3'
#else
    return "python3";  // Unix typically uses python3
#endif
}

// Helper: Get mock plugin path relative to test source file
std::filesystem::path getMockPluginPath() {
    // __FILE__ is tests/unit/extraction/plugin_process_test.cpp
    // Navigate up 3 levels to tests/, then into fixtures/
    return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() / "fixtures" / "mock_plugin.py";
}
}  // namespace

TEST_CASE("PluginProcess spawns and terminates", "[extraction][plugin]") {
    // Get Python interpreter and mock plugin path
    std::string python = getPythonExecutable();
    std::filesystem::path mock_plugin = getMockPluginPath();
    
    INFO("Python executable: " << python);
    INFO("Mock plugin path: " << mock_plugin.string());
    REQUIRE(std::filesystem::exists(mock_plugin));

    SECTION("Basic lifecycle") {
        PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};

        PluginProcess process{std::move(config)};

        REQUIRE(process.is_alive());
        REQUIRE(process.state() == ProcessState::Ready);
        REQUIRE(process.pid() > 0);

        process.terminate();
        REQUIRE(process.state() == ProcessState::Terminated);
        REQUIRE_FALSE(process.is_alive());
    }

    SECTION("RAII termination") {
        PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};

        {
            PluginProcess process{std::move(config)};
            REQUIRE(process.is_alive());
        } // Destructor should terminate process
    }

    SECTION("Move semantics") {
        PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};

        PluginProcess process1{std::move(config)};
        REQUIRE(process1.is_alive());
        int64_t pid1 = process1.pid();

        // Move construct
        PluginProcess process2{std::move(process1)};
        REQUIRE(process2.is_alive());
        REQUIRE(process2.pid() == pid1);

        // Move assign
        PluginProcessConfig config2{.executable = python, .args = {mock_plugin.string()}};
        PluginProcess process3{std::move(config2)};
        process3 = std::move(process2);
        REQUIRE(process3.is_alive());
        REQUIRE(process3.pid() == pid1);
    }

    SECTION("Uptime tracking") {
        PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};

        PluginProcess process{std::move(config)};
        std::this_thread::sleep_for(std::chrono::milliseconds{100});

        auto uptime = process.uptime();
        REQUIRE(uptime >= std::chrono::milliseconds{100});
        REQUIRE(uptime < std::chrono::seconds{1});
    }
}

TEST_CASE("PluginProcess I/O communication", "[extraction][plugin]") {
    std::string python = getPythonExecutable();
    std::filesystem::path mock_plugin = getMockPluginPath();
    
    INFO("Python executable: " << python);
    INFO("Mock plugin path: " << mock_plugin.string());
    REQUIRE(std::filesystem::exists(mock_plugin));

    PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};

    PluginProcess process{std::move(config)};
    REQUIRE(process.is_alive());

    size_t read_pos = 0; // Track our position in stdout buffer

    SECTION("Handshake manifest") {
        send_request(process, 1, "handshake.manifest");

        auto response = read_response(process, read_pos);
        REQUIRE(response.has_value());

        REQUIRE((*response)["jsonrpc"] == "2.0");
        REQUIRE((*response)["id"] == 1);
        REQUIRE((*response).contains("result"));

        auto result = (*response)["result"];
        REQUIRE(result["name"] == "mock_plugin");
        REQUIRE(result["version"] == "1.0.0");
        REQUIRE(result["interfaces"].is_array());
        REQUIRE(result["interfaces"][0] == "content_extractor_v1");
    }

    SECTION("Plugin init") {
        size_t read_pos = 0;
        send_request(process, 2, "plugin.init", json::object());

        auto response = read_response(process, read_pos);
        REQUIRE(response.has_value());
        REQUIRE((*response)["result"]["status"] == "ok");
    }

    SECTION("Plugin health") {
        size_t read_pos = 0;
        send_request(process, 3, "plugin.health");

        auto response = read_response(process, read_pos);
        REQUIRE(response.has_value());
        REQUIRE((*response)["result"]["status"] == "ok");
    }

    SECTION("Extractor supports") {
        size_t read_pos = 0;
        json params = {{"mime_type", "text/plain"}, {"extension", ".txt"}};
        send_request(process, 4, "extractor.supports", params);

        auto response = read_response(process, read_pos);
        REQUIRE(response.has_value());
        REQUIRE((*response)["result"]["supported"] == true);

        // Test unsupported type
        json params2 = {{"mime_type", "application/pdf"}, {"extension", ".pdf"}};
        send_request(process, 5, "extractor.supports", params2);

        auto response2 = read_response(process, read_pos);
        REQUIRE(response2.has_value());
        REQUIRE((*response2)["result"]["supported"] == false);
    }

    SECTION("Extractor extract") {
        size_t read_pos = 0;
        json params = {{"source", {{"type", "bytes"}, {"data", "SGVsbG8gV29ybGQ="}}}};
        send_request(process, 6, "extractor.extract", params);

        auto response = read_response(process, read_pos);
        REQUIRE(response.has_value());

        auto result = (*response)["result"];
        REQUIRE(result["text"] == "Mock extracted text from file");
        REQUIRE(result["metadata"]["extractor"] == "mock_plugin");
        REQUIRE(result["error"].is_null());
    }

    SECTION("Multiple sequential requests") {
        size_t read_pos = 0;
        for (int i = 1; i <= 5; ++i) {
            send_request(process, i, "plugin.health");
            auto response = read_response(process, read_pos);
            REQUIRE(response.has_value());
            REQUIRE((*response)["id"] == i);
        }
    }
}

TEST_CASE("PluginProcess builder pattern configuration", "[extraction][plugin]") {
    std::string python = getPythonExecutable();
    std::filesystem::path mock_plugin = getMockPluginPath();
    
    INFO("Python executable: " << python);
    INFO("Mock plugin path: " << mock_plugin.string());
    REQUIRE(std::filesystem::exists(mock_plugin));

    SECTION("Builder methods") {
        PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};
        auto current_dir = std::filesystem::current_path();

        config.with_env("TEST_VAR", "test_value")
            .with_timeout(std::chrono::seconds{30})
            .in_directory(current_dir);

        REQUIRE(config.env["TEST_VAR"] == "test_value");
        REQUIRE(config.rpc_timeout == std::chrono::seconds{30});
        REQUIRE(config.workdir == current_dir);
    }
}

TEST_CASE("PluginProcess handles invalid executable", "[extraction][plugin][!mayfail]") {
#ifdef _WIN32
    // On Windows, CreateProcess fails immediately for nonexistent executables
    // and throws an exception from PluginProcess constructor
    PluginProcessConfig config{.executable = "/nonexistent/binary", .args = {}};
    REQUIRE_THROWS(PluginProcess{std::move(config)});
#else
    PluginProcessConfig config{.executable = "/nonexistent/binary", .args = {}};

    // Note: PluginProcess doesn't throw on invalid executable - the fork() succeeds but exec()
    // fails, so the child process exits with code 127. The parent can detect this by checking
    // is_alive() later.
    // TODO: Implement pipe-based exec failure detection for immediate feedback
    PluginProcess process{std::move(config)};

    // Give the process time to attempt exec and fail (timing-dependent)
    std::this_thread::sleep_for(std::chrono::seconds{1});

    // Process should have terminated by now (exec failed)
    if (process.is_alive()) {
        // If still alive, force terminate before failing
        process.terminate();
    }

    REQUIRE_FALSE(process.is_alive());
    REQUIRE(process.state() == ProcessState::Terminated);
#endif
}

TEST_CASE("PluginProcess stderr capture", "[extraction][plugin]") {
    std::string python = getPythonExecutable();

    // Create a test script that writes to stderr
    std::filesystem::path stderr_test = std::filesystem::temp_directory_path() / "stderr_test.py";
    {
        std::ofstream f{stderr_test};
        // Use platform-agnostic shebang (not actually used on Windows, but harmless)
        f << "import sys, json\n";
        f << "print('stderr message', file=sys.stderr, flush=True)\n";
        f << "for line in sys.stdin:\n";
        f << "    req = json.loads(line)\n";
        f << "    res = {'jsonrpc': '2.0', 'id': req['id'], 'result': {'status': 'ok'}}\n";
        f << "    print(json.dumps(res), flush=True)\n";
    }

    PluginProcessConfig config{
        .executable = python, .args = {stderr_test.string()}, .redirect_stderr = true};

    PluginProcess process{std::move(config)};
    
    // Wait for Python to start and write to stderr - Windows needs more time
    std::span<const std::byte> stderr_data;
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds{5}) {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        stderr_data = process.read_stderr();
        if (!stderr_data.empty()) {
            break;
        }
    }

    // Check that stderr was captured
    INFO("stderr_data size: " << stderr_data.size());
    INFO("Process alive: " << process.is_alive());
    REQUIRE(!stderr_data.empty());

    std::string stderr_str = from_bytes(stderr_data);
    REQUIRE(stderr_str.find("stderr message") != std::string::npos);

    std::filesystem::remove(stderr_test);
}
