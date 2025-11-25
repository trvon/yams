/**
 * Integration test: Verify we can spawn and communicate with Ghidra plugin
 *
 * This test doesn't require Ghidra to be installed - it only tests the
 * communication layer (process spawning, JSON-RPC over stdio).
 */

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>

#include <array>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/wait.h>
#include <yams/compat/unistd.h>

namespace fs = std::filesystem;

namespace {

struct PluginProcess {
    pid_t pid = -1;
    int stdinFd = -1;
    int stdoutFd = -1;
    int stderrFd = -1;

    ~PluginProcess() { cleanup(); }

    void cleanup() {
        if (stdinFd >= 0) {
            close(stdinFd);
            stdinFd = -1;
        }
        if (stdoutFd >= 0) {
            close(stdoutFd);
            stdoutFd = -1;
        }
        if (stderrFd >= 0) {
            close(stderrFd);
            stderrFd = -1;
        }

        if (pid > 0) {
            kill(pid, SIGTERM);
            int status;
            waitpid(pid, &status, 0);
            pid = -1;
        }
    }
};

/**
 * Spawn plugin process with stdio pipes
 */
bool spawnPlugin(PluginProcess& proc, const fs::path& pluginPath, const fs::path& sdkPath) {
    int stdinPipe[2], stdoutPipe[2], stderrPipe[2];

    if (pipe(stdinPipe) == -1 || pipe(stdoutPipe) == -1 || pipe(stderrPipe) == -1) {
        return false;
    }

    pid_t pid = fork();
    if (pid == -1) {
        close(stdinPipe[0]);
        close(stdinPipe[1]);
        close(stdoutPipe[0]);
        close(stdoutPipe[1]);
        close(stderrPipe[0]);
        close(stderrPipe[1]);
        return false;
    }

    if (pid == 0) {
        // Child process
        dup2(stdinPipe[0], STDIN_FILENO);
        dup2(stdoutPipe[1], STDOUT_FILENO);
        dup2(stderrPipe[1], STDERR_FILENO);

        close(stdinPipe[0]);
        close(stdinPipe[1]);
        close(stdoutPipe[0]);
        close(stdoutPipe[1]);
        close(stderrPipe[0]);
        close(stderrPipe[1]);

        // Set PYTHONPATH environment variable
        std::string pythonPath = sdkPath.string();
        if (const char* existing = getenv("PYTHONPATH")) {
            pythonPath += ":" + std::string(existing);
        }
        setenv("PYTHONPATH", pythonPath.c_str(), 1);

        // Execute plugin
        execlp("python3", "python3", pluginPath.c_str(), nullptr);

        // If we get here, exec failed
        std::cerr << "Failed to exec plugin: " << strerror(errno) << std::endl;
        _exit(1);
    }

    // Parent process
    close(stdinPipe[0]);
    close(stdoutPipe[1]);
    close(stderrPipe[1]);

    proc.pid = pid;
    proc.stdinFd = stdinPipe[1];
    proc.stdoutFd = stdoutPipe[0];
    proc.stderrFd = stderrPipe[0];

    return true;
}

/**
 * Send JSON-RPC request and read response
 */
bool sendRequest(PluginProcess& proc, const nlohmann::json& request, nlohmann::json& response) {
    // Write request
    std::string requestStr = request.dump() + "\n";
    ssize_t written = write(proc.stdinFd, requestStr.c_str(), requestStr.size());
    if (written != static_cast<ssize_t>(requestStr.size())) {
        return false;
    }

    // Read response line
    std::string responseLine;
    char buffer[4096];
    while (true) {
        ssize_t bytesRead = read(proc.stdoutFd, buffer, sizeof(buffer));
        if (bytesRead <= 0) {
            return false;
        }

        responseLine.append(buffer, bytesRead);

        // Check for newline
        auto newlinePos = responseLine.find('\n');
        if (newlinePos != std::string::npos) {
            responseLine = responseLine.substr(0, newlinePos);
            break;
        }
    }

    // Parse response
    try {
        response = nlohmann::json::parse(responseLine);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

} // anonymous namespace

TEST_CASE("Ghidra plugin - Spawn and handshake", "[integration][ghidra][plugin]") {
    // Find plugin and SDK paths
    auto repoRoot = fs::path(__FILE__).parent_path().parent_path().parent_path();
    auto pluginPath = repoRoot / "plugins" / "yams-ghidra-plugin" / "plugin.py";
    auto sdkPath = repoRoot / "external" / "yams_sdk";

    REQUIRE(fs::exists(pluginPath));
    REQUIRE(fs::exists(sdkPath));

    PluginProcess proc;
    REQUIRE(spawnPlugin(proc, pluginPath, sdkPath));

    SECTION("Handshake - manifest") {
        nlohmann::json request = {{"jsonrpc", "2.0"},
                                  {"id", 1},
                                  {"method", "handshake.manifest"},
                                  {"params", nlohmann::json::object()}};

        nlohmann::json response;
        REQUIRE(sendRequest(proc, request, response));

        REQUIRE(response.contains("result"));
        REQUIRE(response["result"]["name"] == "yams_ghidra");
        REQUIRE(response["result"]["version"] == "0.0.1");
        REQUIRE(response["result"]["interfaces"].is_array());
        REQUIRE(response["result"]["interfaces"][0] == "ghidra_analysis_v1");
    }

    SECTION("Health check before init") {
        nlohmann::json request = {{"jsonrpc", "2.0"},
                                  {"id", 2},
                                  {"method", "plugin.health"},
                                  {"params", nlohmann::json::object()}};

        nlohmann::json response;
        REQUIRE(sendRequest(proc, request, response));

        REQUIRE(response.contains("result"));
        REQUIRE(response["result"]["status"] == "ok");
        REQUIRE(response["result"]["started"] == false);
    }

    SECTION("Init without Ghidra fails gracefully") {
        nlohmann::json request = {{"jsonrpc", "2.0"},
                                  {"id", 3},
                                  {"method", "plugin.init"},
                                  {"params", {{"config", nlohmann::json::object()}}}};

        nlohmann::json response;
        REQUIRE(sendRequest(proc, request, response));

        // Should return error since pyghidra not installed
        REQUIRE(response.contains("error"));
        REQUIRE(response["error"]["code"] == -32603);
        REQUIRE(response["error"]["message"].get<std::string>().find("pyghidra") !=
                std::string::npos);
    }
}

TEST_CASE("Ghidra plugin - Process lifecycle", "[integration][ghidra][plugin]") {
    auto repoRoot = fs::path(__FILE__).parent_path().parent_path().parent_path();
    auto pluginPath = repoRoot / "plugins" / "yams-ghidra-plugin" / "plugin.py";
    auto sdkPath = repoRoot / "external" / "yams_sdk";

    SECTION("Multiple requests on same process") {
        PluginProcess proc;
        REQUIRE(spawnPlugin(proc, pluginPath, sdkPath));

        // Send multiple requests
        for (int i = 1; i <= 3; ++i) {
            nlohmann::json request = {{"jsonrpc", "2.0"},
                                      {"id", i},
                                      {"method", "plugin.health"},
                                      {"params", nlohmann::json::object()}};

            nlohmann::json response;
            REQUIRE(sendRequest(proc, request, response));
            REQUIRE(response["id"] == i);
            REQUIRE(response.contains("result"));
        }
    }

    SECTION("Process cleans up on destruction") {
        {
            PluginProcess proc;
            REQUIRE(spawnPlugin(proc, pluginPath, sdkPath));
            REQUIRE(proc.pid > 0);
            // proc goes out of scope, should cleanup
        }
        // If we get here without hanging, cleanup worked
        REQUIRE(true);
    }
}

TEST_CASE("Ghidra plugin - Error handling", "[integration][ghidra][plugin]") {
    auto repoRoot = fs::path(__FILE__).parent_path().parent_path().parent_path();
    auto pluginPath = repoRoot / "plugins" / "yams-ghidra-plugin" / "plugin.py";
    auto sdkPath = repoRoot / "external" / "yams_sdk";

    PluginProcess proc;
    REQUIRE(spawnPlugin(proc, pluginPath, sdkPath));

    SECTION("Invalid JSON-RPC request") {
        nlohmann::json request = {{"jsonrpc", "2.0"},
                                  {"id", 1},
                                  {"method", "nonexistent.method"},
                                  {"params", nlohmann::json::object()}};

        nlohmann::json response;
        REQUIRE(sendRequest(proc, request, response));

        // Should return method not found error
        REQUIRE(response.contains("error"));
        REQUIRE(response["error"]["code"] == -32601); // Method not found
    }

    SECTION("Missing required params") {
        nlohmann::json request = {
            {"jsonrpc", "2.0"}, {"id", 1}, {"method", "ghidra.analyze"} // Missing params
        };

        nlohmann::json response;
        // Plugin might handle this differently, just verify it doesn't crash
        bool sent = sendRequest(proc, request, response);
        if (sent) {
            // Should have error or result
            REQUIRE((response.contains("error") || response.contains("result")));
        }
    }
}
