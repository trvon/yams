#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::extraction {

/**
 * @brief Process state for external plugins
 *
 * Strongly typed enum representing the lifecycle state of a plugin process.
 */
enum class ProcessState : uint8_t {
    Unstarted,    ///< Process not yet spawned
    Starting,     ///< Process spawn in progress
    Ready,        ///< Process ready to accept requests
    Busy,         ///< Process handling request
    ShuttingDown, ///< Shutdown initiated
    Terminated,   ///< Process has exited
    Failed        ///< Process failed/crashed
};

/**
 * @brief Configuration for spawning external plugin processes
 *
 * Uses builder pattern for fluent configuration.
 *
 * Example:
 * @code
 * PluginProcessConfig config{
 *     .executable = "/usr/bin/python3",
 *     .args = {"plugin.py"}
 * };
 * config.with_env("PYTHONPATH", "/path/to/sdk")
 *       .with_timeout(std::chrono::seconds{30})
 *       .in_directory("/path/to/plugin");
 * @endcode
 */
struct PluginProcessConfig {
    std::filesystem::path executable; ///< Executable path (python3, ruby, node, etc.)
    std::vector<std::string> args;    ///< Command-line arguments
    std::unordered_map<std::string, std::string> env; ///< Environment variables
    std::optional<std::filesystem::path> workdir;     ///< Working directory (optional)
    std::chrono::milliseconds init_timeout{30'000};   ///< Initialization timeout
    std::chrono::milliseconds rpc_timeout{60'000};    ///< Default RPC call timeout
    bool redirect_stderr{true};                       ///< Capture stderr for logging

    /**
     * @brief Add environment variable (builder pattern)
     */
    auto& with_env(std::string key, std::string value) {
        env[std::move(key)] = std::move(value);
        return *this;
    }

    /**
     * @brief Set RPC timeout (builder pattern)
     */
    auto& with_timeout(std::chrono::milliseconds timeout) {
        rpc_timeout = timeout;
        return *this;
    }

    /**
     * @brief Set working directory (builder pattern)
     */
    auto& in_directory(std::filesystem::path dir) {
        workdir = std::move(dir);
        return *this;
    }
};

/**
 * @brief RAII wrapper for external plugin process
 *
 * Manages the lifecycle of an external plugin process with automatic cleanup.
 * Provides non-blocking I/O access to stdin/stdout/stderr pipes.
 *
 * **Features:**
 * - Cross-platform process spawning (Linux/macOS: fork/exec, Windows: CreateProcess)
 * - RAII lifecycle management (auto-terminate on destruction)
 * - Non-owning views of I/O buffers using std::span (C++20)
 * - Thread-safe I/O operations
 * - Timeout support for process operations
 *
 * **Thread Safety:**
 * All public methods are thread-safe.
 *
 * **Example:**
 * @code
 * PluginProcessConfig config{
 *     .executable = "/usr/bin/python3",
 *     .args = {"plugin.py"}
 * };
 *
 * PluginProcess process{std::move(config)};
 * if (process.is_alive()) {
 *     auto data = std::as_bytes(std::span{"request"});
 *     process.write_stdin(data);
 *     auto response = process.read_stdout();
 * }
 * @endcode
 */
class PluginProcess {
public:
    /**
     * @brief Construct and spawn plugin process
     * @param config Process configuration
     * @throws std::runtime_error if process spawn fails
     */
    explicit PluginProcess(PluginProcessConfig config);

    /**
     * @brief Destructor ensures process termination
     *
     * Attempts graceful shutdown with timeout, then forceful termination.
     */
    ~PluginProcess();

    // Non-copyable (RAII semantics)
    PluginProcess(const PluginProcess&) = delete;
    PluginProcess& operator=(const PluginProcess&) = delete;

    // Move-only (transfer ownership)
    PluginProcess(PluginProcess&&) noexcept;
    PluginProcess& operator=(PluginProcess&&) noexcept;

    /**
     * @brief Get current process state
     * @return ProcessState enum value
     */
    [[nodiscard]] ProcessState state() const noexcept;

    /**
     * @brief Check if process is running
     * @return true if process is alive and not failed
     */
    [[nodiscard]] bool is_alive() const noexcept;

    /**
     * @brief Terminate process gracefully
     *
     * Attempts graceful shutdown (SIGTERM on Unix, close handles on Windows).
     * If timeout expires, forcefully kills process (SIGKILL/TerminateProcess).
     *
     * @param timeout Timeout for graceful shutdown
     */
    void terminate(std::chrono::milliseconds timeout = std::chrono::seconds{5});

    /**
     * @brief Read available data from stdout (non-blocking)
     *
     * Returns a non-owning view of buffered stdout data. Data remains valid
     * until next call to read_stdout() or object destruction.
     *
     * @return Span of bytes from stdout buffer (C++20)
     */
    [[nodiscard]] std::span<const std::byte> read_stdout();

    /**
     * @brief Read available data from stderr (non-blocking)
     *
     * Returns a non-owning view of buffered stderr data. Data remains valid
     * until next call to read_stderr() or object destruction.
     *
     * @return Span of bytes from stderr buffer (C++20)
     */
    [[nodiscard]] std::span<const std::byte> read_stderr();

    /**
     * @brief Write data to stdin (blocking)
     *
     * @param data Span of bytes to write (C++20)
     * @return Number of bytes written, or 0 on error
     */
    size_t write_stdin(std::span<const std::byte> data);

    /**
     * @brief Get process ID
     * @return Platform-specific process ID (pid_t on Unix, DWORD on Windows)
     */
    [[nodiscard]] int64_t pid() const noexcept;

    /**
     * @brief Get process uptime
     * @return Duration since process started
     */
    [[nodiscard]] std::chrono::milliseconds uptime() const noexcept;

    /**
     * @brief Wait for process to exit
     *
     * @param timeout Maximum time to wait
     * @return true if process exited within timeout
     */
    [[nodiscard]] bool wait_for_exit(std::chrono::milliseconds timeout);

    /**
     * @brief Get process exit code (only valid if terminated)
     * @return Exit code, or std::nullopt if still running
     */
    [[nodiscard]] std::optional<int> exit_code() const noexcept;

private:
    class Impl; // Pimpl idiom for platform-specific implementation
    std::unique_ptr<Impl> impl_;
};

} // namespace yams::extraction
