#include <yams/compat/thread_stop_compat.h>
#include <yams/extraction/plugin_process.hpp>

#include <spdlog/spdlog.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>

// Platform-specific includes
#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#endif

namespace yams::extraction {

namespace {

// Helper: convert span to string_view
std::string_view span_to_string_view(std::span<const std::byte> data) {
    return {reinterpret_cast<const char*>(data.data()), data.size()};
}

// Helper: convert string to byte span
std::span<const std::byte> string_to_span(std::string_view str) {
    return std::as_bytes(std::span{str.data(), str.size()});
}

} // anonymous namespace

/**
 * @brief Platform-specific process implementation (Pimpl)
 */
class PluginProcess::Impl {
public:
    explicit Impl(PluginProcessConfig config);
    ~Impl();

    // Non-copyable, move-only
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) noexcept = default;
    Impl& operator=(Impl&&) noexcept = default;

    [[nodiscard]] ProcessState state() const noexcept;
    [[nodiscard]] bool is_alive() const noexcept;
    void terminate(std::chrono::milliseconds timeout);
    [[nodiscard]] std::span<const std::byte> read_stdout();
    [[nodiscard]] std::span<const std::byte> read_stderr();
    size_t write_stdin(std::span<const std::byte> data);
    [[nodiscard]] int64_t pid() const noexcept;
    [[nodiscard]] std::chrono::milliseconds uptime() const noexcept;
    [[nodiscard]] bool wait_for_exit(std::chrono::milliseconds timeout);
    [[nodiscard]] std::optional<int> exit_code() const noexcept;

private:
    void spawn_process();
    void setup_pipes();
    void start_io_threads();
    void stop_io_threads();
    void read_stdout_loop();
    void read_stderr_loop();

    PluginProcessConfig config_;
    std::atomic<ProcessState> state_{ProcessState::Unstarted};
    std::chrono::steady_clock::time_point start_time_;
    std::optional<int> exit_code_;

    // I/O buffers and synchronization
    std::vector<std::byte> stdout_buffer_;
    std::vector<std::byte> stderr_buffer_;
    mutable std::mutex stdout_mutex_;
    mutable std::mutex stderr_mutex_;
    mutable std::mutex stdin_mutex_;

    // I/O threads
    yams::compat::jthread stdout_thread_; // C++20: auto-join on destruction
    yams::compat::jthread stderr_thread_;

#ifdef _WIN32
    // Windows-specific handles
    HANDLE process_handle_{INVALID_HANDLE_VALUE};
    HANDLE stdin_write_{INVALID_HANDLE_VALUE};
    HANDLE stdout_read_{INVALID_HANDLE_VALUE};
    HANDLE stderr_read_{INVALID_HANDLE_VALUE};
    DWORD process_id_{0};
#else
    // Unix-specific handles
    pid_t process_id_{-1};
    int stdin_fd_{-1};
    int stdout_fd_{-1};
    int stderr_fd_{-1};
#endif
};

// ============================================================================
// Unix (Linux/macOS) Implementation
// ============================================================================

#ifndef _WIN32

PluginProcess::Impl::Impl(PluginProcessConfig config) : config_{std::move(config)} {
    spdlog::debug("PluginProcess: Spawning process: {}", config_.executable.string());

    // Ignore SIGPIPE globally to prevent crashes when plugin terminates unexpectedly
    // PBI-002: Critical fix for external_plugin_extractor SIGPIPE crashes (signal 13)
    signal(SIGPIPE, SIG_IGN);
    spdlog::debug("PluginProcess: SIGPIPE handling configured (ignored globally)");

    state_.store(ProcessState::Starting, std::memory_order_release);
    start_time_ = std::chrono::steady_clock::now();

    try {
        setup_pipes();
        spawn_process();
        start_io_threads();
        state_.store(ProcessState::Ready, std::memory_order_release);
    } catch (...) {
        state_.store(ProcessState::Failed, std::memory_order_release);
        throw;
    }
}

PluginProcess::Impl::~Impl() {
    spdlog::debug("PluginProcess::~Impl(): Destructor called, state={}, is_alive={}",
                  static_cast<int>(state()), is_alive());

    if (is_alive()) {
        spdlog::debug("PluginProcess::~Impl(): Process still alive, calling terminate");
        terminate(std::chrono::seconds{5});
    }
    stop_io_threads();

    // Close file descriptors
    if (stdin_fd_ >= 0)
        close(stdin_fd_);
    if (stdout_fd_ >= 0)
        close(stdout_fd_);
    if (stderr_fd_ >= 0)
        close(stderr_fd_);

    spdlog::debug("PluginProcess::~Impl(): Destructor completed");
}

void PluginProcess::Impl::setup_pipes() {
    int stdin_pipe[2], stdout_pipe[2], stderr_pipe[2];

    if (pipe(stdin_pipe) < 0 || pipe(stdout_pipe) < 0 || pipe(stderr_pipe) < 0) {
        throw std::runtime_error("Failed to create pipes: " + std::string(strerror(errno)));
    }

    // Set non-blocking on read ends
    fcntl(stdout_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(stderr_pipe[0], F_SETFL, O_NONBLOCK);

    stdin_fd_ = stdin_pipe[1];   // Parent writes to child stdin
    stdout_fd_ = stdout_pipe[0]; // Parent reads from child stdout
    stderr_fd_ = stderr_pipe[0]; // Parent reads from child stderr

    // Store child ends for fork
    config_.env["__YAMS_STDIN_FD__"] = std::to_string(stdin_pipe[0]);
    config_.env["__YAMS_STDOUT_FD__"] = std::to_string(stdout_pipe[1]);
    config_.env["__YAMS_STDERR_FD__"] = std::to_string(stderr_pipe[1]);
}

void PluginProcess::Impl::spawn_process() {
    pid_t pid = fork();

    if (pid < 0) {
        throw std::runtime_error("fork() failed: " + std::string(strerror(errno)));
    }

    if (pid == 0) {
        // Child process
        int child_stdin = std::stoi(config_.env.at("__YAMS_STDIN_FD__"));
        int child_stdout = std::stoi(config_.env.at("__YAMS_STDOUT_FD__"));
        int child_stderr = std::stoi(config_.env.at("__YAMS_STDERR_FD__"));

        // Redirect standard streams
        dup2(child_stdin, STDIN_FILENO);
        dup2(child_stdout, STDOUT_FILENO);
        if (config_.redirect_stderr) {
            dup2(child_stderr, STDERR_FILENO);
        }

        // Close unused pipe ends
        close(stdin_fd_);
        close(stdout_fd_);
        close(stderr_fd_);
        close(child_stdin);
        close(child_stdout);
        close(child_stderr);

        // Set working directory
        if (config_.workdir) {
            if (chdir(config_.workdir->c_str()) < 0) {
                _exit(127);
            }
        }

        // Set environment variables
        for (const auto& [key, value] : config_.env) {
            if (!key.starts_with("__YAMS_")) {
                setenv(key.c_str(), value.c_str(), 1);
            }
        }

        // Build argv
        std::vector<char*> argv;
        std::string exe_str = config_.executable.string();
        argv.push_back(const_cast<char*>(exe_str.c_str()));
        for (const auto& arg : config_.args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);

        // Execute
        execvp(argv[0], argv.data());

        // If exec fails
        _exit(127);
    }

    // Parent process
    process_id_ = pid;

    // Close child ends of pipes
    close(std::stoi(config_.env.at("__YAMS_STDIN_FD__")));
    close(std::stoi(config_.env.at("__YAMS_STDOUT_FD__")));
    close(std::stoi(config_.env.at("__YAMS_STDERR_FD__")));

    spdlog::info("PluginProcess: Spawned process {} (pid={})", config_.executable.string(),
                 process_id_);
}

void PluginProcess::Impl::terminate(std::chrono::milliseconds timeout) {
    spdlog::debug("PluginProcess::terminate(): Called, state={}, is_alive={}, pid={}",
                  static_cast<int>(state()), is_alive(), process_id_);

    if (!is_alive()) {
        spdlog::debug("PluginProcess::terminate(): Process not alive, returning early");
        return;
    }

    // Safety check: ensure we have a valid PID before sending signals
    if (process_id_ <= 0) {
        spdlog::warn("PluginProcess: Invalid process ID {} during termination", process_id_);
        state_.store(ProcessState::Terminated, std::memory_order_release);
        return;
    }

    spdlog::info("PluginProcess: Terminating process {} (pid={})", config_.executable.string(),
                 process_id_);
    state_.store(ProcessState::ShuttingDown, std::memory_order_release);

    // Close stdin to signal EOF to the child process first
    {
        std::lock_guard lock{stdin_mutex_};
        if (stdin_fd_ >= 0) {
            close(stdin_fd_);
            stdin_fd_ = -1;
        }
    }

    // Try graceful shutdown (SIGTERM)
    if (kill(process_id_, SIGTERM) == 0) {
        if (wait_for_exit(timeout)) {
            state_.store(ProcessState::Terminated, std::memory_order_release);
            return;
        }
    }

    // Forceful kill (SIGKILL)
    spdlog::warn("PluginProcess: Forcefully killing process {}", process_id_);
    kill(process_id_, SIGKILL);
    (void)wait_for_exit(std::chrono::seconds{1});  // Best-effort wait after SIGKILL
    state_.store(ProcessState::Terminated, std::memory_order_release);
}

bool PluginProcess::Impl::wait_for_exit(std::chrono::milliseconds timeout) {
    auto start = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() - start < timeout) {
        int status;
        pid_t result = waitpid(process_id_, &status, WNOHANG);

        if (result > 0) {
            if (WIFEXITED(status)) {
                exit_code_ = WEXITSTATUS(status);
            } else if (WIFSIGNALED(status)) {
                exit_code_ = 128 + WTERMSIG(status);
            }
            return true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }

    return false;
}

size_t PluginProcess::Impl::write_stdin(std::span<const std::byte> data) {
    std::lock_guard lock{stdin_mutex_};

    if (stdin_fd_ < 0 || !is_alive()) {
        return 0;
    }

    ssize_t written = write(stdin_fd_, data.data(), data.size());
    if (written < 0) {
        // PBI-002: Detect broken pipe (plugin terminated unexpectedly)
        if (errno == EPIPE) {
            spdlog::error("PluginProcess: Broken pipe detected (plugin terminated) - errno: EPIPE");
            state_.store(ProcessState::Terminated, std::memory_order_release);
            return 0;
        }
        spdlog::error("PluginProcess: Failed to write to stdin: {} (errno: {})", strerror(errno),
                      errno);
        return 0;
    }

    return static_cast<size_t>(written);
}

#endif // !_WIN32

// ============================================================================
// Windows Implementation
// ============================================================================

#ifdef _WIN32

PluginProcess::Impl::Impl(PluginProcessConfig config) : config_{std::move(config)} {
    spdlog::debug("PluginProcess: Spawning process: {}", config_.executable.string());
    state_.store(ProcessState::Starting, std::memory_order_release);
    start_time_ = std::chrono::steady_clock::now();

    try {
        setup_pipes();
        spawn_process();
        start_io_threads();
        state_.store(ProcessState::Ready, std::memory_order_release);
    } catch (...) {
        state_.store(ProcessState::Failed, std::memory_order_release);
        throw;
    }
}

PluginProcess::Impl::~Impl() {
    if (is_alive()) {
        terminate(std::chrono::seconds{5});
    }
    stop_io_threads();

    // Close remaining handles (stdout/stderr may already be closed by stop_io_threads)
    if (stdin_write_ != INVALID_HANDLE_VALUE)
        CloseHandle(stdin_write_);
    if (stdout_read_ != INVALID_HANDLE_VALUE)
        CloseHandle(stdout_read_);
    if (stderr_read_ != INVALID_HANDLE_VALUE)
        CloseHandle(stderr_read_);
    if (process_handle_ != INVALID_HANDLE_VALUE)
        CloseHandle(process_handle_);
}

void PluginProcess::Impl::setup_pipes() {
    SECURITY_ATTRIBUTES sa{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};

    HANDLE stdin_rd, stdin_wr, stdout_rd, stdout_wr, stderr_rd, stderr_wr;

    if (!CreatePipe(&stdin_rd, &stdin_wr, &sa, 0) || !CreatePipe(&stdout_rd, &stdout_wr, &sa, 0) ||
        !CreatePipe(&stderr_rd, &stderr_wr, &sa, 0)) {
        throw std::runtime_error("Failed to create pipes");
    }

    stdin_write_ = stdin_wr;
    stdout_read_ = stdout_rd;
    stderr_read_ = stderr_rd;

    // Store child ends for CreateProcess
    config_.env["__YAMS_STDIN_RD__"] = std::to_string(reinterpret_cast<uintptr_t>(stdin_rd));
    config_.env["__YAMS_STDOUT_WR__"] = std::to_string(reinterpret_cast<uintptr_t>(stdout_wr));
    config_.env["__YAMS_STDERR_WR__"] = std::to_string(reinterpret_cast<uintptr_t>(stderr_wr));
}

void PluginProcess::Impl::spawn_process() {
    // Build command line
    std::wstring cmdline = L"\"" + config_.executable.wstring() + L"\"";
    for (const auto& arg : config_.args) {
        cmdline += L" \"" + std::wstring(arg.begin(), arg.end()) + L"\"";
    }

    spdlog::info("PluginProcess: Spawning '{}' with {} args", config_.executable.string(),
                 config_.args.size());
    for (size_t i = 0; i < config_.args.size(); ++i) {
        spdlog::info("  arg[{}]: '{}'", i, config_.args[i]);
    }
    if (config_.workdir) {
        spdlog::info("  workdir: '{}'", config_.workdir->string());
    }

    // Build environment block
    // We need to either pass nullptr to inherit parent environment,
    // or provide a complete environment block with CREATE_UNICODE_ENVIRONMENT flag.
    // For simplicity, we'll inherit the parent environment and only set custom vars
    // by prepending them to the inherited environment.
    std::wstring envblock;
    bool hasCustomEnv = false;
    for (const auto& [key, value] : config_.env) {
        if (!key.starts_with("__YAMS_")) {
            envblock += std::wstring(key.begin(), key.end()) + L"=" +
                        std::wstring(value.begin(), value.end()) + L'\0';
            hasCustomEnv = true;
        }
    }

    // If we have custom env vars, we need to also copy parent environment
    // and add CREATE_UNICODE_ENVIRONMENT flag. Otherwise pass nullptr to inherit.
    LPVOID envPtr = nullptr;
    DWORD creationFlags = CREATE_NO_WINDOW;

    if (hasCustomEnv) {
        // Get parent environment and append to our block
        LPWCH parentEnv = GetEnvironmentStringsW();
        if (parentEnv) {
            // Find the end of parent environment (double null terminated)
            LPWCH p = parentEnv;
            while (*p || *(p + 1)) {
                p++;
            }
            p += 2; // Include final double null
            size_t parentLen = p - parentEnv;

            // Append parent env to our custom vars
            envblock.append(parentEnv, parentLen);
            FreeEnvironmentStringsW(parentEnv);
        } else {
            // No parent env, just terminate our block
            envblock += L'\0';
        }
        envPtr = envblock.data();
        creationFlags |= CREATE_UNICODE_ENVIRONMENT;
    }

    STARTUPINFOW si{sizeof(STARTUPINFOW)};
    si.dwFlags = STARTF_USESTDHANDLES;
    si.hStdInput = reinterpret_cast<HANDLE>(std::stoull(config_.env.at("__YAMS_STDIN_RD__")));
    si.hStdOutput = reinterpret_cast<HANDLE>(std::stoull(config_.env.at("__YAMS_STDOUT_WR__")));
    si.hStdError = reinterpret_cast<HANDLE>(std::stoull(config_.env.at("__YAMS_STDERR_WR__")));

    PROCESS_INFORMATION pi{};

    if (!CreateProcessW(nullptr, cmdline.data(), nullptr, nullptr, TRUE, creationFlags, envPtr,
                        config_.workdir ? config_.workdir->wstring().c_str() : nullptr, &si, &pi)) {
        DWORD error = GetLastError();
        spdlog::error("CreateProcessW failed with error {}", error);
        throw std::runtime_error("CreateProcessW failed with error " + std::to_string(error));
    }

    process_handle_ = pi.hProcess;
    process_id_ = pi.dwProcessId;
    CloseHandle(pi.hThread);

    // Close child ends
    CloseHandle(si.hStdInput);
    CloseHandle(si.hStdOutput);
    CloseHandle(si.hStdError);

    spdlog::info("PluginProcess: Spawned process {} (pid={})", config_.executable.string(),
                 process_id_);
}

void PluginProcess::Impl::terminate(std::chrono::milliseconds timeout) {
    if (!is_alive()) {
        return;
    }

    spdlog::info("PluginProcess: Terminating process {} (pid={})", config_.executable.string(),
                 process_id_);
    state_.store(ProcessState::ShuttingDown, std::memory_order_release);

    // Close stdin to signal EOF to the child process
    // This is the graceful way to tell a stdin-reading process to exit
    {
        std::lock_guard lock{stdin_mutex_};
        if (stdin_write_ != INVALID_HANDLE_VALUE) {
            // Flush before closing
            FlushFileBuffers(stdin_write_);
            CloseHandle(stdin_write_);
            stdin_write_ = INVALID_HANDLE_VALUE;
            spdlog::debug("PluginProcess: Closed stdin pipe to signal EOF");
        }
    }

    // On Windows, closing stdin pipe may not immediately signal EOF to child
    // due to buffering. Give a short grace period then check if process exited.
    auto grace_period = std::min(timeout, std::chrono::milliseconds{500});
    if (wait_for_exit(grace_period)) {
        spdlog::debug("PluginProcess: Process exited gracefully after stdin close");
        stop_io_threads(); // Stop I/O threads before marking terminated
        state_.store(ProcessState::Terminated, std::memory_order_release);
        return;
    }

    // Try to terminate the process tree using Windows Job Objects would be ideal,
    // but for now just terminate the process directly
    spdlog::debug("PluginProcess: Process did not exit after stdin close, terminating");
    if (!TerminateProcess(process_handle_, 0)) {
        spdlog::warn("PluginProcess: TerminateProcess failed with error {}", GetLastError());
    }

    // Wait for termination to complete
    wait_for_exit(std::chrono::seconds{1});

    // Stop I/O threads - this closes stdout/stderr handles to unblock ReadFile
    stop_io_threads();

    state_.store(ProcessState::Terminated, std::memory_order_release);
}

bool PluginProcess::Impl::wait_for_exit(std::chrono::milliseconds timeout) {
    DWORD result = WaitForSingleObject(process_handle_, static_cast<DWORD>(timeout.count()));

    if (result == WAIT_OBJECT_0) {
        DWORD code;
        if (GetExitCodeProcess(process_handle_, &code)) {
            exit_code_ = static_cast<int>(code);
        }
        return true;
    }

    return false;
}

size_t PluginProcess::Impl::write_stdin(std::span<const std::byte> data) {
    std::lock_guard lock{stdin_mutex_};

    if (stdin_write_ == INVALID_HANDLE_VALUE || !is_alive()) {
        return 0;
    }

    DWORD written;
    if (!WriteFile(stdin_write_, data.data(), static_cast<DWORD>(data.size()), &written, nullptr)) {
        spdlog::error("PluginProcess: Failed to write to stdin");
        return 0;
    }

    return static_cast<size_t>(written);
}

#endif // _WIN32

// ============================================================================
// Common Implementation (Platform-Independent)
// ============================================================================

void PluginProcess::Impl::start_io_threads() {
    // Stdout pump thread (C++20 jthread with stop_token)
    stdout_thread_ =
        yams::compat::jthread{[this](yams::compat::stop_token /*stop*/) { read_stdout_loop(); }};

    // Stderr pump thread
    if (config_.redirect_stderr) {
        stderr_thread_ = yams::compat::jthread{
            [this](yams::compat::stop_token /*stop*/) { read_stderr_loop(); }};
    }
}

void PluginProcess::Impl::stop_io_threads() {
    spdlog::debug("PluginProcess: Stopping I/O threads");

    // Request stop first
    stdout_thread_.request_stop();
    stderr_thread_.request_stop();

#ifdef _WIN32
    // On Windows, ReadFile on a pipe blocks indefinitely. We must close the handles
    // to unblock the reader threads before joining them.
    spdlog::debug("PluginProcess: Closing pipe handles to unblock readers");
    if (stdout_read_ != INVALID_HANDLE_VALUE) {
        // Cancel any pending I/O on this handle
        CancelIoEx(stdout_read_, nullptr);
        CloseHandle(stdout_read_);
        stdout_read_ = INVALID_HANDLE_VALUE;
    }
    if (stderr_read_ != INVALID_HANDLE_VALUE) {
        CancelIoEx(stderr_read_, nullptr);
        CloseHandle(stderr_read_);
        stderr_read_ = INVALID_HANDLE_VALUE;
    }
#endif

    // Explicitly join the threads with timeout
    // jthread will join on destruction, but we do it explicitly here for clarity
    spdlog::debug("PluginProcess: Joining stdout thread");
    if (stdout_thread_.joinable()) {
        stdout_thread_.join();
    }
    spdlog::debug("PluginProcess: Joining stderr thread");
    if (stderr_thread_.joinable()) {
        stderr_thread_.join();
    }
    spdlog::debug("PluginProcess: I/O threads stopped");
}

void PluginProcess::Impl::read_stdout_loop() {
    std::array<std::byte, 4096> buffer;

    while (is_alive()) {
#ifdef _WIN32
        // Check handle validity before blocking ReadFile call
        if (stdout_read_ == INVALID_HANDLE_VALUE) {
            break;
        }
        DWORD bytes_read;
        if (ReadFile(stdout_read_, buffer.data(), static_cast<DWORD>(buffer.size()), &bytes_read,
                     nullptr) &&
            bytes_read > 0) {
#else
        ssize_t bytes_read = read(stdout_fd_, buffer.data(), buffer.size());
        if (bytes_read > 0) {
#endif
            std::lock_guard lock{stdout_mutex_};
            stdout_buffer_.insert(stdout_buffer_.end(), buffer.begin(),
                                  buffer.begin() + bytes_read);
        } else {
            // ReadFile failed or returned 0 bytes - could be handle closed or process terminated
#ifdef _WIN32
            // If handle was closed to unblock us, exit the loop
            if (GetLastError() == ERROR_INVALID_HANDLE || stdout_read_ == INVALID_HANDLE_VALUE) {
                break;
            }
#endif
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
    }
}

void PluginProcess::Impl::read_stderr_loop() {
    std::array<std::byte, 4096> buffer;

    while (is_alive()) {
#ifdef _WIN32
        // Check handle validity before blocking ReadFile call
        if (stderr_read_ == INVALID_HANDLE_VALUE) {
            break;
        }
        DWORD bytes_read;
        if (ReadFile(stderr_read_, buffer.data(), static_cast<DWORD>(buffer.size()), &bytes_read,
                     nullptr) &&
            bytes_read > 0) {
#else
        ssize_t bytes_read = read(stderr_fd_, buffer.data(), buffer.size());
        if (bytes_read > 0) {
#endif
            std::lock_guard lock{stderr_mutex_};
            stderr_buffer_.insert(stderr_buffer_.end(), buffer.begin(),
                                  buffer.begin() + bytes_read);

            // Log stderr output
            std::string_view msg = span_to_string_view(
                std::span<const std::byte>(buffer.data(), static_cast<size_t>(bytes_read)));
            spdlog::debug("Plugin stderr: {}", msg);
        } else {
            // ReadFile failed or returned 0 bytes - could be handle closed or process terminated
#ifdef _WIN32
            // If handle was closed to unblock us, exit the loop
            if (GetLastError() == ERROR_INVALID_HANDLE || stderr_read_ == INVALID_HANDLE_VALUE) {
                break;
            }
#endif
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
    }
}

ProcessState PluginProcess::Impl::state() const noexcept {
    return state_.load(std::memory_order_acquire);
}

bool PluginProcess::Impl::is_alive() const noexcept {
    auto current_state = state();
    if (current_state != ProcessState::Starting && current_state != ProcessState::Ready &&
        current_state != ProcessState::Busy) {
        return false;
    }

#ifdef _WIN32
    // On Windows, additionally check if the process is actually still running
    if (process_handle_ != INVALID_HANDLE_VALUE) {
        DWORD exit_code;
        if (GetExitCodeProcess(process_handle_, &exit_code)) {
            // STILL_ACTIVE means process hasn't exited yet
            if (exit_code != STILL_ACTIVE) {
                // Process has exited - update internal state
                // Note: this is mutable even though we're const, because
                // we're just caching observed external state
                const_cast<std::atomic<ProcessState>&>(state_).store(ProcessState::Terminated,
                                                                     std::memory_order_release);
                const_cast<std::optional<int>&>(exit_code_) = static_cast<int>(exit_code);
                return false;
            }
        }
    }
#else
    // On Unix, check if the process is still running using kill with signal 0
    if (process_id_ > 0) {
        if (kill(process_id_, 0) == -1 && errno == ESRCH) {
            // Process doesn't exist anymore
            const_cast<std::atomic<ProcessState>&>(state_).store(ProcessState::Terminated,
                                                                 std::memory_order_release);
            return false;
        }
    }
#endif
    return true;
}

std::span<const std::byte> PluginProcess::Impl::read_stdout() {
    std::lock_guard lock{stdout_mutex_};
    return {stdout_buffer_.data(), stdout_buffer_.size()};
}

std::span<const std::byte> PluginProcess::Impl::read_stderr() {
    std::lock_guard lock{stderr_mutex_};
    return {stderr_buffer_.data(), stderr_buffer_.size()};
}

int64_t PluginProcess::Impl::pid() const noexcept {
    return static_cast<int64_t>(process_id_);
}

std::chrono::milliseconds PluginProcess::Impl::uptime() const noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                 start_time_);
}

std::optional<int> PluginProcess::Impl::exit_code() const noexcept {
    return exit_code_;
}

// ============================================================================
// PluginProcess Public Interface (forwards to Impl)
// ============================================================================

PluginProcess::PluginProcess(PluginProcessConfig config)
    : impl_{std::make_unique<Impl>(std::move(config))} {}

PluginProcess::~PluginProcess() = default;

PluginProcess::PluginProcess(PluginProcess&&) noexcept = default;
PluginProcess& PluginProcess::operator=(PluginProcess&&) noexcept = default;

ProcessState PluginProcess::state() const noexcept {
    return impl_->state();
}

bool PluginProcess::is_alive() const noexcept {
    return impl_->is_alive();
}

void PluginProcess::terminate(std::chrono::milliseconds timeout) {
    impl_->terminate(timeout);
}

std::span<const std::byte> PluginProcess::read_stdout() {
    return impl_->read_stdout();
}

std::span<const std::byte> PluginProcess::read_stderr() {
    return impl_->read_stderr();
}

size_t PluginProcess::write_stdin(std::span<const std::byte> data) {
    return impl_->write_stdin(data);
}

int64_t PluginProcess::pid() const noexcept {
    return impl_->pid();
}

std::chrono::milliseconds PluginProcess::uptime() const noexcept {
    return impl_->uptime();
}

bool PluginProcess::wait_for_exit(std::chrono::milliseconds timeout) {
    return impl_->wait_for_exit(timeout);
}

std::optional<int> PluginProcess::exit_code() const noexcept {
    return impl_->exit_code();
}

} // namespace yams::extraction
