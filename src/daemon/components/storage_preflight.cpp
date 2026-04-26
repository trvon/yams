#include <yams/daemon/components/storage_preflight.h>

#include <spdlog/spdlog.h>

#include <array>
#include <chrono>
#include <fstream>
#include <future>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace yams::daemon {

namespace fs = std::filesystem;

namespace {

struct ProbeOutcome {
    bool ok{false};
    std::string detail;
};

ProbeOutcome runProbe(const fs::path& dataDir) {
    ProbeOutcome out;

    std::error_code ec;
    if (!fs::exists(dataDir, ec)) {
        fs::create_directories(dataDir, ec);
        if (ec) {
            out.detail = "create_directories failed: " + ec.message();
            return out;
        }
    } else if (!fs::is_directory(dataDir, ec)) {
        out.detail = "data dir is not a directory: " + dataDir.string();
        return out;
    }

    auto statusOk = fs::status(dataDir, ec);
    if (ec) {
        out.detail = "stat failed: " + ec.message();
        return out;
    }
    (void)statusOk;

    const auto probeName = ".yams-preflight-" + std::to_string(static_cast<long long>(::getpid()));
    const fs::path probeFile = dataDir / probeName;

#ifndef _WIN32
    auto errnoMsg = [](int err) { return std::generic_category().message(err); };
    const int fd = ::open(probeFile.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
        out.detail = "open(probe) failed: " + errnoMsg(errno);
        return out;
    }
    constexpr std::array<char, 2> payload{'o', 'k'};
    if (::write(fd, payload.data(), payload.size()) != static_cast<ssize_t>(payload.size())) {
        out.detail = "write(probe) failed: " + errnoMsg(errno);
        ::close(fd);
        ::unlink(probeFile.c_str());
        return out;
    }
    if (::fsync(fd) != 0) {
        out.detail = "fsync(probe) failed: " + errnoMsg(errno);
        ::close(fd);
        ::unlink(probeFile.c_str());
        return out;
    }
    ::close(fd);
    ::unlink(probeFile.c_str());
#else
    {
        std::ofstream of(probeFile, std::ios::binary | std::ios::trunc);
        if (!of) {
            out.detail = "open(probe) failed";
            return out;
        }
        of << "ok";
        of.flush();
        if (!of) {
            out.detail = "write(probe) failed";
            return out;
        }
    }
    fs::remove(probeFile, ec);
    if (ec) {
        out.detail = "unlink(probe) failed: " + ec.message();
        return out;
    }
#endif

    out.ok = true;
    return out;
}

} // namespace

Result<StoragePreflightResult> probeStorage(const fs::path& dataDir,
                                            std::chrono::milliseconds hardDeadline,
                                            std::chrono::milliseconds slowThreshold) {
    if (dataDir.empty()) {
        return Error{ErrorCode::InvalidArgument, "data dir is empty"};
    }

    auto promise = std::make_shared<std::promise<ProbeOutcome>>();
    auto future = promise->get_future();
    auto path = dataDir;
    const auto started = std::chrono::steady_clock::now();

    std::thread([promise = std::move(promise), path = std::move(path)]() mutable {
        try {
            promise->set_value(runProbe(path));
        } catch (...) {
            try {
                promise->set_exception(std::current_exception());
            } catch (...) {
                // promise destroyed before we could set anything; nothing to do
            }
        }
    }).detach();

    if (future.wait_for(hardDeadline) != std::future_status::ready) {
        return Error{ErrorCode::IOError, "data dir unresponsive (probe exceeded " +
                                             std::to_string(hardDeadline.count()) +
                                             "ms): " + dataDir.string()};
    }

    ProbeOutcome outcome;
    try {
        outcome = future.get();
    } catch (const std::exception& e) {
        return Error{ErrorCode::IOError,
                     std::string("probe threw: ") + e.what() + " (" + dataDir.string() + ")"};
    } catch (...) {
        return Error{ErrorCode::IOError, "probe threw unknown exception: " + dataDir.string()};
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started);

    if (!outcome.ok) {
        return Error{ErrorCode::IOError,
                     "data dir not writable: " + dataDir.string() + ": " + outcome.detail};
    }

    StoragePreflightResult res;
    res.reachable = true;
    res.probeDuration = elapsed;
    res.slow = elapsed >= slowThreshold;
    if (res.slow) {
        res.detail = "data dir probe took " + std::to_string(elapsed.count()) +
                     "ms (>= " + std::to_string(slowThreshold.count()) +
                     "ms threshold): " + dataDir.string();
    }
    return res;
}

} // namespace yams::daemon
