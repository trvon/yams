#include "../common/benchmark_tracker.h"
#include "benchmark_base.h"

#include <chrono>
#include <filesystem>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

namespace yams::benchmark {

namespace {

using yams::daemon::MessageFramer;
using yams::test::BenchmarkTracker;

std::string makeSnippet(size_t length, char seed) {
    std::string out(length, seed);
    for (size_t i = 0; i < length; ++i) {
        out[i] = static_cast<char>(seed + static_cast<int>(i % 23));
    }
    return out;
}

daemon::SearchResult makeResult(size_t index, size_t snippetBytes) {
    daemon::SearchResult result;
    result.id = "doc-" + std::to_string(index);
    result.path = "/bench/doc-" + std::to_string(index) + ".txt";
    result.title = "Benchmark Doc " + std::to_string(index);
    result.snippet = makeSnippet(snippetBytes, static_cast<char>('a' + (index % 13)));
    result.score = 1.0 / static_cast<double>(index + 1);
    result.metadata = {{"generator", "ipc_stream"}, {"bucket", std::to_string(index % 5)}};
    return result;
}

daemon::Message makeMessage(uint64_t requestId, const daemon::Response& response) {
    daemon::Message msg;
    msg.version = daemon::PROTOCOL_VERSION;
    msg.requestId = requestId;
    msg.timestamp = std::chrono::steady_clock::now();
    msg.expectsStreamingResponse = true;
    msg.payload = response;
    return msg;
}

} // namespace

class StreamingFramerBenchmark : public BenchmarkBase {
public:
    StreamingFramerBenchmark(std::string name, size_t resultsPerChunk, size_t chunkCount,
                             size_t snippetBytes, const Config& config = Config())
        : BenchmarkBase(std::move(name), config), resultsPerChunk_(resultsPerChunk),
          chunkCount_(chunkCount), snippetBytes_(snippetBytes) {
        buildPayloads();
    }

protected:
    size_t runIteration() override {
        const uint64_t requestId = requestCounter_++;
        std::size_t operations = 0;

        // Header frame
        frameBuffer_.clear();
        frameBuffer_.reserve(reserveBytes_);
        auto headerMessage = makeMessage(requestId, headerResponse_);
        auto headerResult = framer_.frame_message_header_into(headerMessage, frameBuffer_);
        if (!headerResult) {
            throw std::runtime_error("Failed to frame streaming header: " +
                                     headerResult.error().message);
        }
        bytesAccumulated_ += frameBuffer_.size();
        operations++;

        // Chunk frames
        for (size_t i = 0; i < chunkResponses_.size(); ++i) {
            frameBuffer_.clear();
            auto chunkMessage = makeMessage(requestId, chunkResponses_[i]);
            bool last = (i + 1 == chunkResponses_.size());
            auto chunkResult = framer_.frame_message_chunk_into(chunkMessage, frameBuffer_, last);
            if (!chunkResult) {
                throw std::runtime_error("Failed to frame streaming chunk: " +
                                         chunkResult.error().message);
            }
            bytesAccumulated_ += frameBuffer_.size();
            operations++;
        }

        iterations_++;
        return operations;
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        if (iterations_ == 0)
            return;
        metrics["bytes_per_iteration"] =
            static_cast<double>(bytesAccumulated_) / static_cast<double>(iterations_);
        metrics["chunks_per_iteration"] = static_cast<double>(chunkCount_);
        metrics["results_per_chunk"] = static_cast<double>(resultsPerChunk_);
    }

private:
    void buildPayloads() {
        const size_t totalResults = resultsPerChunk_ * chunkCount_;

        daemon::SearchResponse headerPayload;
        headerPayload.totalCount = totalResults;
        headerPayload.elapsed = std::chrono::milliseconds(12);
        headerResponse_ =
            daemon::Response{std::in_place_type<daemon::SearchResponse>, headerPayload};

        chunkResponses_.clear();
        chunkResponses_.reserve(chunkCount_);

        size_t resultIndex = 0;
        for (size_t chunk = 0; chunk < chunkCount_; ++chunk) {
            daemon::SearchResponse chunkPayload;
            chunkPayload.totalCount = totalResults;
            chunkPayload.elapsed = std::chrono::milliseconds(20 + static_cast<int>(chunk));
            chunkPayload.results.reserve(resultsPerChunk_);

            for (size_t i = 0; i < resultsPerChunk_; ++i) {
                chunkPayload.results.push_back(makeResult(resultIndex++, snippetBytes_));
            }

            daemon::Response chunkResp{std::in_place_type<daemon::SearchResponse>, chunkPayload};
            chunkResponses_.push_back(std::move(chunkResp));
        }

        reserveBytes_ = MessageFramer::HEADER_SIZE + resultsPerChunk_ * (snippetBytes_ + 256);
        frameBuffer_.reserve(reserveBytes_);
    }

    MessageFramer framer_;
    std::vector<uint8_t> frameBuffer_;
    daemon::Response headerResponse_;
    std::vector<daemon::Response> chunkResponses_;
    size_t resultsPerChunk_;
    size_t chunkCount_;
    size_t snippetBytes_;
    size_t reserveBytes_ = MessageFramer::HEADER_SIZE * 2;
    size_t iterations_ = 0;
    uint64_t requestCounter_ = 1;
    size_t bytesAccumulated_ = 0;
};

class UnaryFramerBenchmark : public BenchmarkBase {
public:
    explicit UnaryFramerBenchmark(std::string name, size_t payloadBytes,
                                  const Config& config = Config())
        : BenchmarkBase(std::move(name), config), payloadBytes_(payloadBytes) {
        buildPayload();
    }

protected:
    size_t runIteration() override {
        frameBuffer_.clear();
        frameBuffer_.reserve(reserveBytes_);

        daemon::Message msg = makeMessage(requestId_++, response_);
        auto result = framer_.frame_message_into(msg, frameBuffer_);
        if (!result) {
            throw std::runtime_error("Failed to frame unary message: " + result.error().message);
        }

        bytesAccumulated_ += frameBuffer_.size();
        iterations_++;
        return 1;
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        if (iterations_ == 0)
            return;
        metrics["bytes_per_frame"] =
            static_cast<double>(bytesAccumulated_) / static_cast<double>(iterations_);
    }

private:
    void buildPayload() {
        daemon::SuccessResponse success;
        success.message = std::string(payloadBytes_, 's');
        response_ = daemon::Response{std::in_place_type<daemon::SuccessResponse>, success};
        reserveBytes_ = MessageFramer::HEADER_SIZE + payloadBytes_ + 64;
        frameBuffer_.reserve(reserveBytes_);
    }

    MessageFramer framer_;
    std::vector<uint8_t> frameBuffer_;
    daemon::Response response_;
    size_t payloadBytes_;
    size_t reserveBytes_ = MessageFramer::HEADER_SIZE * 2;
    uint64_t requestId_ = 1000;
    size_t iterations_ = 0;
    size_t bytesAccumulated_ = 0;
};

} // namespace yams::benchmark

using yams::benchmark::BenchmarkBase;
using yams::benchmark::StreamingFramerBenchmark;
using yams::benchmark::UnaryFramerBenchmark;
using yams::test::BenchmarkTracker;

int main(int argc, char** argv) {
    BenchmarkBase::Config config;
    config.verbose = true;
    config.benchmark_iterations = 8;
    config.track_memory = false; // /proc/self/status not available on all platforms

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--quiet") {
            config.verbose = false;
        } else if (arg == "--iterations" && i + 1 < argc) {
            config.benchmark_iterations = std::stoi(argv[++i]);
        } else if (arg == "--output" && i + 1 < argc) {
            config.output_file = argv[++i];
        }
    }

    std::cout << "YAMS IPC Streaming Benchmarks\n";
    std::cout << "====================================\n\n";

    std::filesystem::path outDir = "bench_results";
    std::error_code ec;
    std::filesystem::create_directories(outDir, ec);
    if (ec) {
        std::cerr << "WARNING: unable to create bench_results directory: " << ec.message()
                  << std::endl;
    }
    if (config.output_file.empty()) {
        config.output_file = (outDir / "ipc_stream_benchmarks.json").string();
    }

    BenchmarkTracker tracker(outDir / "ipc_stream_benchmarks.json");

    std::vector<std::unique_ptr<BenchmarkBase>> benchmarks;
    benchmarks.push_back(std::make_unique<StreamingFramerBenchmark>("StreamingFramer_32x10_256B",
                                                                    32, 10, 256, config));
    benchmarks.push_back(std::make_unique<StreamingFramerBenchmark>("StreamingFramer_64x6_512B", 64,
                                                                    6, 512, config));
    benchmarks.push_back(
        std::make_unique<UnaryFramerBenchmark>("UnaryFramer_Success_8KB", 8 * 1024, config));

    for (auto& bench : benchmarks) {
        auto result = bench->run();
        BenchmarkTracker::BenchmarkResult trackerResult;
        trackerResult.name = result.name;
        trackerResult.value = result.duration_ms;
        trackerResult.unit = "ms";
        trackerResult.timestamp = std::chrono::system_clock::now();
        trackerResult.metrics = result.custom_metrics;
        tracker.recordResult(trackerResult);
    }

    tracker.generateReport(outDir / "ipc_stream_benchmark_report.json");
    tracker.generateMarkdownReport(outDir / "ipc_stream_benchmark_report.md");

    std::cout << "\n====================================\n";
    std::cout << "Benchmark complete. Reports generated.\n";

    return 0;
}
