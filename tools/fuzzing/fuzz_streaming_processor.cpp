// SPDX-License-Identifier: GPL-3.0-or-later
// StreamingRequestProcessor Fuzzer - Targets chunked streaming response handling

#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/streaming_processor.h>

using namespace yams::daemon;

// Mock delegate processor that generates responses from fuzz data
class FuzzDelegateProcessor : public RequestProcessor {
public:
    explicit FuzzDelegateProcessor(const uint8_t* data, size_t size) : data_(data), size_(size) {}

    boost::asio::awaitable<Response> process(const Request& request) override {
        // Generate response based on request type and fuzz data
        if (std::holds_alternative<SearchRequest>(request)) {
            SearchResponse resp;
            resp.totalCount = (size_ > 0) ? data_[0] % 100 : 0;

            // Create synthetic search results from fuzz data
            size_t num_results = std::min(resp.totalCount, size_t(20));
            for (size_t i = 0; i < num_results && (i * 10) < size_; ++i) {
                SearchResult result;
                result.id = "fuzz_id_" + std::to_string(i);
                result.path = "fuzz_path_" + std::to_string(i);
                result.title = "fuzz_title_" + std::to_string(i);
                result.score = (size_ > i) ? static_cast<double>(data_[i]) / 255.0 : 0.5;
                result.snippet = std::string(reinterpret_cast<const char*>(data_ + (i * 10)),
                                             std::min(size_ - (i * 10), size_t(64)));
                resp.results.push_back(result);
            }

            co_return resp;
        } else if (std::holds_alternative<ListRequest>(request)) {
            ListResponse resp;
            resp.totalCount = (size_ > 0) ? data_[0] % 50 : 0;

            // Create synthetic list entries
            size_t num_items = std::min(resp.totalCount, size_t(15));
            for (size_t i = 0; i < num_items && (i * 8) < size_; ++i) {
                ListEntry entry;
                entry.hash = "list_hash_" + std::to_string(i);
                entry.name = std::string(reinterpret_cast<const char*>(data_ + (i * 8)),
                                         std::min(size_ - (i * 8), size_t(32)));
                resp.items.push_back(entry);
            }

            co_return resp;
        } else if (std::holds_alternative<GrepRequest>(request)) {
            GrepResponse resp;
            resp.totalMatches = (size_ > 0) ? data_[0] % 200 : 0;
            resp.filesSearched = (size_ > 1) ? data_[1] % 50 : 0;

            // Create synthetic grep matches
            size_t num_matches = std::min(resp.totalMatches, size_t(30));
            for (size_t i = 0; i < num_matches && (i * 12) < size_; ++i) {
                GrepMatch match;
                match.file = "file_" + std::to_string(i);
                match.lineNumber = (size_ > i) ? data_[i] : 0;
                match.line = std::string(reinterpret_cast<const char*>(data_ + (i * 12)),
                                         std::min(size_ - (i * 12), size_t(80)));
                resp.matches.push_back(match);
            }

            co_return resp;
        }

        co_return SuccessResponse{"Fuzz processed"};
    }

private:
    const uint8_t* data_;
    size_t size_;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0)
        return 0;

    try {
        boost::asio::io_context io;

        // Create delegate processor with fuzz data
        auto delegate = std::make_shared<FuzzDelegateProcessor>(data, size);

        // Configure streaming processor with fuzz-driven settings
        RequestHandler::Config config;
        config.enable_streaming = true;
        config.chunk_size = (size > 0) ? (data[0] * 1024) + 1024 : 128 * 1024;
        config.auto_detect_streaming = (size > 2) ? (data[2] & 1) : true;
        config.force_streaming = (size > 3) ? (data[3] & 1) : false;

        auto processor = std::make_shared<StreamingRequestProcessor>(delegate, config);

        // Test 1: Process various request types with streaming
        if (size > 4) {
            uint8_t req_type = data[4] % 4;
            Request test_request;

            switch (req_type) {
                case 0: {
                    SearchRequest req;
                    req.query = std::string(reinterpret_cast<const char*>(data),
                                            std::min(size, size_t(128)));
                    req.limit = (size > 5) ? data[5] : 20;
                    test_request = req;
                    break;
                }
                case 1: {
                    ListRequest req;
                    req.limit = (size > 5) ? data[5] : 20;
                    req.showMetadata = (size > 6) ? (data[6] & 1) : false;
                    test_request = req;
                    break;
                }
                case 2: {
                    GrepRequest req;
                    req.pattern = std::string(reinterpret_cast<const char*>(data),
                                              std::min(size, size_t(64)));
                    req.maxMatches = (size > 5) ? static_cast<size_t>(data[5]) : 100;
                    test_request = req;
                    break;
                }
                default: {
                    test_request = PingRequest{};
                    break;
                }
            }

            // Test streaming support check
            bool supports = processor->supports_streaming(test_request);
            (void)supports;

            // Spawn coroutine to test process_streaming
            boost::asio::co_spawn(
                io,
                [&]() -> boost::asio::awaitable<void> {
                    try {
                        // Test process_streaming
                        auto response = co_await processor->process_streaming(test_request);

                        // If nullopt, there are chunks to read
                        if (!response) {
                            size_t chunk_count = 0;
                            const size_t max_chunks = 100; // Limit to prevent infinite loops

                            while (chunk_count < max_chunks) {
                                auto chunk = co_await processor->next_chunk();
                                chunk_count++;

                                if (chunk.is_last_chunk) {
                                    break;
                                }
                            }
                        }
                    } catch (...) {
                        // Catch any coroutine exceptions
                    }
                },
                boost::asio::detached);

            // Run IO context briefly (timeout to avoid hanging)
            io.run_for(std::chrono::milliseconds(100));
        }

        // Test 2: Test chunking logic with different data sizes
        if (size > 10) {
            // Create a large response to trigger chunking
            SearchRequest large_req;
            large_req.query = "fuzz";
            large_req.limit = 1000; // Request many results

            boost::asio::co_spawn(
                io,
                [&]() -> boost::asio::awaitable<void> {
                    try {
                        auto response = co_await processor->process_streaming(large_req);
                        if (!response) {
                            // Consume chunks
                            for (size_t i = 0; i < 50; ++i) {
                                auto chunk = co_await processor->next_chunk();
                                if (chunk.is_last_chunk)
                                    break;
                            }
                        }
                    } catch (...) {
                    }
                },
                boost::asio::detached);

            io.restart();
            io.run_for(std::chrono::milliseconds(100));
        }

    } catch (...) {
        // Catch any top-level exceptions
    }

    return 0;
}
