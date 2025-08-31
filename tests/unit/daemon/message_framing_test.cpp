#include <random>
#include <thread>
#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif
#include <gtest/gtest.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

namespace yams::daemon::test {

using namespace std::chrono_literals;

class MessageFramingTest : public ::testing::Test {
protected:
    void SetUp() override { framer_ = std::make_unique<MessageFramer>(); }

    std::unique_ptr<MessageFramer> framer_;
};

// Test basic frame creation and parsing
TEST_F(MessageFramingTest, BasicFraming) {
    // Create a simple message
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 12345;
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = PingRequest{};

    // Frame the message
    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    auto& framedData = framedResult.value();
    EXPECT_GT(framedData.size(), sizeof(MessageFramer::FrameHeader));

    // Parse the frame back
    auto parsedResult = framer_->parse_frame(framedData);
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    auto& parsedMsg = parsedResult.value();
    EXPECT_EQ(parsedMsg.version, msg.version);
    EXPECT_EQ(parsedMsg.requestId, msg.requestId);
    EXPECT_TRUE(std::holds_alternative<Request>(parsedMsg.payload));
}

// Test CRC32 validation
TEST_F(MessageFramingTest, CrcValidation) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 54321;
    msg.payload = StatusRequest{true};

    // Frame the message
    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult);

    auto framedData = framedResult.value();

    // Corrupt a byte in the payload (after header)
    if (framedData.size() > sizeof(MessageFramer::FrameHeader) + 10) {
        framedData[sizeof(MessageFramer::FrameHeader) + 5] ^= 0xFF;
    }

    // Parsing should fail due to CRC mismatch
    auto parsedResult = framer_->parse_frame(framedData);
    EXPECT_FALSE(parsedResult) << "Should fail with corrupted data";
    if (!parsedResult) {
        EXPECT_EQ(parsedResult.error().code, ErrorCode::InvalidData);
    }
}

// Test large message framing
TEST_F(MessageFramingTest, LargeMessage) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 99999;

    // Create a large search request
    SearchRequest req;
    req.query = std::string(10000, 'x'); // 10KB query
    req.limit = 100;
    msg.payload = req;

    // Frame the message
    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame large message";

    auto& framedData = framedResult.value();
    EXPECT_GT(framedData.size(), 10000); // Should be larger than the query

    // Parse it back
    auto parsedResult = framer_->parse_frame(framedData);
    ASSERT_TRUE(parsedResult) << "Failed to parse large frame";

    // Verify the query is intact
    auto& parsedMsg = parsedResult.value();
    if (auto* parsedReq = std::get_if<SearchRequest>(&std::get<Request>(parsedMsg.payload))) {
        EXPECT_EQ(parsedReq->query.size(), 10000);
        EXPECT_EQ(parsedReq->query, std::string(10000, 'x'));
    } else {
        FAIL() << "Failed to extract SearchRequest from parsed message";
    }
}

// Test frame header validation
TEST_F(MessageFramingTest, InvalidFrameHeader) {
    // Create invalid frame with wrong magic
    std::vector<uint8_t> badFrame(sizeof(MessageFramer::FrameHeader) + 100);
    auto* header = reinterpret_cast<MessageFramer::FrameHeader*>(badFrame.data());
    header->magic = htonl(0xDEADBEEF); // Wrong magic
    header->version = htonl(MessageFramer::FRAME_VERSION);
    header->payload_size = htonl(100);
    header->checksum = htonl(0);

    // Should fail to parse
    auto result = framer_->parse_frame(badFrame);
    EXPECT_FALSE(result) << "Should reject invalid magic";
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::InvalidData);
    }
}

// Test frame version mismatch
TEST_F(MessageFramingTest, VersionMismatch) {
    std::vector<uint8_t> badFrame(sizeof(MessageFramer::FrameHeader) + 100);
    auto* header = reinterpret_cast<MessageFramer::FrameHeader*>(badFrame.data());
    header->magic = htonl(MessageFramer::FRAME_MAGIC);
    header->version = htonl(99); // Wrong version
    header->payload_size = htonl(100);
    header->checksum = htonl(0);

    auto result = framer_->parse_frame(badFrame);
    EXPECT_FALSE(result) << "Should reject wrong version";
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::InvalidData);
    }
}

// Test empty payload
TEST_F(MessageFramingTest, EmptyPayload) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 111;
    msg.payload = PingRequest{}; // Minimal payload

    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult);

    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult);

    EXPECT_EQ(parsedResult.value().requestId, 111);
}

// Test maximum message size
TEST_F(MessageFramingTest, MaxMessageSize) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 222;

    // Create a message well under the size limit
    // Account for JSON serialization overhead (field names, quotes, brackets, etc.)
    SearchRequest req;
    req.query =
        std::string(MAX_MESSAGE_SIZE / 2, 'y'); // Well under max to account for JSON overhead
    msg.payload = req;

    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Should frame message under size limit";

    // Now try one that's too large
    req.query = std::string(MAX_MESSAGE_SIZE + 1000, 'z'); // Over max
    msg.payload = req;

    framedResult = framer_->frame_message(msg);
    EXPECT_FALSE(framedResult) << "Should reject oversized message";
    if (!framedResult) {
        // The framer returns InvalidData for oversized messages
        EXPECT_EQ(framedResult.error().code, ErrorCode::InvalidData);
    }
}

// Test concurrent framing (thread safety)
TEST_F(MessageFramingTest, ConcurrentFraming) {
    const int numThreads = 10;
    const int messagesPerThread = 100;
    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, t, &successCount, &failCount]() {
            for (int i = 0; i < messagesPerThread; ++i) {
                Message msg;
                msg.version = PROTOCOL_VERSION;
                msg.requestId = t * 1000 + i;
                msg.payload = StatusRequest{i % 2 == 0};

                auto framedResult = framer_->frame_message(msg);
                if (framedResult) {
                    // Try to parse it back
                    auto parsedResult = framer_->parse_frame(framedResult.value());
                    if (parsedResult && parsedResult.value().requestId == msg.requestId) {
                        successCount++;
                    } else {
                        failCount++;
                    }
                } else {
                    failCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(successCount, numThreads * messagesPerThread);
    EXPECT_EQ(failCount, 0);
}

// Test partial frame handling
TEST_F(MessageFramingTest, PartialFrame) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 333;
    msg.payload = ShutdownRequest{true};

    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult);

    auto& fullFrame = framedResult.value();

    // Try to parse with only partial data
    if (fullFrame.size() > 10) {
        std::vector<uint8_t> partialFrame(fullFrame.begin(),
                                          fullFrame.begin() + fullFrame.size() / 2);

        auto result = framer_->parse_frame(partialFrame);
        EXPECT_FALSE(result) << "Should fail with partial frame";
    }
}

// Test various request/response types
TEST_F(MessageFramingTest, AllMessageTypes) {
    struct TestCase {
        std::string name;
        std::variant<Request, Response> payload;
    };

    std::vector<TestCase> testCases = {
        {"PingRequest", PingRequest{}},
        {"StatusRequest", StatusRequest{true}},
        {"ShutdownRequest", ShutdownRequest{false}},
        {"SearchRequest", SearchRequest{"test query",
                                        10,
                                        false,
                                        false,
                                        0.7,
                                        {},
                                        "keyword",
                                        false,
                                        false,
                                        false,
                                        false,
                                        false,
                                        0,
                                        0,
                                        0,
                                        ""}},
        {"GenerateEmbeddingRequest", GenerateEmbeddingRequest{"sample text", "model"}},
        {"LoadModelRequest", LoadModelRequest{"test-model"}},
        {"PongResponse",
         PongResponse{std::chrono::steady_clock::now(), std::chrono::milliseconds{0}}},
        {"StatusResponse",
         []() {
             StatusResponse sr;
             sr.running = true;
             sr.ready = true;
             sr.uptimeSeconds = 5;
             sr.requestsProcessed = 2;
             sr.activeConnections = 0;
             sr.memoryUsageMb = 512.0;
             sr.cpuUsagePercent = 25.5;
             return sr;
         }()},
        {"ErrorResponse", ErrorResponse{ErrorCode::InvalidArgument, "test error"}},
        {"SuccessResponse", SuccessResponse{"operation completed"}}};

    for (const auto& tc : testCases) {
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = 444;
        msg.payload = tc.payload;

        auto framedResult = framer_->frame_message(msg);
        ASSERT_TRUE(framedResult) << "Failed to frame " << tc.name;

        auto parsedResult = framer_->parse_frame(framedResult.value());
        ASSERT_TRUE(parsedResult) << "Failed to parse " << tc.name;

        EXPECT_EQ(parsedResult.value().requestId, 444) << "RequestId mismatch for " << tc.name;
    }
}

// Test frame size calculation
TEST_F(MessageFramingTest, FrameSizeCalculation) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 555;

    // Small message
    msg.payload = PingRequest{};
    auto result1 = framer_->frame_message(msg);
    ASSERT_TRUE(result1);
    size_t smallSize = result1.value().size();

    // Large message
    SearchRequest req;
    req.query = std::string(1000, 'a');
    msg.payload = req;
    auto result2 = framer_->frame_message(msg);
    ASSERT_TRUE(result2);
    size_t largeSize = result2.value().size();

    // Large frame should be bigger
    EXPECT_GT(largeSize, smallSize);
    EXPECT_GT(largeSize - smallSize, 900); // Should be at least 900 bytes bigger
}

} // namespace yams::daemon::test