#include <gtest/gtest.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/response_of.hpp>

namespace yams::daemon::test {

class DownloadProtocolTest : public ::testing::Test {
protected:
    void SetUp() override { framer_ = std::make_unique<MessageFramer>(); }

    std::unique_ptr<MessageFramer> framer_;
};

// Test DownloadRequest through message framing (which tests serialization)
TEST_F(DownloadProtocolTest, DownloadRequestSerialization) {
    // Create a message with DownloadRequest
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 100;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadRequest req;
    req.url = "https://example.com/file.txt";
    req.outputPath = "/tmp/output.txt";
    req.tags = {"tag1", "tag2", "tag3"};
    req.metadata = {{"key1", "value1"}, {"key2", "value2"}};
    req.quiet = true;

    msg.payload = Request{req};

    // Frame the message (this will serialize it)
    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    // Parse it back (this will deserialize it)
    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    // Extract and verify the request
    auto& parsedMsg = parsedResult.value();
    ASSERT_TRUE(std::holds_alternative<Request>(parsedMsg.payload));
    auto& parsedReq = std::get<Request>(parsedMsg.payload);

    auto* downloadReq = std::get_if<DownloadRequest>(&parsedReq);
    ASSERT_NE(downloadReq, nullptr) << "Failed to extract DownloadRequest";

    EXPECT_EQ(downloadReq->url, req.url);
    EXPECT_EQ(downloadReq->outputPath, req.outputPath);
    EXPECT_EQ(downloadReq->tags, req.tags);
    EXPECT_EQ(downloadReq->metadata, req.metadata);
    EXPECT_EQ(downloadReq->quiet, req.quiet);
}

// Test DownloadResponse through message framing
TEST_F(DownloadProtocolTest, DownloadResponseSerialization) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 101;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadResponse res;
    res.hash = "sha256:abcdef1234567890";
    res.localPath = "/tmp/downloaded_file.txt";
    res.url = "https://example.com/file.txt";
    res.size = 12345;
    res.success = true;
    res.error = "";

    msg.payload = Response{res};

    // Frame and parse
    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    // Extract and verify
    auto& parsedMsg = parsedResult.value();
    ASSERT_TRUE(std::holds_alternative<Response>(parsedMsg.payload));
    auto& parsedRes = std::get<Response>(parsedMsg.payload);

    auto* downloadRes = std::get_if<DownloadResponse>(&parsedRes);
    ASSERT_NE(downloadRes, nullptr) << "Failed to extract DownloadResponse";

    EXPECT_EQ(downloadRes->hash, res.hash);
    EXPECT_EQ(downloadRes->localPath, res.localPath);
    EXPECT_EQ(downloadRes->url, res.url);
    EXPECT_EQ(downloadRes->size, res.size);
    EXPECT_EQ(downloadRes->success, res.success);
    EXPECT_EQ(downloadRes->error, res.error);
}

// Test DownloadResponse with error
TEST_F(DownloadProtocolTest, DownloadResponseWithError) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 102;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadResponse res;
    res.hash = "";
    res.localPath = "";
    res.url = "https://example.com/nonexistent.txt";
    res.size = 0;
    res.success = false;
    res.error = "404 Not Found";

    msg.payload = Response{res};

    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    auto& parsedMsg = parsedResult.value();
    ASSERT_TRUE(std::holds_alternative<Response>(parsedMsg.payload));
    auto& parsedRes = std::get<Response>(parsedMsg.payload);

    auto* downloadRes = std::get_if<DownloadResponse>(&parsedRes);
    ASSERT_NE(downloadRes, nullptr);

    EXPECT_FALSE(downloadRes->success);
    EXPECT_EQ(downloadRes->error, "404 Not Found");
    EXPECT_EQ(downloadRes->size, 0);
}

// Test empty DownloadRequest
TEST_F(DownloadProtocolTest, EmptyDownloadRequest) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 103;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadRequest req;
    // All fields at default values
    msg.payload = Request{req};

    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    auto& parsedMsg = parsedResult.value();
    ASSERT_TRUE(std::holds_alternative<Request>(parsedMsg.payload));
    auto& parsedReq = std::get<Request>(parsedMsg.payload);

    auto* downloadReq = std::get_if<DownloadRequest>(&parsedReq);
    ASSERT_NE(downloadReq, nullptr);

    EXPECT_EQ(downloadReq->url, "");
    EXPECT_EQ(downloadReq->outputPath, "");
    EXPECT_TRUE(downloadReq->tags.empty());
    EXPECT_TRUE(downloadReq->metadata.empty());
    EXPECT_FALSE(downloadReq->quiet);
}

// Test DownloadRequest with special characters
TEST_F(DownloadProtocolTest, DownloadRequestSpecialChars) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 104;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadRequest req;
    req.url = "https://example.com/file with spaces & special=chars?param=value#anchor";
    req.outputPath = "/path/with spaces/and-特殊文字/file.txt";
    req.tags = {"tag with spaces", "tag/with/slashes", "tag&special"};
    req.metadata = {{"key with spaces", "value\nwith\nnewlines"}, {"特殊", "文字"}};

    msg.payload = Request{req};

    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    auto& parsedMsg = parsedResult.value();
    ASSERT_TRUE(std::holds_alternative<Request>(parsedMsg.payload));
    auto& parsedReq = std::get<Request>(parsedMsg.payload);

    auto* downloadReq = std::get_if<DownloadRequest>(&parsedReq);
    ASSERT_NE(downloadReq, nullptr);

    EXPECT_EQ(downloadReq->url, req.url);
    EXPECT_EQ(downloadReq->outputPath, req.outputPath);
    EXPECT_EQ(downloadReq->tags, req.tags);
    EXPECT_EQ(downloadReq->metadata, req.metadata);
}

// Test full message framing with DownloadRequest
TEST_F(DownloadProtocolTest, DownloadRequestMessageFraming) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 42;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadRequest req;
    req.url = "https://example.com/test.zip";
    req.tags = {"test", "download"};
    msg.payload = Request{req};

    // Frame the message
    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    // Parse it back
    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    auto& parsedMsg = parsedResult.value();
    EXPECT_EQ(parsedMsg.version, msg.version);
    EXPECT_EQ(parsedMsg.requestId, msg.requestId);

    // Extract the request
    ASSERT_TRUE(std::holds_alternative<Request>(parsedMsg.payload));
    auto& parsedReq = std::get<Request>(parsedMsg.payload);

    auto* downloadReq = std::get_if<DownloadRequest>(&parsedReq);
    ASSERT_NE(downloadReq, nullptr) << "Failed to extract DownloadRequest";
    EXPECT_EQ(downloadReq->url, "https://example.com/test.zip");
    EXPECT_EQ(downloadReq->tags, req.tags);
}

// Test full message framing with DownloadResponse
TEST_F(DownloadProtocolTest, DownloadResponseMessageFraming) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 43;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadResponse res;
    res.hash = "sha256:deadbeef";
    res.localPath = "/tmp/file.zip";
    res.url = "https://example.com/test.zip";
    res.size = 1024;
    res.success = true;
    msg.payload = Response{res};

    // Frame the message
    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    // Parse it back
    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    auto& parsedMsg = parsedResult.value();
    EXPECT_EQ(parsedMsg.version, msg.version);
    EXPECT_EQ(parsedMsg.requestId, msg.requestId);

    // Extract the response
    ASSERT_TRUE(std::holds_alternative<Response>(parsedMsg.payload));
    auto& parsedRes = std::get<Response>(parsedMsg.payload);

    auto* downloadRes = std::get_if<DownloadResponse>(&parsedRes);
    ASSERT_NE(downloadRes, nullptr) << "Failed to extract DownloadResponse";
    EXPECT_EQ(downloadRes->hash, "sha256:deadbeef");
    EXPECT_EQ(downloadRes->localPath, "/tmp/file.zip");
    EXPECT_EQ(downloadRes->size, 1024);
    EXPECT_TRUE(downloadRes->success);
}

// Test that ResponseOf mapping is correct
TEST_F(DownloadProtocolTest, ResponseOfMapping) {
    static_assert(std::is_same_v<ResponseOfT<DownloadRequest>, DownloadResponse>,
                  "DownloadRequest should map to DownloadResponse");
    SUCCEED();
}

// Test large metadata
TEST_F(DownloadProtocolTest, LargeMetadata) {
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 105;
    msg.timestamp = std::chrono::steady_clock::now();

    DownloadRequest req;
    req.url = "https://example.com/large";

    // Add lots of metadata
    for (int i = 0; i < 100; ++i) {
        req.metadata["key" + std::to_string(i)] = "value" + std::to_string(i);
    }

    // Add lots of tags
    for (int i = 0; i < 50; ++i) {
        req.tags.push_back("tag" + std::to_string(i));
    }

    msg.payload = Request{req};

    auto framedResult = framer_->frame_message(msg);
    ASSERT_TRUE(framedResult) << "Failed to frame message: " << framedResult.error().message;

    auto parsedResult = framer_->parse_frame(framedResult.value());
    ASSERT_TRUE(parsedResult) << "Failed to parse frame: " << parsedResult.error().message;

    auto& parsedMsg = parsedResult.value();
    ASSERT_TRUE(std::holds_alternative<Request>(parsedMsg.payload));
    auto& parsedReq = std::get<Request>(parsedMsg.payload);

    auto* downloadReq = std::get_if<DownloadRequest>(&parsedReq);
    ASSERT_NE(downloadReq, nullptr);

    EXPECT_EQ(downloadReq->metadata.size(), 100);
    EXPECT_EQ(downloadReq->tags.size(), 50);
}

} // namespace yams::daemon::test