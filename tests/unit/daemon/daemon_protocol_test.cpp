// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 Trevon Helm
// Consolidated protocol tests (6 → 1): message framing, serialization, type mappings, roundtrip
// Covers: MessageFramer, ProtoSerializer, response_of.hpp, getMessageType, getRequestName

#include <atomic>
#include <filesystem>
#include <random>
#include <thread>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <yams/common/utf8_utils.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/response_of.hpp>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

using namespace yams;
using namespace yams::daemon;

namespace {
// Helper to create messages with Request/Response payloads
Message makeMessageWith(Request r, uint64_t id = 42) {
    Message m;
    m.requestId = id;
    m.payload = std::move(r);
    return m;
}

Message makeMessageWith(Response r, uint64_t id = 43) {
    Message m;
    m.requestId = id;
    m.payload = std::move(r);
    return m;
}

std::string invalidUtf8(std::string_view prefix, std::string_view suffix = {}) {
    std::string value(prefix);
    value.push_back(static_cast<char>(0xFF));
    value.append(suffix);
    return value;
}

void checkSanitizedField(std::string_view fieldName, const std::string& actual,
                         const std::string& original) {
    INFO(fieldName);
    CHECK(actual == yams::common::sanitizeUtf8(original));
}
} // namespace

// =============================================================================
// Message Framing Tests
// =============================================================================

TEST_CASE("MessageFramer: Basic frame creation and parsing", "[daemon][protocol][framing]") {
    auto framer = std::make_unique<MessageFramer>();

    SECTION("Round-trip with PingRequest") {
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = 12345;
        msg.timestamp = std::chrono::steady_clock::now();
        msg.payload = PingRequest{};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);
        REQUIRE(framedResult.value().size() > sizeof(MessageFramer::FrameHeader));

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);
        REQUIRE(parsedResult.value().version == msg.version);
        REQUIRE(parsedResult.value().requestId == msg.requestId);
        REQUIRE(std::holds_alternative<Request>(parsedResult.value().payload));
    }

    SECTION("Empty payload handling") {
        Message msg{PROTOCOL_VERSION, 111, std::chrono::steady_clock::now(), PingRequest{}};
        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);
        REQUIRE(parsedResult.value().requestId == 111);
    }
}

TEST_CASE("MessageFramer: CRC32 validation", "[daemon][protocol][framing][validation]") {
    auto framer = std::make_unique<MessageFramer>();

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 54321;
    msg.payload = StatusRequest{true};

    auto framedResult = framer->frame_message(msg);
    REQUIRE(framedResult);

    auto framedData = framedResult.value();

    // Corrupt a byte in the payload (after header)
    if (framedData.size() > sizeof(MessageFramer::FrameHeader) + 10) {
        framedData[sizeof(MessageFramer::FrameHeader) + 5] ^= 0xFF;
    }

    // Parsing should fail due to CRC mismatch
    auto parsedResult = framer->parse_frame(framedData);
    REQUIRE_FALSE(parsedResult);
    REQUIRE(parsedResult.error().code == ErrorCode::InvalidData);
}

TEST_CASE("MessageFramer: Large message handling", "[daemon][protocol][framing][size]") {
    auto framer = std::make_unique<MessageFramer>();

    SECTION("Large query string (10KB)") {
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = 99999;

        SearchRequest req;
        req.query = std::string(10000, 'x');
        req.limit = 100;
        msg.payload = req;

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);
        REQUIRE(framedResult.value().size() > 10000);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* parsedReq =
            std::get_if<SearchRequest>(&std::get<Request>(parsedResult.value().payload));
        REQUIRE(parsedReq != nullptr);
        REQUIRE(parsedReq->query.size() == 10000);
        REQUIRE(parsedReq->query == std::string(10000, 'x'));
    }

    SECTION("Message size limits") {
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = 222;

        SearchRequest req;
        req.query = std::string(MAX_MESSAGE_SIZE / 2, 'y'); // Under limit
        msg.payload = req;

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        // Oversized message should fail
        req.query = std::string(MAX_MESSAGE_SIZE + 1000, 'z');
        msg.payload = req;

        framedResult = framer->frame_message(msg);
        REQUIRE_FALSE(framedResult);
        REQUIRE(framedResult.error().code == ErrorCode::InvalidData);
    }
}

TEST_CASE("MessageFramer: SearchRequest sanitizes invalid UTF-8 for protobuf",
          "[daemon][protocol][framing][utf8]") {
    auto framer = std::make_unique<MessageFramer>();

    SearchRequest req;
    req.query = std::string("task=performance owner=opencode");
    req.query.push_back(static_cast<char>(0xFF));
    req.query.append(" tail");
    req.searchType = "keyword";
    req.hashQuery = std::string("\xFEhash", 5);
    req.pathPattern = std::string("\xFF*.md", 5);

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 444;
    msg.payload = req;

    auto framedResult = framer->frame_message(msg);
    REQUIRE(framedResult);

    auto parsedResult = framer->parse_frame(framedResult.value());
    REQUIRE(parsedResult);

    auto* parsedReq = std::get_if<SearchRequest>(&std::get<Request>(parsedResult.value().payload));
    REQUIRE(parsedReq != nullptr);
    REQUIRE(parsedReq->query == yams::common::sanitizeUtf8(req.query));
    REQUIRE(parsedReq->hashQuery == yams::common::sanitizeUtf8(req.hashQuery));
    REQUIRE(parsedReq->pathPattern == yams::common::sanitizeUtf8(req.pathPattern));
    REQUIRE(parsedReq->searchType == "keyword");
}

TEST_CASE("MessageFramer: Header validation", "[daemon][protocol][framing][validation]") {
    auto framer = std::make_unique<MessageFramer>();

    SECTION("Invalid magic number") {
        std::vector<uint8_t> badFrame(sizeof(MessageFramer::FrameHeader) + 100);
        auto* header = reinterpret_cast<MessageFramer::FrameHeader*>(badFrame.data());
        header->magic = htonl(0xDEADBEEF); // Wrong magic
        header->version = htonl(MessageFramer::FRAME_VERSION);
        header->payload_size = htonl(100);
        header->checksum = htonl(0);

        auto result = framer->parse_frame(badFrame);
        REQUIRE_FALSE(result);
        REQUIRE(result.error().code == ErrorCode::InvalidData);
    }

    SECTION("Version mismatch") {
        std::vector<uint8_t> badFrame(sizeof(MessageFramer::FrameHeader) + 100);
        auto* header = reinterpret_cast<MessageFramer::FrameHeader*>(badFrame.data());
        header->magic = htonl(MessageFramer::FRAME_MAGIC);
        header->version = htonl(99); // Wrong version
        header->payload_size = htonl(100);
        header->checksum = htonl(0);

        auto result = framer->parse_frame(badFrame);
        REQUIRE_FALSE(result);
        REQUIRE(result.error().code == ErrorCode::InvalidData);
    }

    SECTION("Partial frame data") {
        Message msg{PROTOCOL_VERSION, 333, std::chrono::steady_clock::now(), ShutdownRequest{true}};
        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto& fullFrame = framedResult.value();
        if (fullFrame.size() > 10) {
            std::vector<uint8_t> partialFrame(fullFrame.begin(),
                                              fullFrame.begin() + fullFrame.size() / 2);
            auto result = framer->parse_frame(partialFrame);
            REQUIRE_FALSE(result);
        }
    }
}

TEST_CASE("MessageFramer: Concurrency and thread safety",
          "[daemon][protocol][framing][concurrency]") {
    auto framer = std::make_unique<MessageFramer>();
    const int numThreads = 10;
    const int messagesPerThread = 100;
    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&framer, t, &successCount, &failCount]() {
            for (int i = 0; i < messagesPerThread; ++i) {
                Message msg;
                msg.version = PROTOCOL_VERSION;
                msg.requestId = t * 1000 + i;
                msg.payload = StatusRequest{i % 2 == 0};

                auto framedResult = framer->frame_message(msg);
                if (framedResult) {
                    auto parsedResult = framer->parse_frame(framedResult.value());
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

    REQUIRE(successCount == numThreads * messagesPerThread);
    REQUIRE(failCount == 0);
}

TEST_CASE("MessageFramer: All message types", "[daemon][protocol][framing][types]") {
    auto framer = std::make_unique<MessageFramer>();

    struct TestCase {
        std::string name;
        std::variant<Request, Response> payload;
    };

    auto testCase = GENERATE(table<std::string, std::variant<Request, Response>>({
        {"PingRequest", std::variant<Request, Response>{PingRequest{}}},
        {"StatusRequest", std::variant<Request, Response>{StatusRequest{true}}},
        {"ShutdownRequest", std::variant<Request, Response>{ShutdownRequest{false}}},
        {"SearchRequest", std::variant<Request, Response>{SearchRequest{
                              "test", 10, false, false, 0.7, {}, "keyword"}}},
        {"GenerateEmbeddingRequest",
         std::variant<Request, Response>{GenerateEmbeddingRequest{"text", "model"}}},
        {"LoadModelRequest", std::variant<Request, Response>{LoadModelRequest{"test-model"}}},
        {"PongResponse", std::variant<Request, Response>{PongResponse{
                             std::chrono::steady_clock::now(), std::chrono::milliseconds{0}}}},
    }));

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 444;
    msg.payload = std::get<1>(testCase);

    auto framedResult = framer->frame_message(msg);
    REQUIRE(framedResult);

    auto parsedResult = framer->parse_frame(framedResult.value());
    REQUIRE(parsedResult);
    REQUIRE(parsedResult.value().requestId == 444);
}

// =============================================================================
// Protocol Serialization Tests (DownloadRequest/Response)
// =============================================================================

TEST_CASE("DownloadProtocol: Request serialization", "[daemon][protocol][download]") {
    auto framer = std::make_unique<MessageFramer>();

    SECTION("Full DownloadRequest roundtrip") {
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = 100;
        msg.timestamp = std::chrono::steady_clock::now();

        DownloadRequest req;
        req.url = "https://example.com/file.txt";
        req.outputPath = "/tmp/output.txt";
        req.checksum = "sha256:abcdef0123456789";
        req.tags = {"tag1", "tag2", "tag3"};
        req.metadata = {{"key1", "value1"}, {"key2", "value2"}};
        req.quiet = true;
        msg.payload = Request{req};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto& parsedMsg = parsedResult.value();
        REQUIRE(std::holds_alternative<Request>(parsedMsg.payload));
        auto* downloadReq = std::get_if<DownloadRequest>(&std::get<Request>(parsedMsg.payload));
        REQUIRE(downloadReq != nullptr);
        REQUIRE(downloadReq->url == req.url);
        REQUIRE(downloadReq->outputPath == req.outputPath);
        REQUIRE(downloadReq->checksum == req.checksum);
        REQUIRE(downloadReq->tags == req.tags);
        REQUIRE(downloadReq->metadata == req.metadata);
        REQUIRE(downloadReq->quiet == req.quiet);
    }

    SECTION("Special characters and Unicode") {
        Message msg{PROTOCOL_VERSION, 104, std::chrono::steady_clock::now(), Request{}};
        DownloadRequest req;
        req.url = "https://example.com/file with spaces & special=chars?param=value#anchor";
        req.outputPath = "/path/with spaces/and-特殊文字/file.txt";
        req.checksum = "sha512:ABCDEF123456";
        req.tags = {"tag with spaces", "tag/with/slashes", "tag&special"};
        req.metadata = {{"key with spaces", "value\nwith\nnewlines"}, {"特殊", "文字"}};
        msg.payload = Request{req};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* downloadReq =
            std::get_if<DownloadRequest>(&std::get<Request>(parsedResult.value().payload));
        REQUIRE(downloadReq != nullptr);
        REQUIRE(downloadReq->url == req.url);
        REQUIRE(downloadReq->outputPath == req.outputPath);
        REQUIRE(downloadReq->checksum == req.checksum);
        REQUIRE(downloadReq->tags == req.tags);
        REQUIRE(downloadReq->metadata == req.metadata);
    }

    SECTION("Large metadata and tags") {
        Message msg{PROTOCOL_VERSION, 105, std::chrono::steady_clock::now(), Request{}};
        DownloadRequest req;
        req.url = "https://example.com/large";

        for (int i = 0; i < 100; ++i) {
            req.metadata["key" + std::to_string(i)] = "value" + std::to_string(i);
        }
        for (int i = 0; i < 50; ++i) {
            req.tags.push_back("tag" + std::to_string(i));
        }
        msg.payload = Request{req};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* downloadReq =
            std::get_if<DownloadRequest>(&std::get<Request>(parsedResult.value().payload));
        REQUIRE(downloadReq != nullptr);
        REQUIRE(downloadReq->metadata.size() == 100);
        REQUIRE(downloadReq->tags.size() == 50);
    }

    SECTION("DownloadStatusRequest roundtrip") {
        Message msg{PROTOCOL_VERSION, 106, std::chrono::steady_clock::now(), Request{}};
        DownloadStatusRequest req;
        req.jobId = "download-17";
        msg.payload = Request{req};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* statusReq =
            std::get_if<DownloadStatusRequest>(&std::get<Request>(parsedResult.value().payload));
        REQUIRE(statusReq != nullptr);
        REQUIRE(statusReq->jobId == req.jobId);
    }

    SECTION("CancelDownloadJobRequest roundtrip") {
        Message msg{PROTOCOL_VERSION, 106, std::chrono::steady_clock::now(), Request{}};
        CancelDownloadJobRequest req;
        req.jobId = "download-18";
        msg.payload = Request{req};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* cancelReq =
            std::get_if<CancelDownloadJobRequest>(&std::get<Request>(parsedResult.value().payload));
        REQUIRE(cancelReq != nullptr);
        REQUIRE(cancelReq->jobId == req.jobId);
    }

    SECTION("ListDownloadJobsRequest roundtrip") {
        Message msg{PROTOCOL_VERSION, 107, std::chrono::steady_clock::now(), Request{}};
        ListDownloadJobsRequest req;
        req.limit = 7;
        msg.payload = Request{req};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* listReq =
            std::get_if<ListDownloadJobsRequest>(&std::get<Request>(parsedResult.value().payload));
        REQUIRE(listReq != nullptr);
        REQUIRE(listReq->limit == req.limit);
    }
}

TEST_CASE("DownloadProtocol: Response serialization", "[daemon][protocol][download]") {
    auto framer = std::make_unique<MessageFramer>();

    SECTION("Success response") {
        Message msg{PROTOCOL_VERSION, 101, std::chrono::steady_clock::now(), Response{}};
        DownloadResponse res;
        res.hash = "sha256:abcdef1234567890";
        res.localPath = "/tmp/downloaded_file.txt";
        res.url = "https://example.com/file.txt";
        res.jobId = "download-42";
        res.state = "completed";
        res.createdAtMs = 1111;
        res.updatedAtMs = 2222;
        res.size = 12345;
        res.success = true;
        res.error = "";
        msg.payload = Response{res};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* downloadRes =
            std::get_if<DownloadResponse>(&std::get<Response>(parsedResult.value().payload));
        REQUIRE(downloadRes != nullptr);
        REQUIRE(downloadRes->hash == res.hash);
        REQUIRE(downloadRes->localPath == res.localPath);
        REQUIRE(downloadRes->url == res.url);
        REQUIRE(downloadRes->jobId == res.jobId);
        REQUIRE(downloadRes->state == res.state);
        REQUIRE(downloadRes->createdAtMs == res.createdAtMs);
        REQUIRE(downloadRes->updatedAtMs == res.updatedAtMs);
        REQUIRE(downloadRes->size == res.size);
        REQUIRE(downloadRes->success == true);
    }

    SECTION("Error response") {
        Message msg{PROTOCOL_VERSION, 102, std::chrono::steady_clock::now(), Response{}};
        DownloadResponse res;
        res.hash = "";
        res.localPath = "";
        res.url = "https://example.com/nonexistent.txt";
        res.jobId = "download-43";
        res.state = "failed";
        res.createdAtMs = 3333;
        res.updatedAtMs = 4444;
        res.size = 0;
        res.success = false;
        res.error = "404 Not Found";
        msg.payload = Response{res};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* downloadRes =
            std::get_if<DownloadResponse>(&std::get<Response>(parsedResult.value().payload));
        REQUIRE(downloadRes != nullptr);
        REQUIRE_FALSE(downloadRes->success);
        REQUIRE(downloadRes->error == "404 Not Found");
        REQUIRE(downloadRes->size == 0);
    }

    SECTION("ListDownloadJobsResponse roundtrip") {
        Message msg{PROTOCOL_VERSION, 103, std::chrono::steady_clock::now(), Response{}};
        ListDownloadJobsResponse res;
        DownloadResponse first;
        first.url = "https://example.com/a";
        first.jobId = "download-1";
        first.state = "completed";
        first.createdAtMs = 100;
        first.updatedAtMs = 200;
        first.success = true;
        DownloadResponse second;
        second.url = "https://example.com/b";
        second.jobId = "download-2";
        second.state = "failed";
        second.createdAtMs = 300;
        second.updatedAtMs = 400;
        second.success = false;
        second.error = "boom";
        res.jobs = {first, second};
        msg.payload = Response{res};

        auto framedResult = framer->frame_message(msg);
        REQUIRE(framedResult);

        auto parsedResult = framer->parse_frame(framedResult.value());
        REQUIRE(parsedResult);

        auto* listRes = std::get_if<ListDownloadJobsResponse>(
            &std::get<Response>(parsedResult.value().payload));
        REQUIRE(listRes != nullptr);
        REQUIRE(listRes->jobs.size() == 2);
        REQUIRE(listRes->jobs.at(0).jobId == "download-1");
        REQUIRE(listRes->jobs.at(1).error == "boom");
    }
}

// =============================================================================
// ProtoSerializer Roundtrip Tests
// =============================================================================

TEST_CASE("ProtoSerializer: Request roundtrip", "[daemon][protocol][serialization]") {
    SECTION("CatRequest") {
        CatRequest cr;
        cr.hash = "";
        cr.name = "inline.txt";

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{cr}, 1));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);
        REQUIRE(std::holds_alternative<Request>(dec.value().payload));

        auto* got = std::get_if<CatRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        REQUIRE(got->name == "inline.txt");
    }

    SECTION("ListSessionsRequest") {
        auto enc =
            ProtoSerializer::encode_payload(makeMessageWith(Request{ListSessionsRequest{}}, 2));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto req = std::get<Request>(dec.value().payload);
        REQUIRE(std::holds_alternative<ListSessionsRequest>(req));
    }

    SECTION("SearchRequest carries session") {
        SearchRequest sr;
        sr.query = "hello";
        sr.useSession = true;
        sr.sessionName = "feature-auth";
        sr.instanceId = "inst-abc-123";

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{sr}, 10));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);
        REQUIRE(std::holds_alternative<Request>(dec.value().payload));

        auto* got = std::get_if<SearchRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        REQUIRE(got->useSession);
        REQUIRE(got->sessionName == "feature-auth");
        REQUIRE(got->instanceId == "inst-abc-123");
    }

    SECTION("GrepRequest carries session") {
        GrepRequest gr;
        gr.pattern = "TODO";
        gr.useSession = true;
        gr.sessionName = "feature-auth";
        gr.instanceId = "inst-grep-456";

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{gr}, 11));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);
        REQUIRE(std::holds_alternative<Request>(dec.value().payload));

        auto* got = std::get_if<GrepRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        REQUIRE(got->useSession);
        REQUIRE(got->sessionName == "feature-auth");
        REQUIRE(got->instanceId == "inst-grep-456");
    }

    SECTION("AddDocumentRequest carries session") {
        AddDocumentRequest ar;
        ar.path = "/tmp/a.txt";
        ar.sessionId = "feature-auth";
        ar.instanceId = "inst-add-789";

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{ar}, 12));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);
        REQUIRE(std::holds_alternative<Request>(dec.value().payload));

        auto* got = std::get_if<AddDocumentRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        REQUIRE(got->sessionId == "feature-auth");
        REQUIRE(got->instanceId == "inst-add-789");
    }

    SECTION("ListRequest carries session") {
        ListRequest lr;
        lr.limit = 5;
        lr.sessionId = "feature-auth";
        lr.instanceId = "inst-list-012";

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{lr}, 13));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);
        REQUIRE(std::holds_alternative<Request>(dec.value().payload));

        auto* got = std::get_if<ListRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        REQUIRE(got->sessionId == "feature-auth");
        REQUIRE(got->instanceId == "inst-list-012");
    }

    SECTION("BatchEmbeddingRequest sanitizes invalid UTF-8") {
        BatchEmbeddingRequest req;
        req.texts = {"hello", std::string("\xFF\xFE", 2) + "broken"};
        req.modelName = "all-MiniLM-L6-v2";
        req.normalize = true;
        req.batchSize = 8;

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 14));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);
        REQUIRE(std::holds_alternative<Request>(dec.value().payload));

        auto* got = std::get_if<BatchEmbeddingRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        REQUIRE(got->texts.size() == req.texts.size());
        CHECK(got->texts[0] == yams::common::sanitizeUtf8(req.texts[0]));
        CHECK(got->texts[1] == yams::common::sanitizeUtf8(req.texts[1]));
    }

    SECTION("GetRequest sanitizes invalid UTF-8 text fields") {
        GetRequest req;
        req.hash = invalidUtf8("sha256:");
        req.name = invalidUtf8("notes", ".md");
        req.fileType = invalidUtf8("text");
        req.mimeType = invalidUtf8("text/plain");
        req.extension = invalidUtf8(".md");
        req.createdAfter = invalidUtf8("2025-01-01");
        req.modifiedBefore = invalidUtf8("2025-12-31");
        req.indexedAfter = invalidUtf8("yesterday");
        req.outputPath = invalidUtf8("/tmp/out", ".txt");

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 15));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<GetRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        checkSanitizedField("GetRequest.hash", got->hash, req.hash);
        checkSanitizedField("GetRequest.name", got->name, req.name);
        checkSanitizedField("GetRequest.fileType", got->fileType, req.fileType);
        checkSanitizedField("GetRequest.mimeType", got->mimeType, req.mimeType);
        checkSanitizedField("GetRequest.extension", got->extension, req.extension);
        checkSanitizedField("GetRequest.createdAfter", got->createdAfter, req.createdAfter);
        checkSanitizedField("GetRequest.modifiedBefore", got->modifiedBefore, req.modifiedBefore);
        checkSanitizedField("GetRequest.indexedAfter", got->indexedAfter, req.indexedAfter);
        checkSanitizedField("GetRequest.outputPath", got->outputPath, req.outputPath);
    }

    SECTION("GetRequest preserves content selection flags") {
        GetRequest req;
        req.hash = "sha256:abc123";
        req.metadataOnly = false;
        req.includeContent = false;
        req.maxBytes = 4096;

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 16));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<GetRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        CHECK(got->hash == req.hash);
        CHECK(got->metadataOnly == req.metadataOnly);
        CHECK(got->includeContent == req.includeContent);
        CHECK(got->maxBytes == req.maxBytes);
    }

    SECTION("UpdateDocumentResponse preserves detailed update fields") {
        UpdateDocumentResponse resp;
        resp.hash = "sha256:update123";
        resp.contentUpdated = false;
        resp.metadataUpdated = true;
        resp.tagsUpdated = true;
        resp.updatesApplied = 2;
        resp.tagsAdded = 1;
        resp.tagsRemoved = 1;

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Response{resp}, 16));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<UpdateDocumentResponse>(&std::get<Response>(dec.value().payload));
        REQUIRE(got != nullptr);
        CHECK(got->hash == resp.hash);
        CHECK(got->contentUpdated == resp.contentUpdated);
        CHECK(got->metadataUpdated == resp.metadataUpdated);
        CHECK(got->tagsUpdated == resp.tagsUpdated);
        CHECK(got->updatesApplied == resp.updatesApplied);
        CHECK(got->tagsAdded == resp.tagsAdded);
        CHECK(got->tagsRemoved == resp.tagsRemoved);
    }

    SECTION("ListRequest sanitizes invalid UTF-8 filter and session fields") {
        ListRequest req;
        req.format = invalidUtf8("table");
        req.sortBy = invalidUtf8("date");
        req.fileType = invalidUtf8("text");
        req.mimeType = invalidUtf8("text/plain");
        req.extensions = invalidUtf8("md,txt");
        req.createdAfter = invalidUtf8("2025-01-01");
        req.changeWindow = invalidUtf8("24h");
        req.tags = {invalidUtf8("task=perf"), "owner=opencode"};
        req.filterTags = invalidUtf8("phase=checkpoint");
        req.namePattern = invalidUtf8("*.md");
        req.metadataFilters = {{invalidUtf8("task"), invalidUtf8("performance")},
                               {"owner", invalidUtf8("opencode")}};
        req.sessionId = invalidUtf8("feature-auth");
        req.instanceId = invalidUtf8("inst-list");

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 16));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<ListRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        checkSanitizedField("ListRequest.format", got->format, req.format);
        checkSanitizedField("ListRequest.sortBy", got->sortBy, req.sortBy);
        checkSanitizedField("ListRequest.fileType", got->fileType, req.fileType);
        checkSanitizedField("ListRequest.mimeType", got->mimeType, req.mimeType);
        checkSanitizedField("ListRequest.extensions", got->extensions, req.extensions);
        checkSanitizedField("ListRequest.createdAfter", got->createdAfter, req.createdAfter);
        checkSanitizedField("ListRequest.changeWindow", got->changeWindow, req.changeWindow);
        REQUIRE(got->tags.size() == req.tags.size());
        checkSanitizedField("ListRequest.tags[0]", got->tags[0], req.tags[0]);
        checkSanitizedField("ListRequest.filterTags", got->filterTags, req.filterTags);
        checkSanitizedField("ListRequest.namePattern", got->namePattern, req.namePattern);
        checkSanitizedField(
            "ListRequest.metadataFilters.task",
            got->metadataFilters.at(yams::common::sanitizeUtf8(req.metadataFilters.begin()->first)),
            req.metadataFilters.begin()->second);
        checkSanitizedField("ListRequest.sessionId", got->sessionId, req.sessionId);
        checkSanitizedField("ListRequest.instanceId", got->instanceId, req.instanceId);
    }

    SECTION("AddDocumentRequest sanitizes invalid UTF-8 metadata and routing fields") {
        AddDocumentRequest req;
        req.path = invalidUtf8("/tmp/doc", ".md");
        req.name = invalidUtf8("doc", ".md");
        req.tags = {invalidUtf8("tag-a"), "tag-b"};
        req.metadata = {{invalidUtf8("task"), invalidUtf8("performance")}};
        req.includePatterns = {invalidUtf8("*.md")};
        req.excludePatterns = {invalidUtf8("*.bin")};
        req.collection = invalidUtf8("notes");
        req.snapshotId = invalidUtf8("snap-123");
        req.snapshotLabel = invalidUtf8("checkpoint");
        req.sessionId = invalidUtf8("session-a");
        req.instanceId = invalidUtf8("instance-a");
        req.mimeType = invalidUtf8("text/plain");

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 17));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<AddDocumentRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        checkSanitizedField("AddDocumentRequest.path", got->path, req.path);
        checkSanitizedField("AddDocumentRequest.name", got->name, req.name);
        checkSanitizedField("AddDocumentRequest.tags[0]", got->tags.at(0), req.tags.at(0));
        checkSanitizedField("AddDocumentRequest.collection", got->collection, req.collection);
        checkSanitizedField("AddDocumentRequest.snapshotId", got->snapshotId, req.snapshotId);
        checkSanitizedField("AddDocumentRequest.snapshotLabel", got->snapshotLabel,
                            req.snapshotLabel);
        checkSanitizedField("AddDocumentRequest.sessionId", got->sessionId, req.sessionId);
        checkSanitizedField("AddDocumentRequest.instanceId", got->instanceId, req.instanceId);
        checkSanitizedField("AddDocumentRequest.mimeType", got->mimeType, req.mimeType);
    }

    SECTION("GrepRequest sanitizes invalid UTF-8 filter and session fields") {
        GrepRequest req;
        req.pattern = invalidUtf8("task=performance");
        req.path = invalidUtf8("/tmp");
        req.paths = {invalidUtf8("/repo/src")};
        req.includePatterns = {invalidUtf8("*.cpp")};
        req.filterTags = {invalidUtf8("owner=opencode")};
        req.colorMode = invalidUtf8("auto");
        req.useSession = true;
        req.sessionName = invalidUtf8("session-grep");
        req.instanceId = invalidUtf8("instance-grep");

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 18));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<GrepRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        checkSanitizedField("GrepRequest.pattern", got->pattern, req.pattern);
        checkSanitizedField("GrepRequest.path", got->path, req.path);
        checkSanitizedField("GrepRequest.paths[0]", got->paths.at(0), req.paths.at(0));
        checkSanitizedField("GrepRequest.includePatterns[0]", got->includePatterns.at(0),
                            req.includePatterns.at(0));
        checkSanitizedField("GrepRequest.filterTags[0]", got->filterTags.at(0),
                            req.filterTags.at(0));
        checkSanitizedField("GrepRequest.colorMode", got->colorMode, req.colorMode);
        checkSanitizedField("GrepRequest.sessionName", got->sessionName, req.sessionName);
        checkSanitizedField("GrepRequest.instanceId", got->instanceId, req.instanceId);
    }

    SECTION("DownloadRequest sanitizes invalid UTF-8 text fields") {
        DownloadRequest req;
        req.url = invalidUtf8("https://example.com/file");
        req.outputPath = invalidUtf8("/tmp/download", ".txt");
        req.checksum = invalidUtf8("sha256:", "deadbeef");
        req.tags = {invalidUtf8("tag-download")};
        req.metadata = {{invalidUtf8("source"), invalidUtf8("cli")}};

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 19));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<DownloadRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        checkSanitizedField("DownloadRequest.url", got->url, req.url);
        checkSanitizedField("DownloadRequest.outputPath", got->outputPath, req.outputPath);
        checkSanitizedField("DownloadRequest.checksum", got->checksum, req.checksum);
        checkSanitizedField("DownloadRequest.tags[0]", got->tags.at(0), req.tags.at(0));
    }

    SECTION("UpdateDocumentRequest sanitizes invalid UTF-8 text fields") {
        UpdateDocumentRequest req;
        req.hash = invalidUtf8("sha256:");
        req.name = invalidUtf8("doc", ".md");
        req.newContent = invalidUtf8("updated content");
        req.addTags = {invalidUtf8("tag-add")};
        req.removeTags = {invalidUtf8("tag-remove")};
        req.metadata = {{invalidUtf8("task"), invalidUtf8("performance")}};

        auto enc = ProtoSerializer::encode_payload(makeMessageWith(Request{req}, 23));
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        auto* got = std::get_if<UpdateDocumentRequest>(&std::get<Request>(dec.value().payload));
        REQUIRE(got != nullptr);
        checkSanitizedField("UpdateDocumentRequest.hash", got->hash, req.hash);
        checkSanitizedField("UpdateDocumentRequest.name", got->name, req.name);
        checkSanitizedField("UpdateDocumentRequest.newContent", got->newContent, req.newContent);
        checkSanitizedField("UpdateDocumentRequest.addTags[0]", got->addTags.at(0),
                            req.addTags.at(0));
        checkSanitizedField("UpdateDocumentRequest.removeTags[0]", got->removeTags.at(0),
                            req.removeTags.at(0));
    }
}

TEST_CASE("ProtoSerializer: Response roundtrip", "[daemon][protocol][serialization]") {
    DownloadResponse dr;
    dr.hash = "deadbeef";
    dr.localPath = "/tmp/file";
    dr.url = "https://example.com";
    dr.jobId = "download-9";
    dr.state = "completed";
    dr.createdAtMs = 123;
    dr.updatedAtMs = 456;
    dr.size = 1234;
    dr.success = true;
    dr.error = "";

    auto enc = ProtoSerializer::encode_payload(makeMessageWith(Response{dr}, 3));
    REQUIRE(enc);

    auto dec = ProtoSerializer::decode_payload(enc.value());
    REQUIRE(dec);
    REQUIRE(std::holds_alternative<Response>(dec.value().payload));

    auto* got = std::get_if<DownloadResponse>(&std::get<Response>(dec.value().payload));
    REQUIRE(got != nullptr);
    REQUIRE(got->hash == "deadbeef");
    REQUIRE(got->jobId == "download-9");
    REQUIRE(got->state == "completed");
    REQUIRE(got->createdAtMs == 123);
    REQUIRE(got->updatedAtMs == 456);
    REQUIRE(got->size == 1234);
    REQUIRE(got->success == true);

    SECTION("GraphQueryResponse includes edges") {
        GraphQueryResponse gr;
        gr.originNode.nodeId = 1;
        gr.originNode.nodeKey = "path:file:/root/a.cpp";
        gr.originNode.label = "a.cpp";
        gr.originNode.type = "file";

        GraphNode cn;
        cn.nodeId = 2;
        cn.nodeKey = "symbol:foo";
        cn.label = "foo";
        cn.type = "function";
        cn.distance = 1;
        gr.connectedNodes.push_back(cn);

        GraphEdge edge;
        edge.edgeId = 42;
        edge.srcNodeId = 1;
        edge.dstNodeId = 2;
        edge.relation = "defines";
        edge.weight = 0.9f;
        edge.properties = "{\"source\":\"test\"}";
        gr.edges.push_back(edge);

        gr.totalNodesFound = 1;
        gr.totalEdgesTraversed = 1;
        gr.kgAvailable = true;

        auto encGraph = ProtoSerializer::encode_payload(
            makeMessageWith(Response{std::in_place_type<GraphQueryResponse>, gr}, 4));
        REQUIRE(encGraph);

        auto decGraph = ProtoSerializer::decode_payload(encGraph.value());
        REQUIRE(decGraph);
        REQUIRE(std::holds_alternative<Response>(decGraph.value().payload));

        auto* gotGraph =
            std::get_if<GraphQueryResponse>(&std::get<Response>(decGraph.value().payload));
        REQUIRE(gotGraph != nullptr);
        REQUIRE(gotGraph->edges.size() == 1);
        CHECK(gotGraph->edges[0].edgeId == 42);
        CHECK(gotGraph->edges[0].relation == "defines");
        CHECK(gotGraph->edges[0].properties == "{\"source\":\"test\"}");
    }

    SECTION("ErrorResponse sanitizes invalid UTF-8 message") {
        ErrorResponse err;
        err.code = ErrorCode::InvalidData;
        err.message = invalidUtf8("bad-search-query");

        auto encoded = ProtoSerializer::encode_payload(makeMessageWith(Response{err}, 20));
        REQUIRE(encoded);

        auto decoded = ProtoSerializer::decode_payload(encoded.value());
        REQUIRE(decoded);

        auto* decodedError =
            std::get_if<ErrorResponse>(&std::get<Response>(decoded.value().payload));
        REQUIRE(decodedError != nullptr);
        checkSanitizedField("ErrorResponse.message", decodedError->message, err.message);
    }

    SECTION("RepairEvent sanitizes invalid UTF-8 progress text") {
        RepairEvent ev;
        ev.phase = invalidUtf8("repairing");
        ev.operation = invalidUtf8("fts5");
        ev.message = invalidUtf8("rebuilding-index");

        auto encoded = ProtoSerializer::encode_payload(makeMessageWith(Response{ev}, 21));
        REQUIRE(encoded);

        auto decoded = ProtoSerializer::decode_payload(encoded.value());
        REQUIRE(decoded);

        auto* decodedEvent = std::get_if<RepairEvent>(&std::get<Response>(decoded.value().payload));
        REQUIRE(decodedEvent != nullptr);
        checkSanitizedField("RepairEvent.phase", decodedEvent->phase, ev.phase);
        checkSanitizedField("RepairEvent.operation", decodedEvent->operation, ev.operation);
        checkSanitizedField("RepairEvent.message", decodedEvent->message, ev.message);
    }

    SECTION("RepairResponse sanitizes invalid UTF-8 errors and operation messages") {
        RepairResponse resp;
        resp.success = false;
        resp.errors = {invalidUtf8("fts5 failed"), invalidUtf8("mime failed")};

        RepairOperationResult op;
        op.operation = invalidUtf8("embeddings");
        op.message = invalidUtf8("bad utf8 in batch");
        resp.operationResults.push_back(op);

        auto encoded = ProtoSerializer::encode_payload(makeMessageWith(Response{resp}, 22));
        REQUIRE(encoded);

        auto decoded = ProtoSerializer::decode_payload(encoded.value());
        REQUIRE(decoded);

        auto* decodedResponse =
            std::get_if<RepairResponse>(&std::get<Response>(decoded.value().payload));
        REQUIRE(decodedResponse != nullptr);
        REQUIRE(decodedResponse->errors.size() == resp.errors.size());
        checkSanitizedField("RepairResponse.errors[0]", decodedResponse->errors.at(0),
                            resp.errors.at(0));
        REQUIRE(decodedResponse->operationResults.size() == 1);
        checkSanitizedField("RepairResponse.operationResults[0].operation",
                            decodedResponse->operationResults.at(0).operation,
                            resp.operationResults.at(0).operation);
        checkSanitizedField("RepairResponse.operationResults[0].message",
                            decodedResponse->operationResults.at(0).message,
                            resp.operationResults.at(0).message);
    }

    SECTION("GetResponse sanitizes invalid UTF-8 descriptor fields without content") {
        GetResponse resp;
        resp.hash = invalidUtf8("sha256:");
        resp.name = invalidUtf8("doc", ".md");
        resp.path = invalidUtf8("/tmp/doc", ".md");
        resp.hasContent = false;
        resp.metadata = {{invalidUtf8("task"), invalidUtf8("performance")}};

        auto encoded = ProtoSerializer::encode_payload(makeMessageWith(Response{resp}, 24));
        REQUIRE(encoded);

        auto decoded = ProtoSerializer::decode_payload(encoded.value());
        REQUIRE(decoded);

        auto* decodedResponse =
            std::get_if<GetResponse>(&std::get<Response>(decoded.value().payload));
        REQUIRE(decodedResponse != nullptr);
        checkSanitizedField("GetResponse.hash", decodedResponse->hash, resp.hash);
        checkSanitizedField("GetResponse.name", decodedResponse->name, resp.name);
        checkSanitizedField("GetResponse.path", decodedResponse->path, resp.path);
        REQUIRE_FALSE(decodedResponse->hasContent);
    }
}

TEST_CASE("GrepResponse: Serialization edge cases", "[daemon][protocol][grep]") {
    SECTION("Paths-only mode (file only, no line)") {
        GrepResponse in{};
        GrepMatch m{};
        std::string candidate = "/tmp/test/README.md";
        m.file = candidate;
        m.line = "";
        in.matches.push_back(m);

        Message msg{};
        msg.payload = Response{std::in_place_type<GrepResponse>, in};

        auto enc = ProtoSerializer::encode_payload(msg);
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        const auto& resp = std::get<Response>(dec.value().payload);
        REQUIRE(std::holds_alternative<GrepResponse>(resp));
        const GrepResponse& gr = std::get<GrepResponse>(resp);

        REQUIRE(gr.matches.size() == 1);
        REQUIRE(gr.matches[0].file == candidate);
        REQUIRE(gr.matches[0].line.empty());
    }

    SECTION("Line match preserves content") {
        GrepResponse in{};
        GrepMatch m{};
        m.file = "";
        m.line = "hello world";
        in.matches.push_back(m);

        Message msg{};
        msg.payload = Response{std::in_place_type<GrepResponse>, in};

        auto enc = ProtoSerializer::encode_payload(msg);
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        const GrepResponse& gr = std::get<GrepResponse>(std::get<Response>(dec.value().payload));
        REQUIRE(gr.matches.size() == 1);
        REQUIRE(gr.matches[0].line == "hello world");
        REQUIRE(gr.matches[0].file.empty());
    }

    SECTION("Binary content preservation") {
        GrepResponse in{};
        GrepMatch m{};
        m.file = "binary.bin";
        m.line = std::string("\xFF\x00\x1B", 3);
        m.contextBefore.push_back(std::string("\x01\x02", 2));
        m.contextAfter.push_back(std::string("ok", 2));
        in.matches.push_back(m);

        Message msg{};
        msg.payload = Response{std::in_place_type<GrepResponse>, in};

        auto enc = ProtoSerializer::encode_payload(msg);
        REQUIRE(enc);

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec);

        const GrepResponse& gr = std::get<GrepResponse>(std::get<Response>(dec.value().payload));
        REQUIRE(gr.matches.size() == 1);
        REQUIRE(gr.matches[0].line == std::string("\xFF\x00\x1B", 3));
        REQUIRE(gr.matches[0].contextBefore.size() == 1);
        REQUIRE(gr.matches[0].contextAfter.size() == 1);
    }
}

// =============================================================================
// Type System Tests (ResponseOf, MessageType)
// =============================================================================

TEST_CASE("ResponseOf: Compile-time request→response mappings", "[daemon][protocol][types]") {
    // Search operations
    static_assert(std::is_same_v<ResponseOfT<SearchRequest>, SearchResponse>);
    static_assert(std::is_same_v<ResponseOfT<AddDocumentRequest>, AddDocumentResponse>);
    static_assert(std::is_same_v<ResponseOfT<GetRequest>, GetResponse>);
    static_assert(std::is_same_v<ResponseOfT<DeleteRequest>, DeleteResponse>);

    // System operations
    static_assert(std::is_same_v<ResponseOfT<StatusRequest>, StatusResponse>);
    static_assert(std::is_same_v<ResponseOfT<PingRequest>, PongResponse>);

    // Model operations
    static_assert(std::is_same_v<ResponseOfT<GenerateEmbeddingRequest>, EmbeddingResponse>);
    static_assert(std::is_same_v<ResponseOfT<BatchEmbeddingRequest>, BatchEmbeddingResponse>);
    static_assert(std::is_same_v<ResponseOfT<LoadModelRequest>, ModelLoadResponse>);
    static_assert(std::is_same_v<ResponseOfT<UnloadModelRequest>, SuccessResponse>);
    static_assert(std::is_same_v<ResponseOfT<ModelStatusRequest>, ModelStatusResponse>);

    // Download and other operations
    static_assert(std::is_same_v<ResponseOfT<DownloadRequest>, DownloadResponse>);
    static_assert(std::is_same_v<ResponseOfT<GrepRequest>, GrepResponse>);
    static_assert(std::is_same_v<ResponseOfT<UpdateDocumentRequest>, UpdateDocumentResponse>);
    static_assert(std::is_same_v<ResponseOfT<GetStatsRequest>, GetStatsResponse>);

    SUCCEED();
}

TEST_CASE("ResponseOf: Runtime usage with actual instances", "[daemon][protocol][types]") {
    SearchRequest searchReq{"test query", 10,    false, false, 0.7, {}, "keyword", false,
                            false,        false, false, false, 0,   0,  0,         ""};
    using SearchResType = ResponseOfT<decltype(searchReq)>;
    static_assert(std::is_same_v<SearchResType, SearchResponse>);

    SearchResType searchRes;
    searchRes.totalCount = 5;
    REQUIRE(searchRes.totalCount == 5);

    GenerateEmbeddingRequest embedReq{"sample text", "model"};
    using EmbedResType = ResponseOfT<decltype(embedReq)>;
    static_assert(std::is_same_v<EmbedResType, EmbeddingResponse>);

    EmbedResType embedRes;
    embedRes.dimensions = 384;
    REQUIRE(embedRes.dimensions == 384);
}

TEST_CASE("MessageType: Request and response enum mappings", "[daemon][protocol][types]") {
    SECTION("Request type mappings") {
        REQUIRE(MessageType::CatRequest == getMessageType(Request{CatRequest{}}));
        REQUIRE(MessageType::ListSessionsRequest == getMessageType(Request{ListSessionsRequest{}}));
        REQUIRE(MessageType::DownloadStatusRequest ==
                getMessageType(Request{DownloadStatusRequest{.jobId = "download-1"}}));
        REQUIRE(MessageType::CancelDownloadJobRequest ==
                getMessageType(Request{CancelDownloadJobRequest{.jobId = "download-2"}}));
        REQUIRE(MessageType::ListDownloadJobsRequest ==
                getMessageType(Request{ListDownloadJobsRequest{.limit = 5}}));
        REQUIRE(MessageType::UseSessionRequest ==
                getMessageType(Request{UseSessionRequest{.session_name = "s"}}));
        REQUIRE(MessageType::AddPathSelectorRequest ==
                getMessageType(Request{AddPathSelectorRequest{.session_name = "s", .path = "/p"}}));
        REQUIRE(
            MessageType::RemovePathSelectorRequest ==
            getMessageType(Request{RemovePathSelectorRequest{.session_name = "s", .path = "/p"}}));
    }

    SECTION("Response type mappings") {
        REQUIRE(MessageType::CatResponse == getMessageType(Response{CatResponse{}}));
        REQUIRE(MessageType::ListResponse == getMessageType(Response{ListResponse{}}));
        REQUIRE(MessageType::StatusResponse == getMessageType(Response{StatusResponse{}}));
        REQUIRE(MessageType::ListDownloadJobsResponse ==
                getMessageType(Response{ListDownloadJobsResponse{}}));
    }

    SECTION("Request name extraction") {
        REQUIRE(std::string("Cat") == getRequestName(Request{CatRequest{}}));
        REQUIRE(std::string("ListSessions") == getRequestName(Request{ListSessionsRequest{}}));
        REQUIRE(std::string("DownloadStatus") ==
                getRequestName(Request{DownloadStatusRequest{.jobId = "download-1"}}));
        REQUIRE(std::string("CancelDownloadJob") ==
                getRequestName(Request{CancelDownloadJobRequest{.jobId = "download-2"}}));
        REQUIRE(std::string("ListDownloadJobs") ==
                getRequestName(Request{ListDownloadJobsRequest{.limit = 5}}));
        REQUIRE(std::string("UseSession") ==
                getRequestName(Request{UseSessionRequest{.session_name = "x"}}));
        REQUIRE(std::string("AddPathSelector") ==
                getRequestName(Request{AddPathSelectorRequest{}}));
        REQUIRE(std::string("RemovePathSelector") ==
                getRequestName(Request{RemovePathSelectorRequest{}}));
    }
}
