// ProtoSerializer roundtrip tests: encode → decode for top message types
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/ipc_protocol_envelope.h>
#include <yams/daemon/ipc/proto_serializer.h>

#include <chrono>
#include <string>
#include <vector>

namespace yams::daemon::test {

TEST_CASE("ProtoSerializer PingRequest roundtrip", "[proto][serializer][ping]") {
    PingRequest req{};
    req.timestamp = std::chrono::steady_clock::now();

    Message msg{};
    msg.requestId = 1;
    msg.payload = Request{req};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());
    REQUIRE(!encoded.value().empty());

    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
    REQUIRE(decoded.value().requestId == 1);
}

TEST_CASE("ProtoSerializer SearchRequest roundtrip", "[proto][serializer][search]") {
    SearchRequest req{};
    req.query = "test query";
    req.limit = 10;
    req.fuzzy = true;
    req.similarity = 0.85;
    req.cancellationSignal = std::make_shared<std::atomic<bool>>(true);

    Message msg{};
    msg.requestId = 2;
    msg.payload = Request{req};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());

    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
    REQUIRE(decoded.value().requestId == 2);
    const auto* decodedRequest =
        std::get_if<SearchRequest>(&std::get<Request>(decoded.value().payload));
    REQUIRE(decodedRequest != nullptr);
    CHECK_FALSE(decodedRequest->cancellationSignal);
}

TEST_CASE("ProtoSerializer StatusRequest roundtrip", "[proto][serializer][status]") {
    StatusRequest req{};
    req.detailed = true;

    Message msg{};
    msg.requestId = 3;
    msg.payload = Request{req};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());

    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
}

TEST_CASE("ProtoSerializer MemorySync request and response preserve binary values",
          "[proto][serializer][memory-sync]") {
    MemorySyncRequest request{MemorySyncOperation::Delete, "binary", std::string{"a\0b", 3}};
    Message requestMessage{};
    requestMessage.requestId = 90;
    requestMessage.payload = Request{request};

    auto encodedRequest = ProtoSerializer::encode_payload(requestMessage);
    REQUIRE(encodedRequest.has_value());
    auto decodedRequest = ProtoSerializer::decode_payload(std::span{encodedRequest.value()});
    REQUIRE(decodedRequest.has_value());
    const auto* roundTrippedRequest =
        std::get_if<MemorySyncRequest>(&std::get<Request>(decodedRequest.value().payload));
    REQUIRE(roundTrippedRequest != nullptr);
    CHECK(roundTrippedRequest->operation == MemorySyncOperation::Delete);
    CHECK(roundTrippedRequest->key == "binary");
    CHECK(roundTrippedRequest->value == std::string{"a\0b", 3});

    MemorySyncResponse response;
    response.published = true;
    response.started = true;
    response.value = std::string{"x\0y", 3};
    response.records = 4;
    response.quarantinedRecords = 3;
    response.authFailures = 2;
    response.successfulCycles = 11;
    response.failedCycles = 5;
    response.lastSuccessAgeMs = 250;
    response.backend = "filesystem";
    response.nodeId = "node-a";
    response.corpusId = "corpus-a";
    response.corpusEpoch = 7;
    response.mode = "persistent";
    response.trustMode = "authenticated-writers";
    Message responseMessage{};
    responseMessage.requestId = 91;
    responseMessage.payload = Response{response};

    auto encodedResponse = ProtoSerializer::encode_payload(responseMessage);
    REQUIRE(encodedResponse.has_value());
    auto decodedResponse = ProtoSerializer::decode_payload(std::span{encodedResponse.value()});
    REQUIRE(decodedResponse.has_value());
    const auto* roundTrippedResponse =
        std::get_if<MemorySyncResponse>(&std::get<Response>(decodedResponse.value().payload));
    REQUIRE(roundTrippedResponse != nullptr);
    CHECK(roundTrippedResponse->published);
    CHECK(roundTrippedResponse->started);
    CHECK(roundTrippedResponse->value == std::string{"x\0y", 3});
    CHECK(roundTrippedResponse->records == 4);
    CHECK(roundTrippedResponse->quarantinedRecords == 3);
    CHECK(roundTrippedResponse->authFailures == 2);
    CHECK(roundTrippedResponse->successfulCycles == 11);
    CHECK(roundTrippedResponse->failedCycles == 5);
    CHECK(roundTrippedResponse->lastSuccessAgeMs == 250);
    CHECK(roundTrippedResponse->backend == "filesystem");
    CHECK(roundTrippedResponse->nodeId == "node-a");
    CHECK(roundTrippedResponse->corpusId == "corpus-a");
    CHECK(roundTrippedResponse->corpusEpoch == 7);
    CHECK(roundTrippedResponse->mode == "persistent");
    CHECK(roundTrippedResponse->trustMode == "authenticated-writers");
}

TEST_CASE("ProtoSerializer StatusResponse preserves daemon log file path",
          "[proto][serializer][status][daemon-log]") {
    yams::daemon::StatusResponse response;
    response.running = true;
    response.ready = true;
    response.version = "0.19.0";
    response.overallStatus = "ready";
    response.dataDir = "/tmp/yams-data";
    response.logFile = "/tmp/yams-test-daemon.log";
    response.requestsProcessed = 7;

    Message message{};
    message.requestId = 92;
    message.payload = Response{response};

    auto encoded = ProtoSerializer::encode_payload(message);
    REQUIRE(encoded.has_value());
    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
    const auto* roundTripped =
        std::get_if<yams::daemon::StatusResponse>(&std::get<Response>(decoded.value().payload));
    REQUIRE(roundTripped != nullptr);
    CHECK(roundTripped->logFile == "/tmp/yams-test-daemon.log");
    CHECK(roundTripped->dataDir == "/tmp/yams-data");
}

TEST_CASE("ProtoSerializer GetRequest roundtrip", "[proto][serializer][get]") {
    GetRequest req{};
    req.hash = "abc123def456";
    req.name = "test_document";
    req.byName = true;

    Message msg{};
    msg.requestId = 4;
    msg.payload = Request{req};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());

    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
}

TEST_CASE("ProtoSerializer DeleteRequest roundtrip", "[proto][serializer][delete]") {
    DeleteRequest req{};
    req.hash = "deadbeef";
    req.name = "delete_me";
    req.pattern = "*.tmp";

    Message msg{};
    msg.requestId = 5;
    msg.payload = Request{req};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());

    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
}

TEST_CASE("ProtoSerializer DeleteResponse preserves per-target outcomes",
          "[proto][serializer][delete]") {
    DeleteResponse response{};
    response.dryRun = true;
    response.successCount = 1;
    response.failureCount = 1;
    response.results.push_back(DeleteResponse::DeleteResult{
        .name = "deleted.txt", .hash = "deleted-hash", .success = true, .error = ""});
    response.results.push_back(DeleteResponse::DeleteResult{.name = "failed.txt",
                                                            .hash = "failed-hash",
                                                            .success = false,
                                                            .errorCode = ErrorCode::CorruptedData,
                                                            .error = "typed failure"});

    Message msg{};
    msg.requestId = 55;
    msg.payload = Response{response};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());
    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
    REQUIRE(std::holds_alternative<Response>(decoded.value().payload));
    const auto& decodedResponse = std::get<Response>(decoded.value().payload);
    REQUIRE(std::holds_alternative<DeleteResponse>(decodedResponse));
    const auto& value = std::get<DeleteResponse>(decodedResponse);
    CHECK(value.dryRun);
    CHECK((value.successCount == 1));
    CHECK((value.failureCount == 1));
    REQUIRE((value.results.size() == 2));
    CHECK((value.results[0].name == "deleted.txt"));
    CHECK(value.results[0].success);
    CHECK((value.results[1].name == "failed.txt"));
    CHECK_FALSE(value.results[1].success);
    CHECK(value.results[1].errorCode == ErrorCode::CorruptedData);
    CHECK((value.results[1].error == "typed failure"));
}

TEST_CASE("ProtoSerializer with session info", "[proto][serializer][session]") {
    SearchRequest req{};
    req.query = "session test";
    req.limit = 5;

    Message msg{};
    msg.requestId = 6;
    msg.sessionId = "session-abc-123";
    msg.clientVersion = "1.2.3";
    msg.payload = Request{req};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());

    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
    REQUIRE(decoded.value().requestId == 6);
}

TEST_CASE("ProtoSerializer error on empty input", "[proto][serializer][error]") {
    std::vector<uint8_t> empty{};
    auto result = ProtoSerializer::decode_payload(std::span{empty});
    REQUIRE(!result.has_value());
}

TEST_CASE("ProtoSerializer error on garbage input", "[proto][serializer][error]") {
    std::vector<uint8_t> garbage{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03};
    auto result = ProtoSerializer::decode_payload(std::span{garbage});
    REQUIRE(!result.has_value());
}

TEST_CASE("ProtoSerializer encode_payload_into appends", "[proto][serializer][buffer]") {
    SearchRequest req{};
    req.query = "buffer test";
    req.limit = 3;

    Message msg{};
    msg.requestId = 7;
    msg.payload = Request{req};

    std::vector<uint8_t> buffer{0xAA, 0xBB, 0xCC};
    const auto initialSize = buffer.size();

    auto result = ProtoSerializer::encode_payload_into(msg, buffer);
    REQUIRE(result.has_value());
    REQUIRE(buffer.size() > initialSize);
    REQUIRE(buffer[0] == 0xAA);
    REQUIRE(buffer[1] == 0xBB);
    REQUIRE(buffer[2] == 0xCC);
}

TEST_CASE("ProtoSerializer multi-type roundtrip", "[proto][serializer][roundtrip]") {
    auto testEncodeDecode = [](Message msg) {
        auto encoded = ProtoSerializer::encode_payload(msg);
        REQUIRE(encoded.has_value());
        REQUIRE(!encoded.value().empty());

        auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
        REQUIRE(decoded.has_value());
    };

    // PingRequest
    {
        Message msg{};
        msg.requestId = 100;
        msg.payload = Request{PingRequest{std::chrono::steady_clock::now()}};
        testEncodeDecode(msg);
    }

    // ShutdownRequest
    {
        Message msg{};
        msg.requestId = 101;
        msg.payload = Request{ShutdownRequest{}};
        testEncodeDecode(msg);
    }

    // ModelStatusRequest
    {
        Message msg{};
        msg.requestId = 102;
        msg.payload = Request{ModelStatusRequest{}};
        testEncodeDecode(msg);
    }

    // PrepareSessionRequest
    {
        Message msg{};
        msg.requestId = 103;
        msg.payload = Request{PrepareSessionRequest{}};
        testEncodeDecode(msg);
    }
}

} // namespace yams::daemon::test
