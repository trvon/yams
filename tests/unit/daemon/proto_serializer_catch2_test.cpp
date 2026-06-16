// ProtoSerializer roundtrip tests: encode → decode for top message types
#include <catch2/catch_approx.hpp>
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

    Message msg{};
    msg.requestId = 2;
    msg.payload = Request{req};

    auto encoded = ProtoSerializer::encode_payload(msg);
    REQUIRE(encoded.has_value());

    auto decoded = ProtoSerializer::decode_payload(std::span{encoded.value()});
    REQUIRE(decoded.has_value());
    REQUIRE(decoded.value().requestId == 2);
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
