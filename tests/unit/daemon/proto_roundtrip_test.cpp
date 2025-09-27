#include <gtest/gtest.h>

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>

using namespace yams::daemon;

static Message make_message_with(Request r, uint64_t id = 42) {
    Message m;
    m.requestId = id;
    m.payload = std::move(r);
    return m;
}

static Message make_message_with(Response r, uint64_t id = 43) {
    Message m;
    m.requestId = id;
    m.payload = std::move(r);
    return m;
}

TEST(ProtoRoundtripTest, CatRequestRoundtrip) {
    CatRequest cr;
    cr.hash = "";
    cr.name = "inline.txt";

    auto enc = ProtoSerializer::encode_payload(make_message_with(Request{cr}, 1));
    ASSERT_TRUE(enc) << enc.error().message;
    auto dec = ProtoSerializer::decode_payload(enc.value());
    ASSERT_TRUE(dec) << dec.error().message;
    ASSERT_TRUE(std::holds_alternative<Request>(dec.value().payload));
    auto req = std::get<Request>(dec.value().payload);
    auto* got = std::get_if<CatRequest>(&req);
    ASSERT_TRUE(got);
    EXPECT_EQ("inline.txt", got->name);
}

TEST(ProtoRoundtripTest, ListSessionsRequestRoundtrip) {
    auto enc =
        ProtoSerializer::encode_payload(make_message_with(Request{ListSessionsRequest{}}, 2));
    ASSERT_TRUE(enc) << enc.error().message;
    auto dec = ProtoSerializer::decode_payload(enc.value());
    ASSERT_TRUE(dec) << dec.error().message;
    auto req = std::get<Request>(dec.value().payload);
    EXPECT_TRUE(std::holds_alternative<ListSessionsRequest>(req));
}

TEST(ProtoRoundtripTest, DownloadResponseRoundtrip) {
    DownloadResponse dr;
    dr.hash = "deadbeef";
    dr.localPath = "/tmp/file";
    dr.url = "https://example.com";
    dr.size = 1234;
    dr.success = true;
    dr.error = "";

    auto enc = ProtoSerializer::encode_payload(make_message_with(Response{dr}, 3));
    ASSERT_TRUE(enc) << enc.error().message;
    auto dec = ProtoSerializer::decode_payload(enc.value());
    ASSERT_TRUE(dec) << dec.error().message;
    ASSERT_TRUE(std::holds_alternative<Response>(dec.value().payload));
    auto res = std::get<Response>(dec.value().payload);
    auto* got = std::get_if<DownloadResponse>(&res);
    ASSERT_TRUE(got);
    EXPECT_EQ("deadbeef", got->hash);
    EXPECT_EQ(1234u, got->size);
    EXPECT_TRUE(got->success);
}
