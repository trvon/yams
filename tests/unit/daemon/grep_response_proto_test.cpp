#include <gtest/gtest.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>

using namespace yams;
using namespace yams::daemon;

TEST(GrepResponseProto, RoundTripPathsOnlyEmitsFile) {
    // Arrange: simulate a pathsOnly grep where matches carry file only
    GrepResponse in{};
    GrepMatch m{};
    // Use an absolute path that should exist in the workspace (repo root)
    // We just need the decode heuristic to recognize it as a path.
    // Prefer something likely to exist in CI as well; fallback to '/' if not existing.
    std::string candidate = "/Volumes/picaso/work/tools/yams/README.md";
    if (!std::filesystem::exists(candidate)) {
        candidate = "/"; // still absolute; existence check will pass
    }
    m.file = candidate;
    m.line = ""; // no line when pathsOnly
    in.matches.push_back(m);

    Message msg{};
    msg.payload = Response{std::in_place_type<GrepResponse>, in};

    // Act: encode then decode
    auto enc = ProtoSerializer::encode_payload(msg);
    ASSERT_TRUE(enc) << enc.error().message;

    auto dec = ProtoSerializer::decode_payload(enc.value());
    ASSERT_TRUE(dec) << dec.error().message;

    const Message& out = dec.value();
    ASSERT_TRUE(std::holds_alternative<Response>(out.payload));
    const auto& resp = std::get<Response>(out.payload);
    ASSERT_TRUE(std::holds_alternative<GrepResponse>(resp));
    const GrepResponse& gr = std::get<GrepResponse>(resp);

    ASSERT_EQ(gr.matches.size(), 1u);
    EXPECT_EQ(gr.matches[0].file, candidate);
    EXPECT_TRUE(gr.matches[0].line.empty());
}

TEST(GrepResponseProto, RoundTripLineMatchPreservesLine) {
    GrepResponse in{};
    GrepMatch m{};
    m.file = ""; // not used for regular line output
    m.line = "hello world";
    in.matches.push_back(m);

    Message msg{};
    msg.payload = Response{std::in_place_type<GrepResponse>, in};

    auto enc = ProtoSerializer::encode_payload(msg);
    ASSERT_TRUE(enc) << enc.error().message;

    auto dec = ProtoSerializer::decode_payload(enc.value());
    ASSERT_TRUE(dec) << dec.error().message;

    const Message& out = dec.value();
    ASSERT_TRUE(std::holds_alternative<Response>(out.payload));
    const auto& resp = std::get<Response>(out.payload);
    ASSERT_TRUE(std::holds_alternative<GrepResponse>(resp));
    const GrepResponse& gr = std::get<GrepResponse>(resp);

    ASSERT_EQ(gr.matches.size(), 1u);
    EXPECT_EQ(gr.matches[0].line, "hello world");
    EXPECT_TRUE(gr.matches[0].file.empty());
}

TEST(GrepResponseProto, RoundTripBinaryContentPreservesBytes) {
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
    ASSERT_TRUE(enc) << enc.error().message;

    auto dec = ProtoSerializer::decode_payload(enc.value());
    ASSERT_TRUE(dec) << dec.error().message;

    const Message& out = dec.value();
    ASSERT_TRUE(std::holds_alternative<Response>(out.payload));
    const auto& resp = std::get<Response>(out.payload);
    ASSERT_TRUE(std::holds_alternative<GrepResponse>(resp));
    const GrepResponse& gr = std::get<GrepResponse>(resp);

    ASSERT_EQ(gr.matches.size(), 1u);
    const auto& outMatch = gr.matches[0];
    EXPECT_EQ(outMatch.file, "binary.bin");
    ASSERT_EQ(outMatch.line.size(), 3u);
    EXPECT_EQ(static_cast<unsigned char>(outMatch.line[0]), 0xFF);
    EXPECT_EQ(static_cast<unsigned char>(outMatch.line[1]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(outMatch.line[2]), 0x1B);
    ASSERT_EQ(outMatch.contextBefore.size(), 1u);
    EXPECT_EQ(outMatch.contextBefore[0], std::string("\x01\x02", 2));
    ASSERT_EQ(outMatch.contextAfter.size(), 1u);
    EXPECT_EQ(outMatch.contextAfter[0], "ok");
}
