// Ensure placement-new and string APIs are visible and not macro-polluted
#include <new>
#include <string>

#include <gtest/gtest.h>

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>

using namespace yams;
using namespace yams::daemon;

TEST(StatusResponseProto, RoundTripLifecycleAndError) {
    // Build a Message carrying a StatusResponse with explicit fields
    StatusResponse s{};
    s.running = true;
    s.ready = false;
    s.uptimeSeconds = 42;
    s.requestsProcessed = 7;
    s.activeConnections = 1;
    s.memoryUsageMb = 12.5;
    s.cpuUsagePercent = 3.2;
    s.version = "test";
    s.overallStatus = "failed";    // authoritative textual state
    s.lifecycleState = "starting"; // will be mirrored to overallStatus on decode
    s.lastError = "boom";          // encoded via reserved request_counts key
    s.requestCounts["worker_threads"] = 4;

    Message m{};
    m.payload = Response{std::in_place_type<StatusResponse>, s};

    auto enc = ProtoSerializer::encode_payload(m);
    ASSERT_TRUE(enc) << enc.error().message;

    // Now decode
    auto dec = ProtoSerializer::decode_payload(enc.value());
    ASSERT_TRUE(dec) << dec.error().message;

    const Message& out = dec.value();
    ASSERT_TRUE(std::holds_alternative<Response>(out.payload));
    const auto& resp = std::get<Response>(out.payload);
    ASSERT_TRUE(std::holds_alternative<StatusResponse>(resp));
    const StatusResponse& r = std::get<StatusResponse>(resp);

    // lifecycleState should be populated and equal to overallStatus
    EXPECT_EQ(r.overallStatus, "failed");
    EXPECT_EQ(r.lifecycleState, r.overallStatus);
    // lastError should survive roundtrip
    EXPECT_EQ(r.lastError, "boom");
    // Numeric map entries still parsed
    auto it = r.requestCounts.find("worker_threads");
    ASSERT_NE(it, r.requestCounts.end());
    EXPECT_EQ(it->second, 4u);
}
