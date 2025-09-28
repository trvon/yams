#include <gtest/gtest.h>

#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;

TEST(IpcMessageTypeTest, RequestTypeMappings) {
    EXPECT_EQ(MessageType::CatRequest, getMessageType(Request{CatRequest{}}));
    EXPECT_EQ(MessageType::ListSessionsRequest, getMessageType(Request{ListSessionsRequest{}}));
    EXPECT_EQ(MessageType::UseSessionRequest,
              getMessageType(Request{UseSessionRequest{.session_name = "s"}}));
    EXPECT_EQ(MessageType::AddPathSelectorRequest,
              getMessageType(Request{AddPathSelectorRequest{.session_name = "s", .path = "/p"}}));
    EXPECT_EQ(
        MessageType::RemovePathSelectorRequest,
        getMessageType(Request{RemovePathSelectorRequest{.session_name = "s", .path = "/p"}}));
}

TEST(IpcMessageTypeTest, ResponseTypeMappings) {
    EXPECT_EQ(MessageType::CatResponse, getMessageType(Response{CatResponse{}}));
    EXPECT_EQ(MessageType::ListResponse, getMessageType(Response{ListResponse{}}));
    EXPECT_EQ(MessageType::StatusResponse, getMessageType(Response{StatusResponse{}}));
}

TEST(IpcMessageTypeTest, RequestNames) {
    EXPECT_EQ(std::string("Cat"), getRequestName(Request{CatRequest{}}));
    EXPECT_EQ(std::string("ListSessions"), getRequestName(Request{ListSessionsRequest{}}));
    EXPECT_EQ(std::string("UseSession"),
              getRequestName(Request{UseSessionRequest{.session_name = "x"}}));
    EXPECT_EQ(std::string("AddPathSelector"), getRequestName(Request{AddPathSelectorRequest{}}));
    EXPECT_EQ(std::string("RemovePathSelector"),
              getRequestName(Request{RemovePathSelectorRequest{}}));
}
