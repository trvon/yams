#include <type_traits>
#include <gtest/gtest.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>

namespace yams::daemon::test {

// Test that each request type maps to the correct response type
class ResponseOfTest : public ::testing::Test {};

// Compile-time tests using static_assert
TEST_F(ResponseOfTest, CompileTimeMappings) {
    // Search operations
    static_assert(std::is_same_v<ResponseOfT<SearchRequest>, SearchResponse>);

    // Document operations
    static_assert(std::is_same_v<ResponseOfT<AddRequest>, AddResponse>);
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

    // Download operations
    static_assert(std::is_same_v<ResponseOfT<DownloadRequest>, DownloadResponse>);

    // Other new operations
    static_assert(std::is_same_v<ResponseOfT<AddDocumentRequest>, AddDocumentResponse>);
    static_assert(std::is_same_v<ResponseOfT<GrepRequest>, GrepResponse>);
    static_assert(std::is_same_v<ResponseOfT<UpdateDocumentRequest>, UpdateDocumentResponse>);
    static_assert(std::is_same_v<ResponseOfT<GetStatsRequest>, GetStatsResponse>);

    SUCCEED() << "All compile-time type mappings are correct";
}

// Test that the trait works with actual request/response instances
TEST_F(ResponseOfTest, RuntimeUsage) {
    // Create a search request
    SearchRequest searchReq{"test query", 10,    false, false, 0.7, {}, "keyword", false,
                            false,        false, false, false, 0,   0,  0,         ""};

    // The response type should be SearchResponse
    using SearchResType = ResponseOfT<decltype(searchReq)>;
    static_assert(std::is_same_v<SearchResType, SearchResponse>);

    // Create the corresponding response
    SearchResType searchRes;
    searchRes.totalCount = 5;
    EXPECT_EQ(searchRes.totalCount, 5);

    // Test with model request
    GenerateEmbeddingRequest embedReq{"sample text", "model"};
    using EmbedResType = ResponseOfT<decltype(embedReq)>;
    static_assert(std::is_same_v<EmbedResType, EmbeddingResponse>);

    EmbedResType embedRes;
    embedRes.dimensions = 384;
    EXPECT_EQ(embedRes.dimensions, 384);
}

// Test that ResponseOf is SFINAE-friendly (doesn't cause hard errors for invalid types)
template <typename T, typename = void> struct HasResponseOf : std::false_type {};

template <typename T> struct HasResponseOf<T, std::void_t<ResponseOfT<T>>> : std::true_type {};

TEST_F(ResponseOfTest, SfinaeFriendly) {
    // Valid request types should have ResponseOf
    static_assert(HasResponseOf<SearchRequest>::value);
    static_assert(HasResponseOf<GetRequest>::value);
    static_assert(HasResponseOf<StatusRequest>::value);

    // Invalid types should not have ResponseOf
    struct InvalidRequest {};
    static_assert(!HasResponseOf<InvalidRequest>::value);
    static_assert(!HasResponseOf<int>::value);
    static_assert(!HasResponseOf<std::string>::value);

    SUCCEED() << "ResponseOf trait is SFINAE-friendly";
}

// Test variant compatibility
TEST_F(ResponseOfTest, VariantCompatibility) {
    // All request types should be constructible into Request variant
    Request req1 = SearchRequest{"query", 10,    false, false, 0.7, {}, "keyword", false,
                                 false,   false, false, false, 0,   0,  0,         ""};
    GetRequest getByHash{};
    getByHash.hash = "hash123";
    Request req2 = getByHash;
    Request req3 = StatusRequest{true};

    // All response types should be constructible into Response variant
    Response res1 = SearchResponse{};
    Response res2 = GetResponse{};
    Response res3 = StatusResponse{};

    // The mapped types should be extractable from Response
    auto tryExtract = [](const Response& res) {
        if ([[maybe_unused]] auto* sr = std::get_if<SearchResponse>(&res)) {
            return true;
        }
        if ([[maybe_unused]] auto* gr = std::get_if<GetResponse>(&res)) {
            return true;
        }
        if ([[maybe_unused]] auto* str = std::get_if<StatusResponse>(&res)) {
            return true;
        }
        return false;
    };

    EXPECT_TRUE(tryExtract(res1));
    EXPECT_TRUE(tryExtract(res2));
    EXPECT_TRUE(tryExtract(res3));
}

// Test that ShutdownRequest maps to SuccessResponse (multiple requests can map to same response)
TEST_F(ResponseOfTest, MultipleRequestsToSameResponse) {
    // Both DeleteRequest and UnloadModelRequest map to SuccessResponse
    static_assert(std::is_same_v<ResponseOfT<DeleteRequest>, DeleteResponse>);
    static_assert(std::is_same_v<ResponseOfT<UnloadModelRequest>, SuccessResponse>);

    // They map to different types
    static_assert(!std::is_same_v<ResponseOfT<DeleteRequest>, ResponseOfT<UnloadModelRequest>>);

    SUCCEED() << "Multiple requests can map to the same response type";
}

// Test helper function to verify request-response pairs at runtime
template <typename Req>
bool verifyRequestResponsePair([[maybe_unused]] const Req& req, const Response& res) {
    using ExpectedRes = ResponseOfT<Req>;
    return std::holds_alternative<ExpectedRes>(res);
}

TEST_F(ResponseOfTest, RequestResponsePairVerification) {
    // Create various requests and their expected responses
    SearchRequest searchReq{"test", 10,    false, false, 0.7, {}, "keyword", false,
                            false,  false, false, false, 0,   0,  0,         ""};
    Response searchRes = SearchResponse{{}, 10, std::chrono::milliseconds(0)};
    EXPECT_TRUE(verifyRequestResponsePair(searchReq, searchRes));

    GetRequest getReq{};
    getReq.hash = "abc123";
    GetResponse gr{};
    gr.hash = "abc123";
    gr.content = "content";
    gr.hasContent = true;
    Response getRes = gr;
    EXPECT_TRUE(verifyRequestResponsePair(getReq, getRes));

    // Wrong response type should fail
    StatusResponse sr;
    sr.running = true;
    sr.ready = true;
    sr.uptimeSeconds = 0;
    sr.requestsProcessed = 0;
    sr.activeConnections = 0;
    sr.memoryUsageMb = 0.0;
    sr.cpuUsagePercent = 0.0;
    Response wrongRes = sr;
    EXPECT_FALSE(verifyRequestResponsePair(searchReq, wrongRes));
    EXPECT_FALSE(verifyRequestResponsePair(getReq, wrongRes));

    // But StatusRequest should match StatusResponse
    StatusRequest statusReq{true};
    EXPECT_TRUE(verifyRequestResponsePair(statusReq, wrongRes));
}

// Test that all Request variant members have a ResponseOf mapping
TEST_F(ResponseOfTest, AllRequestsHaveMapping) {
    // This would fail to compile if any request type lacks a ResponseOf specialization
    auto testMapping = []<typename Req>(const Req&) {
        using ResType = ResponseOfT<Req>;
        return std::is_default_constructible_v<ResType>;
    };

    EXPECT_TRUE(testMapping(SearchRequest{
        "", 10, false, false, 0.7, {}, "keyword", false, false, false, false, false, 0, 0, 0, ""}));
    EXPECT_TRUE(testMapping(AddRequest{}));
    EXPECT_TRUE(testMapping(GetRequest{}));
    EXPECT_TRUE(testMapping(DeleteRequest{}));
    EXPECT_TRUE(testMapping(StatusRequest{}));
    EXPECT_TRUE(testMapping(PingRequest{}));
    EXPECT_TRUE(testMapping(GenerateEmbeddingRequest{}));
    EXPECT_TRUE(testMapping(BatchEmbeddingRequest{}));
    EXPECT_TRUE(testMapping(LoadModelRequest{}));
    EXPECT_TRUE(testMapping(UnloadModelRequest{}));
    EXPECT_TRUE(testMapping(ModelStatusRequest{}));
}

} // namespace yams::daemon::test