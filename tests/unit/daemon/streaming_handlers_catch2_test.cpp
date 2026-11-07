// Streaming handlers unit tests

#include <iostream>
#include <sstream>
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/client/daemon_client.h>

#include <yams/core/types.h>
#include <yams/daemon/client/streaming_handlers.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;
using namespace yams::daemon::client;
using namespace yams;

namespace {
class ScopedStdoutCapture {
public:
    ScopedStdoutCapture() : previous_(std::cout.rdbuf(stream_.rdbuf())) {}
    ~ScopedStdoutCapture() { std::cout.rdbuf(previous_); }
    ScopedStdoutCapture(const ScopedStdoutCapture&) = delete;
    ScopedStdoutCapture& operator=(const ScopedStdoutCapture&) = delete;
    std::string str() const { return stream_.str(); }

private:
    std::ostringstream stream_;
    std::streambuf* previous_;
};
} // namespace

TEST_CASE("Accumulating path handlers never render behind the caller's result cap",
          "[daemon][streaming][paths-output]") {
    auto checkGrep = [](bool empty) {
        DaemonClient::StreamingGrepHandler handler(true, 1);
        GrepResponse header;
        GrepResponse chunk;
        if (!empty) {
            GrepMatch first;
            first.file = "/repo/first.cpp";
            header.matches.push_back(first);
            chunk.matches.push_back(first); // Per-file cap still applies across chunks.
            GrepMatch second;
            second.file = "/repo/second.cpp";
            chunk.matches.push_back(second);
        }
        std::string output;
        {
            ScopedStdoutCapture capture;
            handler.onHeaderReceived(header);
            handler.onChunkReceived(chunk, true);
            handler.onComplete();
            output = capture.str();
        }
        CHECK(output.empty());
        auto result = handler.getResults();
        REQUIRE(result);
        CHECK(result.value().matches.size() == (empty ? 0 : 2));
    };
    auto checkList = [](bool empty) {
        DaemonClient::StreamingListHandler handler(true, 2);
        ListResponse header;
        ListResponse chunk;
        if (!empty) {
            ListEntry first;
            first.path = "/repo/first.cpp";
            header.items.push_back(first);
            ListEntry second;
            second.path = "/repo/second.cpp";
            chunk.items.push_back(second);
            ListEntry third;
            third.path = "/repo/third.cpp";
            chunk.items.push_back(third);
        }
        std::string output;
        {
            ScopedStdoutCapture capture;
            handler.onHeaderReceived(header);
            handler.onChunkReceived(chunk, true);
            handler.onComplete();
            output = capture.str();
        }
        CHECK(output.empty());
        auto result = handler.getResults();
        REQUIRE(result);
        CHECK(result.value().items.size() == (empty ? 0 : 2));
    };
    SECTION("grep header and chunks") {
        checkGrep(false);
    }
    SECTION("grep empty") {
        checkGrep(true);
    }
    SECTION("list header and chunks") {
        checkList(false);
    }
    SECTION("list empty") {
        checkList(true);
    }
}

TEST_CASE("UnaryHandler stores response on header", "[daemon][streaming][unit]") {
    auto handler = std::make_shared<UnaryHandler<SuccessResponse>>();

    SECTION("Stores response when received") {
        SuccessResponse resp;
        Response response = resp;
        handler->onHeaderReceived(response);

        REQUIRE(handler->value.has_value());
        REQUIRE(handler->hasError() == false);
    }

    SECTION("Stores error on ErrorResponse") {
        ErrorResponse err;
        err.code = ErrorCode::InvalidArgument;
        err.message = "Test error";
        Response response = err;
        handler->onHeaderReceived(response);

        REQUIRE(handler->hasError() == true);
        REQUIRE(handler->error().code == ErrorCode::InvalidArgument);
        REQUIRE(handler->error().message == "Test error");
    }
}

TEST_CASE("SuccessHandler tracks success flag", "[daemon][streaming][unit]") {
    auto handler = std::make_shared<SuccessHandler<PongResponse>>();

    SECTION("Sets success on PongResponse") {
        PongResponse resp;
        Response response = resp;
        handler->onHeaderReceived(response);

        REQUIRE(handler->success == true);
        REQUIRE(handler->hasError() == false);
    }

    SECTION("Sets error on ErrorResponse") {
        ErrorResponse err;
        err.code = ErrorCode::Timeout;
        err.message = "Connection timeout";
        Response response = err;
        handler->onHeaderReceived(response);

        REQUIRE(handler->hasError() == true);
        REQUIRE(handler->error().code == ErrorCode::Timeout);
    }
}

TEST_CASE("ErrorOnlyHandler tracks success/error", "[daemon][streaming][unit]") {
    auto handler = std::make_shared<ErrorOnlyHandler>();

    SECTION("Sets success on non-error response") {
        StatusResponse resp;
        resp.lifecycleState = "running";
        Response response = resp;
        handler->onHeaderReceived(response);

        REQUIRE(handler->success == true);
        REQUIRE(handler->hasError() == false);
    }

    SECTION("Sets error on ErrorResponse") {
        ErrorResponse err;
        err.code = ErrorCode::InternalError;
        err.message = "Internal failure";
        Response response = err;
        handler->onHeaderReceived(response);

        REQUIRE(handler->success == false);
        REQUIRE(handler->hasError() == true);
    }
}

TEST_CASE("BaseStreamingHandler onError propagates error", "[daemon][streaming][unit]") {
    auto handler = std::make_shared<UnaryHandler<StatusResponse>>();

    Error testError{ErrorCode::NetworkError, "Connection refused"};
    handler->onError(testError);

    REQUIRE(handler->hasError() == true);
    REQUIRE(handler->error().code == ErrorCode::NetworkError);
    REQUIRE(handler->error().message == "Connection refused");
}

TEST_CASE("UnaryHandler ignores chunks after error", "[daemon][streaming][unit]") {
    auto handler = std::make_shared<UnaryHandler<StatusResponse>>();

    SECTION("Stops processing on error chunks") {
        ErrorResponse err;
        err.code = ErrorCode::InvalidState;
        Response errorResponse = err;
        bool result = handler->onChunkReceived(errorResponse, false);

        REQUIRE(result == false);
        REQUIRE(handler->hasError() == true);
    }

    SECTION("Continues processing on non-error chunks (design allows chunks after header)") {
        StatusResponse status;
        status.lifecycleState = "running";
        Response successResponse = status;

        // UnaryHandler continues processing on chunks
        // It only stores value in onHeaderReceived, not onChunkReceived
        bool result = handler->onChunkReceived(successResponse, false);

        REQUIRE(result == true);
        REQUIRE(handler->hasError() == false);
    }
}

TEST_CASE("SuccessHandler onChunkReceived logic", "[daemon][streaming][unit]") {
    auto handler = std::make_shared<SuccessHandler<PongResponse>>();

    SECTION("Continues processing on PongResponse chunks") {
        PongResponse resp;
        Response response = resp;
        bool result = handler->onChunkReceived(response, false);

        REQUIRE(result == true);
        REQUIRE(handler->success == true);
    }

    SECTION("Stops processing on error chunks") {
        ErrorResponse err;
        err.code = ErrorCode::PermissionDenied;
        Response errorResponse = err;
        bool result = handler->onChunkReceived(errorResponse, false);

        REQUIRE(result == false);
        REQUIRE(handler->hasError() == true);
    }
}
