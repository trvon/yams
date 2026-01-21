// Streaming handlers unit tests

#include <catch2/catch_test_macros.hpp>

#include <yams/core/types.h>
#include <yams/daemon/client/streaming_handlers.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;
using namespace yams::daemon::client;
using namespace yams;

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
