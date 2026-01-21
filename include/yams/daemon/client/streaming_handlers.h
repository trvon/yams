#pragma once

#include <functional>
#include <optional>
#include <variant>

#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon::client {

class BaseStreamingHandler : public DaemonClient::ChunkedResponseHandler {
protected:
    std::optional<Error> error_;

public:
    void onError(const Error& e) override { error_ = e; }
    bool hasError() const { return error_.has_value(); }
    Error error() const { return error_.value_or(Error{ErrorCode::Success, "No error"}); }
};

template <typename ResponseType> class UnaryHandler : public BaseStreamingHandler {
public:
    std::optional<ResponseType> value;

    void onHeaderReceived(const Response& r) override {
        if (auto* resp = std::get_if<ResponseType>(&r)) {
            value = *resp;
        } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
        }
    }

    bool onChunkReceived(const Response& r, bool) override {
        if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
            return false;
        }
        return true;
    }
};

template <typename ResponseType, typename Accumulator>
class ComplexAccumulatingHandler : public BaseStreamingHandler {
public:
    Accumulator accumulator;
    std::optional<ResponseType> value;

    void onHeaderReceived(const Response& r) override {
        if (auto* resp = std::get_if<ResponseType>(&r)) {
            value = *resp;
        } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
        }
    }

    bool onChunkReceived(const Response& r, bool isLast) override {
        if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
            return false;
        }
        if constexpr (requires { accumulator.add(r, isLast); }) {
            if (!accumulator.add(r, isLast)) {
                return false;
            }
        }
        return true;
    }

    Accumulator&& release() { return std::move(accumulator); }
};

template <typename SuccessResponseType> class SuccessHandler : public BaseStreamingHandler {
public:
    bool success = false;

    void onHeaderReceived(const Response& r) override {
        if (std::holds_alternative<SuccessResponseType>(r)) {
            success = true;
        } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
        }
    }

    bool onChunkReceived(const Response& r, bool) override {
        if (std::holds_alternative<SuccessResponseType>(r)) {
            success = true;
        } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
            return false;
        }
        return true;
    }
};

class ErrorOnlyHandler : public BaseStreamingHandler {
public:
    bool success = false;

    void onHeaderReceived(const Response& r) override {
        if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
        } else {
            success = true;
        }
    }

    bool onChunkReceived(const Response& r, bool) override {
        if (auto* err = std::get_if<ErrorResponse>(&r)) {
            error_ = Error{err->code, err->message};
            return false;
        }
        success = true;
        return true;
    }
};

} // namespace yams::daemon::client
