#pragma once
// Independent Boost-only characterization; no YAMS headers or runtime state.
#include <cstdint>
#include <string>
#include <utility>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/local/stream_protocol.hpp>

struct ProbeStatus {
    int code{};
    std::string message;
};
struct ProbeRequest {
    std::string payload;
};
class ProbeHandler {
public:
    boost::asio::awaitable<ProbeStatus> stream(boost::asio::local::stream_protocol::socket&,
                                               const ProbeRequest&, std::uint64_t);
    boost::asio::awaitable<ProbeStatus> request(boost::asio::local::stream_protocol::socket&,
                                                const ProbeRequest&, std::uint64_t);
};
