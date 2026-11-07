#include "boost_awaitable_analyzer_probe.h"

boost::asio::awaitable<ProbeStatus>
ProbeHandler::stream(boost::asio::local::stream_protocol::socket&, const ProbeRequest&,
                     std::uint64_t) {
    co_return ProbeStatus{};
}
