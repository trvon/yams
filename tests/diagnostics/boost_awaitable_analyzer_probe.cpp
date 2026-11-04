#include "boost_awaitable_analyzer_probe.h"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>

// The callee is valid and defined in a second translation unit, so this reproduces
// an opaque delegation boundary rather than inlining the trivial child coroutine.
boost::asio::awaitable<ProbeStatus>
ProbeHandler::request(boost::asio::local::stream_protocol::socket& socket,
                      const ProbeRequest& input, std::uint64_t id) {
    co_return co_await stream(socket, input, id);
}
boost::asio::awaitable<int> executorControl() {
    auto executor = co_await boost::asio::this_coro::executor;
    (void)executor;
    co_return 1;
}
int main() {
    boost::asio::io_context context;
    boost::asio::local::stream_protocol::socket socket(context);
    const ProbeRequest input{"owned until completion"};
    ProbeHandler handler;
    auto result =
        boost::asio::co_spawn(context, handler.request(socket, input, 1), boost::asio::use_future);
    auto control = boost::asio::co_spawn(context, executorControl(), boost::asio::use_future);
    context.run();
    return result.get().code == 0 && control.get() == 1 ? 0 : 1;
}
