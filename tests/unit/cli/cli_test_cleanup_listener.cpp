#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

#include <yams/daemon/client/global_io_context.h>

namespace {

class CliTestCleanupListener final : public Catch::EventListenerBase {
public:
    using Catch::EventListenerBase::EventListenerBase;

    void testRunEnded(const Catch::TestRunStats& /*stats*/) override {
        yams::daemon::GlobalIOContext::reset();
    }
};

} // namespace

CATCH_REGISTER_LISTENER(CliTestCleanupListener)
