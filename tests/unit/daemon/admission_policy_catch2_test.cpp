#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/AdmissionPolicy.h>

using namespace yams::daemon;

TEST_CASE("AdmissionPolicy classifies representative requests",
          "[daemon][admission-policy][catch2]") {
    CHECK(AdmissionPolicy::classify(Request{StatusRequest{}}) == CommandClass::control);
    CHECK(AdmissionPolicy::classify(Request{PingRequest{}}) == CommandClass::control);
    CHECK(AdmissionPolicy::classify(Request{ShutdownRequest{}}) == CommandClass::control);
    CHECK(AdmissionPolicy::classify(Request{CancelRequest{}}) == CommandClass::control);

    CHECK(AdmissionPolicy::classify(Request{ListRequest{}}) == CommandClass::read);
    CHECK(AdmissionPolicy::classify(Request{GetRequest{}}) == CommandClass::read);
    CHECK(AdmissionPolicy::classify(Request{GetInitRequest{}}) == CommandClass::read);
    CHECK(AdmissionPolicy::classify(Request{GetChunkRequest{}}) == CommandClass::read);
    CHECK(AdmissionPolicy::classify(Request{GetEndRequest{}}) == CommandClass::read);
    CHECK(AdmissionPolicy::classify(Request{CatRequest{}}) == CommandClass::read);
    CHECK(AdmissionPolicy::classify(Request{DownloadRequest{}}) == CommandClass::read);
    CHECK(AdmissionPolicy::classify(Request{MetadataValueCountsRequest{}}) == CommandClass::read);

    CHECK(AdmissionPolicy::classify(Request{SearchRequest{}}) == CommandClass::search);
    CHECK(AdmissionPolicy::classify(Request{GrepRequest{}}) == CommandClass::search);
    CHECK(AdmissionPolicy::classify(Request{GraphQueryRequest{}}) == CommandClass::search);

    CHECK(AdmissionPolicy::classify(Request{AddDocumentRequest{}}) == CommandClass::write);
    CHECK(AdmissionPolicy::classify(Request{UpdateDocumentRequest{}}) == CommandClass::write);
    CHECK(AdmissionPolicy::classify(Request{DeleteRequest{}}) == CommandClass::write);
    CHECK(AdmissionPolicy::classify(Request{BatchRequest{}}) == CommandClass::write);
    CHECK(AdmissionPolicy::classify(Request{RepairRequest{}}) == CommandClass::write);
    CHECK(AdmissionPolicy::classify(Request{PluginLoadRequest{}}) == CommandClass::write);
}

TEST_CASE("AdmissionPolicy computes socket headroom bands", "[daemon][admission-policy][catch2]") {
    SECTION("small limit") {
        const auto bands = AdmissionPolicy::computeSocketHeadroomBands(1);
        CHECK(bands.general == 1);
        CHECK(bands.write == 2);
        CHECK(bands.interactive == 2);
        CHECK(bands.search == 3);
    }

    SECTION("medium limit") {
        const auto bands = AdmissionPolicy::computeSocketHeadroomBands(12);
        CHECK(bands.general == 2);
        CHECK(bands.write == 4);
        CHECK(bands.interactive == 3);
        CHECK(bands.search == 4);
    }
}

TEST_CASE("AdmissionPolicy evaluates socket admission consistently",
          "[daemon][admission-policy][catch2]") {
    constexpr std::size_t kLimit = 12;

    SECTION("control commands bypass admission") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 100, kLimit, CommandClass::control,
                                                       false) == SocketAdmissionVerdict::bypass);
    }

    SECTION("proxy traffic bypasses admission") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 100, kLimit, CommandClass::write,
                                                       true) == SocketAdmissionVerdict::bypass);
    }

    SECTION("under soft limit admits all non-control commands") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit - 1, kLimit, CommandClass::read,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit - 1, kLimit, CommandClass::search,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit - 1, kLimit, CommandClass::write,
                                                       false) == SocketAdmissionVerdict::admit);
    }

    SECTION("general headroom admits every command class") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 1, kLimit, CommandClass::read,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 1, kLimit, CommandClass::write,
                                                       false) == SocketAdmissionVerdict::admit);
    }

    SECTION("interactive headroom favors reads") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 3, kLimit, CommandClass::read,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 3, kLimit, CommandClass::search,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 3, kLimit, CommandClass::write,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 5, kLimit, CommandClass::read,
                                                       false) == SocketAdmissionVerdict::reject);
    }

    SECTION("search gets one extra headroom band") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 5, kLimit, CommandClass::search,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 6, kLimit, CommandClass::search,
                                                       false) == SocketAdmissionVerdict::reject);
    }

    SECTION("write headroom extends beyond interactive headroom") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 5, kLimit, CommandClass::write,
                                                       false) == SocketAdmissionVerdict::admit);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 5, kLimit, CommandClass::read,
                                                       false) == SocketAdmissionVerdict::reject);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 6, kLimit, CommandClass::read,
                                                       false) == SocketAdmissionVerdict::reject);
    }

    SECTION("beyond all headroom rejects") {
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 7, kLimit, CommandClass::read,
                                                       false) == SocketAdmissionVerdict::reject);
        CHECK(AdmissionPolicy::evaluateSocketAdmission(kLimit + 8, kLimit, CommandClass::write,
                                                       false) == SocketAdmissionVerdict::reject);
    }
}

TEST_CASE("AdmissionPolicy emergency session limit mirrors legacy behavior",
          "[daemon][admission-policy][catch2]") {
    CHECK(AdmissionPolicy::emergencySessionLimit(0) == 64);
    CHECK(AdmissionPolicy::emergencySessionLimit(12) == 32);
    CHECK(AdmissionPolicy::emergencySessionLimit(40) == 80);
}
