// SPDX-License-Identifier: GPL-3.0-or-later
// Daemon download job fuzzer - targets download job IPC request/response encoding.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>

using namespace yams::daemon;

namespace {

std::string fuzzString(const uint8_t* data, size_t size, size_t offset, size_t maxLen) {
    if (size == 0 || offset >= size) {
        return {};
    }
    const size_t remaining = size - offset;
    const size_t len = std::min(remaining, maxLen);
    return std::string(reinterpret_cast<const char*>(data + offset), len);
}

DownloadResponse makeJob(const uint8_t* data, size_t size, size_t offset) {
    DownloadResponse resp;
    resp.hash = fuzzString(data, size, offset, 32);
    resp.localPath = fuzzString(data, size, offset + 8, 64);
    resp.url = fuzzString(data, size, offset + 16, 96);
    resp.jobId = fuzzString(data, size, offset + 24, 32);

    static constexpr const char* kStates[] = {"queued", "running", "completed", "failed",
                                              "canceled"};
    const uint8_t selector = (size > offset) ? data[offset] : 0;
    resp.state = kStates[selector % (sizeof(kStates) / sizeof(kStates[0]))];

    resp.createdAtMs = (size > offset + 1) ? data[offset + 1] * 1000ULL : 0;
    resp.updatedAtMs = resp.createdAtMs + ((size > offset + 2) ? data[offset + 2] : 0);
    resp.size = (size > offset + 3) ? static_cast<size_t>(data[offset + 3]) * 1024U : 0U;
    resp.success = (size > offset + 4) ? ((data[offset + 4] & 1U) != 0) : false;
    resp.error = fuzzString(data, size, offset + 32, 96);
    return resp;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    {
        auto decodeResult = ProtoSerializer::decode_payload(std::span<const uint8_t>(data, size));
        if (decodeResult) {
            auto encodeResult = ProtoSerializer::encode_payload(decodeResult.value());
            (void)encodeResult;
        }
    }

    Message msg;
    msg.requestId = data[0];

    switch (data[0] % 6U) {
        case 0: {
            DownloadRequest req;
            req.url = fuzzString(data, size, 1, 128);
            req.outputPath = fuzzString(data, size, 17, 96);
            req.checksum = fuzzString(data, size, 33, 96);
            req.tags.push_back(fuzzString(data, size, 49, 16));
            req.tags.push_back(fuzzString(data, size, 65, 16));
            req.metadata["owner"] = fuzzString(data, size, 81, 16);
            req.metadata["task"] = fuzzString(data, size, 97, 16);
            req.quiet = (size > 113) ? ((data[113] & 1U) != 0) : false;
            msg.payload = std::move(req);
            break;
        }
        case 1: {
            DownloadStatusRequest req;
            req.jobId = fuzzString(data, size, 1, 48);
            msg.payload = std::move(req);
            break;
        }
        case 2: {
            CancelDownloadJobRequest req;
            req.jobId = fuzzString(data, size, 1, 48);
            msg.payload = std::move(req);
            break;
        }
        case 3: {
            ListDownloadJobsRequest req;
            req.limit = (size > 1) ? static_cast<uint32_t>(data[1]) : 0U;
            msg.payload = req;
            break;
        }
        case 4: {
            msg.payload = makeJob(data, size, 1);
            break;
        }
        default: {
            ListDownloadJobsResponse resp;
            const size_t count = std::min<size_t>(3, size > 1 ? (data[1] % 4U) : 1U);
            for (size_t i = 0; i < count; ++i) {
                resp.jobs.push_back(makeJob(data, size, 2 + (i * 11)));
            }
            msg.payload = std::move(resp);
            break;
        }
    }

    auto encodeResult = ProtoSerializer::encode_payload(msg);
    if (encodeResult) {
        auto roundTrip = ProtoSerializer::decode_payload(encodeResult.value());
        (void)roundTrip;
    }

    return 0;
}
