#pragma once

#include <algorithm>
#include <cstddef>

#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

enum class CommandClass {
    control,
    read,
    search,
    write,
};

enum class SocketAdmissionVerdict {
    bypass,
    admit,
    reject,
};

struct HeadroomBands {
    std::size_t general{0};
    std::size_t write{0};
    std::size_t interactive{0};
};

class AdmissionPolicy {
public:
    static CommandClass classify(const Request& request) {
        if (std::holds_alternative<StatusRequest>(request) ||
            std::holds_alternative<PingRequest>(request) ||
            std::holds_alternative<ShutdownRequest>(request) ||
            std::holds_alternative<CancelRequest>(request)) {
            return CommandClass::control;
        }

        if (std::holds_alternative<SearchRequest>(request) ||
            std::holds_alternative<GrepRequest>(request) ||
            std::holds_alternative<GraphQueryRequest>(request) ||
            std::holds_alternative<GraphPathHistoryRequest>(request)) {
            return CommandClass::search;
        }

        if (std::holds_alternative<AddDocumentRequest>(request) ||
            std::holds_alternative<UpdateDocumentRequest>(request) ||
            std::holds_alternative<DeleteRequest>(request) ||
            std::holds_alternative<BatchRequest>(request) ||
            std::holds_alternative<GenerateEmbeddingRequest>(request) ||
            std::holds_alternative<BatchEmbeddingRequest>(request) ||
            std::holds_alternative<EmbedDocumentsRequest>(request) ||
            std::holds_alternative<LoadModelRequest>(request) ||
            std::holds_alternative<UnloadModelRequest>(request) ||
            std::holds_alternative<RepairRequest>(request) ||
            std::holds_alternative<GraphRepairRequest>(request) ||
            std::holds_alternative<KgIngestRequest>(request) ||
            std::holds_alternative<CancelDownloadJobRequest>(request) ||
            std::holds_alternative<PluginScanRequest>(request) ||
            std::holds_alternative<PluginLoadRequest>(request) ||
            std::holds_alternative<PluginUnloadRequest>(request) ||
            std::holds_alternative<PluginTrustAddRequest>(request) ||
            std::holds_alternative<PluginTrustRemoveRequest>(request) ||
            std::holds_alternative<UseSessionRequest>(request) ||
            std::holds_alternative<AddPathSelectorRequest>(request) ||
            std::holds_alternative<RemovePathSelectorRequest>(request) ||
            std::holds_alternative<PruneRequest>(request) ||
            std::holds_alternative<RestoreCollectionRequest>(request) ||
            std::holds_alternative<RestoreSnapshotRequest>(request)) {
            return CommandClass::write;
        }

        return CommandClass::read;
    }

    static HeadroomBands computeSocketHeadroomBands(std::size_t softLimit) {
        HeadroomBands bands;
        bands.general = std::max<std::size_t>(1, softLimit / 6);
        bands.write = std::max<std::size_t>(2, softLimit / 3);
        bands.interactive = std::max<std::size_t>(1, softLimit / 6);
        return bands;
    }

    static SocketAdmissionVerdict evaluateSocketAdmission(std::size_t activeConnections,
                                                          std::size_t softLimit,
                                                          CommandClass commandClass, bool isProxy) {
        if (isProxy || commandClass == CommandClass::control) {
            return SocketAdmissionVerdict::bypass;
        }
        if (softLimit == 0 || activeConnections < softLimit) {
            return SocketAdmissionVerdict::admit;
        }

        const auto bands = computeSocketHeadroomBands(softLimit);
        if (activeConnections < (softLimit + bands.general)) {
            return SocketAdmissionVerdict::admit;
        }

        if (commandClass == CommandClass::write &&
            activeConnections < (softLimit + bands.general + bands.write)) {
            return SocketAdmissionVerdict::admit;
        }

        if ((commandClass == CommandClass::read || commandClass == CommandClass::search) &&
            activeConnections < (softLimit + bands.general + bands.interactive)) {
            return SocketAdmissionVerdict::admit;
        }

        return SocketAdmissionVerdict::reject;
    }

    static SocketAdmissionVerdict evaluateSocketAdmission(std::size_t activeConnections,
                                                          std::size_t softLimit,
                                                          const Request& request, bool isProxy) {
        return evaluateSocketAdmission(activeConnections, softLimit, classify(request), isProxy);
    }

    static std::size_t emergencySessionLimit(std::size_t softLimit) {
        if (softLimit == 0) {
            return 64;
        }
        return std::max<std::size_t>(32, std::max<std::size_t>(softLimit * 2, softLimit + 16));
    }
};

} // namespace yams::daemon
