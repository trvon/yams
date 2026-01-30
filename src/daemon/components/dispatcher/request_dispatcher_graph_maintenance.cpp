// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 YAMS Contributors

#include <spdlog/spdlog.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>

namespace yams::daemon {

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphRepairRequest(const GraphRepairRequest& req) {
    spdlog::info("[GraphRepair] request: dryRun={}", req.dryRun);

    auto graphComponent = serviceManager_ ? serviceManager_->getGraphComponent() : nullptr;
    if (!graphComponent) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "GraphComponent not available"};
    }

    auto result = graphComponent->repairGraph(req.dryRun);
    if (!result) {
        co_return ErrorResponse{.code = result.error().code, .message = result.error().message};
    }

    const auto& stats = result.value();
    GraphRepairResponse resp;
    resp.nodesCreated = stats.nodesCreated;
    resp.nodesUpdated = stats.nodesUpdated;
    resp.edgesCreated = stats.edgesCreated;
    resp.errors = stats.errors;
    resp.issues = stats.issues;
    resp.dryRun = req.dryRun;

    spdlog::info("[GraphRepair] complete: nodes={}, edges={}, errors={}, dryRun={}",
                 stats.nodesCreated, stats.edgesCreated, stats.errors, req.dryRun);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGraphValidateRequest(const GraphValidateRequest& /*req*/) {
    spdlog::info("[GraphValidate] request");

    auto graphComponent = serviceManager_ ? serviceManager_->getGraphComponent() : nullptr;
    if (!graphComponent) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "GraphComponent not available"};
    }

    auto result = graphComponent->validateGraph();
    if (!result) {
        co_return ErrorResponse{.code = result.error().code, .message = result.error().message};
    }

    const auto& report = result.value();
    GraphValidateResponse resp;
    resp.totalNodes = report.totalNodes;
    resp.totalEdges = report.totalEdges;
    resp.orphanedNodes = report.orphanedNodes;
    resp.unreachableNodes = report.unreachableNodes;
    resp.issues = report.issues;

    spdlog::info("[GraphValidate] complete: nodes={}, edges={}, orphans={}, issues={}",
                 report.totalNodes, report.totalEdges, report.orphanedNodes, report.issues.size());
    co_return resp;
}

} // namespace yams::daemon
