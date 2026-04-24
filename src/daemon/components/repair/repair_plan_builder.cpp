#include <yams/daemon/components/repair/repair_plan_builder.h>

namespace yams::daemon::repair {

RepairRequest RepairPlanBuilder::buildFast(const RepairHealthSnapshot& health) {
    RepairRequest req;
    // Fast auto-repair must stay cheap: it runs frequently and should only
    // schedule bounded integrity repair derived from the fast health probe.
    // Semantic dedupe is intentionally excluded; it can delete documents and
    // scans duplicate groups, so it belongs behind explicit user repair.
    req.repairGraph = (!health.graphIntegrityOk || health.graphDocNodeGap > 0);
    return req;
}

RepairRequest RepairPlanBuilder::buildWarm(const RepairHealthSnapshot& health, int32_t maxRetries) {
    RepairRequest req;
    // Warm auto-repair should not invoke the synchronous full-corpus FTS5 or
    // embedding repair RPC paths. The background RepairService loop already
    // handles missing FTS5/embedding work incrementally through bounded queues;
    // this plan is only for cheap safety checks that don't monopolize SQLite or
    // embedding workers.
    (void)health.missingFts5;
    (void)health.missingEmbeddings;
    req.repairGraph = (!health.graphIntegrityOk || health.graphDocNodeGap > 0);
    req.repairStuckDocs = true;
    if (maxRetries > 0)
        req.maxRetries = maxRetries;
    return req;
}

RepairRequest RepairPlanBuilder::buildCold() {
    RepairRequest req;
    req.repairOrphans = true;
    req.repairMime = true;
    req.repairDownloads = true;
    req.repairPathTree = true;
    req.repairChunks = true;
    req.repairBlockRefs = true;
    // Dedupe and VACUUM/ANALYZE are heavyweight and potentially disruptive.
    // Keep them as explicit user-requested operations, not background repair.
    return req;
}

bool RepairPlanBuilder::hasWork(const RepairRequest& request) {
    return request.repairOrphans || request.repairMime || request.repairDownloads ||
           request.repairPathTree || request.repairChunks || request.repairBlockRefs ||
           request.repairFts5 || request.repairEmbeddings || request.repairStuckDocs ||
           request.repairGraph || request.repairDedupe || request.optimizeDb;
}

} // namespace yams::daemon::repair
