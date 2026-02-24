#include <yams/daemon/components/repair/repair_plan_builder.h>

namespace yams::daemon::repair {

RepairRequest RepairPlanBuilder::buildFast(const RepairHealthSnapshot& health) {
    RepairRequest req;
    req.repairGraph = (!health.graphIntegrityOk || health.graphDocNodeGap > 0);
    return req;
}

RepairRequest RepairPlanBuilder::buildWarm(const RepairHealthSnapshot& health, int32_t maxRetries) {
    RepairRequest req;
    req.repairFts5 = (health.missingFts5 > 0);
    req.repairEmbeddings = (health.missingEmbeddings > 0);
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
    req.optimizeDb = true;
    return req;
}

bool RepairPlanBuilder::hasWork(const RepairRequest& request) {
    return request.repairOrphans || request.repairMime || request.repairDownloads ||
           request.repairPathTree || request.repairChunks || request.repairBlockRefs ||
           request.repairFts5 || request.repairEmbeddings || request.repairStuckDocs ||
           request.repairGraph || request.optimizeDb;
}

} // namespace yams::daemon::repair
