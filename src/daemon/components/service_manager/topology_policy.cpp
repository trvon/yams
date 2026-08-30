#define YAMS_DAEMON_TEST_HOOKS_IMPL 1
#include <yams/daemon/components/ServiceManager.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include <spdlog/spdlog.h>

#include <yams/config/config_helpers.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/TopologyTuner.h>
#include <yams/topology/topology_factory.h>

#undef YAMS_DAEMON_TEST_HOOKS_IMPL

namespace yams::daemon {

void ServiceManager::configureTopologyRuntime() {
    {
        auto enginePolicy = ConfigResolver::resolveTopologyEnginePolicy();
        if (enginePolicy.engine) {
            const auto resolved = std::string{topology::resolveFactoryKey(*enginePolicy.engine)};
            tuningConfig_.topologyAlgorithm = resolved;
            spdlog::info("Topology engine applied via config: {} (resolved={})",
                         *enginePolicy.engine, resolved);
        }
        if (enginePolicy.routingRepresentativeCount) {
            topologyManager_.setRoutingRepresentativeCount(
                *enginePolicy.routingRepresentativeCount);
            spdlog::info("Topology routing representatives applied via config: {}",
                         topologyManager_.routingRepresentativeCount());
        }
        if (enginePolicy.boundarySpillEnabled) {
            topologyManager_.setBoundarySpillPolicy(
                *enginePolicy.boundarySpillEnabled, enginePolicy.boundarySpillLimit.value_or(1),
                enginePolicy.boundarySpillDistanceRatio.value_or(1.05),
                enginePolicy.boundarySpillResidualPenalty.value_or(1.0));
            spdlog::info("Topology SOAR boundary spill applied via config: enabled={}",
                         topologyManager_.boundarySpillEnabled());
        }
        topology::FeatureComposition featureComposition;
        featureComposition.enableEntityFusion =
            enginePolicy.featureEntityFusion.value_or(featureComposition.enableEntityFusion);
        featureComposition.entitySignatureK =
            enginePolicy.featureEntitySignatureK.value_or(featureComposition.entitySignatureK);
        featureComposition.entityFusionAlpha =
            enginePolicy.featureEntityFusionAlpha.value_or(featureComposition.entityFusionAlpha);
        featureComposition.entityMinConfidence = enginePolicy.featureEntityMinConfidence.value_or(
            featureComposition.entityMinConfidence);
        featureComposition.enableMatryoshkaCoarseView =
            enginePolicy.featureMatryoshkaCoarseView.value_or(
                featureComposition.enableMatryoshkaCoarseView);
        featureComposition.matryoshkaTargetDim = enginePolicy.featureMatryoshkaTargetDim.value_or(
            featureComposition.matryoshkaTargetDim);
        featureComposition.enableMinHashSketch =
            enginePolicy.featureMinHashSketch.value_or(featureComposition.enableMinHashSketch);
        featureComposition.minhashSketchDim =
            enginePolicy.featureMinHashSketchDim.value_or(featureComposition.minhashSketchDim);
        featureComposition.minhashAlpha =
            enginePolicy.featureMinHashAlpha.value_or(featureComposition.minhashAlpha);
        topologyManager_.setFeatureComposition(std::move(featureComposition));
    }

    // Throttle topology rebuild scheduling during bulk ingest. The environment override retains
    // compatibility with existing deployments; zero disables throttling.
    {
        std::int64_t throttleMs = 60'000;
        if (const std::string raw =
                yams::config::getenv_copy("YAMS_TOPOLOGY_REBUILD_MIN_INTERVAL_MS");
            !raw.empty()) {
            try {
                throttleMs = std::max<std::int64_t>(0, std::stoll(raw));
            } catch (const std::exception& error) {
                spdlog::debug("Invalid YAMS_TOPOLOGY_REBUILD_MIN_INTERVAL_MS '{}': {}", raw,
                              error.what());
            } catch (...) {
                spdlog::debug("Invalid YAMS_TOPOLOGY_REBUILD_MIN_INTERVAL_MS '{}': unknown "
                              "error",
                              raw);
            }
        }
        topologyManager_.setRebuildMinIntervalMs(throttleMs);
        spdlog::info("[ServiceManager] TopologyManager rebuild throttle = {} ms", throttleMs);
    }

    // The adaptive topology tuner is opt-in via [topology.tuner].enabled=true.
    auto tunerPolicy = ConfigResolver::resolveTopologyTunerPolicy();
    if (!tunerPolicy.enabled.value_or(false)) {
        return;
    }

    TopologyTunerConfig tunerConfig;
    tunerConfig.enabled = true;
    if (tunerPolicy.cooldownMinutes) {
        tunerConfig.cooldown = std::chrono::minutes{*tunerPolicy.cooldownMinutes};
    }
    if (tunerPolicy.docCountDelta) {
        tunerConfig.docCountDelta = *tunerPolicy.docCountDelta;
    }
    if (tunerPolicy.rewardAlphaSingleton) {
        tunerConfig.weights.alphaSingleton = *tunerPolicy.rewardAlphaSingleton;
    }
    if (tunerPolicy.rewardBetaGiantCluster) {
        tunerConfig.weights.betaGiantCluster = *tunerPolicy.rewardBetaGiantCluster;
    }
    if (tunerPolicy.rewardGammaGiniDeviation) {
        tunerConfig.weights.gammaGiniDeviation = *tunerPolicy.rewardGammaGiniDeviation;
    }
    if (tunerPolicy.rewardDeltaIntraEdge) {
        tunerConfig.weights.deltaIntraEdge = *tunerPolicy.rewardDeltaIntraEdge;
    }
    if (tunerPolicy.rewardMode) {
        std::string mode = *tunerPolicy.rewardMode;
        std::transform(mode.begin(), mode.end(), mode.begin(),
                       [](unsigned char character) { return std::tolower(character); });
        if (mode == "persistence") {
            tunerConfig.rewardMode = TunerRewardMode::Persistence;
        } else if (mode == "hybrid") {
            tunerConfig.rewardMode = TunerRewardMode::Hybrid;
        } else {
            tunerConfig.rewardMode = TunerRewardMode::Geometric;
        }
    }
    if (tunerPolicy.persistenceSampleSize) {
        tunerConfig.persistenceSampleSize = *tunerPolicy.persistenceSampleSize;
    }
    if (!config_.dataDir.empty()) {
        tunerConfig.statePath = config_.dataDir / "topology_tuner_state.json";
    }

    constexpr std::size_t kInitialCorpusEstimate = 5000;
    auto tuner = std::make_shared<TopologyTuner>(tunerConfig);
    tuner->setArms(defaultArmGrid(kInitialCorpusEstimate));
    if (tunerConfig.statePath) {
        if (auto result = tuner->loadState(*tunerConfig.statePath); !result) {
            spdlog::debug("[ServiceManager] topology tuner: no prior state at {} ({})",
                          tunerConfig.statePath->string(), result.error().message);
        } else {
            spdlog::info("[ServiceManager] topology tuner loaded state from {}",
                         tunerConfig.statePath->string());
        }
    }
    topologyManager_.setTopologyTuner(tuner);
    spdlog::info("[ServiceManager] topology tuner enabled (cooldown={}min "
                 "doc_delta={} arms={} state={})",
                 std::chrono::duration_cast<std::chrono::minutes>(tunerConfig.cooldown).count(),
                 tunerConfig.docCountDelta, tuner->arms().size(),
                 tunerConfig.statePath ? tunerConfig.statePath->string()
                                       : std::string{"<ephemeral>"});
}

} // namespace yams::daemon
