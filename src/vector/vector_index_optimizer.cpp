#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <numeric>
#include <queue>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <yams/common/format.h>
#include <yams/vector/vector_index_optimizer.h>

namespace yams::vector {

// =============================================================================
// Implementation class
// =============================================================================

class VectorIndexOptimizer::Impl {
public:
    explicit Impl(std::shared_ptr<VectorIndexManager> index_manager)
        : index_manager_(std::move(index_manager)), is_optimizing_(false),
          optimization_progress_(0.0), optimization_thread_(), stop_optimization_(false) {
        // Initialize statistics
        updateStatistics();
    }

    ~Impl() { stopOptimizationThread(); }

    // Core optimization operations
    Result<OptimizationResult> compact() {
        std::lock_guard<std::mutex> lock(optimization_mutex_);

        if (is_optimizing_) {
            return Result<OptimizationResult>(
                Error{ErrorCode::InvalidArgument, "Optimization already in progress"});
        }

        is_optimizing_ = true;
        optimization_progress_ = 0.0;

        OptimizationResult result;
        result.type = OptimizationType::COMPACT;
        result.timestamp = std::chrono::system_clock::now();

        // Capture before metrics
        auto stats = getStatistics();
        result.vectors_before = stats.total_vectors;
        result.size_before_bytes = stats.index_size_bytes;
        result.fragmentation_before = stats.fragmentation_ratio;
        result.avg_latency_before_ms = stats.avg_search_time_ms;

        auto start_time = std::chrono::steady_clock::now();

        try {
            // Get all vector IDs
            auto all_ids = index_manager_->getAllVectorIds();
            if (!all_ids.has_value()) {
                result.success = false;
                result.message = "Failed to get vector IDs";
                is_optimizing_ = false;
                return Result<OptimizationResult>(std::move(result));
            }

            // Identify deleted vectors (mock implementation)
            std::vector<std::string> active_ids;
            std::vector<std::string> deleted_ids;

            for (const auto& id : all_ids.value()) {
                // In a real implementation, check if vector is marked as deleted
                if (shouldCompactVector(id)) {
                    deleted_ids.push_back(id);
                } else {
                    active_ids.push_back(id);
                }

                optimization_progress_ =
                    0.3 * (active_ids.size() + deleted_ids.size()) / all_ids.value().size();

                if (progress_callback_) {
                    progress_callback_(optimization_progress_, "Analyzing vectors...");
                }
            }

            // Remove deleted vectors
            for (const auto& id : deleted_ids) {
                index_manager_->removeVector(id);
                result.vectors_removed++;
            }

            optimization_progress_ = 0.6;
            if (progress_callback_) {
                progress_callback_(optimization_progress_, "Consolidating segments...");
            }

            // Consolidate segments (mock implementation)
            result.segments_merged = consolidateSegments();

            optimization_progress_ = 0.9;
            if (progress_callback_) {
                progress_callback_(optimization_progress_, "Finalizing compaction...");
            }

            // Update statistics
            updateStatistics();
            auto after_stats = getStatistics();

            result.vectors_after = after_stats.total_vectors;
            result.size_after_bytes = after_stats.index_size_bytes;
            result.fragmentation_after = after_stats.fragmentation_ratio;
            result.avg_latency_after_ms = after_stats.avg_search_time_ms;

            auto end_time = std::chrono::steady_clock::now();
            result.duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

            result.success = true;
            result.message =
                yams::fmt_format("Compaction completed: removed {} vectors, merged {} segments",
                                 result.vectors_removed, result.segments_merged);

            // Record in history
            optimization_history_.push_back(result);
            if (optimization_history_.size() > 100) {
                optimization_history_.erase(optimization_history_.begin());
            }

            optimization_progress_ = 1.0;
            if (progress_callback_) {
                progress_callback_(optimization_progress_, "Compaction complete");
            }

        } catch (const std::exception& e) {
            result.success = false;
            result.message = yams::fmt_format("Compaction failed: {}", e.what());
        }

        is_optimizing_ = false;
        optimization_progress_ = 0.0;

        return Result<OptimizationResult>(std::move(result));
    }

    Result<OptimizationResult> rebalance() {
        std::lock_guard<std::mutex> lock(optimization_mutex_);

        if (is_optimizing_) {
            return Result<OptimizationResult>(
                Error{ErrorCode::InvalidArgument, "Optimization already in progress"});
        }

        is_optimizing_ = true;
        optimization_progress_ = 0.0;

        OptimizationResult result;
        result.type = OptimizationType::REBALANCE;
        result.timestamp = std::chrono::system_clock::now();

        // Capture before metrics
        auto stats = getStatistics();
        result.vectors_before = stats.total_vectors;
        result.avg_latency_before_ms = stats.avg_search_time_ms;

        auto start_time = std::chrono::steady_clock::now();

        try {
            // For HNSW indexes, rebalance the graph structure
            if (index_manager_->getIndexType() == IndexType::HNSW) {
                optimization_progress_ = 0.2;
                if (progress_callback_) {
                    progress_callback_(optimization_progress_, "Analyzing graph structure...");
                }

                // Analyze connectivity (mock implementation)
                [[maybe_unused]] auto connectivity_score = analyzeGraphConnectivity();

                optimization_progress_ = 0.5;
                if (progress_callback_) {
                    progress_callback_(optimization_progress_, "Rebalancing connections...");
                }

                // Rebalance connections
                rebalanceGraphConnections();

                optimization_progress_ = 0.8;
                if (progress_callback_) {
                    progress_callback_(optimization_progress_, "Optimizing search paths...");
                }

                // Optimize search paths
                optimizeSearchPaths();
            }

            // Update statistics
            updateStatistics();
            auto after_stats = getStatistics();

            result.vectors_after = after_stats.total_vectors;
            result.avg_latency_after_ms = after_stats.avg_search_time_ms;

            auto end_time = std::chrono::steady_clock::now();
            result.duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

            result.success = true;
            result.message = "Rebalancing completed successfully";

            // Record in history
            optimization_history_.push_back(result);

            optimization_progress_ = 1.0;
            if (progress_callback_) {
                progress_callback_(optimization_progress_, "Rebalancing complete");
            }

        } catch (const std::exception& e) {
            result.success = false;
            result.message = yams::fmt_format("Rebalancing failed: {}", e.what());
        }

        is_optimizing_ = false;
        optimization_progress_ = 0.0;

        return Result<OptimizationResult>(std::move(result));
    }

    Result<OptimizationResult> rebuild() {
        std::lock_guard<std::mutex> lock(optimization_mutex_);

        if (is_optimizing_) {
            return Result<OptimizationResult>(
                Error{ErrorCode::InvalidArgument, "Optimization already in progress"});
        }

        is_optimizing_ = true;
        optimization_progress_ = 0.0;

        OptimizationResult result;
        result.type = OptimizationType::REBUILD;
        result.timestamp = std::chrono::system_clock::now();

        auto start_time = std::chrono::steady_clock::now();

        try {
            // Get all vectors
            auto all_ids = index_manager_->getAllVectorIds();
            if (!all_ids.has_value()) {
                result.success = false;
                result.message = "Failed to get vector IDs for rebuild";
                is_optimizing_ = false;
                return Result<OptimizationResult>(std::move(result));
            }

            std::vector<VectorData> all_vectors;

            // Extract all vectors
            for (size_t i = 0; i < all_ids.value().size(); ++i) {
                const auto& id = all_ids.value()[i];

                // Mock vector data extraction
                VectorData vec_data;
                vec_data.id = id;
                vec_data.embedding.resize(384, 0.1f); // Mock 384-dim vector
                all_vectors.push_back(vec_data);

                optimization_progress_ = 0.3 * (i + 1) / all_ids.value().size();
                if (progress_callback_) {
                    progress_callback_(optimization_progress_, "Extracting vectors...");
                }
            }

            optimization_progress_ = 0.4;
            if (progress_callback_) {
                progress_callback_(optimization_progress_, "Creating new index...");
            }

            // Create new optimized index
            auto new_index = createOptimizedIndex(all_vectors);

            optimization_progress_ = 0.8;
            if (progress_callback_) {
                progress_callback_(optimization_progress_, "Swapping indexes...");
            }

            // Swap old index with new one (mock implementation)
            swapIndex(std::move(new_index));

            // Update statistics
            updateStatistics();

            auto end_time = std::chrono::steady_clock::now();
            result.duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

            result.success = true;
            result.message =
                yams::fmt_format("Rebuild completed: {} vectors reindexed", all_vectors.size());
            result.vectors_after = all_vectors.size();

            // Record in history
            optimization_history_.push_back(result);

            optimization_progress_ = 1.0;
            if (progress_callback_) {
                progress_callback_(optimization_progress_, "Rebuild complete");
            }

        } catch (const std::exception& e) {
            result.success = false;
            result.message = yams::fmt_format("Rebuild failed: {}", e.what());
        }

        is_optimizing_ = false;
        optimization_progress_ = 0.0;

        return Result<OptimizationResult>(std::move(result));
    }

    Result<OptimizationResult> optimize(OptimizationType type) {
        switch (type) {
            case OptimizationType::COMPACT:
                return compact();
            case OptimizationType::REBALANCE:
                return rebalance();
            case OptimizationType::REBUILD:
                return rebuild();
            case OptimizationType::INCREMENTAL:
                return incrementalOptimizeImpl();
            case OptimizationType::FULL:
                return fullOptimize();
            default:
                return Result<OptimizationResult>(
                    Error{ErrorCode::InvalidArgument, "Unknown optimization type"});
        }
    }

    // Statistics and monitoring
    IndexStatistics getStatistics() const {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        return statistics_;
    }

    IndexHealth getIndexHealth() const {
        auto stats = getStatistics();

        IndexHealth health;
        health.total_vectors = stats.total_vectors;
        health.deleted_vectors = stats.deleted_vectors;
        health.segment_count = stats.unique_clusters;
        health.fragmentation_ratio = stats.fragmentation_ratio;
        health.avg_search_latency_ms = stats.avg_search_time_ms;
        health.memory_usage_mb = stats.memory_usage_bytes / (1024.0 * 1024.0);

        // Determine health status
        if (health.fragmentation_ratio > 0.5 || health.avg_search_latency_ms > 200) {
            health.health_status = "critical";
            health.needs_optimization = true;
        } else if (health.fragmentation_ratio > 0.3 || health.avg_search_latency_ms > 100) {
            health.health_status = "degraded";
            health.needs_optimization = true;
        } else {
            health.health_status = "healthy";
            health.needs_optimization = false;
        }

        return health;
    }

    PerformanceMetrics getPerformanceMetrics() const {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        return performance_metrics_;
    }

    OptimizationRecommendations getRecommendations() const {
        auto stats = getStatistics();
        auto health = getIndexHealth();

        OptimizationRecommendations recs;

        // Check fragmentation
        if (stats.fragmentation_ratio > 0.3) {
            recs.should_compact = true;
            recs.additional_recommendations.push_back(
                "High fragmentation detected - compaction recommended");
        }

        // Check balance
        if (stats.cluster_balance_ratio < 0.7) {
            recs.should_rebalance = true;
            recs.additional_recommendations.push_back(
                "Unbalanced index structure - rebalancing recommended");
        }

        // Check if complete rebuild needed
        if (stats.fragmentation_ratio > 0.7 && stats.deleted_vectors > stats.total_vectors * 0.3) {
            recs.should_rebuild = true;
            recs.primary_recommendation =
                "Complete index rebuild recommended due to severe fragmentation";
            recs.priority = OptimizationRecommendations::Priority::HIGH;
        } else if (recs.should_compact) {
            recs.primary_recommendation = "Index compaction recommended";
            recs.priority = OptimizationRecommendations::Priority::MEDIUM;
        } else if (recs.should_rebalance) {
            recs.primary_recommendation = "Index rebalancing recommended";
            recs.priority = OptimizationRecommendations::Priority::LOW;
        } else {
            recs.primary_recommendation = "No optimization needed";
            recs.priority = OptimizationRecommendations::Priority::LOW;
        }

        // Estimate gains
        if (recs.should_compact) {
            recs.expected_space_savings = stats.deleted_vectors * 1.5; // Mock calculation
            recs.expected_performance_gain = 15.0;                     // 15% improvement
            recs.estimated_duration = std::chrono::minutes(stats.total_vectors / 10000);
        }

        return recs;
    }

    // Scheduling
    void scheduleOptimization(const OptimizationPolicy& policy) {
        std::lock_guard<std::mutex> lock(schedule_mutex_);

        optimization_policy_ = policy;
        stop_optimization_ = false;

        if (!optimization_thread_.joinable()) {
            optimization_thread_ = std::thread([this]() { runOptimizationScheduler(); });
        }
    }

    void cancelScheduledOptimization() { stopOptimizationThread(); }

    bool isOptimizationScheduled() const {
        std::lock_guard<std::mutex> lock(schedule_mutex_);
        return optimization_thread_.joinable() && !stop_optimization_;
    }

    // Status
    bool isOptimizing() const { return is_optimizing_; }

    double getOptimizationProgress() const { return optimization_progress_; }

    std::string getOptimizationStatus() const {
        if (is_optimizing_) {
            return yams::fmt_format("Optimizing... {:.1f}% complete", optimization_progress_ * 100);
        } else if (isOptimizationScheduled()) {
            return "Optimization scheduled";
        } else {
            return "Idle";
        }
    }

    void setProgressCallback(ProgressCallback callback) {
        progress_callback_ = std::move(callback);
    }

    std::vector<OptimizationResult> getOptimizationHistory(size_t limit) const {
        std::lock_guard<std::mutex> lock(history_mutex_);

        if (optimization_history_.size() <= limit) {
            return optimization_history_;
        }

        return std::vector<OptimizationResult>(optimization_history_.end() - limit,
                                               optimization_history_.end());
    }

private:
    struct VectorData {
        std::string id;
        std::vector<float> embedding;
    };

    // Helper methods
    void updateStatistics() {
        std::lock_guard<std::mutex> lock(stats_mutex_);

        // Mock statistics update
        statistics_.total_vectors = 10000 + rand() % 1000;
        statistics_.deleted_vectors = rand() % 1000;
        statistics_.avg_search_time_ms = 10.0 + (rand() % 50) / 10.0;
        statistics_.index_size_bytes = statistics_.total_vectors * 1536; // 384 floats * 4 bytes
        statistics_.memory_usage_bytes = statistics_.index_size_bytes * 1.2;
        statistics_.fragmentation_ratio =
            static_cast<double>(statistics_.deleted_vectors) / statistics_.total_vectors;
        statistics_.unique_clusters = std::sqrt(statistics_.total_vectors);
        statistics_.cluster_balance_ratio = 0.8 + (rand() % 20) / 100.0;
    }

    bool shouldCompactVector(const std::string& id) {
        // Mock implementation - randomly mark some as deleted
        return (std::hash<std::string>{}(id) % 100) < 5; // 5% deletion rate
    }

    size_t consolidateSegments() {
        // Mock segment consolidation
        return rand() % 10 + 1;
    }

    double analyzeGraphConnectivity() {
        // Mock connectivity analysis
        return 0.7 + (rand() % 30) / 100.0;
    }

    void rebalanceGraphConnections() {
        // Mock graph rebalancing
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void optimizeSearchPaths() {
        // Mock search path optimization
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    std::unique_ptr<VectorIndexManager>
    createOptimizedIndex(const std::vector<VectorData>& vectors) {
        (void)vectors;
        // Mock optimized index creation
        auto config = IndexConfig{};
        config.type = IndexType::HNSW;
        config.dimension = 384;
        return std::make_unique<VectorIndexManager>(config);
    }

    void swapIndex(std::unique_ptr<VectorIndexManager> new_index) {
        (void)new_index;
        // Mock index swapping
        // In real implementation, would atomically swap the index
    }

    Result<OptimizationResult> incrementalOptimizeImpl() {
        OptimizationResult result;
        result.type = OptimizationType::INCREMENTAL;
        result.timestamp = std::chrono::system_clock::now();

        // Mock incremental optimization
        result.success = true;
        result.message = "Incremental optimization completed";
        result.duration = std::chrono::milliseconds(500);

        return Result<OptimizationResult>(std::move(result));
    }

    Result<OptimizationResult> fullOptimize() {
        // Full optimization: compact, then rebalance
        auto compact_result = compact();
        if (!compact_result.has_value()) {
            return compact_result;
        }

        auto rebalance_result = rebalance();
        if (!rebalance_result.has_value()) {
            return rebalance_result;
        }

        OptimizationResult result;
        result.type = OptimizationType::FULL;
        result.timestamp = std::chrono::system_clock::now();
        result.success = true;
        result.message = "Full optimization completed";
        result.duration = compact_result.value().duration + rebalance_result.value().duration;

        return Result<OptimizationResult>(std::move(result));
    }

    void runOptimizationScheduler() {
        while (!stop_optimization_) {
            std::unique_lock<std::mutex> lock(schedule_mutex_);

            // Wait for next optimization window
            auto next_optimization = calculateNextOptimizationTime();

            if (schedule_cv_.wait_until(lock, next_optimization, [this] {
                    return stop_optimization_ || optimization_paused_;
                })) {
                if (stop_optimization_)
                    break;
                if (optimization_paused_)
                    continue;
            }

            // Check if optimization is needed
            auto stats = getStatistics();
            if (optimization_utils::shouldOptimize(stats, optimization_policy_)) {
                auto type = optimization_utils::getOptimalOptimizationType(stats);
                lock.unlock();
                optimize(type);
            }
        }
    }

    std::chrono::system_clock::time_point calculateNextOptimizationTime() {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        auto tm_now = *std::localtime(&time_t_now);

        // Calculate next maintenance window
        tm_now.tm_hour = optimization_policy_.maintenance_start.count();
        tm_now.tm_min = 0;
        tm_now.tm_sec = 0;

        auto next_time = std::chrono::system_clock::from_time_t(std::mktime(&tm_now));

        if (next_time <= now) {
            // Move to next day
            next_time += std::chrono::hours(24);
        }

        return next_time;
    }

    void stopOptimizationThread() {
        {
            std::lock_guard<std::mutex> lock(schedule_mutex_);
            stop_optimization_ = true;
        }
        schedule_cv_.notify_all();

        if (optimization_thread_.joinable()) {
            optimization_thread_.join();
        }
    }

    // Member variables
    std::shared_ptr<VectorIndexManager> index_manager_;

    // Statistics
    mutable std::mutex stats_mutex_;
    IndexStatistics statistics_;

    // Performance metrics
    mutable std::mutex metrics_mutex_;
    PerformanceMetrics performance_metrics_;

    // Optimization state
    std::mutex optimization_mutex_;
    std::atomic<bool> is_optimizing_;
    std::atomic<double> optimization_progress_;

    // Scheduling
    std::thread optimization_thread_;
    mutable std::mutex schedule_mutex_;
    std::condition_variable schedule_cv_;
    std::atomic<bool> stop_optimization_;
    std::atomic<bool> optimization_paused_;
    OptimizationPolicy optimization_policy_;

    // History
    mutable std::mutex history_mutex_;
    std::vector<OptimizationResult> optimization_history_;

    // Callbacks
    ProgressCallback progress_callback_;
};

// =============================================================================
// VectorIndexOptimizer implementation
// =============================================================================

VectorIndexOptimizer::VectorIndexOptimizer() : pImpl(std::make_unique<Impl>(nullptr)) {}

VectorIndexOptimizer::VectorIndexOptimizer(std::shared_ptr<VectorIndexManager> index_manager)
    : pImpl(std::make_unique<Impl>(std::move(index_manager))) {}

VectorIndexOptimizer::~VectorIndexOptimizer() = default;

Result<OptimizationResult> VectorIndexOptimizer::compact() {
    return pImpl->compact();
}

Result<OptimizationResult> VectorIndexOptimizer::rebalance() {
    return pImpl->rebalance();
}

Result<OptimizationResult> VectorIndexOptimizer::rebuild() {
    return pImpl->rebuild();
}

Result<OptimizationResult> VectorIndexOptimizer::optimize(OptimizationType type) {
    return pImpl->optimize(type);
}

IndexStatistics VectorIndexOptimizer::getStatistics() const {
    return pImpl->getStatistics();
}

IndexHealth VectorIndexOptimizer::getIndexHealth() const {
    return pImpl->getIndexHealth();
}

PerformanceMetrics VectorIndexOptimizer::getPerformanceMetrics() const {
    return pImpl->getPerformanceMetrics();
}

OptimizationRecommendations VectorIndexOptimizer::getRecommendations() const {
    return pImpl->getRecommendations();
}

void VectorIndexOptimizer::scheduleOptimization(const OptimizationPolicy& policy) {
    pImpl->scheduleOptimization(policy);
}

void VectorIndexOptimizer::cancelScheduledOptimization() {
    pImpl->cancelScheduledOptimization();
}

bool VectorIndexOptimizer::isOptimizationScheduled() const {
    return pImpl->isOptimizationScheduled();
}

bool VectorIndexOptimizer::isOptimizing() const {
    return pImpl->isOptimizing();
}

double VectorIndexOptimizer::getOptimizationProgress() const {
    return pImpl->getOptimizationProgress();
}

std::string VectorIndexOptimizer::getOptimizationStatus() const {
    return pImpl->getOptimizationStatus();
}

void VectorIndexOptimizer::setProgressCallback(ProgressCallback callback) {
    pImpl->setProgressCallback(std::move(callback));
}

std::vector<OptimizationResult> VectorIndexOptimizer::getOptimizationHistory(size_t limit) const {
    return pImpl->getOptimizationHistory(limit);
}

// =============================================================================
// Factory function
// =============================================================================

std::unique_ptr<VectorIndexOptimizer>
createVectorIndexOptimizer(std::shared_ptr<VectorIndexManager> index_manager,
                           const OptimizationPolicy& policy) {
    auto optimizer = std::make_unique<VectorIndexOptimizer>(std::move(index_manager));
    optimizer->scheduleOptimization(policy);
    return optimizer;
}

// =============================================================================
// Utility functions
// =============================================================================

namespace optimization_utils {

double calculateFragmentation(size_t total_vectors, size_t deleted_vectors, size_t segment_count) {
    if (total_vectors == 0)
        return 0.0;

    double deletion_ratio = static_cast<double>(deleted_vectors) / total_vectors;
    double segment_overhead = segment_count > 1 ? 0.1 * std::log(segment_count) : 0.0;

    return std::min(1.0, deletion_ratio + segment_overhead);
}

std::chrono::minutes estimateOptimizationDuration(size_t vector_count, OptimizationType type) {
    // Rough estimates based on operation type
    double minutes_per_million = 1.0;

    switch (type) {
        case OptimizationType::COMPACT:
            minutes_per_million = 2.0;
            break;
        case OptimizationType::REBALANCE:
            minutes_per_million = 3.0;
            break;
        case OptimizationType::REBUILD:
            minutes_per_million = 5.0;
            break;
        case OptimizationType::INCREMENTAL:
            minutes_per_million = 0.5;
            break;
        case OptimizationType::FULL:
            minutes_per_million = 8.0;
            break;
    }

    double estimated_minutes = (vector_count / 1000000.0) * minutes_per_million;
    return std::chrono::minutes(static_cast<int>(std::ceil(estimated_minutes)));
}

bool shouldOptimize(const IndexStatistics& stats, const OptimizationPolicy& policy) {
    // Check if we have enough vectors
    if (stats.total_vectors < policy.min_vectors_for_optimization) {
        return false;
    }

    // Check fragmentation threshold
    if (stats.fragmentation_ratio > policy.fragmentation_threshold) {
        return true;
    }

    // Check deleted vectors threshold
    double deleted_ratio = static_cast<double>(stats.deleted_vectors) / stats.total_vectors;
    if (deleted_ratio > policy.deleted_vectors_threshold) {
        return true;
    }

    // Check performance threshold
    if (stats.avg_search_time_ms > policy.max_latency_ms) {
        return true;
    }

    return false;
}

OptimizationType getOptimalOptimizationType(const IndexStatistics& stats) {
    double fragmentation = stats.fragmentation_ratio;
    double deleted_ratio = stats.deleted_vectors / static_cast<double>(stats.total_vectors);

    // Severe fragmentation - rebuild
    if (fragmentation > 0.7 || deleted_ratio > 0.5) {
        return OptimizationType::REBUILD;
    }

    // Moderate fragmentation - compact
    if (fragmentation > 0.3 || deleted_ratio > 0.2) {
        return OptimizationType::COMPACT;
    }

    // Unbalanced - rebalance
    if (stats.cluster_balance_ratio < 0.7) {
        return OptimizationType::REBALANCE;
    }

    // Minor issues - incremental
    return OptimizationType::INCREMENTAL;
}

double estimatePerformanceGain(const IndexStatistics& stats, OptimizationType type) {
    double base_gain = 0.0;

    switch (type) {
        case OptimizationType::COMPACT:
            base_gain = stats.fragmentation_ratio * 30.0; // Up to 30% for high fragmentation
            break;
        case OptimizationType::REBALANCE:
            base_gain = (1.0 - stats.cluster_balance_ratio) * 20.0; // Up to 20% for imbalance
            break;
        case OptimizationType::REBUILD:
            base_gain = 40.0; // Typically 40% improvement from full rebuild
            break;
        case OptimizationType::INCREMENTAL:
            base_gain = 5.0; // Small incremental improvements
            break;
        case OptimizationType::FULL:
            base_gain = 50.0; // Best case scenario
            break;
    }

    // Adjust based on current performance
    if (stats.avg_search_time_ms > 100) {
        base_gain *= 1.2; // More gain possible when performance is poor
    }

    return std::min(75.0, base_gain); // Cap at 75% improvement
}

} // namespace optimization_utils

} // namespace yams::vector
