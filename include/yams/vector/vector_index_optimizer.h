#pragma once

#include <yams/core/types.h>
#include <yams/vector/vector_index_manager.h>
#include <memory>
#include <chrono>
#include <atomic>
#include <thread>
#include <functional>

namespace yams::vector {

/**
 * Index health metrics
 */
struct IndexHealth {
    double fragmentation_ratio = 0.0;  // 0.0 (good) to 1.0 (bad)
    size_t deleted_vectors = 0;
    size_t total_vectors = 0;
    size_t segment_count = 0;
    double avg_search_latency_ms = 0.0;
    double memory_usage_mb = 0.0;
    bool needs_optimization = false;
    std::string health_status;  // "healthy", "degraded", "critical"
};

/**
 * Index statistics for optimization decisions
 */
struct IndexStatistics {
    size_t total_vectors = 0;
    size_t deleted_vectors = 0;
    size_t total_searches = 0;
    size_t total_inserts = 0;
    size_t total_updates = 0;
    size_t total_deletes = 0;
    
    // Performance metrics
    double avg_search_time_ms = 0.0;
    double p95_search_time_ms = 0.0;
    double p99_search_time_ms = 0.0;
    
    // Storage metrics
    size_t index_size_bytes = 0;
    size_t memory_usage_bytes = 0;
    double fragmentation_ratio = 0.0;
    
    // Distribution metrics
    size_t unique_clusters = 0;
    double avg_cluster_size = 0.0;
    double cluster_balance_ratio = 0.0;  // 0.0 (unbalanced) to 1.0 (balanced)
    
    std::chrono::system_clock::time_point last_optimization;
    std::chrono::system_clock::time_point last_compaction;
    std::chrono::system_clock::time_point last_rebalance;
};

/**
 * Performance metrics for monitoring
 */
struct PerformanceMetrics {
    // Real-time metrics
    double current_qps = 0.0;  // Queries per second
    double current_latency_ms = 0.0;
    double current_memory_mb = 0.0;
    double current_cpu_usage = 0.0;
    
    // Historical metrics (last hour)
    double avg_qps = 0.0;
    double avg_latency_ms = 0.0;
    double peak_qps = 0.0;
    double peak_latency_ms = 0.0;
    
    // Optimization impact
    double latency_improvement = 0.0;  // Percentage improvement after last optimization
    double memory_reduction = 0.0;     // MB reduced after last optimization
    
    std::chrono::system_clock::time_point measurement_time;
};

/**
 * Optimization recommendations based on analysis
 */
struct OptimizationRecommendations {
    bool should_compact = false;
    bool should_rebalance = false;
    bool should_rebuild = false;
    bool should_increase_memory = false;
    
    std::string primary_recommendation;
    std::vector<std::string> additional_recommendations;
    
    double expected_performance_gain = 0.0;  // Percentage
    double expected_space_savings = 0.0;     // MB
    std::chrono::minutes estimated_duration;
    
    enum class Priority {
        LOW,
        MEDIUM,
        HIGH,
        CRITICAL
    } priority = Priority::LOW;
};

/**
 * Optimization policy configuration
 */
struct OptimizationPolicy {
    // Scheduling
    std::chrono::hours optimization_interval{24};
    std::chrono::hours compaction_interval{168};  // Weekly
    std::chrono::hours rebalance_interval{720};   // Monthly
    
    // Thresholds for automatic optimization
    double fragmentation_threshold = 0.3;  // Trigger at 30% fragmentation
    double deleted_vectors_threshold = 0.1;  // Trigger at 10% deleted
    size_t min_vectors_for_optimization = 10000;
    
    // Performance thresholds
    double max_latency_ms = 100.0;
    double target_memory_usage_mb = 1024.0;
    
    // Optimization windows
    std::chrono::hours maintenance_start{2};  // 2 AM
    std::chrono::hours maintenance_end{6};    // 6 AM
    bool allow_online_optimization = true;
    
    // Resource limits
    size_t max_memory_for_optimization_mb = 2048;
    size_t max_concurrent_optimizations = 1;
    double max_cpu_usage_percent = 50.0;
};

/**
 * Optimization operation types
 */
enum class OptimizationType {
    COMPACT,      // Remove deleted vectors and consolidate
    REBALANCE,    // Rebalance index structure
    REBUILD,      // Complete index rebuild
    INCREMENTAL,  // Small incremental improvements
    FULL          // Full optimization (all operations)
};

/**
 * Optimization result information
 */
struct OptimizationResult {
    OptimizationType type;
    bool success = false;
    
    // Metrics before optimization
    size_t vectors_before = 0;
    size_t size_before_bytes = 0;
    double fragmentation_before = 0.0;
    double avg_latency_before_ms = 0.0;
    
    // Metrics after optimization
    size_t vectors_after = 0;
    size_t size_after_bytes = 0;
    double fragmentation_after = 0.0;
    double avg_latency_after_ms = 0.0;
    
    // Optimization details
    std::chrono::milliseconds duration;
    size_t vectors_removed = 0;
    size_t segments_merged = 0;
    size_t memory_peak_mb = 0;
    
    std::string message;
    std::chrono::system_clock::time_point timestamp;
};

/**
 * Vector index optimizer for performance and storage optimization
 */
class VectorIndexOptimizer {
public:
    VectorIndexOptimizer();
    explicit VectorIndexOptimizer(std::shared_ptr<VectorIndexManager> index_manager);
    ~VectorIndexOptimizer();
    
    // Core optimization operations
    Result<OptimizationResult> compact();
    Result<OptimizationResult> rebalance();
    Result<OptimizationResult> rebuild();
    Result<OptimizationResult> optimize(OptimizationType type = OptimizationType::FULL);
    
    // Incremental optimization
    Result<void> incrementalOptimize(std::chrono::milliseconds time_budget);
    Result<void> optimizeSegment(const std::string& segment_id);
    
    // Statistics and monitoring
    IndexStatistics getStatistics() const;
    IndexHealth getIndexHealth() const;
    PerformanceMetrics getPerformanceMetrics() const;
    OptimizationRecommendations getRecommendations() const;
    
    // Policy and scheduling
    void setOptimizationPolicy(const OptimizationPolicy& policy);
    OptimizationPolicy getOptimizationPolicy() const;
    void scheduleOptimization(const OptimizationPolicy& policy);
    void cancelScheduledOptimization();
    bool isOptimizationScheduled() const;
    std::chrono::system_clock::time_point getNextOptimizationTime() const;
    
    // Manual triggers
    Result<void> triggerEmergencyOptimization();
    Result<void> analyzeAndOptimize();
    
    // Configuration
    void setMaxMemoryUsage(size_t max_memory_mb);
    void setMaxCpuUsage(double max_cpu_percent);
    void setOnlineOptimization(bool allow_online);
    
    // Callbacks
    using ProgressCallback = std::function<void(double progress, const std::string& message)>;
    void setProgressCallback(ProgressCallback callback);
    
    // History
    std::vector<OptimizationResult> getOptimizationHistory(size_t limit = 10) const;
    Result<void> clearOptimizationHistory();
    
    // Pause/Resume
    void pauseOptimization();
    void resumeOptimization();
    bool isOptimizationPaused() const;
    
    // Status
    bool isOptimizing() const;
    double getOptimizationProgress() const;
    std::string getOptimizationStatus() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Factory function to create optimizer with specific configuration
 */
std::unique_ptr<VectorIndexOptimizer> createVectorIndexOptimizer(
    std::shared_ptr<VectorIndexManager> index_manager,
    const OptimizationPolicy& policy = OptimizationPolicy{}
);

/**
 * Utility functions for optimization analysis
 */
namespace optimization_utils {
    // Calculate fragmentation ratio
    double calculateFragmentation(size_t total_vectors, size_t deleted_vectors, size_t segment_count);
    
    // Estimate optimization duration
    std::chrono::minutes estimateOptimizationDuration(
        size_t vector_count,
        OptimizationType type
    );
    
    // Check if optimization is needed
    bool shouldOptimize(const IndexStatistics& stats, const OptimizationPolicy& policy);
    
    // Get optimal optimization type based on statistics
    OptimizationType getOptimalOptimizationType(const IndexStatistics& stats);
    
    // Calculate expected performance improvement
    double estimatePerformanceGain(
        const IndexStatistics& stats,
        OptimizationType type
    );
}

} // namespace yams::vector