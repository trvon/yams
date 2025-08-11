# Vector Search Performance Tuning Guide

## Overview

This guide provides detailed instructions for optimizing the performance of YAMS Vector Search system. Performance tuning involves balancing trade-offs between speed, accuracy, memory usage, and cost.

## Performance Metrics

### Key Performance Indicators (KPIs)

| Metric | Description | Target | Measurement |
|--------|-------------|--------|-------------|
| Query Latency | Time to return search results | <50ms p95 | Response time |
| Throughput | Queries processed per second | >1000 QPS | Requests/sec |
| Recall@K | Accuracy of top-K results | >0.95 | Ground truth comparison |
| Index Build Time | Time to build/rebuild index | <10 min/1M vectors | Build duration |
| Memory Usage | RAM consumption | <16GB/1M vectors | Process memory |

## Index Tuning

### Index Type Selection

#### Decision Matrix

```
Dataset Size:
├─ < 10K vectors
│  └─ Use FLAT (exact search, highest quality)
├─ 10K - 100K vectors
│  ├─ Speed priority → HNSW
│  └─ Quality priority → FLAT or IVF_FLAT
├─ 100K - 1M vectors
│  ├─ Balanced → HNSW
│  └─ Memory constrained → IVF_PQ
└─ > 1M vectors
   ├─ Speed critical → HNSW with tuning
   └─ Memory critical → IVF_PQ
```

### HNSW Tuning

HNSW (Hierarchical Navigable Small World) provides the best balance of speed and accuracy.

#### Key Parameters

```cpp
IndexConfig config;
config.type = IndexType::HNSW;

// M: Number of bi-directional links per node
// Higher M = better recall, more memory, slower build
config.hnsw_m = 16;  // Default: 16, Range: 4-64

// ef_construction: Size of dynamic candidate list
// Higher = better recall, slower build
config.hnsw_ef_construction = 200;  // Default: 200, Range: 100-500

// ef_search: Size of dynamic candidate list for search
// Higher = better recall, slower search
config.hnsw_ef_search = 50;  // Default: 50, Range: 10-500
```

#### Tuning Recommendations

| Use Case | M | ef_construction | ef_search | Trade-off |
|----------|---|-----------------|-----------|-----------|
| High Speed | 8 | 100 | 20 | Fast but lower recall |
| Balanced | 16 | 200 | 50 | Good for most cases |
| High Quality | 32 | 400 | 100 | Slower but better recall |
| Maximum Quality | 48 | 500 | 200 | Best recall, slowest |

### IVF Tuning

IVF (Inverted File) indices are suitable for large datasets with memory constraints.

#### Key Parameters

```cpp
IndexConfig config;
config.type = IndexType::IVF_PQ;

// nlist: Number of clusters
// Higher = better recall, slower build
config.ivf_nlist = 100;  // sqrt(N) is a good starting point

// nprobe: Clusters to search
// Higher = better recall, slower search
config.ivf_nprobe = 10;  // Start with nlist/10

// PQ parameters (for IVF_PQ)
config.pq_n_subquantizers = 8;  // Dimension must be divisible
config.pq_bits_per_code = 8;    // 8 bits = 256 centroids
```

#### IVF Tuning Guide

```python
# Optimal nlist calculation
import math
optimal_nlist = int(math.sqrt(num_vectors))

# Optimal nprobe for target recall
if target_recall > 0.95:
    nprobe = nlist // 4
elif target_recall > 0.9:
    nprobe = nlist // 8
else:
    nprobe = nlist // 16
```

## Embedding Optimization

### Model Selection

| Model | Dimension | Speed | Quality | Memory | Use Case |
|-------|-----------|-------|---------|--------|----------|
| all-MiniLM-L6-v2 | 384 | 1000/s | Good | 100MB | General purpose |
| all-MiniLM-L12-v2 | 384 | 500/s | Better | 150MB | Quality focus |
| all-mpnet-base-v2 | 768 | 200/s | Best | 400MB | Maximum quality |
| distilbert-base | 768 | 400/s | Good | 250MB | Balanced |

### Batch Processing

```cpp
// Optimal batch sizes for embedding generation
EmbeddingConfig config;

// For CPU inference
config.batch_size = 32;  // Good for most CPUs

// For GPU inference
config.batch_size = 64;  // Utilize GPU parallelism

// For limited memory
config.batch_size = 16;  // Reduce memory pressure
```

### Preprocessing Optimization

```cpp
// Text preprocessing for better embeddings
TextPreprocessor preprocessor;

// Enable caching for repeated texts
preprocessor.enableCache(1000);  // Cache 1000 texts

// Optimize tokenization
preprocessor.setMaxLength(256);  // Reduce from default 512
preprocessor.setPadding(false);   // Don't pad if not needed
```

## Memory Optimization

### Memory Usage Estimation

```
Memory Usage = Index Size + Cache Size + Working Memory

Index Size = num_vectors × dimension × sizeof(float) × index_overhead
- FLAT: overhead = 1.0
- HNSW: overhead = 1.5 (with M=16)
- IVF_PQ: overhead = 0.25

Cache Size = model_size + embedding_cache + result_cache

Working Memory = batch_size × dimension × sizeof(float) × 2
```

### Memory Reduction Strategies

#### 1. Quantization

```cpp
// Use Product Quantization to reduce memory
IndexConfig config;
config.type = IndexType::IVF_PQ;
config.pq_n_subquantizers = 8;

// Memory reduction: 32x (from float32 to uint8)
// Accuracy impact: ~5% recall loss
```

#### 2. Dimension Reduction

```cpp
// Use smaller embedding models
EmbeddingConfig config;
config.model_path = "models/all-MiniLM-L6-v2.onnx";  // 384 dims
// Instead of:
// config.model_path = "models/all-mpnet-base-v2.onnx";  // 768 dims

// 50% memory reduction with minimal quality loss
```

#### 3. Memory Mapping

```cpp
// Use memory-mapped files for large indices
DatabaseConfig config;
config.use_mmap = true;
config.mmap_preload = false;  // Load on demand

// OS manages memory, supports datasets larger than RAM
```

## Query Optimization

### Search Parameter Tuning

```cpp
// Fast search configuration
VectorSearchParams fast_params;
fast_params.k = 10;  // Limit results
fast_params.similarity_threshold = 0.8;  // Higher threshold

// Quality search configuration
VectorSearchParams quality_params;
quality_params.k = 50;  // More candidates
quality_params.similarity_threshold = 0.6;  // Lower threshold
```

### Query Caching

```cpp
// Enable result caching
SearchCache cache;
cache.setMaxSize(10000);  // Cache 10K queries
cache.setTTL(3600);       // 1 hour TTL

// Cache key includes: query_vector_hash + k + filters
std::string cache_key = computeCacheKey(query, params);
if (auto cached = cache.get(cache_key)) {
    return cached.value();
}
```

### Parallel Search

```cpp
// Parallel query processing
const size_t num_threads = std::thread::hardware_concurrency();
ThreadPool pool(num_threads);

std::vector<std::future<SearchResult>> futures;
for (const auto& query : queries) {
    futures.push_back(
        pool.enqueue([&](auto q) {
            return index->search(q, params);
        }, query)
    );
}
```

## Database Tuning

### LanceDB Configuration

```yaml
lancedb:
  # Connection pool
  max_connections: 100
  connection_timeout: 5s
  
  # Write optimization
  batch_size: 1000
  write_buffer_size: 64MB
  
  # Read optimization
  read_cache_size: 1GB
  prefetch_size: 10
  
  # Compaction
  auto_compact: true
  compact_threshold: 0.8
```

### Storage Optimization

```cpp
// Periodic maintenance
void optimizeStorage() {
    // Compact database
    database->compactDatabase();
    
    // Rebuild indices
    if (fragmentation_ratio > 0.3) {
        database->rebuildIndex();
    }
    
    // Clean up stale embeddings
    database->purgeDeleted(std::chrono::hours(24));
}
```

## System-Level Tuning

### CPU Optimization

```bash
# Set CPU governor to performance
sudo cpupower frequency-set -g performance

# Disable CPU frequency scaling
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Pin processes to specific CPUs
taskset -c 0-7 ./vector_search_server
```

### Memory Settings

```bash
# Increase system limits
ulimit -n 65536  # File descriptors
ulimit -m unlimited  # Memory

# Tune kernel parameters
echo 'vm.swappiness=10' | sudo tee -a /etc/sysctl.conf
echo 'vm.dirty_ratio=15' | sudo tee -a /etc/sysctl.conf
echo 'vm.dirty_background_ratio=5' | sudo tee -a /etc/sysctl.conf
```

### Network Optimization

```bash
# TCP tuning for high throughput
echo 'net.core.rmem_max=134217728' | sudo tee -a /etc/sysctl.conf
echo 'net.core.wmem_max=134217728' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem=4096 87380 134217728' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem=4096 65536 134217728' | sudo tee -a /etc/sysctl.conf
```

## Monitoring and Profiling

### Performance Monitoring

```cpp
// Enable performance metrics
MetricsCollector metrics;
metrics.enable();

// Track key metrics
metrics.recordLatency("search", search_time_ms);
metrics.recordThroughput("embedding", embeddings_per_sec);
metrics.recordMemory("index", index_memory_bytes);

// Export to monitoring system
metrics.exportToPrometheus();
```

### Profiling Tools

```bash
# CPU profiling with perf
perf record -g ./vector_search_benchmark
perf report

# Memory profiling with valgrind
valgrind --tool=massif ./vector_search_benchmark
ms_print massif.out.*

# Cache analysis
perf stat -e cache-misses,cache-references ./vector_search_benchmark
```

## Benchmarking

### Standard Benchmark

```cpp
// Run standard benchmark
BenchmarkRunner runner;
runner.runStandardBenchmark();

// Expected results for 1M vectors, 384 dimensions:
// - Index build: < 5 minutes
// - Search latency: < 10ms (p95)
// - Throughput: > 1000 QPS
// - Memory: < 2GB
```

### Custom Benchmark

```cpp
// Create custom benchmark for your use case
class CustomBenchmark : public VectorBenchmark {
    void runIteration() override {
        // Your specific workload
        auto results = index->search(real_query, production_params);
    }
};

// Run and analyze
auto results = benchmark.run();
analyzeResults(results);
```

## Troubleshooting Performance Issues

### High Latency

**Symptoms:** Search queries taking >100ms

**Diagnosis:**
1. Check index type and parameters
2. Monitor CPU and memory usage
3. Analyze query complexity

**Solutions:**
```cpp
// Reduce search scope
config.hnsw_ef_search = 30;  // Reduce from 50

// Enable query preprocessing
preprocessor.enableQueryOptimization();

// Use approximate search
params.use_approximate = true;
```

### Low Throughput

**Symptoms:** < 100 QPS with adequate resources

**Diagnosis:**
1. Check for lock contention
2. Monitor I/O wait
3. Profile hot paths

**Solutions:**
```cpp
// Increase parallelism
config.num_threads = 16;

// Enable batch processing
config.enable_batch_mode = true;

// Use connection pooling
database->setConnectionPoolSize(50);
```

### High Memory Usage

**Symptoms:** OOM errors or excessive swapping

**Diagnosis:**
1. Monitor memory allocation patterns
2. Check for memory leaks
3. Analyze index size

**Solutions:**
```cpp
// Use memory-efficient index
config.type = IndexType::IVF_PQ;

// Reduce cache sizes
cache_config.max_memory_bytes = 512 * 1024 * 1024;  // 512MB

// Enable memory limiting
config.memory_limit_bytes = 8ULL * 1024 * 1024 * 1024;  // 8GB
```

## Best Practices Summary

### Do's
✅ Start with FLAT index for baseline
✅ Benchmark with real data
✅ Monitor production metrics
✅ Use batch operations
✅ Enable caching where appropriate
✅ Regular index optimization
✅ Profile before optimizing

### Don'ts
❌ Over-optimize prematurely
❌ Ignore memory constraints
❌ Use default parameters blindly
❌ Neglect monitoring
❌ Skip capacity planning
❌ Forget about data growth

## Performance Checklist

- [ ] Selected appropriate index type
- [ ] Tuned index parameters
- [ ] Optimized batch sizes
- [ ] Configured caching
- [ ] Enabled monitoring
- [ ] Set up alerting
- [ ] Documented configuration
- [ ] Tested failover scenarios
- [ ] Benchmarked with production data
- [ ] Created runbooks

## Additional Resources

- [Benchmark Results](../benchmarks/results/)
- [Configuration Templates](../config/templates/)
- [Monitoring Dashboards](../monitoring/dashboards/)
- [Capacity Planning Guide](./capacity_planning.md)