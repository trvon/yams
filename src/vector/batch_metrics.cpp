#include <yams/vector/batch_metrics.h>

namespace yams::vector::batchmetrics {

Metrics& get() {
    static Metrics instance{};
    return instance;
}

} // namespace yams::vector::batchmetrics
