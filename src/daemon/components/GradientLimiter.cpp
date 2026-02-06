#include <yams/daemon/components/GradientLimiter.h>

#include <algorithm>
#include <cmath>

#ifdef TRACY_ENABLE
#include <tracy/Tracy.hpp>
#endif

namespace yams::daemon {

GradientLimiter::GradientLimiter(std::string name)
    : name_(std::move(name)), config_(Config{}) // Default config
      ,
      limit_(config_.initialLimit) {}

GradientLimiter::GradientLimiter(std::string name, Config config)
    : name_(std::move(name)), config_(config), limit_(config_.initialLimit) {}

bool GradientLimiter::tryAcquire() {
    const uint32_t currentLimit = static_cast<uint32_t>(limit_.load(std::memory_order_relaxed));
    uint32_t currentInFlight = inFlight_.load(std::memory_order_relaxed);

    // Attempt to increment in-flight
    if (currentInFlight < currentLimit) {
        if (inFlight_.compare_exchange_weak(currentInFlight, currentInFlight + 1,
                                            std::memory_order_relaxed)) {
            acquireCount_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }

    rejectCount_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

void GradientLimiter::onJobStart() {
    // Already counted in tryAcquire
}

void GradientLimiter::onJobEnd() {
    // Decrement in-flight when job ends
    inFlight_.fetch_sub(1, std::memory_order_relaxed);
}

void GradientLimiter::onJobComplete(std::chrono::nanoseconds rtt, bool success) {
    // Decrement in-flight
    inFlight_.fetch_sub(1, std::memory_order_relaxed);

    if (!success) {
        // On failure, reduce limit aggressively (like TCP loss)
        double current = limit_.load(std::memory_order_relaxed);
        double newLimit = std::max(config_.minLimit, current * 0.9);
        limit_.store(newLimit, std::memory_order_relaxed);

#ifdef TRACY_ENABLE
        TracyPlot((name_ + ".limit").c_str(), newLimit);
#endif
        return;
    }

    double rttNanos = static_cast<double>(rtt.count());
    updateLimit(rttNanos);
}

void GradientLimiter::updateLimit(double rttNanos) {
    // Update sample count and check warmup
    uint64_t samples = sampleCount_.fetch_add(1, std::memory_order_relaxed) + 1;

    if (samples >= config_.warmupSamples && inWarmup_.load(std::memory_order_relaxed)) {
        inWarmup_.store(false, std::memory_order_relaxed);
    }

    // Update min RTT (baseline)
    double currentMin = minRtt_.load(std::memory_order_relaxed);
    if (currentMin == 0.0 || rttNanos < currentMin) {
        minRtt_.store(rttNanos, std::memory_order_relaxed);
        currentMin = rttNanos;
    }

    // Update short-window EMA (fast response to changes)
    double prevSmoothed = smoothedRtt_.load(std::memory_order_relaxed);
    double newSmoothed;
    if (prevSmoothed == 0.0) {
        newSmoothed = rttNanos;
    } else {
        newSmoothed =
            config_.smoothingAlpha * rttNanos + (1.0 - config_.smoothingAlpha) * prevSmoothed;
    }
    smoothedRtt_.store(newSmoothed, std::memory_order_relaxed);

    // Update long-window EMA (drift correction)
    double prevLong = longRtt_.load(std::memory_order_relaxed);
    double newLong;
    if (prevLong == 0.0) {
        newLong = rttNanos;
    } else {
        newLong = config_.longWindowAlpha * rttNanos + (1.0 - config_.longWindowAlpha) * prevLong;
    }
    longRtt_.store(newLong, std::memory_order_relaxed);

    // Skip adjustment during warmup
    if (inWarmup_.load(std::memory_order_relaxed)) {
        return;
    }

    // Compute gradient: ratio of short-term to long-term RTT
    // gradient > 1.0 means short-term RTT improved (less queuing)
    // gradient < 1.0 means short-term RTT degraded (more queuing)
    double gradient;
    if (newLong > 0.0) {
        gradient = newSmoothed / newLong;
    } else {
        gradient = 1.0;
    }
    gradient_.store(gradient, std::memory_order_relaxed);

    // Netflix Gradient2 formula:
    // newLimit = currentLimit * gradient + queueAllowance
    // where queueAllowance = sqrt(currentLimit) for stability
    double currentLimit = limit_.load(std::memory_order_relaxed);
    double queueAllowance = std::sqrt(currentLimit);
    double newLimit = currentLimit * gradient + queueAllowance;

    // Apply tolerance bounds
    if (gradient >= 1.0 && config_.enableProbing) {
        // RTT improving or stable - allow growth
        // But cap the growth to prevent runaway
        double maxGrowth = currentLimit * config_.tolerance;
        newLimit = std::min(newLimit, maxGrowth);
    }

    // Clamp to bounds
    newLimit = std::clamp(newLimit, config_.minLimit, config_.maxLimit);

    limit_.store(newLimit, std::memory_order_relaxed);

#ifdef TRACY_ENABLE
    TracyPlot((name_ + ".limit").c_str(), newLimit);
    TracyPlot((name_ + ".gradient").c_str(), gradient);
    TracyPlot((name_ + ".minRtt").c_str(), currentMin);
    TracyPlot((name_ + ".smoothedRtt").c_str(), newSmoothed);
    TracyPlot((name_ + ".longRtt").c_str(), newLong);
#endif
}

GradientLimiter::Metrics GradientLimiter::metrics() const {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    Metrics m;
    m.limit = limit_.load(std::memory_order_relaxed);
    m.minRtt = minRtt_.load(std::memory_order_relaxed);
    m.smoothedRtt = smoothedRtt_.load(std::memory_order_relaxed);
    m.longRtt = longRtt_.load(std::memory_order_relaxed);
    m.gradient = gradient_.load(std::memory_order_relaxed);
    m.inFlight = inFlight_.load(std::memory_order_relaxed);
    m.acquireCount = acquireCount_.load(std::memory_order_relaxed);
    m.rejectCount = rejectCount_.load(std::memory_order_relaxed);
    return m;
}

void GradientLimiter::applyPressure(uint8_t level) {
    // 0=Normal: no-op, let gradient algorithm recover organically
    // 1=Warning: clamp to 75% of maxLimit
    // 2=Critical: force to minLimit
    // 3+=Emergency: force to 0 (full stop)
    switch (level) {
        case 0:
            return; // Normal — no intervention
        case 1: {
            double cap = config_.maxLimit * 0.75;
            double current = limit_.load(std::memory_order_relaxed);
            if (current > cap) {
                limit_.store(cap, std::memory_order_relaxed);
            }
            break;
        }
        case 2:
            limit_.store(config_.minLimit, std::memory_order_relaxed);
            break;
        default:
            // Emergency or unknown — full stop
            limit_.store(0.0, std::memory_order_relaxed);
            break;
    }
}

void GradientLimiter::reset() {
    limit_.store(config_.initialLimit, std::memory_order_relaxed);
    inFlight_.store(0, std::memory_order_relaxed);
    minRtt_.store(0.0, std::memory_order_relaxed);
    smoothedRtt_.store(0.0, std::memory_order_relaxed);
    longRtt_.store(0.0, std::memory_order_relaxed);
    gradient_.store(1.0, std::memory_order_relaxed);
    sampleCount_.store(0, std::memory_order_relaxed);
    inWarmup_.store(true, std::memory_order_relaxed);
    acquireCount_.store(0, std::memory_order_relaxed);
    rejectCount_.store(0, std::memory_order_relaxed);
}

} // namespace yams::daemon
