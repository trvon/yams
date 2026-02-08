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
    uint32_t prevInFlight = inFlight_.fetch_sub(1, std::memory_order_relaxed);

    if (!success) {
        double current = limit_.load(std::memory_order_relaxed);
        double newLimit = std::max(config_.minLimit, current * 0.9);
        limit_.store(newLimit, std::memory_order_relaxed);

#ifdef TRACY_ENABLE
        TracyPlot((name_ + ".limit").c_str(), newLimit);
#endif
        return;
    }

    double rttNanos = static_cast<double>(rtt.count());
    updateLimit(rttNanos, prevInFlight);
}

void GradientLimiter::updateLimit(double rttNanos, uint32_t inFlightSnapshot) {
    uint64_t samples = sampleCount_.fetch_add(1, std::memory_order_relaxed) + 1;

    if (samples >= config_.warmupSamples && inWarmup_.load(std::memory_order_relaxed)) {
        inWarmup_.store(false, std::memory_order_relaxed);
    }

    double currentMin = minRtt_.load(std::memory_order_relaxed);
    if (currentMin == 0.0 || rttNanos < currentMin) {
        minRtt_.store(rttNanos, std::memory_order_relaxed);
        currentMin = rttNanos;
    }

    // Short-window EMA
    double prevSmoothed = smoothedRtt_.load(std::memory_order_relaxed);
    double newSmoothed;
    if (prevSmoothed == 0.0) {
        newSmoothed = rttNanos;
    } else {
        newSmoothed =
            config_.smoothingAlpha * rttNanos + (1.0 - config_.smoothingAlpha) * prevSmoothed;
    }
    smoothedRtt_.store(newSmoothed, std::memory_order_relaxed);

    // Long-window EMA
    double prevLong = longRtt_.load(std::memory_order_relaxed);
    double newLong;
    if (prevLong == 0.0) {
        newLong = rttNanos;
    } else {
        newLong = config_.longWindowAlpha * rttNanos + (1.0 - config_.longWindowAlpha) * prevLong;
    }

    // Long RTT recovery: decay pessimistic long EMA when system has recovered
    if (newSmoothed > 0.0 && newLong / newSmoothed > config_.longRttRecoveryThreshold) {
        newLong *= config_.longRttDecayFactor;
    }

    longRtt_.store(newLong, std::memory_order_relaxed);

    if (inWarmup_.load(std::memory_order_relaxed)) {
        return;
    }

    // Window gate: only recompute the limit once per windowSize samples
    if (config_.windowSize > 1) {
        uint64_t postWarmup = samples - config_.warmupSamples;
        if (postWarmup % config_.windowSize != 0)
            return;
    }

    double currentLimit = limit_.load(std::memory_order_relaxed);

    // App-limited guard: skip adjustment when under-utilized
    if (inFlightSnapshot < static_cast<uint32_t>(currentLimit * config_.appLimitedRatio)) {
        return;
    }

    // Dual gradient: relative (long/short) + absolute congestion (min/short)
    double gradient;
    if (newSmoothed > 0.0) {
        double gradientLong = config_.tolerance * newLong / newSmoothed;
        double gradientMin = config_.tolerance * currentMin / newSmoothed;
        gradient = std::clamp(std::min(gradientLong, gradientMin), 0.5, 1.0);
    } else {
        gradient = 1.0;
    }
    gradient_.store(gradient, std::memory_order_relaxed);

    double queueAllowance = config_.enableProbing ? std::sqrt(currentLimit) : 0.0;
    double newLimit = currentLimit * gradient + queueAllowance;

    // Limit smoothing
    newLimit = currentLimit * (1.0 - config_.limitSmoothing) + newLimit * config_.limitSmoothing;

    newLimit = std::clamp(newLimit, config_.minLimit, config_.maxLimit);

    // Pressure ceiling
    uint8_t pressure = pressureLevel_.load(std::memory_order_relaxed);
    switch (pressure) {
        case 1: {
            double warningCap = config_.maxLimit * 0.75;
            newLimit = std::min(newLimit, warningCap);
            break;
        }
        case 2:
            newLimit = std::min(newLimit, config_.minLimit);
            break;
        case 3:
            newLimit = std::min(newLimit, config_.minLimit);
            break;
        default:
            break;
    }

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
    pressureLevel_.store(level, std::memory_order_relaxed);

    switch (level) {
        case 0:
            return;
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
            limit_.store(config_.minLimit, std::memory_order_relaxed);
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
    pressureLevel_.store(0, std::memory_order_relaxed);
    acquireCount_.store(0, std::memory_order_relaxed);
    rejectCount_.store(0, std::memory_order_relaxed);
}

} // namespace yams::daemon
