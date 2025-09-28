#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>
#include <yams/daemon/components/InternalEventBus.h>

using Clock = std::chrono::high_resolution_clock;

static uint64_t env_u64(const char* key, uint64_t def) {
    const char* v = std::getenv(key);
    if (!v)
        return def;
    try {
        return static_cast<uint64_t>(std::stoull(std::string(v)));
    } catch (...) {
        return def;
    }
}

int main() {
    const uint64_t producers = env_u64("YAMS_BENCH_P", 4);
    const uint64_t consumers = env_u64("YAMS_BENCH_C", 4);
    const uint64_t iters = env_u64("YAMS_BENCH_ITERS", 200000);
    const uint64_t capacity = env_u64("YAMS_BENCH_CAP", 4096);

    yams::daemon::SpscQueue<int> q(static_cast<size_t>(capacity));
    std::atomic<uint64_t> produced{0}, consumed{0};
    std::atomic<bool> done{false};

    std::vector<std::thread> threads;
    threads.reserve(producers + consumers);

    const uint64_t per = iters / producers;
    for (uint64_t p = 0; p < producers; ++p) {
        threads.emplace_back([&]() {
            for (uint64_t i = 0; i < per; ++i) {
                while (!q.try_push(1))
                    std::this_thread::yield();
                produced.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (uint64_t c = 0; c < consumers; ++c) {
        threads.emplace_back([&]() {
            int v;
            while (!done.load(std::memory_order_acquire)) {
                if (q.try_pop(v))
                    consumed.fetch_add(v, std::memory_order_relaxed);
                else
                    std::this_thread::yield();
            }
            while (q.try_pop(v))
                consumed.fetch_add(v, std::memory_order_relaxed);
        });
    }

    auto t0 = Clock::now();
    for (auto& t : threads)
        if (t.joinable() && &t - &threads[0] < static_cast<long>(producers))
            t.join();
    done.store(true, std::memory_order_release);
    for (auto& t : threads)
        if (t.joinable())
            t.join();
    auto t1 = Clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    uint64_t total = produced.load();
    double mops = (total / ms);

    std::printf("mode=%s P=%llu C=%llu ITERS=%llu CAP=%llu time_ms=%.2f throughput_ops_per_ms=%.3f "
                "consumed=%llu\n",
#if YAMS_INTERNAL_BUS_MPMC
                "mpmc",
#else
                "spsc",
#endif
                (unsigned long long)producers, (unsigned long long)consumers,
                (unsigned long long)iters, (unsigned long long)capacity, ms, mops,
                (unsigned long long)consumed.load());
    return 0;
}
