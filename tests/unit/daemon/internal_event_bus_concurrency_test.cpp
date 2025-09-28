#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include <yams/daemon/components/InternalEventBus.h>

namespace yams::daemon::test {

TEST(InternalEventBusQueueTest, MpmcConcurrencyDoesNotCorruptOrDeadlock) {
    // This test stresses the queue with multiple producers and consumers.
    // It would intermittently fail or crash prior to making SpscQueue MPMC-safe.
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kPerProducer = 5000;

    SpscQueue<int> q(1024);
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};
    std::atomic<bool> done{false};

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&]() {
            for (int i = 0; i < kPerProducer; ++i) {
                // Busy try until accepted, simulate bursty ingest
                while (!q.try_push(1)) {
                    std::this_thread::yield();
                }
                produced.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::vector<std::thread> consumers;
    consumers.reserve(kConsumers);
    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&]() {
            int v;
            while (!done.load(std::memory_order_acquire)) {
                if (q.try_pop(v)) {
                    consumed.fetch_add(v, std::memory_order_relaxed);
                } else {
                    std::this_thread::yield();
                }
            }
            // drain any residual items
            while (q.try_pop(v)) {
                consumed.fetch_add(v, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : producers)
        t.join();
    done.store(true, std::memory_order_release);
    for (auto& t : consumers)
        t.join();

    EXPECT_EQ(produced.load(), kProducers * kPerProducer);
    EXPECT_EQ(consumed.load(), kProducers * kPerProducer);
}

} // namespace yams::daemon::test
