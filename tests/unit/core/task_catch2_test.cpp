// yams::Task must stay usable as a coroutine return type. The compiler calls
// the promise members (get_return_object, initial_suspend, return_value /
// return_void, unhandled_exception) implicitly, so nothing else in the tree
// names them and dead-code tooling cannot see they are needed.
#include <catch2/catch_test_macros.hpp>

#include <stdexcept>

#include <yams/core/task.h>

namespace {

yams::Task<int> answer() {
    co_return 42;
}

yams::Task<int> addOne() {
    const int base = co_await answer();
    co_return base + 1;
}

yams::Task<int> fails() {
    throw std::runtime_error("task failed");
    co_return 0;
}

yams::Task<void> setFlag(bool& flag) {
    flag = true;
    co_return;
}

} // namespace

TEST_CASE("Task<T> returns a value and composes with co_await", "[core][task]") {
    CHECK(answer().get() == 42);
    CHECK(addOne().get() == 43);
}

TEST_CASE("Task<T> rethrows the coroutine's exception from get()", "[core][task]") {
    CHECK_THROWS_AS(fails().get(), std::runtime_error);
}

TEST_CASE("Task<void> runs its body", "[core][task]") {
    bool flag = false;
    setFlag(flag).get();
    CHECK(flag);
}
