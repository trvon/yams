// This file intentionally provides weak, no-op symbols to satisfy Boost.Test
// exec/prg monitor references that may leak in via transitive Boost linkage.
// We do NOT want to define 'test_main' here to avoid clashing with GTest main.

extern "C" {
// Some linkers might search for these symbols from Boost.Test; define dummies.
// They are not used at runtime because we run GoogleTest binaries.
__attribute__((weak)) int init_unit_test_suite(int, char**) {
    return 0;
}
__attribute__((weak)) int unit_test_main(int (*)(int, char**), int, char**) {
    return 0;
}
}
