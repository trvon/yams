// Custom Catch2 main for smoke tests that exits without running static destructors.
#include <catch2/catch_session.hpp>
#include <cstdlib>

int main(int argc, char** argv) {
    int rc = Catch::Session().run(argc, argv);
#if defined(_WIN32)
    _exit(rc == 0 ? 0 : 1);
#else
    _Exit(rc == 0 ? 0 : 1);
#endif
}
