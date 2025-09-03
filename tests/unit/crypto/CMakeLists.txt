add_executable(crypto_tests
    sha256_hasher_test.cpp
)

target_link_libraries(crypto_tests
    PRIVATE
        test_utils
        yams::crypto
        gtest_main
)

gtest_discover_tests(crypto_tests
    PROPERTIES
        LABELS "unit;crypto"
)
