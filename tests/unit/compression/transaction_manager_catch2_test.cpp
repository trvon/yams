// Catch2 tests for Transaction Manager
// Migrated from GTest: transaction_manager_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <random>
#include <thread>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/error_handler.h>
#include <yams/compression/transaction_manager.h>

using namespace yams;
using namespace yams::compression;

namespace {
struct TransactionManagerFixture {
    TransactionManagerFixture() {
        config_ = TransactionConfig{};
        config_.defaultIsolation = IsolationLevel::ReadCommitted;
        config_.transactionTimeout = std::chrono::seconds{10};
        config_.maxConcurrentTransactions = 50;
        config_.enableAutoRollback = true;
        config_.enableTransactionLog = true;

        errorHandler_ = std::make_shared<CompressionErrorHandler>(ErrorHandlingConfig{});
        manager_ = std::make_unique<TransactionManager>(config_, errorHandler_);

        auto result = manager_->start();
        if (!result.has_value()) {
            manager_.reset();
        }
    }

    ~TransactionManagerFixture() {
        if (manager_) {
            manager_->stop();
        }
    }

    std::vector<std::byte> generateTestData(size_t size) {
        std::vector<std::byte> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<std::byte>(dis(gen));
        }

        return data;
    }

    TransactionConfig config_;
    std::shared_ptr<CompressionErrorHandler> errorHandler_;
    std::unique_ptr<TransactionManager> manager_;
};
} // namespace

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - BasicTransactionLifecycle",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto txResult =
        manager_->beginTransaction(TransactionOperation::Compress, IsolationLevel::ReadCommitted);

    REQUIRE(txResult.has_value());
    auto txId = txResult.value();

    CHECK(manager_->getTransactionState(txId) == TransactionState::Active);

    auto commitResult = manager_->commitTransaction(txId);
    REQUIRE(commitResult.has_value());

    CHECK(manager_->getTransactionState(txId) == TransactionState::Committed);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionAbort",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto txResult = manager_->beginTransaction(TransactionOperation::Decompress,
                                               IsolationLevel::RepeatableRead);

    REQUIRE(txResult.has_value());
    auto txId = txResult.value();

    auto abortResult = manager_->abortTransaction(txId, "Test abort");
    REQUIRE(abortResult.has_value());

    CHECK(manager_->getTransactionState(txId) == TransactionState::Aborted);

    auto record = manager_->getTransaction(txId);
    REQUIRE(record.has_value());
    REQUIRE(record->error.has_value());
    CHECK(record->error->code == ErrorCode::TransactionAborted);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionRollback",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto txResult =
        manager_->beginTransaction(TransactionOperation::Compress, IsolationLevel::Serializable);

    REQUIRE(txResult.has_value());
    auto txId = txResult.value();

    auto rollbackResult = manager_->rollbackTransaction(txId);
    REQUIRE(rollbackResult.has_value());

    CHECK(manager_->getTransactionState(txId) == TransactionState::RolledBack);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - CompressInTransaction",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    auto data = generateTestData(1024);

    auto result = manager_->compressInTransaction(data, CompressionAlgorithm::Zstandard, 5);

    // Should succeed or fail based on algorithm availability
    if (result.has_value()) {
        CHECK(result.value().algorithm == CompressionAlgorithm::Zstandard);
        CHECK(result.value().originalSize == data.size());
    }
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - DecompressInTransaction",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Create some mock compressed data
    auto data = generateTestData(512);

    auto result = manager_->decompressInTransaction(data, CompressionAlgorithm::LZMA, 1024);

    // This should fail as the data is random
    CHECK_FALSE(result.has_value());
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionWithExplicitId",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Begin transaction
    auto txResult =
        manager_->beginTransaction(TransactionOperation::Compress, IsolationLevel::ReadCommitted);

    REQUIRE(txResult.has_value());
    auto txId = txResult.value();

    // Use the transaction ID for compression
    auto data = generateTestData(2048);
    auto compressResult =
        manager_->compressInTransaction(data, CompressionAlgorithm::Zstandard, 3, txId);

    // Commit the transaction
    auto commitResult = manager_->commitTransaction(txId);
    REQUIRE(commitResult.has_value());

    // Verify transaction state
    CHECK(manager_->getTransactionState(txId) == TransactionState::Committed);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - ConcurrentTransactions",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    const int numTransactions = 10;
    std::vector<TransactionId> txIds;

    // Start multiple transactions
    for (int i = 0; i < numTransactions; ++i) {
        auto txResult = manager_->beginTransaction(i % 2 == 0 ? TransactionOperation::Compress
                                                              : TransactionOperation::Decompress,
                                                   IsolationLevel::ReadCommitted);

        REQUIRE(txResult.has_value());
        txIds.push_back(txResult.value());
    }

    // Verify all are active
    auto activeTransactions = manager_->getActiveTransactions();
    CHECK(activeTransactions.size() == static_cast<size_t>(numTransactions));

    // Commit half, abort half
    for (size_t i = 0; i < txIds.size(); ++i) {
        if (i % 2 == 0) {
            REQUIRE(manager_->commitTransaction(txIds[i]).has_value());
        } else {
            REQUIRE(manager_->abortTransaction(txIds[i], "Test").has_value());
        }
    }

    // Verify no active transactions remain
    activeTransactions = manager_->getActiveTransactions();
    CHECK(activeTransactions.size() == 0);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionScope",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    TransactionId capturedTxId = 0;
    bool transactionCommitted = false;

    manager_->registerCallback([&](const TransactionRecord& record) {
        if (record.state == TransactionState::Committed) {
            transactionCommitted = true;
            capturedTxId = record.id;
        }
    });

    {
        TransactionScope scope(*manager_, TransactionOperation::Validate,
                               IsolationLevel::ReadUncommitted);

        CHECK(scope.isValid());
        CHECK(scope.id() > 0);

        // Scope should auto-commit on destruction
    }

    CHECK(transactionCommitted);
    CHECK(capturedTxId > 0);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionScopeAbort",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    bool transactionAborted = false;

    manager_->registerCallback([&](const TransactionRecord& record) {
        if (record.state == TransactionState::Aborted) {
            transactionAborted = true;
        }
    });

    {
        TransactionScope scope(*manager_, TransactionOperation::Compress,
                               IsolationLevel::Serializable);

        CHECK(scope.isValid());

        // Explicitly abort
        auto result = scope.abort("Test abort from scope");
        REQUIRE(result.has_value());
    }

    CHECK(transactionAborted);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionScopeRollback",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    bool transactionRolledBack = false;

    manager_->registerCallback([&](const TransactionRecord& record) {
        if (record.state == TransactionState::RolledBack) {
            transactionRolledBack = true;
        }
    });

    {
        TransactionScope scope(*manager_, TransactionOperation::Decompress);

        CHECK(scope.isValid());

        // Mark for rollback
        scope.markForRollback();

        // Scope should rollback on destruction
    }

    CHECK(transactionRolledBack);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionStatistics",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Create and complete various transactions
    for (int i = 0; i < 10; ++i) {
        auto txResult = manager_->beginTransaction(TransactionOperation::Compress,
                                                   IsolationLevel::ReadCommitted);

        REQUIRE(txResult.has_value());
        auto txId = txResult.value();

        if (i < 6) {
            manager_->commitTransaction(txId);
        } else if (i < 8) {
            manager_->abortTransaction(txId, "Test");
        } else {
            manager_->rollbackTransaction(txId);
        }
    }

    auto stats = manager_->getStats();
    CHECK(stats[TransactionState::Committed] == 6);
    CHECK(stats[TransactionState::Aborted] == 2);
    CHECK(stats[TransactionState::RolledBack] == 2);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - MaxConcurrentTransactions",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Fill up to max concurrent transactions
    std::vector<TransactionId> txIds;

    for (size_t i = 0; i < config_.maxConcurrentTransactions; ++i) {
        auto txResult = manager_->beginTransaction(TransactionOperation::Compress,
                                                   IsolationLevel::ReadCommitted);

        REQUIRE(txResult.has_value());
        txIds.push_back(txResult.value());
    }

    // Try to create one more - should fail
    auto extraTxResult =
        manager_->beginTransaction(TransactionOperation::Compress, IsolationLevel::ReadCommitted);

    REQUIRE_FALSE(extraTxResult.has_value());
    CHECK(extraTxResult.error().code == ErrorCode::ResourceExhausted);

    // Commit one transaction
    manager_->commitTransaction(txIds[0]);

    // Now we should be able to create another
    extraTxResult =
        manager_->beginTransaction(TransactionOperation::Compress, IsolationLevel::ReadCommitted);

    CHECK(extraTxResult.has_value());
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionLog",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Create some transactions
    for (int i = 0; i < 5; ++i) {
        auto txResult = manager_->beginTransaction(TransactionOperation::Compress,
                                                   IsolationLevel::ReadCommitted);

        if (txResult.has_value()) {
            manager_->commitTransaction(txResult.value());
        }
    }

    auto log = manager_->exportTransactionLog();
    CHECK_FALSE(log.empty());
    CHECK(log.find("\"transactions\"") != std::string::npos);
    CHECK(log.find("\"state\": \"Committed\"") != std::string::npos);
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - TransactionUtilities",
                 "[compression][transaction][catch2]") {
    // Test operation name conversion
    CHECK(std::string(transaction_utils::operationName(TransactionOperation::Compress)) ==
          "Compress");
    CHECK(std::string(transaction_utils::operationName(TransactionOperation::Decompress)) ==
          "Decompress");

    // Test state name conversion
    CHECK(std::string(transaction_utils::stateName(TransactionState::Active)) == "Active");
    CHECK(std::string(transaction_utils::stateName(TransactionState::Committed)) == "Committed");

    // Test isolation level name conversion
    CHECK(std::string(transaction_utils::isolationLevelName(IsolationLevel::ReadCommitted)) ==
          "ReadCommitted");
    CHECK(std::string(transaction_utils::isolationLevelName(IsolationLevel::Serializable)) ==
          "Serializable");

    // Test write lock requirement
    CHECK(transaction_utils::requiresWriteLock(TransactionOperation::Compress));
    CHECK(transaction_utils::requiresWriteLock(TransactionOperation::CompressStream));
    CHECK_FALSE(transaction_utils::requiresWriteLock(TransactionOperation::Decompress));
    CHECK_FALSE(transaction_utils::requiresWriteLock(TransactionOperation::Validate));
}

TEST_CASE_METHOD(TransactionManagerFixture, "TransactionManager - DiagnosticsOutput",
                 "[compression][transaction][catch2]") {
    REQUIRE(manager_ != nullptr);

    // Create some transactions
    auto tx1 =
        manager_->beginTransaction(TransactionOperation::Compress, IsolationLevel::ReadCommitted);

    auto tx2 = manager_->beginTransaction(TransactionOperation::Decompress,
                                          IsolationLevel::RepeatableRead);

    auto diagnostics = manager_->getDiagnostics();
    CHECK_FALSE(diagnostics.empty());
    CHECK(diagnostics.find("Transaction Manager Diagnostics") != std::string::npos);
    CHECK(diagnostics.find("Active Transactions: 2") != std::string::npos);
    CHECK(diagnostics.find("Status: Running") != std::string::npos);
}
