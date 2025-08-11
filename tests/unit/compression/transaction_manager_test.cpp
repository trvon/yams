#include <gtest/gtest.h>
#include <yams/compression/transaction_manager.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/error_handler.h>
#include <thread>
#include <random>

using namespace yams;
using namespace yams::compression;

class TransactionManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = TransactionConfig{};
        config_.defaultIsolation = IsolationLevel::ReadCommitted;
        config_.transactionTimeout = std::chrono::seconds{10};
        config_.maxConcurrentTransactions = 50;
        config_.enableAutoRollback = true;
        config_.enableTransactionLog = true;
        
        errorHandler_ = std::make_shared<CompressionErrorHandler>(ErrorHandlingConfig{});
        manager_ = std::make_unique<TransactionManager>(config_, errorHandler_);
        
        ASSERT_TRUE(manager_->start().has_value());
    }
    
    void TearDown() override {
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

TEST_F(TransactionManagerTest, BasicTransactionLifecycle) {
    auto txResult = manager_->beginTransaction(
        TransactionOperation::Compress, 
        IsolationLevel::ReadCommitted);
    
    ASSERT_TRUE(txResult.has_value());
    auto txId = txResult.value();
    
    EXPECT_EQ(manager_->getTransactionState(txId), TransactionState::Active);
    
    auto commitResult = manager_->commitTransaction(txId);
    ASSERT_TRUE(commitResult.has_value());
    
    EXPECT_EQ(manager_->getTransactionState(txId), TransactionState::Committed);
}

TEST_F(TransactionManagerTest, TransactionAbort) {
    auto txResult = manager_->beginTransaction(
        TransactionOperation::Decompress,
        IsolationLevel::RepeatableRead);
    
    ASSERT_TRUE(txResult.has_value());
    auto txId = txResult.value();
    
    auto abortResult = manager_->abortTransaction(txId, "Test abort");
    ASSERT_TRUE(abortResult.has_value());
    
    EXPECT_EQ(manager_->getTransactionState(txId), TransactionState::Aborted);
    
    auto record = manager_->getTransaction(txId);
    ASSERT_TRUE(record.has_value());
    EXPECT_TRUE(record->error.has_value());
    EXPECT_EQ(record->error->code, ErrorCode::TransactionAborted);
}

TEST_F(TransactionManagerTest, TransactionRollback) {
    auto txResult = manager_->beginTransaction(
        TransactionOperation::Compress,
        IsolationLevel::Serializable);
    
    ASSERT_TRUE(txResult.has_value());
    auto txId = txResult.value();
    
    auto rollbackResult = manager_->rollbackTransaction(txId);
    ASSERT_TRUE(rollbackResult.has_value());
    
    EXPECT_EQ(manager_->getTransactionState(txId), TransactionState::RolledBack);
}

TEST_F(TransactionManagerTest, CompressInTransaction) {
    auto data = generateTestData(1024);
    
    auto result = manager_->compressInTransaction(
        data, 
        CompressionAlgorithm::Zstandard,
        5);
    
    // Should succeed or fail based on algorithm availability
    if (result.has_value()) {
        EXPECT_EQ(result.value().algorithm, CompressionAlgorithm::Zstandard);
        EXPECT_EQ(result.value().originalSize, data.size());
    }
}

TEST_F(TransactionManagerTest, DecompressInTransaction) {
    // Create some mock compressed data
    auto data = generateTestData(512);
    
    auto result = manager_->decompressInTransaction(
        data,
        CompressionAlgorithm::LZMA,
        1024);
    
    // This should fail as the data is random
    EXPECT_FALSE(result.has_value());
}

TEST_F(TransactionManagerTest, TransactionWithExplicitId) {
    // Begin transaction
    auto txResult = manager_->beginTransaction(
        TransactionOperation::Compress,
        IsolationLevel::ReadCommitted);
    
    ASSERT_TRUE(txResult.has_value());
    auto txId = txResult.value();
    
    // Use the transaction ID for compression
    auto data = generateTestData(2048);
    auto compressResult = manager_->compressInTransaction(
        data,
        CompressionAlgorithm::Zstandard,
        3,
        txId);
    
    // Commit the transaction
    auto commitResult = manager_->commitTransaction(txId);
    ASSERT_TRUE(commitResult.has_value());
    
    // Verify transaction state
    EXPECT_EQ(manager_->getTransactionState(txId), TransactionState::Committed);
}

TEST_F(TransactionManagerTest, ConcurrentTransactions) {
    const int numTransactions = 10;
    std::vector<TransactionId> txIds;
    
    // Start multiple transactions
    for (int i = 0; i < numTransactions; ++i) {
        auto txResult = manager_->beginTransaction(
            i % 2 == 0 ? TransactionOperation::Compress : TransactionOperation::Decompress,
            IsolationLevel::ReadCommitted);
        
        ASSERT_TRUE(txResult.has_value());
        txIds.push_back(txResult.value());
    }
    
    // Verify all are active
    auto activeTransactions = manager_->getActiveTransactions();
    EXPECT_EQ(activeTransactions.size(), numTransactions);
    
    // Commit half, abort half
    for (size_t i = 0; i < txIds.size(); ++i) {
        if (i % 2 == 0) {
            EXPECT_TRUE(manager_->commitTransaction(txIds[i]).has_value());
        } else {
            EXPECT_TRUE(manager_->abortTransaction(txIds[i], "Test").has_value());
        }
    }
    
    // Verify no active transactions remain
    activeTransactions = manager_->getActiveTransactions();
    EXPECT_EQ(activeTransactions.size(), 0);
}

TEST_F(TransactionManagerTest, TransactionScope) {
    TransactionId capturedTxId = 0;
    bool transactionCommitted = false;
    
    manager_->registerCallback([&](const TransactionRecord& record) {
        if (record.state == TransactionState::Committed) {
            transactionCommitted = true;
            capturedTxId = record.id;
        }
    });
    
    {
        TransactionScope scope(*manager_, 
                             TransactionOperation::Validate,
                             IsolationLevel::ReadUncommitted);
        
        EXPECT_TRUE(scope.isValid());
        EXPECT_GT(scope.id(), 0);
        
        // Scope should auto-commit on destruction
    }
    
    EXPECT_TRUE(transactionCommitted);
    EXPECT_GT(capturedTxId, 0);
}

TEST_F(TransactionManagerTest, TransactionScopeAbort) {
    bool transactionAborted = false;
    
    manager_->registerCallback([&](const TransactionRecord& record) {
        if (record.state == TransactionState::Aborted) {
            transactionAborted = true;
        }
    });
    
    {
        TransactionScope scope(*manager_,
                             TransactionOperation::Compress,
                             IsolationLevel::Serializable);
        
        EXPECT_TRUE(scope.isValid());
        
        // Explicitly abort
        auto result = scope.abort("Test abort from scope");
        EXPECT_TRUE(result.has_value());
    }
    
    EXPECT_TRUE(transactionAborted);
}

TEST_F(TransactionManagerTest, TransactionScopeRollback) {
    bool transactionRolledBack = false;
    
    manager_->registerCallback([&](const TransactionRecord& record) {
        if (record.state == TransactionState::RolledBack) {
            transactionRolledBack = true;
        }
    });
    
    {
        TransactionScope scope(*manager_,
                             TransactionOperation::Decompress);
        
        EXPECT_TRUE(scope.isValid());
        
        // Mark for rollback
        scope.markForRollback();
        
        // Scope should rollback on destruction
    }
    
    EXPECT_TRUE(transactionRolledBack);
}

TEST_F(TransactionManagerTest, TransactionStatistics) {
    // Create and complete various transactions
    for (int i = 0; i < 10; ++i) {
        auto txResult = manager_->beginTransaction(
            TransactionOperation::Compress,
            IsolationLevel::ReadCommitted);
        
        ASSERT_TRUE(txResult.has_value());
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
    EXPECT_EQ(stats[TransactionState::Committed], 6);
    EXPECT_EQ(stats[TransactionState::Aborted], 2);
    EXPECT_EQ(stats[TransactionState::RolledBack], 2);
}

TEST_F(TransactionManagerTest, MaxConcurrentTransactions) {
    // Fill up to max concurrent transactions
    std::vector<TransactionId> txIds;
    
    for (size_t i = 0; i < config_.maxConcurrentTransactions; ++i) {
        auto txResult = manager_->beginTransaction(
            TransactionOperation::Compress,
            IsolationLevel::ReadCommitted);
        
        ASSERT_TRUE(txResult.has_value());
        txIds.push_back(txResult.value());
    }
    
    // Try to create one more - should fail
    auto extraTxResult = manager_->beginTransaction(
        TransactionOperation::Compress,
        IsolationLevel::ReadCommitted);
    
    EXPECT_FALSE(extraTxResult.has_value());
    EXPECT_EQ(extraTxResult.error().code, ErrorCode::ResourceExhausted);
    
    // Commit one transaction
    manager_->commitTransaction(txIds[0]);
    
    // Now we should be able to create another
    extraTxResult = manager_->beginTransaction(
        TransactionOperation::Compress,
        IsolationLevel::ReadCommitted);
    
    EXPECT_TRUE(extraTxResult.has_value());
}

TEST_F(TransactionManagerTest, TransactionLog) {
    // Create some transactions
    for (int i = 0; i < 5; ++i) {
        auto txResult = manager_->beginTransaction(
            TransactionOperation::Compress,
            IsolationLevel::ReadCommitted);
        
        if (txResult.has_value()) {
            manager_->commitTransaction(txResult.value());
        }
    }
    
    auto log = manager_->exportTransactionLog();
    EXPECT_FALSE(log.empty());
    EXPECT_NE(log.find("\"transactions\""), std::string::npos);
    EXPECT_NE(log.find("\"state\": \"Committed\""), std::string::npos);
}

TEST_F(TransactionManagerTest, TransactionUtilities) {
    // Test operation name conversion
    EXPECT_STREQ(transaction_utils::operationName(TransactionOperation::Compress), 
                 "Compress");
    EXPECT_STREQ(transaction_utils::operationName(TransactionOperation::Decompress), 
                 "Decompress");
    
    // Test state name conversion
    EXPECT_STREQ(transaction_utils::stateName(TransactionState::Active), 
                 "Active");
    EXPECT_STREQ(transaction_utils::stateName(TransactionState::Committed), 
                 "Committed");
    
    // Test isolation level name conversion
    EXPECT_STREQ(transaction_utils::isolationLevelName(IsolationLevel::ReadCommitted), 
                 "ReadCommitted");
    EXPECT_STREQ(transaction_utils::isolationLevelName(IsolationLevel::Serializable), 
                 "Serializable");
    
    // Test write lock requirement
    EXPECT_TRUE(transaction_utils::requiresWriteLock(TransactionOperation::Compress));
    EXPECT_TRUE(transaction_utils::requiresWriteLock(TransactionOperation::CompressStream));
    EXPECT_FALSE(transaction_utils::requiresWriteLock(TransactionOperation::Decompress));
    EXPECT_FALSE(transaction_utils::requiresWriteLock(TransactionOperation::Validate));
}

TEST_F(TransactionManagerTest, DiagnosticsOutput) {
    // Create some transactions
    auto tx1 = manager_->beginTransaction(
        TransactionOperation::Compress,
        IsolationLevel::ReadCommitted);
    
    auto tx2 = manager_->beginTransaction(
        TransactionOperation::Decompress,
        IsolationLevel::RepeatableRead);
    
    auto diagnostics = manager_->getDiagnostics();
    EXPECT_FALSE(diagnostics.empty());
    EXPECT_NE(diagnostics.find("Transaction Manager Diagnostics"), std::string::npos);
    EXPECT_NE(diagnostics.find("Active Transactions: 2"), std::string::npos);
    EXPECT_NE(diagnostics.find("Status: Running"), std::string::npos);
}