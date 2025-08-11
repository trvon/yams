#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <cstdlib>
#include <sstream>

namespace fs = std::filesystem;

class CLIDeleteTest : public ::testing::Test {
protected:
    fs::path testDir;
    fs::path yamsBinary;
    fs::path storageDir;
    
    void SetUp() override {
        // Create temporary test directory
        testDir = fs::temp_directory_path() / ("yams_delete_test_" + std::to_string(std::rand()));
        fs::create_directories(testDir);
        
        // Set up storage directory
        storageDir = testDir / "storage";
        
        // Find yams binary (should be in build directory)
        yamsBinary = fs::current_path() / "tools" / "yams-cli" / "yams";
        if (!fs::exists(yamsBinary)) {
            yamsBinary = fs::current_path() / "yams";
        }
        ASSERT_TRUE(fs::exists(yamsBinary)) << "YAMS binary not found at: " << yamsBinary;
        
        // Initialize YAMS storage
        runCommand("init --non-interactive");
    }
    
    void TearDown() override {
        // Clean up test directory
        if (fs::exists(testDir)) {
            fs::remove_all(testDir);
        }
    }
    
    std::string runCommand(const std::string& args, bool expectSuccess = true) {
        std::string cmd = yamsBinary.string() + " --storage " + storageDir.string() + " " + args + " 2>&1";
        
        FILE* pipe = popen(cmd.c_str(), "r");
        EXPECT_NE(pipe, nullptr) << "Failed to run command: " << cmd;
        
        std::string result;
        char buffer[128];
        while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
            result += buffer;
        }
        
        int exitCode = pclose(pipe);
        if (expectSuccess) {
            EXPECT_EQ(exitCode, 0) << "Command failed: " << cmd << "\nOutput: " << result;
        }
        
        return result;
    }
    
    std::string addFile(const std::string& name, const std::string& content) {
        fs::path filePath = testDir / name;
        std::ofstream file(filePath);
        file << content;
        file.close();
        
        std::string output = runCommand("add " + filePath.string() + " --name \"" + name + "\"");
        
        // Extract hash from output
        std::string hashPrefix = "SHA256: ";
        size_t pos = output.find(hashPrefix);
        if (pos != std::string::npos) {
            pos += hashPrefix.length();
            size_t endPos = output.find('\n', pos);
            return output.substr(pos, endPos - pos);
        }
        return "";
    }
    
    bool documentExists(const std::string& nameOrHash) {
        std::string output = runCommand("list --format minimal", false);
        return output.find(nameOrHash) != std::string::npos;
    }
};

TEST_F(CLIDeleteTest, DeleteByHash) {
    // Add a test file
    std::string hash = addFile("test1.txt", "Test content 1");
    ASSERT_FALSE(hash.empty());
    ASSERT_TRUE(documentExists(hash.substr(0, 12)));
    
    // Delete by hash with confirmation (use --force to skip)
    std::string output = runCommand("delete " + hash + " --force");
    EXPECT_NE(output.find("Successfully deleted"), std::string::npos);
    
    // Verify document is deleted
    EXPECT_FALSE(documentExists(hash.substr(0, 12)));
}

TEST_F(CLIDeleteTest, DeleteByName) {
    // Add a test file
    std::string hash = addFile("unique_file.txt", "Unique content");
    ASSERT_FALSE(hash.empty());
    ASSERT_TRUE(documentExists("unique_file.txt"));
    
    // Delete by name
    std::string output = runCommand("delete --name \"unique_file.txt\" --force");
    EXPECT_NE(output.find("Successfully deleted"), std::string::npos);
    
    // Verify document is deleted
    EXPECT_FALSE(documentExists("unique_file.txt"));
}

TEST_F(CLIDeleteTest, DeleteMultipleByNames) {
    // Add multiple test files
    addFile("file1.txt", "Content 1");
    addFile("file2.txt", "Content 2");
    addFile("file3.txt", "Content 3");
    
    ASSERT_TRUE(documentExists("file1.txt"));
    ASSERT_TRUE(documentExists("file2.txt"));
    ASSERT_TRUE(documentExists("file3.txt"));
    
    // Delete multiple files
    std::string output = runCommand("delete --names \"file1.txt,file2.txt\" --force");
    EXPECT_NE(output.find("Successfully deleted 2"), std::string::npos);
    
    // Verify correct files are deleted
    EXPECT_FALSE(documentExists("file1.txt"));
    EXPECT_FALSE(documentExists("file2.txt"));
    EXPECT_TRUE(documentExists("file3.txt"));  // This one should still exist
}

TEST_F(CLIDeleteTest, DeleteByPattern) {
    // Add test files with pattern
    addFile("temp_1.log", "Log 1");
    addFile("temp_2.log", "Log 2");
    addFile("temp_3.log", "Log 3");
    addFile("keep.txt", "Keep this");
    
    // Delete by pattern
    std::string output = runCommand("delete --pattern \"temp_*.log\" --force");
    EXPECT_NE(output.find("Successfully deleted 3"), std::string::npos);
    
    // Verify pattern matching worked
    EXPECT_FALSE(documentExists("temp_1.log"));
    EXPECT_FALSE(documentExists("temp_2.log"));
    EXPECT_FALSE(documentExists("temp_3.log"));
    EXPECT_TRUE(documentExists("keep.txt"));  // This should still exist
}

TEST_F(CLIDeleteTest, DryRunMode) {
    // Add test files
    addFile("dryrun1.txt", "Content 1");
    addFile("dryrun2.txt", "Content 2");
    
    // Run delete in dry-run mode
    std::string output = runCommand("delete --pattern \"dryrun*.txt\" --dry-run");
    EXPECT_NE(output.find("DRY RUN"), std::string::npos);
    EXPECT_NE(output.find("Would delete 2"), std::string::npos);
    
    // Verify files still exist (dry-run shouldn't delete)
    EXPECT_TRUE(documentExists("dryrun1.txt"));
    EXPECT_TRUE(documentExists("dryrun2.txt"));
}

TEST_F(CLIDeleteTest, AmbiguousNameHandling) {
    // Add files with the same name (simulating different paths)
    addFile("duplicate.txt", "Content 1");
    // In real scenario, this would be from different directories
    // For now, we'll skip this test as it requires more complex setup
    
    // This test would verify that when multiple files have the same name,
    // the user is prompted to confirm or use --force
}

TEST_F(CLIDeleteTest, DeleteNonExistentFile) {
    // Try to delete a file that doesn't exist
    std::string output = runCommand("delete --name \"nonexistent.txt\" --force", false);
    EXPECT_NE(output.find("No documents found"), std::string::npos);
}

TEST_F(CLIDeleteTest, DeleteWithVerboseOutput) {
    // Add a test file
    addFile("verbose_test.txt", "Test content");
    
    // Delete with verbose flag
    std::string output = runCommand("delete --name \"verbose_test.txt\" --force --verbose");
    EXPECT_NE(output.find("Deleting verbose_test.txt"), std::string::npos);
    EXPECT_NE(output.find("Successfully deleted"), std::string::npos);
}

TEST_F(CLIDeleteTest, DeleteEmptyPattern) {
    // Add some files
    addFile("test1.txt", "Content 1");
    addFile("test2.txt", "Content 2");
    
    // Try to delete with a pattern that matches nothing
    std::string output = runCommand("delete --pattern \"*.nonexistent\" --force", false);
    EXPECT_NE(output.find("No documents found"), std::string::npos);
    
    // Verify files still exist
    EXPECT_TRUE(documentExists("test1.txt"));
    EXPECT_TRUE(documentExists("test2.txt"));
}

TEST_F(CLIDeleteTest, PartialFailureHandling) {
    // This test would verify that when some deletions succeed and others fail,
    // the command reports both successes and failures correctly
    // For now, this is a placeholder as it requires more complex setup
}

// Main function for running tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}