#include <gtest/gtest.h>
#include <yams/api/content_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/database.h>
#include "../../common/test_data_generator.h"
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <sstream>

using namespace yams;
using namespace yams::api;
using namespace yams::metadata;
using namespace yams::test;

class TaskManagementTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = std::filesystem::temp_directory_path() / "yams_task_management_test";
        std::filesystem::create_directories(testDir_);
        
        initializeYAMS();
        generator_ = std::make_unique<TestDataGenerator>();
    }
    
    void TearDown() override {
        contentStore_.reset();
        metadataRepo_.reset();
        database_.reset();
        std::filesystem::remove_all(testDir_);
    }
    
    void initializeYAMS() {
        auto dbPath = testDir_ / "yams.db";
        database_ = std::make_shared<Database>(dbPath.string());
        database_->initialize();
        metadataRepo_ = std::make_shared<MetadataRepository>(database_);
        contentStore_ = std::make_unique<ContentStore>(testDir_.string());
    }
    
    std::string getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%dT%H:%M:%S");
        return ss.str();
    }
    
    std::string createTaskDocument(const std::string& taskId, const std::string& title, 
                                  const std::string& description) {
        std::stringstream doc;
        doc << "# Task " << taskId << ": " << title << "\n\n";
        doc << "## Description\n" << description << "\n\n";
        doc << "## Acceptance Criteria\n";
        doc << "- [ ] Implement feature\n";
        doc << "- [ ] Write tests\n";
        doc << "- [ ] Update documentation\n\n";
        doc << "## Notes\n";
        doc << "Created: " << getCurrentTimestamp() << "\n";
        return doc.str();
    }
    
    std::string createPBIDocument(const std::string& pbiId, const std::string& title,
                                 const std::vector<std::string>& taskIds) {
        std::stringstream doc;
        doc << "# PBI " << pbiId << ": " << title << "\n\n";
        doc << "## Summary\n";
        doc << "Product backlog item for " << title << "\n\n";
        doc << "## Tasks\n";
        for (const auto& taskId : taskIds) {
            doc << "- " << taskId << "\n";
        }
        doc << "\n## Status\nActive\n";
        return doc.str();
    }
    
    std::filesystem::path testDir_;
    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::unique_ptr<ContentStore> contentStore_;
    std::unique_ptr<TestDataGenerator> generator_;
};

TEST_F(TaskManagementTest, TaskTrackingWorkflow) {
    // Create and track tasks using metadata
    
    struct Task {
        std::string id;
        std::string title;
        std::string status;
        std::string assignee;
        int priority;
        std::string pbiId;
    };
    
    std::vector<Task> tasks = {
        {"TASK-001", "Implement search feature", "pending", "alice", 1, "PBI-001"},
        {"TASK-002", "Write unit tests", "pending", "bob", 2, "PBI-001"},
        {"TASK-003", "Update documentation", "pending", "charlie", 3, "PBI-001"},
        {"TASK-004", "Performance optimization", "pending", "alice", 1, "PBI-002"},
        {"TASK-005", "Security review", "pending", "diana", 1, "PBI-002"}
    };
    
    std::map<std::string, std::string> taskHashes;
    std::map<std::string, std::string> taskDocIds;
    
    // Create task documents
    for (const auto& task : tasks) {
        auto content = createTaskDocument(task.id, task.title, 
                                         "Task description for " + task.title);
        auto file = testDir_ / (task.id + ".md");
        std::ofstream(file) << content;
        
        auto result = contentStore_->addFile(file, {"task", task.pbiId});
        ASSERT_TRUE(result.has_value()) << "Failed to add task " << task.id;
        taskHashes[task.id] = result->contentHash;
        
        // Get document ID and set metadata
        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        auto docId = docResult->value()->id;
        taskDocIds[task.id] = docId;
        
        // Set task metadata
        metadataRepo_->setMetadata(docId, "task_id", MetadataValue(task.id));
        metadataRepo_->setMetadata(docId, "status", MetadataValue(task.status));
        metadataRepo_->setMetadata(docId, "assignee", MetadataValue(task.assignee));
        metadataRepo_->setMetadata(docId, "priority", MetadataValue(static_cast<double>(task.priority)));
        metadataRepo_->setMetadata(docId, "pbi_id", MetadataValue(task.pbiId));
        metadataRepo_->setMetadata(docId, "created", MetadataValue(getCurrentTimestamp()));
    }
    
    // Simulate task lifecycle
    
    // 1. Start work on TASK-001
    metadataRepo_->setMetadata(taskDocIds["TASK-001"], "status", MetadataValue("in_progress"));
    metadataRepo_->setMetadata(taskDocIds["TASK-001"], "started", MetadataValue(getCurrentTimestamp()));
    
    // 2. Query for in-progress tasks
    search::SearchOptions inProgressOptions;
    inProgressOptions.metadataFilter = "status:in_progress";
    auto inProgressResults = contentStore_->searchWithOptions("*", inProgressOptions);
    EXPECT_EQ(inProgressResults.size(), 1) << "Should have exactly one in-progress task";
    
    // 3. Complete TASK-001
    metadataRepo_->setMetadata(taskDocIds["TASK-001"], "status", MetadataValue("completed"));
    metadataRepo_->setMetadata(taskDocIds["TASK-001"], "completed", MetadataValue(getCurrentTimestamp()));
    
    // 4. Start TASK-002
    metadataRepo_->setMetadata(taskDocIds["TASK-002"], "status", MetadataValue("in_progress"));
    
    // 5. Query tasks by assignee
    search::SearchOptions aliceOptions;
    aliceOptions.metadataFilter = "assignee:alice";
    auto aliceTasks = contentStore_->searchWithOptions("*", aliceOptions);
    EXPECT_EQ(aliceTasks.size(), 2) << "Alice should have 2 tasks";
    
    // 6. Query high priority pending tasks
    search::SearchOptions highPriorityOptions;
    highPriorityOptions.metadataFilter = "priority:1 AND status:pending";
    auto highPriorityTasks = contentStore_->searchWithOptions("*", highPriorityOptions);
    EXPECT_EQ(highPriorityTasks.size(), 2) << "Should have 2 high priority pending tasks";
}

TEST_F(TaskManagementTest, PBIManagementPatterns) {
    // Test PBI (Product Backlog Item) management
    
    // Create PBI
    std::vector<std::string> pbi1Tasks = {"TASK-001", "TASK-002", "TASK-003"};
    auto pbiContent = createPBIDocument("PBI-001", "Search Enhancement", pbi1Tasks);
    auto pbiFile = testDir_ / "PBI-001.md";
    std::ofstream(pbiFile) << pbiContent;
    
    auto pbiResult = contentStore_->addFile(pbiFile, {"pbi", "backlog"});
    ASSERT_TRUE(pbiResult.has_value());
    
    auto pbiDocResult = metadataRepo_->getDocumentByHash(pbiResult->contentHash);
    ASSERT_TRUE(pbiDocResult.has_value() && pbiDocResult->has_value());
    auto pbiDocId = pbiDocResult->value()->id;
    
    // Set PBI metadata
    metadataRepo_->setMetadata(pbiDocId, "pbi_id", MetadataValue("PBI-001"));
    metadataRepo_->setMetadata(pbiDocId, "status", MetadataValue("active"));
    metadataRepo_->setMetadata(pbiDocId, "sprint", MetadataValue("2024-Q1"));
    metadataRepo_->setMetadata(pbiDocId, "story_points", MetadataValue(8.0));
    
    // Create associated tasks
    std::vector<std::string> taskDocIds;
    for (const auto& taskId : pbi1Tasks) {
        auto taskContent = createTaskDocument(taskId, "Task for " + taskId, "Description");
        auto taskFile = testDir_ / (taskId + ".md");
        std::ofstream(taskFile) << taskContent;
        
        auto taskResult = contentStore_->addFile(taskFile, {"task"});
        ASSERT_TRUE(taskResult.has_value());
        
        auto taskDocResult = metadataRepo_->getDocumentByHash(taskResult->contentHash);
        ASSERT_TRUE(taskDocResult.has_value() && taskDocResult->has_value());
        auto taskDocId = taskDocResult->value()->id;
        taskDocIds.push_back(taskDocId);
        
        // Link task to PBI
        metadataRepo_->setMetadata(taskDocId, "task_id", MetadataValue(taskId));
        metadataRepo_->setMetadata(taskDocId, "pbi_id", MetadataValue("PBI-001"));
        metadataRepo_->setMetadata(taskDocId, "status", MetadataValue("pending"));
    }
    
    // Track PBI progress through task completion
    
    // Complete first task
    metadataRepo_->setMetadata(taskDocIds[0], "status", MetadataValue("completed"));
    
    // Query tasks for PBI
    search::SearchOptions pbiTaskOptions;
    pbiTaskOptions.metadataFilter = "pbi_id:PBI-001";
    auto pbiTasks = contentStore_->searchWithOptions("*", pbiTaskOptions);
    EXPECT_EQ(pbiTasks.size(), 3) << "Should find all 3 tasks for PBI";
    
    // Count completed tasks
    search::SearchOptions completedTaskOptions;
    completedTaskOptions.metadataFilter = "pbi_id:PBI-001 AND status:completed";
    auto completedTasks = contentStore_->searchWithOptions("*", completedTaskOptions);
    EXPECT_EQ(completedTasks.size(), 1) << "Should have 1 completed task";
    
    // Update PBI progress
    double progress = (completedTasks.size() * 100.0) / pbiTasks.size();
    metadataRepo_->setMetadata(pbiDocId, "progress_percent", MetadataValue(progress));
    
    // Query active PBIs
    search::SearchOptions activePBIOptions;
    activePBIOptions.metadataFilter = "status:active";
    activePBIOptions.tags = {"pbi"};
    auto activePBIs = contentStore_->searchWithOptions("*", activePBIOptions);
    EXPECT_EQ(activePBIs.size(), 1) << "Should have 1 active PBI";
}

TEST_F(TaskManagementTest, StatusUpdateWorkflow) {
    // Test task status transitions
    
    // Create a task
    auto taskContent = createTaskDocument("TASK-100", "Test Status Transitions", "Test task");
    auto taskFile = testDir_ / "TASK-100.md";
    std::ofstream(taskFile) << taskContent;
    
    auto result = contentStore_->addFile(taskFile, {"task", "workflow"});
    ASSERT_TRUE(result.has_value());
    
    auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
    ASSERT_TRUE(docResult.has_value() && docResult->has_value());
    auto docId = docResult->value()->id;
    
    // Track status transitions
    struct StatusTransition {
        std::string from;
        std::string to;
        std::string timestamp;
    };
    
    std::vector<StatusTransition> transitions;
    
    // Initial status
    metadataRepo_->setMetadata(docId, "status", MetadataValue("pending"));
    transitions.push_back({"", "pending", getCurrentTimestamp()});
    
    // Transition: pending -> in_progress
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    metadataRepo_->setMetadata(docId, "status", MetadataValue("in_progress"));
    transitions.push_back({"pending", "in_progress", getCurrentTimestamp()});
    
    // Transition: in_progress -> blocked
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    metadataRepo_->setMetadata(docId, "status", MetadataValue("blocked"));
    metadataRepo_->setMetadata(docId, "blocked_reason", MetadataValue("Waiting for dependencies"));
    transitions.push_back({"in_progress", "blocked", getCurrentTimestamp()});
    
    // Transition: blocked -> in_progress
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    metadataRepo_->setMetadata(docId, "status", MetadataValue("in_progress"));
    metadataRepo_->removeMetadata(docId, "blocked_reason");
    transitions.push_back({"blocked", "in_progress", getCurrentTimestamp()});
    
    // Transition: in_progress -> completed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    metadataRepo_->setMetadata(docId, "status", MetadataValue("completed"));
    transitions.push_back({"in_progress", "completed", getCurrentTimestamp()});
    
    // Store transition history
    for (size_t i = 0; i < transitions.size(); ++i) {
        std::string key = "transition_" + std::to_string(i);
        std::string value = transitions[i].from + "->" + transitions[i].to + "@" + transitions[i].timestamp;
        metadataRepo_->setMetadata(docId, key, MetadataValue(value));
    }
    
    // Verify final status
    auto finalStatus = metadataRepo_->getMetadata(docId, "status");
    ASSERT_TRUE(finalStatus.has_value());
    EXPECT_EQ(finalStatus->toString(), "completed");
    
    // Verify transition history
    for (size_t i = 0; i < transitions.size(); ++i) {
        std::string key = "transition_" + std::to_string(i);
        auto transition = metadataRepo_->getMetadata(docId, key);
        EXPECT_TRUE(transition.has_value()) << "Transition " << i << " not found";
    }
}

TEST_F(TaskManagementTest, TaskDependencies) {
    // Test task dependency management
    
    struct TaskDep {
        std::string id;
        std::string title;
        std::vector<std::string> dependencies;
    };
    
    std::vector<TaskDep> tasks = {
        {"TASK-A", "Database Schema", {}},
        {"TASK-B", "API Implementation", {"TASK-A"}},
        {"TASK-C", "Frontend Integration", {"TASK-B"}},
        {"TASK-D", "Testing", {"TASK-B", "TASK-C"}},
        {"TASK-E", "Documentation", {"TASK-D"}}
    };
    
    std::map<std::string, std::string> taskDocIds;
    
    // Create tasks with dependencies
    for (const auto& task : tasks) {
        auto content = createTaskDocument(task.id, task.title, "Task with dependencies");
        auto file = testDir_ / (task.id + ".md");
        std::ofstream(file) << content;
        
        auto result = contentStore_->addFile(file, {"task", "dependency"});
        ASSERT_TRUE(result.has_value());
        
        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        auto docId = docResult->value()->id;
        taskDocIds[task.id] = docId;
        
        // Set metadata
        metadataRepo_->setMetadata(docId, "task_id", MetadataValue(task.id));
        metadataRepo_->setMetadata(docId, "status", MetadataValue("pending"));
        
        // Set dependencies
        if (!task.dependencies.empty()) {
            std::string depList = "";
            for (size_t i = 0; i < task.dependencies.size(); ++i) {
                if (i > 0) depList += ",";
                depList += task.dependencies[i];
            }
            metadataRepo_->setMetadata(docId, "dependencies", MetadataValue(depList));
            metadataRepo_->setMetadata(docId, "status", MetadataValue("blocked"));
        }
    }
    
    // Function to check if task can be started
    auto canStart = [&](const std::string& taskId) -> bool {
        auto depMeta = metadataRepo_->getMetadata(taskDocIds[taskId], "dependencies");
        if (!depMeta.has_value()) return true; // No dependencies
        
        std::string deps = depMeta->toString();
        std::stringstream ss(deps);
        std::string dep;
        
        while (std::getline(ss, dep, ',')) {
            auto depStatus = metadataRepo_->getMetadata(taskDocIds[dep], "status");
            if (!depStatus.has_value() || depStatus->toString() != "completed") {
                return false;
            }
        }
        return true;
    };
    
    // Complete tasks in order
    
    // Complete TASK-A (no dependencies)
    EXPECT_TRUE(canStart("TASK-A")) << "TASK-A should be able to start";
    metadataRepo_->setMetadata(taskDocIds["TASK-A"], "status", MetadataValue("completed"));
    
    // Now TASK-B can start
    EXPECT_TRUE(canStart("TASK-B")) << "TASK-B should be able to start after A completes";
    metadataRepo_->setMetadata(taskDocIds["TASK-B"], "status", MetadataValue("in_progress"));
    
    // TASK-C still blocked (B not complete)
    EXPECT_FALSE(canStart("TASK-C")) << "TASK-C should still be blocked";
    
    // Complete TASK-B
    metadataRepo_->setMetadata(taskDocIds["TASK-B"], "status", MetadataValue("completed"));
    
    // Now TASK-C can start
    EXPECT_TRUE(canStart("TASK-C")) << "TASK-C should be able to start";
    
    // TASK-D still blocked (C not complete)
    EXPECT_FALSE(canStart("TASK-D")) << "TASK-D should still be blocked";
}

TEST_F(TaskManagementTest, SprintManagement) {
    // Test sprint-based task management
    
    std::string currentSprint = "2024-S1";
    std::string nextSprint = "2024-S2";
    
    // Create tasks for current sprint
    std::vector<std::string> currentSprintTasks = {"TASK-201", "TASK-202", "TASK-203"};
    for (const auto& taskId : currentSprintTasks) {
        auto content = createTaskDocument(taskId, "Sprint task " + taskId, "Current sprint work");
        auto file = testDir_ / (taskId + ".md");
        std::ofstream(file) << content;
        
        auto result = contentStore_->addFile(file, {"task", "sprint"});
        ASSERT_TRUE(result.has_value());
        
        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        
        metadataRepo_->setMetadata(docResult->value()->id, "task_id", MetadataValue(taskId));
        metadataRepo_->setMetadata(docResult->value()->id, "sprint", MetadataValue(currentSprint));
        metadataRepo_->setMetadata(docResult->value()->id, "status", MetadataValue("pending"));
        metadataRepo_->setMetadata(docResult->value()->id, "story_points", 
                                  MetadataValue(static_cast<double>(3 + (taskId.back() - '1'))));
    }
    
    // Create backlog tasks
    std::vector<std::string> backlogTasks = {"TASK-301", "TASK-302"};
    for (const auto& taskId : backlogTasks) {
        auto content = createTaskDocument(taskId, "Backlog task " + taskId, "Future work");
        auto file = testDir_ / (taskId + ".md");
        std::ofstream(file) << content;
        
        auto result = contentStore_->addFile(file, {"task", "backlog"});
        ASSERT_TRUE(result.has_value());
        
        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        
        metadataRepo_->setMetadata(docResult->value()->id, "task_id", MetadataValue(taskId));
        metadataRepo_->setMetadata(docResult->value()->id, "sprint", MetadataValue("backlog"));
        metadataRepo_->setMetadata(docResult->value()->id, "status", MetadataValue("pending"));
    }
    
    // Query current sprint tasks
    search::SearchOptions currentSprintOptions;
    currentSprintOptions.metadataFilter = "sprint:" + currentSprint;
    auto currentTasks = contentStore_->searchWithOptions("*", currentSprintOptions);
    EXPECT_EQ(currentTasks.size(), 3) << "Should have 3 tasks in current sprint";
    
    // Calculate sprint velocity
    double totalPoints = 0;
    for (const auto& result : currentTasks) {
        auto points = metadataRepo_->getMetadata(result.document.id, "story_points");
        if (points.has_value()) {
            totalPoints += points->toDouble();
        }
    }
    EXPECT_GT(totalPoints, 0) << "Sprint should have story points";
    
    // Move task from backlog to next sprint
    search::SearchOptions backlogOptions;
    backlogOptions.metadataFilter = "sprint:backlog";
    auto backlogResults = contentStore_->searchWithOptions("*", backlogOptions);
    
    if (!backlogResults.empty()) {
        auto& taskToMove = backlogResults[0];
        auto docResult = metadataRepo_->getDocumentByHash(taskToMove.document.hash);
        if (docResult.has_value() && docResult->has_value()) {
            metadataRepo_->setMetadata(docResult->value()->id, "sprint", MetadataValue(nextSprint));
        }
    }
    
    // Verify task moved
    search::SearchOptions nextSprintOptions;
    nextSprintOptions.metadataFilter = "sprint:" + nextSprint;
    auto nextSprintTasks = contentStore_->searchWithOptions("*", nextSprintOptions);
    EXPECT_EQ(nextSprintTasks.size(), 1) << "Should have 1 task in next sprint";
}

TEST_F(TaskManagementTest, ReportingAndAnalytics) {
    // Test reporting capabilities
    
    // Create tasks with various statuses and metadata
    struct TaskData {
        std::string id;
        std::string status;
        std::string assignee;
        double hoursSpent;
        double hoursEstimated;
    };
    
    std::vector<TaskData> tasks = {
        {"RPT-001", "completed", "alice", 8.0, 6.0},
        {"RPT-002", "completed", "bob", 4.0, 5.0},
        {"RPT-003", "in_progress", "alice", 3.0, 8.0},
        {"RPT-004", "in_progress", "charlie", 2.0, 4.0},
        {"RPT-005", "pending", "bob", 0.0, 6.0},
        {"RPT-006", "pending", "diana", 0.0, 3.0},
        {"RPT-007", "blocked", "alice", 1.0, 5.0}
    };
    
    for (const auto& task : tasks) {
        auto content = createTaskDocument(task.id, "Report task " + task.id, "Analytics test");
        auto file = testDir_ / (task.id + ".md");
        std::ofstream(file) << content;
        
        auto result = contentStore_->addFile(file, {"task", "report"});
        ASSERT_TRUE(result.has_value());
        
        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        auto docId = docResult->value()->id;
        
        metadataRepo_->setMetadata(docId, "task_id", MetadataValue(task.id));
        metadataRepo_->setMetadata(docId, "status", MetadataValue(task.status));
        metadataRepo_->setMetadata(docId, "assignee", MetadataValue(task.assignee));
        metadataRepo_->setMetadata(docId, "hours_spent", MetadataValue(task.hoursSpent));
        metadataRepo_->setMetadata(docId, "hours_estimated", MetadataValue(task.hoursEstimated));
    }
    
    // Generate reports
    
    // 1. Status distribution
    std::map<std::string, int> statusCount;
    std::vector<std::string> statuses = {"pending", "in_progress", "completed", "blocked"};
    
    for (const auto& status : statuses) {
        search::SearchOptions options;
        options.metadataFilter = "status:" + status;
        options.tags = {"report"};
        auto results = contentStore_->searchWithOptions("*", options);
        statusCount[status] = results.size();
    }
    
    EXPECT_EQ(statusCount["completed"], 2);
    EXPECT_EQ(statusCount["in_progress"], 2);
    EXPECT_EQ(statusCount["pending"], 2);
    EXPECT_EQ(statusCount["blocked"], 1);
    
    // 2. Workload by assignee
    std::map<std::string, double> workloadByAssignee;
    std::vector<std::string> assignees = {"alice", "bob", "charlie", "diana"};
    
    for (const auto& assignee : assignees) {
        search::SearchOptions options;
        options.metadataFilter = "assignee:" + assignee + " AND status:in_progress";
        options.tags = {"report"};
        auto results = contentStore_->searchWithOptions("*", options);
        
        double totalHours = 0;
        for (const auto& result : results) {
            auto hours = metadataRepo_->getMetadata(result.document.id, "hours_estimated");
            if (hours.has_value()) {
                totalHours += hours->toDouble();
            }
        }
        workloadByAssignee[assignee] = totalHours;
    }
    
    // Alice has 1 in_progress task (8 hours)
    EXPECT_EQ(workloadByAssignee["alice"], 8.0);
    
    // 3. Burndown calculation
    double totalEstimated = 0;
    double totalSpent = 0;
    
    search::SearchOptions allTaskOptions;
    allTaskOptions.tags = {"report"};
    auto allTasks = contentStore_->searchWithOptions("*", allTaskOptions);
    
    for (const auto& result : allTasks) {
        auto estimated = metadataRepo_->getMetadata(result.document.id, "hours_estimated");
        auto spent = metadataRepo_->getMetadata(result.document.id, "hours_spent");
        
        if (estimated.has_value()) totalEstimated += estimated->toDouble();
        if (spent.has_value()) totalSpent += spent->toDouble();
    }
    
    double remainingWork = totalEstimated - totalSpent;
    EXPECT_GT(remainingWork, 0) << "Should have remaining work";
    
    std::cout << "Analytics Report:" << std::endl;
    std::cout << "  Total Estimated: " << totalEstimated << " hours" << std::endl;
    std::cout << "  Total Spent: " << totalSpent << " hours" << std::endl;
    std::cout << "  Remaining: " << remainingWork << " hours" << std::endl;
}