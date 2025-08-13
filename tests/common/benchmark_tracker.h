#pragma once

#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <fstream>
#include <filesystem>
#include <optional>
#include <nlohmann/json.hpp>

namespace yams::test {

class BenchmarkTracker {
public:
    struct BenchmarkResult {
        std::string name;
        double value;
        std::string unit;
        std::chrono::system_clock::time_point timestamp;
        std::map<std::string, std::string> environment;
        std::map<std::string, double> metrics; // Additional metrics
        
        nlohmann::json toJSON() const {
            nlohmann::json j;
            j["name"] = name;
            j["value"] = value;
            j["unit"] = unit;
            j["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                timestamp.time_since_epoch()).count();
            j["environment"] = environment;
            j["metrics"] = metrics;
            return j;
        }
        
        static BenchmarkResult fromJSON(const nlohmann::json& j) {
            BenchmarkResult result;
            result.name = j["name"];
            result.value = j["value"];
            result.unit = j["unit"];
            result.timestamp = std::chrono::system_clock::time_point(
                std::chrono::milliseconds(j["timestamp"].get<int64_t>()));
            result.environment = j["environment"].get<std::map<std::string, std::string>>();
            if (j.contains("metrics")) {
                result.metrics = j["metrics"].get<std::map<std::string, double>>();
            }
            return result;
        }
    };
    
    BenchmarkTracker(const std::filesystem::path& dataPath = "benchmark_results.json") 
        : dataPath_(dataPath) {
        loadHistory();
        captureEnvironment();
    }
    
    ~BenchmarkTracker() {
        saveHistory();
    }
    
    void recordResult(const BenchmarkResult& result) {
        results_.push_back(result);
        
        // Update history
        if (history_.find(result.name) == history_.end()) {
            history_[result.name] = std::vector<BenchmarkResult>();
        }
        history_[result.name].push_back(result);
        
        // Check for regression immediately
        if (detectRegression(result.name)) {
            std::cerr << "WARNING: Performance regression detected for " << result.name << std::endl;
        }
    }
    
    std::vector<BenchmarkResult> getHistory(const std::string& name, size_t limit = 100) {
        if (history_.find(name) == history_.end()) {
            return {};
        }
        
        auto& results = history_[name];
        size_t start = results.size() > limit ? results.size() - limit : 0;
        return std::vector<BenchmarkResult>(results.begin() + start, results.end());
    }
    
    bool detectRegression(const std::string& name, double threshold = 0.1) {
        auto history = getHistory(name, 10);
        if (history.size() < 2) {
            return false; // Not enough data
        }
        
        // Compare latest with average of previous results
        double latest = history.back().value;
        double sum = 0;
        for (size_t i = 0; i < history.size() - 1; ++i) {
            sum += history[i].value;
        }
        double average = sum / (history.size() - 1);
        
        // Check if latest is significantly worse (higher for time, lower for throughput)
        if (history.back().unit.find("ms") != std::string::npos ||
            history.back().unit.find("sec") != std::string::npos) {
            // Time-based metric: higher is worse
            return (latest - average) / average > threshold;
        } else {
            // Throughput metric: lower is worse
            return (average - latest) / average > threshold;
        }
    }
    
    void generateReport(const std::filesystem::path& outputPath) {
        nlohmann::json report;
        report["generated_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        report["environment"] = currentEnvironment_;
        
        // Summary statistics
        nlohmann::json summary;
        for (const auto& [name, results] : history_) {
            if (results.empty()) continue;
            
            nlohmann::json benchSummary;
            benchSummary["name"] = name;
            benchSummary["runs"] = results.size();
            
            // Calculate statistics
            double sum = 0, min = results[0].value, max = results[0].value;
            for (const auto& r : results) {
                sum += r.value;
                min = std::min(min, r.value);
                max = std::max(max, r.value);
            }
            
            benchSummary["average"] = sum / results.size();
            benchSummary["min"] = min;
            benchSummary["max"] = max;
            benchSummary["latest"] = results.back().value;
            benchSummary["unit"] = results.back().unit;
            
            // Detect trend
            if (results.size() >= 3) {
                double firstHalf = 0, secondHalf = 0;
                size_t mid = results.size() / 2;
                for (size_t i = 0; i < mid; ++i) {
                    firstHalf += results[i].value;
                }
                for (size_t i = mid; i < results.size(); ++i) {
                    secondHalf += results[i].value;
                }
                firstHalf /= mid;
                secondHalf /= (results.size() - mid);
                
                double change = (secondHalf - firstHalf) / firstHalf;
                benchSummary["trend"] = change > 0.05 ? "degrading" : 
                                       change < -0.05 ? "improving" : "stable";
                benchSummary["trend_percent"] = change * 100;
            }
            
            summary[name] = benchSummary;
        }
        
        report["summary"] = summary;
        report["results"] = nlohmann::json::array();
        
        for (const auto& result : results_) {
            report["results"].push_back(result.toJSON());
        }
        
        // Write report
        std::ofstream file(outputPath);
        file << report.dump(2);
        file.close();
    }
    
    void generateMarkdownReport(const std::filesystem::path& outputPath) {
        std::ofstream file(outputPath);
        file << "# Benchmark Report\n\n";
        file << "Generated: " << getCurrentTimestamp() << "\n\n";
        
        file << "## Environment\n\n";
        for (const auto& [key, value] : currentEnvironment_) {
            file << "- **" << key << "**: " << value << "\n";
        }
        file << "\n";
        
        file << "## Results Summary\n\n";
        file << "| Benchmark | Latest | Average | Min | Max | Trend | Runs |\n";
        file << "|-----------|--------|---------|-----|-----|-------|------|\n";
        
        for (const auto& [name, results] : history_) {
            if (results.empty()) continue;
            
            double sum = 0, min = results[0].value, max = results[0].value;
            for (const auto& r : results) {
                sum += r.value;
                min = std::min(min, r.value);
                max = std::max(max, r.value);
            }
            double avg = sum / results.size();
            
            std::string trend = "→";
            if (results.size() >= 3) {
                double recent = results.back().value;
                double change = (recent - avg) / avg;
                if (change > 0.05) trend = "↗️";
                else if (change < -0.05) trend = "↘️";
            }
            
            file << "| " << name 
                 << " | " << formatValue(results.back().value, results.back().unit)
                 << " | " << formatValue(avg, results.back().unit)
                 << " | " << formatValue(min, results.back().unit)
                 << " | " << formatValue(max, results.back().unit)
                 << " | " << trend
                 << " | " << results.size() << " |\n";
        }
        
        file << "\n## Regression Analysis\n\n";
        
        bool hasRegression = false;
        for (const auto& [name, _] : history_) {
            if (detectRegression(name)) {
                hasRegression = true;
                file << "⚠️ **" << name << "**: Performance regression detected\n";
            }
        }
        
        if (!hasRegression) {
            file << "✅ No performance regressions detected\n";
        }
        
        file.close();
    }
    
    // Compare with baseline
    struct ComparisonResult {
        std::string benchmark;
        double baseline;
        double current;
        double percentChange;
        bool regression;
    };
    
    std::vector<ComparisonResult> compareWithBaseline(const std::filesystem::path& baselinePath) {
        std::vector<ComparisonResult> comparisons;
        
        // Load baseline
        std::ifstream baselineFile(baselinePath);
        if (!baselineFile.is_open()) {
            return comparisons;
        }
        
        nlohmann::json baseline = nlohmann::json::parse(baselineFile);
        baselineFile.close();
        
        // Compare each benchmark
        for (const auto& result : results_) {
            if (baseline["summary"].contains(result.name)) {
                ComparisonResult comp;
                comp.benchmark = result.name;
                comp.baseline = baseline["summary"][result.name]["average"];
                comp.current = result.value;
                comp.percentChange = ((comp.current - comp.baseline) / comp.baseline) * 100;
                
                // Determine if regression based on metric type
                if (result.unit.find("ms") != std::string::npos ||
                    result.unit.find("sec") != std::string::npos) {
                    comp.regression = comp.percentChange > 10; // 10% slower
                } else {
                    comp.regression = comp.percentChange < -10; // 10% less throughput
                }
                
                comparisons.push_back(comp);
            }
        }
        
        return comparisons;
    }

private:
    std::filesystem::path dataPath_;
    std::vector<BenchmarkResult> results_;
    std::map<std::string, std::vector<BenchmarkResult>> history_;
    std::map<std::string, std::string> currentEnvironment_;
    
    void loadHistory() {
        if (!std::filesystem::exists(dataPath_)) {
            return;
        }
        
        std::ifstream file(dataPath_);
        if (!file.is_open()) {
            return;
        }
        
        try {
            nlohmann::json data = nlohmann::json::parse(file);
            if (data.contains("history")) {
                for (const auto& [name, results] : data["history"].items()) {
                    history_[name] = std::vector<BenchmarkResult>();
                    for (const auto& r : results) {
                        history_[name].push_back(BenchmarkResult::fromJSON(r));
                    }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Failed to load benchmark history: " << e.what() << std::endl;
        }
        
        file.close();
    }
    
    void saveHistory() {
        nlohmann::json data;
        data["history"] = nlohmann::json::object();
        
        for (const auto& [name, results] : history_) {
            data["history"][name] = nlohmann::json::array();
            for (const auto& r : results) {
                data["history"][name].push_back(r.toJSON());
            }
        }
        
        std::ofstream file(dataPath_);
        file << data.dump(2);
        file.close();
    }
    
    void captureEnvironment() {
        // Capture system information
        currentEnvironment_["platform"] = 
#ifdef _WIN32
            "Windows";
#elif __APPLE__
            "macOS";
#elif __linux__
            "Linux";
#else
            "Unknown";
#endif
        
        currentEnvironment_["compiler"] = 
#ifdef __clang__
            "Clang " + std::to_string(__clang_major__);
#elif __GNUC__
            "GCC " + std::to_string(__GNUC__);
#elif _MSC_VER
            "MSVC " + std::to_string(_MSC_VER);
#else
            "Unknown";
#endif
        
        currentEnvironment_["build_type"] = 
#ifdef NDEBUG
            "Release";
#else
            "Debug";
#endif
        
        // CPU info would require platform-specific code
        currentEnvironment_["cpu_cores"] = std::to_string(std::thread::hardware_concurrency());
    }
    
    std::string getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
        return ss.str();
    }
    
    std::string formatValue(double value, const std::string& unit) {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << value << " " << unit;
        return ss.str();
    }
};

} // namespace yams::test