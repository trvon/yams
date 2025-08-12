#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>

namespace yams::tools {

/**
 * Simple progress bar for CLI tools
 * 
 * Thread-safe implementation that can be updated from multiple threads
 */
class ProgressBar {
public:
    explicit ProgressBar(size_t total, const std::string& prefix = "")
        : total_(total)
        , prefix_(prefix)
        , startTime_(std::chrono::steady_clock::now())
    {
        if (!prefix_.empty()) {
            prefix_ += ": ";
        }
    }
    
    // Update progress
    void update(size_t current) {
        current_ = current;
        display();
    }
    
    // Increment progress by one
    void increment() {
        current_++;
        display();
    }
    
    // Mark as complete
    void finish() {
        current_ = total_;
        display();
        std::cout << std::endl;
    }
    
    // Set custom message
    void setMessage(const std::string& message) {
        std::lock_guard lock(mutex_);
        message_ = message;
        display();
    }
    
private:
    void display() {
        std::lock_guard lock(mutex_);
        
        if (isQuiet_) return;
        
        // Calculate percentage
        double percentage = total_ > 0 ? 
            (static_cast<double>(current_.load()) / total_) * 100.0 : 0.0;
        
        // Calculate elapsed time
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - startTime_).count();
        
        // Calculate ETA
        size_t eta = 0;
        if (current_ > 0 && current_ < total_) {
            double rate = static_cast<double>(current_) / elapsed;
            eta = static_cast<size_t>((total_ - current_) / rate);
        }
        
        // Build progress bar
        const int barWidth = 50;
        int progress = static_cast<int>(percentage / 100.0 * barWidth);
        
        std::cout << '\r' << prefix_;
        std::cout << '[';
        
        for (int i = 0; i < barWidth; ++i) {
            if (i < progress) {
                std::cout << '=';
            } else if (i == progress) {
                std::cout << '>';
            } else {
                std::cout << ' ';
            }
        }
        
        std::cout << "] ";
        std::cout << std::fixed << std::setprecision(1) << percentage << "% ";
        std::cout << "(" << current_.load() << "/" << total_ << ") ";
        
        if (!message_.empty()) {
            std::cout << message_ << " ";
        }
        
        if (eta > 0) {
            std::cout << "ETA: " << formatTime(eta);
        }
        
        std::cout << std::flush;
    }
    
    std::string formatTime(size_t seconds) const {
        if (seconds < 60) {
            return std::to_string(seconds) + "s";
        } else if (seconds < 3600) {
            return std::to_string(seconds / 60) + "m " + 
                   std::to_string(seconds % 60) + "s";
        } else {
            return std::to_string(seconds / 3600) + "h " +
                   std::to_string((seconds % 3600) / 60) + "m";
        }
    }
    
    size_t total_;
    std::atomic<size_t> current_{0};
    std::string prefix_;
    std::string message_;
    std::chrono::steady_clock::time_point startTime_;
    mutable std::mutex mutex_;
    bool isQuiet_ = false;
};

} // namespace yams::tools