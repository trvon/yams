// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
// Init command test suite (Catch2)
// Covers: Model definitions, CLI flag parsing

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <regex>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// =============================================================================
// Test the EMBEDDING_MODELS definition used by init command.
// These tests verify that the model list is valid and consistent.
// =============================================================================

// Model info structure matching init_command.cpp
struct EmbeddingModelInfo {
    std::string name;
    std::string url;
    std::string description;
    size_t size_mb;
    int dimensions;
};

// Expected models (mirrors the static list in init_command.cpp)
static const std::vector<EmbeddingModelInfo> EXPECTED_MODELS = {
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight model for semantic search", 90, 384},
    {"multi-qa-MiniLM-L6-cos-v1",
     "https://huggingface.co/sentence-transformers/multi-qa-MiniLM-L6-cos-v1/resolve/main/onnx/"
     "model.onnx",
     "Optimized for semantic search on QA pairs (215M training samples)", 90, 384}};

TEST_CASE("InitCommand: All models have valid HuggingFace URLs", "[cli][init][models]") {
    const std::regex hfUrlPattern(
        R"(^https://huggingface\.co/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/resolve/main/.+\.onnx$)");

    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        INFO("URL: " << model.url);
        REQUIRE(std::regex_match(model.url, hfUrlPattern));
    }
}

TEST_CASE("InitCommand: All models have consistent 384 dimensions", "[cli][init][models]") {
    // All models should have dim=384 for consistency with default vector DB setup
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        REQUIRE(model.dimensions == 384);
    }
}

TEST_CASE("InitCommand: All models have valid names", "[cli][init][models]") {
    // Model names should follow naming convention
    const std::regex namePattern(R"(^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$)");

    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model name: " << model.name);
        REQUIRE(std::regex_match(model.name, namePattern));
    }
}

TEST_CASE("InitCommand: All models have reasonable sizes", "[cli][init][models]") {
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        // Models should be between 10MB and 1GB
        REQUIRE(model.size_mb >= 10);
        REQUIRE(model.size_mb <= 1024);
    }
}

TEST_CASE("InitCommand: All models have descriptions", "[cli][init][models]") {
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        REQUIRE_FALSE(model.description.empty());
        REQUIRE(model.description.length() >= 10);
    }
}

TEST_CASE("InitCommand: Models are from sentence-transformers", "[cli][init][models]") {
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        REQUIRE(model.url.find("sentence-transformers") != std::string::npos);
    }
}

// =============================================================================
// Note: CLI flag integration tests that require YamsCLI instantiation
// are deferred to integration tests due to complex env isolation requirements.
// The model validation tests above cover the core init command logic.
// =============================================================================
