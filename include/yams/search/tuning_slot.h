// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

namespace yams::search {

/// Identifies which layer of the tuning pipeline last set a parameter.
enum class TuningLayer : uint8_t {
    Default,   // SearchEngineConfig compile-time default
    Profile,   // getTunedParams() baseline for a TuningState
    Corpus,    // Corpus-aware adjustments (graph richness, symbol density)
    Env,       // Environment variable override (always pinned)
    Runtime,   // EWMA-based runtime adaptation (SearchTuner::observe)
    Zoom,      // Per-query navigation zoom policy
    Intent,    // Per-query intent-based weight scaling
    Community, // Per-query community override (blended)
    Mode,      // Explicit mode override (e.g., semanticOnly)
};

[[nodiscard]] constexpr std::string_view tuningLayerToString(TuningLayer layer) noexcept {
    switch (layer) {
        case TuningLayer::Default:
            return "Default";
        case TuningLayer::Profile:
            return "Profile";
        case TuningLayer::Corpus:
            return "Corpus";
        case TuningLayer::Env:
            return "Env";
        case TuningLayer::Runtime:
            return "Runtime";
        case TuningLayer::Zoom:
            return "Zoom";
        case TuningLayer::Intent:
            return "Intent";
        case TuningLayer::Community:
            return "Community";
        case TuningLayer::Mode:
            return "Mode";
    }
    return "Unknown";
}

/// A tunable parameter that tracks its provenance (which layer set it)
/// and can be pinned to prevent downstream layers from modifying it.
///
/// Pinning is automatic for Env-layer overrides: once a user sets a value
/// via environment variable, no downstream layer (zoom, intent, community,
/// mode) can modify it. This guarantees that A/B testing overrides reach
/// the query unchanged.
template <typename T> struct TuningSlot {
    T value{};
    TuningLayer source = TuningLayer::Default;
    bool pinned = false;

    constexpr TuningSlot() = default;
    constexpr explicit TuningSlot(T val, TuningLayer src = TuningLayer::Default)
        : value(val), source(src) {}

    /// Apply a multiplicative adjustment. Returns false (no-op) if pinned.
    bool scaleBy(float factor, TuningLayer layer) {
        if (pinned) {
            return false;
        }
        value = static_cast<T>(static_cast<float>(value) * factor);
        source = layer;
        return true;
    }

    /// Set an absolute value. Returns false (no-op) if pinned.
    bool set(T val, TuningLayer layer) {
        if (pinned) {
            return false;
        }
        value = val;
        source = layer;
        return true;
    }

    /// Force-set: overrides even when pinned. Env-layer values auto-pin.
    void forceSet(T val, TuningLayer layer) {
        value = val;
        source = layer;
        pinned = (layer == TuningLayer::Env);
    }

    /// Reset pinned state (used when re-initializing from a profile).
    void unpin() { pinned = false; }

    constexpr operator T() const noexcept { return value; } // NOLINT(google-explicit-constructor)
};

/// Specialization helpers for float slots: clamp after scaling.
inline bool scaleByClamp(TuningSlot<float>& slot, float factor, TuningLayer layer, float low = 0.0F,
                         float high = 1.0F) {
    if (slot.pinned) {
        return false;
    }
    slot.value = std::clamp(slot.value * factor, low, high);
    slot.source = layer;
    return true;
}

inline bool setClamp(TuningSlot<float>& slot, float val, TuningLayer layer, float low = 0.0F,
                     float high = 1.0F) {
    if (slot.pinned) {
        return false;
    }
    slot.value = std::clamp(val, low, high);
    slot.source = layer;
    return true;
}

/// Constexpr linear interpolation for arithmetic types.
/// `t` is the blend factor toward `b`: result = a + t*(b - a).
template <typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>>>
constexpr T lerpValue(T a, T b, float t) {
    if constexpr (std::is_floating_point_v<T>) {
        return static_cast<T>(static_cast<float>(a) +
                              t * (static_cast<float>(b) - static_cast<float>(a)));
    } else {
        return static_cast<T>(std::lround(static_cast<double>(a) +
                                          static_cast<double>(t) *
                                              (static_cast<double>(b) - static_cast<double>(a))));
    }
}

/// Blend a TuningSlot toward a target value (respects pinning).
template <typename T> void blendSlot(TuningSlot<T>& slot, T targetVal, float t, TuningLayer layer) {
    slot.set(lerpValue(slot.value, targetVal, t), layer);
}

/// The seven component weights for search fusion, with provenance tracking.
///
/// `normalize()` enforces the sum-to-1.0 invariant while respecting pinned
/// values: pinned weights are treated as fixed budget allocations and the
/// remaining budget is distributed proportionally among non-pinned weights.
struct WeightSlots {
    TuningSlot<float> text;
    TuningSlot<float> vector;
    TuningSlot<float> entityVector;
    TuningSlot<float> pathTree;
    TuningSlot<float> kg;
    TuningSlot<float> tag;
    TuningSlot<float> metadata;

    /// Normalize all weights to sum to 1.0.
    /// Pinned weights are held constant; remaining budget distributed
    /// proportionally among non-pinned weights.
    void normalize() { // NOLINT(readability-function-cognitive-complexity)
        auto slots = allSlots();

        // Clamp negatives and compute sums in one pass
        float pinnedSum = 0.0F;
        float unpinnedSum = 0.0F;
        for (auto* slot : slots) {
            slot->value = std::max(slot->value, 0.0F);
            if (slot->pinned) {
                pinnedSum += slot->value;
            } else {
                unpinnedSum += slot->value;
            }
        }

        const float budget = std::max(0.0F, 1.0F - pinnedSum);

        if (unpinnedSum > 0.0F) {
            const float scale = budget / unpinnedSum;
            for (auto* slot : slots) {
                if (!slot->pinned) {
                    slot->value *= scale;
                }
            }
        } else if (budget > 0.0F) {
            // All non-pinned are zero -- distribute budget equally
            size_t unpinnedCount = 0;
            for (auto* slot : slots) {
                if (!slot->pinned) {
                    ++unpinnedCount;
                }
            }
            if (unpinnedCount > 0) {
                const float share = budget / static_cast<float>(unpinnedCount);
                for (auto* slot : slots) {
                    if (!slot->pinned) {
                        slot->value = share;
                    }
                }
            }
        }
        // If pinnedSum >= 1.0 and no unpinned, leave as-is (over-budget edge case)
    }

    /// Sum of all weight values.
    [[nodiscard]] float sum() const {
        return text.value + vector.value + entityVector.value + pathTree.value + kg.value +
               tag.value + metadata.value;
    }

    /// Initialize all slots from plain values with a given source.
    void setAll(float textVal, float vectorVal, float entityVectorVal, float pathTreeVal,
                float kgVal, float tagVal, float metadataVal, TuningLayer src) {
        text = TuningSlot<float>(textVal, src);
        vector = TuningSlot<float>(vectorVal, src);
        entityVector = TuningSlot<float>(entityVectorVal, src);
        pathTree = TuningSlot<float>(pathTreeVal, src);
        kg = TuningSlot<float>(kgVal, src);
        tag = TuningSlot<float>(tagVal, src);
        metadata = TuningSlot<float>(metadataVal, src);
    }

private:
    std::array<TuningSlot<float>*, 7> allSlots() {
        return {&text, &vector, &entityVector, &pathTree, &kg, &tag, &metadata};
    }
};

} // namespace yams::search
