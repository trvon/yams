// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

/**
 * @file tuning_slot_catch2_test.cpp
 * @brief Unit tests for TuningSlot, WeightSlots, and the provenance-tracking
 *        primitives that underpin the restructured tuning pipeline.
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/tuning_slot.h>

using namespace yams::search;
using Catch::Approx;

// =============================================================================
// TuningLayer string conversion
// =============================================================================

TEST_CASE("TuningLayer: string conversion", "[unit][tuning_slot]") {
    CHECK(tuningLayerToString(TuningLayer::Default) == "Default");
    CHECK(tuningLayerToString(TuningLayer::Profile) == "Profile");
    CHECK(tuningLayerToString(TuningLayer::Corpus) == "Corpus");
    CHECK(tuningLayerToString(TuningLayer::Env) == "Env");
    CHECK(tuningLayerToString(TuningLayer::Runtime) == "Runtime");
    CHECK(tuningLayerToString(TuningLayer::Zoom) == "Zoom");
    CHECK(tuningLayerToString(TuningLayer::Intent) == "Intent");
    CHECK(tuningLayerToString(TuningLayer::Community) == "Community");
    CHECK(tuningLayerToString(TuningLayer::Mode) == "Mode");
}

// =============================================================================
// TuningSlot<float> basics
// =============================================================================

TEST_CASE("TuningSlot: default construction", "[unit][tuning_slot]") {
    TuningSlot<float> slot;
    CHECK(slot.value == 0.0f);
    CHECK(slot.source == TuningLayer::Default);
    CHECK_FALSE(slot.pinned);
}

TEST_CASE("TuningSlot: explicit construction", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Profile);
    CHECK(slot.value == Approx(0.35f));
    CHECK(slot.source == TuningLayer::Profile);
    CHECK_FALSE(slot.pinned);
}

TEST_CASE("TuningSlot: implicit conversion", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.5f);
    float v = slot;
    CHECK(v == Approx(0.5f));
}

// =============================================================================
// TuningSlot::scaleBy
// =============================================================================

TEST_CASE("TuningSlot: scaleBy modifies unpinned value", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Profile);
    bool result = slot.scaleBy(0.90f, TuningLayer::Zoom);
    CHECK(result);
    CHECK(slot.value == Approx(0.315f));
    CHECK(slot.source == TuningLayer::Zoom);
}

TEST_CASE("TuningSlot: scaleBy is no-op when pinned", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Env);
    slot.pinned = true;
    bool result = slot.scaleBy(0.50f, TuningLayer::Zoom);
    CHECK_FALSE(result);
    CHECK(slot.value == Approx(0.35f));
    CHECK(slot.source == TuningLayer::Env);
}

// =============================================================================
// TuningSlot::set
// =============================================================================

TEST_CASE("TuningSlot: set modifies unpinned value", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Profile);
    bool result = slot.set(0.20f, TuningLayer::Corpus);
    CHECK(result);
    CHECK(slot.value == Approx(0.20f));
    CHECK(slot.source == TuningLayer::Corpus);
}

TEST_CASE("TuningSlot: set is no-op when pinned", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Env);
    slot.pinned = true;
    bool result = slot.set(0.10f, TuningLayer::Community);
    CHECK_FALSE(result);
    CHECK(slot.value == Approx(0.35f));
}

// =============================================================================
// TuningSlot::forceSet
// =============================================================================

TEST_CASE("TuningSlot: forceSet overrides pinned", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Env);
    slot.pinned = true;
    slot.forceSet(0.45f, TuningLayer::Mode);
    CHECK(slot.value == Approx(0.45f));
    CHECK(slot.source == TuningLayer::Mode);
    // Mode does not auto-pin
    CHECK_FALSE(slot.pinned);
}

TEST_CASE("TuningSlot: forceSet with Env auto-pins", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Profile);
    CHECK_FALSE(slot.pinned);
    slot.forceSet(0.50f, TuningLayer::Env);
    CHECK(slot.value == Approx(0.50f));
    CHECK(slot.source == TuningLayer::Env);
    CHECK(slot.pinned);
}

TEST_CASE("TuningSlot: forceSet with non-Env does not auto-pin", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Profile);
    slot.forceSet(0.20f, TuningLayer::Corpus);
    CHECK_FALSE(slot.pinned);
}

// =============================================================================
// TuningSlot::unpin
// =============================================================================

TEST_CASE("TuningSlot: unpin allows subsequent modification", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.35f, TuningLayer::Env);
    slot.pinned = true;
    CHECK_FALSE(slot.scaleBy(0.5f, TuningLayer::Zoom));
    slot.unpin();
    CHECK(slot.scaleBy(0.5f, TuningLayer::Zoom));
    CHECK(slot.value == Approx(0.175f));
}

// =============================================================================
// TuningSlot<size_t> (integer type)
// =============================================================================

TEST_CASE("TuningSlot<size_t>: basic operations", "[unit][tuning_slot]") {
    TuningSlot<size_t> slot(2, TuningLayer::Profile);
    CHECK(slot.value == 2);

    SECTION("set works") {
        CHECK(slot.set(1, TuningLayer::Zoom));
        CHECK(slot.value == 1);
    }

    SECTION("pinned blocks set") {
        slot.pinned = true;
        CHECK_FALSE(slot.set(0, TuningLayer::Zoom));
        CHECK(slot.value == 2);
    }

    SECTION("forceSet overrides pinned") {
        slot.pinned = true;
        slot.forceSet(5, TuningLayer::Mode);
        CHECK(slot.value == 5);
    }
}

// =============================================================================
// scaleByClamp / setClamp helpers
// =============================================================================

TEST_CASE("scaleByClamp: clamps to [0, 1]", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.80f, TuningLayer::Profile);

    SECTION("normal scale") {
        scaleByClamp(slot, 0.5f, TuningLayer::Zoom);
        CHECK(slot.value == Approx(0.40f));
    }

    SECTION("scale up clamped to 1.0") {
        scaleByClamp(slot, 2.0f, TuningLayer::Zoom);
        CHECK(slot.value == Approx(1.0f));
    }

    SECTION("respects pinned") {
        slot.pinned = true;
        CHECK_FALSE(scaleByClamp(slot, 0.5f, TuningLayer::Zoom));
        CHECK(slot.value == Approx(0.80f));
    }
}

TEST_CASE("setClamp: clamps to [0, 1]", "[unit][tuning_slot]") {
    TuningSlot<float> slot(0.50f, TuningLayer::Profile);

    SECTION("normal set") {
        setClamp(slot, 0.30f, TuningLayer::Corpus);
        CHECK(slot.value == Approx(0.30f));
    }

    SECTION("clamp below") {
        setClamp(slot, -0.10f, TuningLayer::Corpus);
        CHECK(slot.value == Approx(0.0f));
    }

    SECTION("clamp above") {
        setClamp(slot, 1.50f, TuningLayer::Corpus);
        CHECK(slot.value == Approx(1.0f));
    }
}

// =============================================================================
// WeightSlots: basic operations
// =============================================================================

TEST_CASE("WeightSlots: setAll initializes all slots", "[unit][tuning_slot]") {
    WeightSlots w;
    w.setAll(0.60f, 0.25f, 0.05f, 0.00f, 0.00f, 0.00f, 0.10f, TuningLayer::Profile);
    CHECK(w.text.value == Approx(0.60f));
    CHECK(w.vector.value == Approx(0.25f));
    CHECK(w.entityVector.value == Approx(0.05f));
    CHECK(w.pathTree.value == Approx(0.00f));
    CHECK(w.kg.value == Approx(0.00f));
    CHECK(w.tag.value == Approx(0.00f));
    CHECK(w.metadata.value == Approx(0.10f));
    CHECK(w.sum() == Approx(1.0f));

    for (auto* s : {&w.text, &w.vector, &w.entityVector, &w.pathTree, &w.kg, &w.tag, &w.metadata}) {
        CHECK(s->source == TuningLayer::Profile);
        CHECK_FALSE(s->pinned);
    }
}

// =============================================================================
// WeightSlots::normalize — no pinned values
// =============================================================================

TEST_CASE("WeightSlots: normalize with no pinned values", "[unit][tuning_slot]") {
    WeightSlots w;
    // Unnormalized weights summing to 2.0
    w.setAll(0.80f, 0.40f, 0.20f, 0.20f, 0.20f, 0.10f, 0.10f, TuningLayer::Profile);
    CHECK(w.sum() == Approx(2.0f));

    w.normalize();
    CHECK(w.sum() == Approx(1.0f));
    // Proportions preserved
    CHECK(w.text.value == Approx(0.40f));
    CHECK(w.vector.value == Approx(0.20f));
}

TEST_CASE("WeightSlots: normalize already-normalized weights is idempotent",
          "[unit][tuning_slot]") {
    WeightSlots w;
    w.setAll(0.60f, 0.25f, 0.05f, 0.00f, 0.00f, 0.00f, 0.10f, TuningLayer::Profile);
    w.normalize();
    CHECK(w.sum() == Approx(1.0f));
    CHECK(w.text.value == Approx(0.60f));
    CHECK(w.vector.value == Approx(0.25f));
}

// =============================================================================
// WeightSlots::normalize — with pinned values
// =============================================================================

TEST_CASE("WeightSlots: normalize respects pinned values", "[unit][tuning_slot]") {
    WeightSlots w;
    w.setAll(0.60f, 0.35f, 0.05f, 0.00f, 0.00f, 0.00f, 0.05f, TuningLayer::Profile);

    // Pin vector at 0.35 (as if set by env var)
    w.vector.forceSet(0.35f, TuningLayer::Env);
    CHECK(w.vector.pinned);

    // Increase kg (simulating corpus-aware adjustment)
    w.kg.value = 0.15f;
    // Now sum > 1.0

    w.normalize();

    // Pinned value must be exactly preserved
    CHECK(w.vector.value == Approx(0.35f));
    CHECK(w.vector.pinned);

    // Total should be ~1.0
    CHECK(w.sum() == Approx(1.0f));

    // Budget for non-pinned = 1.0 - 0.35 = 0.65
    float nonPinnedSum = w.text.value + w.entityVector.value + w.pathTree.value + w.kg.value +
                         w.tag.value + w.metadata.value;
    CHECK(nonPinnedSum == Approx(0.65f));
}

TEST_CASE("WeightSlots: normalize with multiple pinned values", "[unit][tuning_slot]") {
    WeightSlots w;
    w.setAll(0.50f, 0.30f, 0.05f, 0.05f, 0.05f, 0.03f, 0.02f, TuningLayer::Profile);

    w.text.forceSet(0.50f, TuningLayer::Env);
    w.vector.forceSet(0.30f, TuningLayer::Env);
    // Pinned sum = 0.80, budget = 0.20

    w.normalize();

    CHECK(w.text.value == Approx(0.50f));
    CHECK(w.vector.value == Approx(0.30f));
    CHECK(w.sum() == Approx(1.0f));

    float nonPinnedSum =
        w.entityVector.value + w.pathTree.value + w.kg.value + w.tag.value + w.metadata.value;
    CHECK(nonPinnedSum == Approx(0.20f));
}

// =============================================================================
// WeightSlots::normalize — edge cases
// =============================================================================

TEST_CASE("WeightSlots: normalize with all-pinned summing to > 1.0", "[unit][tuning_slot]") {
    WeightSlots w;
    w.setAll(0.50f, 0.40f, 0.10f, 0.10f, 0.10f, 0.05f, 0.05f, TuningLayer::Profile);

    // Pin everything
    w.text.pinned = true;
    w.vector.pinned = true;
    w.entityVector.pinned = true;
    w.pathTree.pinned = true;
    w.kg.pinned = true;
    w.tag.pinned = true;
    w.metadata.pinned = true;

    // Sum = 1.30, but all pinned — normalize cannot redistribute
    w.normalize();

    // Values unchanged (over-budget, but pinned takes precedence)
    CHECK(w.text.value == Approx(0.50f));
    CHECK(w.vector.value == Approx(0.40f));
    CHECK(w.sum() == Approx(1.30f));
}

TEST_CASE("WeightSlots: normalize with all non-pinned zero", "[unit][tuning_slot]") {
    WeightSlots w;
    w.setAll(0.00f, 0.00f, 0.00f, 0.00f, 0.00f, 0.00f, 0.00f, TuningLayer::Profile);

    // Pin text at 0.60
    w.text.forceSet(0.60f, TuningLayer::Env);
    // All others are 0.0 and unpinned. Budget = 0.40.

    w.normalize();

    CHECK(w.text.value == Approx(0.60f));
    // Budget distributed equally among 6 unpinned slots
    float expectedShare = 0.40f / 6.0f;
    CHECK(w.vector.value == Approx(expectedShare));
    CHECK(w.entityVector.value == Approx(expectedShare));
    CHECK(w.pathTree.value == Approx(expectedShare));
    CHECK(w.kg.value == Approx(expectedShare));
    CHECK(w.tag.value == Approx(expectedShare));
    CHECK(w.metadata.value == Approx(expectedShare));
    CHECK(w.sum() == Approx(1.0f));
}

TEST_CASE("WeightSlots: normalize clamps negatives", "[unit][tuning_slot]") {
    WeightSlots w;
    w.setAll(0.80f, -0.10f, 0.10f, 0.10f, 0.10f, 0.00f, 0.00f, TuningLayer::Profile);

    w.normalize();

    CHECK(w.vector.value >= 0.0f);
    CHECK(w.sum() == Approx(1.0f));
}

// =============================================================================
// WeightSlots: downstream scaling preserves pinned
// =============================================================================

TEST_CASE("WeightSlots: end-to-end pinning through scaling + normalize", "[unit][tuning_slot]") {
    // Simulate: Profile → Env override → Corpus scaling → Zoom scaling → Normalize
    WeightSlots w;
    w.setAll(0.60f, 0.35f, 0.05f, 0.00f, 0.00f, 0.00f, 0.05f, TuningLayer::Profile);

    // Layer 2: Env pins vector
    w.vector.forceSet(0.35f, TuningLayer::Env);

    // Layer 1: Corpus-aware — boost KG, reduce text/vector
    w.kg.set(0.12f, TuningLayer::Corpus);
    w.text.scaleBy(0.85f, TuningLayer::Corpus);
    w.vector.scaleBy(0.85f, TuningLayer::Corpus); // Should fail — pinned

    CHECK(w.vector.value == Approx(0.35f)); // Unchanged

    // Layer 4: Zoom scaling
    w.text.scaleBy(1.10f, TuningLayer::Zoom);
    w.vector.scaleBy(0.90f, TuningLayer::Zoom); // Should fail — still pinned

    CHECK(w.vector.value == Approx(0.35f)); // Still unchanged

    // Final normalize
    w.normalize();

    CHECK(w.vector.value == Approx(0.35f)); // Pinned survives
    CHECK(w.vector.pinned);
    CHECK(w.sum() == Approx(1.0f));
}
