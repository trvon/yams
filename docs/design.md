---
title: Design Language
description: Unified design language for YAMS Memory and yamsmemory.ai — Quiet Paper, Living Substrate.
---

# YAMS Design Language — *Quiet Paper, Living Substrate*

**Version:** 1.0  
**Applies to:** `YAMS Memory` (Android, iOS, Desktop) and `yamsmemory.ai` (MkDocs)  
**Mascot:** Refined Option A Terracotta Yam + Emerald-Sage Sprout on Warm Charcoal (`#161412`)

---

## 1. Core Philosophy

YAMS should feel like a **quiet archival sheet of paper (or deep silicon substrate) that comes alive only when you touch it** — inspired by the tactile delight of *Draw a Stickman* paired with the rigor of a systems-engineering instrument.

1. **Calm at Rest:** Minimal chrome, generous whitespace, hairline `1px` rules instead of heavy drop shadows, and zero "AI dashboard" noise. Nothing advertises its complexity until invited.
2. **Responsive Canvas:** Hovering, sketching, tapping, or typing wakes subtle, low-latency ink/root-line physics (`< 16ms` frame budget). Animations respond directly to user input rather than looping idly.
3. **Hidden-Until-Invited Technical Depth:** Default surfaces show only what you need to read, search, or think. A single tap or interaction unfolds real engineering telemetry — SHA-256 hashes, compression ratios, chunk graphs, RAM/disk footprint, and retrieval timings.
4. **Honest & Local-First:** Every visual indicator reflects real local state (zero fake spinners, zero decorative telemetry). Always honor OS `prefers-reduced-motion` / `MediaQuery.disableAnimations`.

---

## 2. Color Tokens

### Brand Accents
| Token | Hex | Role |
|---|---|---|
| `--yams-terracotta` | `#C86D3B` | Primary brand seed & warm interactive accent |
| `--yams-terracotta-bright` | `#D97A3E` | Highlight, active ink stroke, mascot body |
| `--yams-gold` | `#DFB147` | Dark-mode silicon gold highlight & mono telemetry |
| `--yams-leaf` | `#5EA84B` | Sprout accent, verified/indexed state, compression gain |
| `--yams-charcoal` | `#161412` | Adaptive icon backdrop & high-contrast ink |

### Light Mode — *"Archival Card Catalog"*
| Token | Hex | Role |
|---|---|---|
| `surface` | `#FBF9F4` | Warm archival paper canvas |
| `surfaceContainer` | `#F4EFE4` | Subtle recessed card / toolbar fill |
| `surfaceHeader` | `#ECE7DB` | Header / tab bar fill |
| `inkPrimary` | `#2B2E30` | Primary body text |
| `inkSecondary` | `#5B6268` | Muted metadata & captions |
| `hairline` | `rgba(74, 92, 104, 0.22)` | `1px` structural borders & dividers |
| `blueprint` | `#3B5B75` | Technical links & archival reference lines |

### Dark Mode — *"Silicon Substrate"*
| Token | Hex | Role |
|---|---|---|
| `surface` | `#0E1117` | Deep silicon canvas |
| `surfaceContainer` | `#141820` | Elevated card / sheet fill |
| `surfaceHeader` | `#121620` | Header / navigation rail fill |
| `inkPrimary` | `#D2D8E0` | Crisp silver body text |
| `inkSecondary` | `#8B949E` | Muted metadata & captions |
| `hairline` | `rgba(223, 177, 71, 0.22)` | Subtle copper/gold structural hairline |
| `accent` | `#DFB147` | Silicon gold focus & active links |

---

## 3. Typography

- **Prose & UI:** `Outfit` (fallback: system sans) — clean geometric warmth for titles, navigation, and long-form reading.
- **Technical Metadata & Code:** `JetBrains Mono` (fallback: `ui-monospace`, `SFMono-Regular`) — used exclusively for *real* technical values: SHA-256 prefixes, byte sizes, compression ratios (`3.2×`), latency (`0.4ms`), page counters, and CLI snippets.
- **Scale & Rhythm:** 4dp spacing grid (`4, 8, 12, 16, 20, 24, 32`), comfortable reading measure capped at `720px`.

---

## 4. Surfaces & Elevation

- **No heavy drop shadows.** Separation comes from tonal surface steps (`surface` → `surfaceContainer`) and crisp `1px` hairline borders.
- **Corner Radii:** `8dp` (`sm`) for mono badges/inputs, `12dp` (`md`) for cards/sheets, `16dp` (`lg`) for modals, `999dp` (`pill`) for floating reader HUDs and chips.
- **Touch Targets:** Minimum `48×48dp` on mobile/foldable controls (`44dp` compact HUD minimum).

---

## 5. Motion & Living Canvas Rules

| Interaction | Curve / Physics | Duration |
|---|---|---|
| Micro-feedback (chips, toggles, ink pulse) | `cubic-bezier(0.22, 1, 0.36, 1)` | `140–180ms` |
| Panel / Inspector unfold | Critically-damped spring (`stiffness: 340, damping: 28`) | `220–280ms` |
| View morph (`PDF \| Text \| Graph`) | Cross-fade + subtle 6px vertical settle | `200ms` |
| Living Canvas (site & app Home header) | Pointer/typing-reactive ink tendrils & content-addressed nodes | Real-time (`60–120fps`), sleeps cleanly when idle |

### The "Living Canvas" Contract
- **At rest:** Subtle, nearly invisible grid/paper texture with 2–3 calm ambient anchor nodes (opacity ≤ 0.18). Never distracts from reading.
- **On pointer / sketch / typing:** Drawing or typing spawns crisp ink-root strokes that settle into content-addressed nodes (`sha256:…`, `zstd 3.4×`, `0.3ms`) connected by delicate hairlines.
- **On reading:** The moment a user opens a PDF or note, the canvas fades away completely so the document owns 100% of the viewport.
- **Accessibility:** When `prefers-reduced-motion: reduce` (web) or `MediaQuery.disableAnimations` (Flutter) is enabled, render a static, crisp archival vector state with zero continuous animation.

---

## 6. Progressive Technical Depth

Every major surface follows a **two-layer reveal**:
1. **Layer 0 (Default — Minimal):** Document title, clean rendered text/PDF, quiet single-line mono pill (e.g., `PDF · 412 KB · 2.9×`).
2. **Layer 1 (On Tap / Hover — Rich Technical Detail):** Expands smoothly to show content hash, chunk count, compression ratio, storage footprint, graph neighbors, and local file path — giving power users full observability without cluttering the calm workspace.
