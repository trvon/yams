# Glint Plugin

GLiNER-based natural language entity extraction for YAMS.

## Interface

`entity_extractor_v2` (v2) - see `include/yams/plugins/entity_extractor_v2.h`

## Supported Content

- `text/plain`
- `text/markdown`
- `application/json`

## Entity Types

person, organization, location, date, event, product, technology, concept

## Build

```bash
meson compile -C builddir
# Output: builddir/plugins/glint/yams_glint.{dylib,so,dll}
```

## Environment

- `YAMS_GLINT_MODEL_PATH` - ONNX model directory
- `YAMS_GLINT_THRESHOLD` - Confidence threshold (default: 0.5)
