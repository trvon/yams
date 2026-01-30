// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Backward compatibility header - use plugin_content_extractor_adapter.h directly
//
// PBI-096: This header now redirects to the unified PluginContentExtractorAdapter
// which supports both ABI (native) and external (Python/JS) plugins.

#pragma once

#include <yams/daemon/resource/plugin_content_extractor_adapter.h>

// AbiContentExtractorAdapter is now an alias for PluginContentExtractorAdapter
// defined in plugin_content_extractor_adapter.h
