// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <map>
#include <string>

namespace yams::search {

using SearchEnvironmentSnapshot = std::map<std::string, std::string>;

/// Copy the enabled legacy search compatibility inputs at one lifecycle boundary. Rebuilds use
/// this value object instead of re-reading ambient process state.
SearchEnvironmentSnapshot snapshotLegacySearchEnvironment();

} // namespace yams::search
