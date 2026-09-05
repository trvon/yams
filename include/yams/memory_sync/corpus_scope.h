// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <string_view>

namespace yams::memory_sync {

// Scope applies to the entire local corpus, including derived vectors and graph
// records. Document metadata is not a replication authorization mechanism.
enum class CorpusScope { Personal, Shared, Invalid };

inline CorpusScope parseCorpusScope(std::string_view value) {
    if (value == "personal") {
        return CorpusScope::Personal;
    }
    if (value == "shared") {
        return CorpusScope::Shared;
    }
    return CorpusScope::Invalid;
}

} // namespace yams::memory_sync
