#pragma once

#include <cstdlib>
#include "../../plugins/symbol_extractor_treesitter/grammar_loader.h"
#include "plugins.h"

#undef YAMS_TRY_AUTO_DOWNLOAD
#define YAMS_TRY_AUTO_DOWNLOAD(lang)                                                               \
    (::getenv("YAMS_TEST_DOWNLOAD_GRAMMARS") != nullptr &&                                         \
     yams::plugins::treesitter::GrammarDownloader::canAutoDownload() &&                            \
     yams::plugins::treesitter::GrammarDownloader::downloadGrammar((lang)).has_value())
