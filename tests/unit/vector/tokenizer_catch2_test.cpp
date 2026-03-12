// Tests for HuggingFaceTokenizer — WordPiece, Unigram, special token injection.
//
// All tests use synthetic tokenizer.json files written to a temp directory
// so no model downloads are required.

#include <catch2/catch_test_macros.hpp>

#include <yams/vector/tokenizer.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using yams::vector::createTokenizer;
using yams::vector::HuggingFaceTokenizer;

// ---------------------------------------------------------------------------
// Helper: write a string to a file in a temp directory
// ---------------------------------------------------------------------------
namespace {

struct TempDir {
    fs::path path;

    TempDir() {
        path = fs::temp_directory_path() / ("yams_tok_test_" + std::to_string(std::rand()));
        fs::create_directories(path);
    }
    ~TempDir() { fs::remove_all(path); }

    fs::path writeFile(const std::string& name, const std::string& content) const {
        auto fp = path / name;
        std::ofstream out(fp, std::ios::binary);
        out << content;
        return fp;
    }
};

// Minimal WordPiece (BERT-style) tokenizer.json
const char* kWordPieceJson = R"({
  "model": {
    "type": "WordPiece",
    "vocab": {
      "hello": 0,
      "world": 1,
      "##ing": 2,
      "##s": 3,
      "test": 4,
      "un": 5,
      "##known": 6,
      "the": 7,
      "quick": 8,
      "brown": 9,
      "fox": 10
    }
  },
  "added_tokens": [
    { "id": 100, "content": "[UNK]", "special": true },
    { "id": 101, "content": "[CLS]", "special": true },
    { "id": 102, "content": "[SEP]", "special": true },
    { "id": 0,   "content": "[PAD]", "special": true }
  ]
})";

// Minimal Unigram (SentencePiece-style) tokenizer.json
const char* kUnigramJson = R"({
  "model": {
    "type": "Unigram",
    "vocab": [
      ["<unk>",   0.0],
      ["\u2581hello", -1.0],
      ["\u2581world", -2.0],
      ["\u2581the",   -3.0],
      ["\u2581",      -4.0],
      ["h",           -5.0],
      ["e",           -6.0],
      ["l",           -7.0],
      ["o",           -8.0],
      ["r",           -9.0],
      ["d",           -10.0]
    ]
  },
  "added_tokens": [
    { "id": 0, "content": "<unk>",  "special": true },
    { "id": 1, "content": "<s>",    "special": true },
    { "id": 2, "content": "</s>",   "special": true }
  ]
})";

// WordPiece tokenizer.json with BERT TemplateProcessing post_processor
const char* kWordPieceWithPostProcessorJson = R"({
  "model": {
    "type": "WordPiece",
    "vocab": {
      "hello": 0,
      "world": 1,
      "##ing": 2,
      "test": 3,
      "the": 4,
      "quick": 5
    }
  },
  "added_tokens": [
    { "id": 100, "content": "[UNK]",  "special": true },
    { "id": 101, "content": "[CLS]",  "special": true },
    { "id": 102, "content": "[SEP]",  "special": true },
    { "id": 0,   "content": "[PAD]",  "special": true }
  ],
  "post_processor": {
    "type": "TemplateProcessing",
    "single": [
      { "SpecialToken": { "id": "[CLS]", "type_id": 0 } },
      { "Sequence": { "id": "A", "type_id": 0 } },
      { "SpecialToken": { "id": "[SEP]", "type_id": 0 } }
    ],
    "pair": [
      { "SpecialToken": { "id": "[CLS]", "type_id": 0 } },
      { "Sequence": { "id": "A", "type_id": 0 } },
      { "SpecialToken": { "id": "[SEP]", "type_id": 0 } },
      { "Sequence": { "id": "B", "type_id": 1 } },
      { "SpecialToken": { "id": "[SEP]", "type_id": 1 } }
    ],
    "special_tokens": {
      "[CLS]": { "id": "[CLS]", "ids": [101], "tokens": ["[CLS]"] },
      "[SEP]": { "id": "[SEP]", "ids": [102], "tokens": ["[SEP]"] }
    }
  }
})";

// Unigram tokenizer.json with Sequence post_processor (<s>...</s>)
const char* kUnigramWithPostProcessorJson = R"({
  "model": {
    "type": "Unigram",
    "vocab": [
      ["<unk>",        0.0],
      ["\u2581hello",  -1.0],
      ["\u2581world",  -2.0],
      ["\u2581",       -3.0],
      ["h",            -5.0],
      ["e",            -6.0],
      ["l",            -7.0],
      ["o",            -8.0]
    ]
  },
  "added_tokens": [
    { "id": 0, "content": "<unk>",  "special": true },
    { "id": 1, "content": "<s>",    "special": true },
    { "id": 2, "content": "</s>",   "special": true }
  ],
  "post_processor": {
    "type": "Sequence",
    "processors": [
      {
        "type": "TemplateProcessing",
        "single": [
          { "SpecialToken": { "id": "<s>", "type_id": 0 } },
          { "Sequence": { "id": "A", "type_id": 0 } }
        ],
        "pair": [
          { "SpecialToken": { "id": "<s>", "type_id": 0 } },
          { "Sequence": { "id": "A", "type_id": 0 } },
          { "Sequence": { "id": "B", "type_id": 0 } }
        ],
        "special_tokens": {
          "<s>": { "id": "<s>", "ids": [1], "tokens": ["<s>"] }
        }
      },
      {
        "type": "TemplateProcessing",
        "single": [
          { "Sequence": { "id": "A", "type_id": 0 } },
          { "SpecialToken": { "id": "</s>", "type_id": 0 } }
        ],
        "pair": [
          { "Sequence": { "id": "A", "type_id": 0 } },
          { "SpecialToken": { "id": "</s>", "type_id": 0 } },
          { "Sequence": { "id": "B", "type_id": 0 } },
          { "SpecialToken": { "id": "</s>", "type_id": 0 } }
        ],
        "special_tokens": {
          "</s>": { "id": "</s>", "ids": [2], "tokens": ["</s>"] }
        }
      }
    ]
  }
})";

} // anonymous namespace

// ===========================================================================
// SECTION 1 — WordPiece encoding
// ===========================================================================

TEST_CASE("WordPiece: load succeeds on valid tokenizer.json", "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));
    REQUIRE(tok.isLoaded());
}

TEST_CASE("WordPiece: vocabSize includes main vocab and added_tokens",
          "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));
    // Main vocab: 11 entries  +  added_tokens: [UNK](100), [CLS](101), [SEP](102), [PAD](0)
    // [PAD] at id=0 overlaps with "hello" at id=0 — last-write wins in the map,
    // so the map key "[PAD]" overwrites key "hello" only if they share the same
    // map key, but they don't — "hello"→0 and "[PAD]"→0 are different keys.
    // vocabSize() returns vocab_.size() which counts distinct keys.
    CHECK(tok.vocabSize() >= 14); // 11 main + 4 added (some ids overlap, keys don't)
}

TEST_CASE("WordPiece: unkTokenId returns [UNK] id", "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));
    CHECK(tok.unkTokenId() == 100);
}

TEST_CASE("WordPiece: tokenToId and idToToken round-trip", "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    CHECK(tok.tokenToId("test") == 4);
    CHECK(tok.tokenToId("[CLS]") == 101);
    CHECK(tok.tokenToId("[SEP]") == 102);

    CHECK(tok.idToToken(4) == "test");
    CHECK(tok.idToToken(101) == "[CLS]");
    CHECK(tok.idToToken(102) == "[SEP]");
}

TEST_CASE("WordPiece: tokenToId returns unkTokenId for missing token",
          "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));
    CHECK(tok.tokenToId("nonexistent") == tok.unkTokenId());
}

TEST_CASE("WordPiece: idToToken returns empty for out-of-range id",
          "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));
    CHECK(tok.idToToken(99999) == "");
    CHECK(tok.idToToken(-1) == "");
}

TEST_CASE("WordPiece: encode whole words in vocab", "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "hello world" → lowercased to "hello world" → tokens [0, 1]
    auto ids = tok.encode("hello world");
    REQUIRE(ids.size() == 2);
    CHECK(ids[0] == tok.tokenToId("hello"));
    CHECK(ids[1] == tok.tokenToId("world"));
}

TEST_CASE("WordPiece: encode uses ## continuations", "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "testing" → lowercased → "test" + "##ing"
    auto ids = tok.encode("testing");
    REQUIRE(ids.size() == 2);
    CHECK(ids[0] == tok.tokenToId("test"));
    CHECK(ids[1] == tok.tokenToId("##ing"));
}

TEST_CASE("WordPiece: encode lowercases input", "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    auto lower = tok.encode("hello");
    auto upper = tok.encode("HELLO");
    CHECK(lower == upper);
}

TEST_CASE("WordPiece: encode unknown word falls back to UNK", "[tokenizer][wordpiece][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "xyz" has no whole-word match and no ## pieces
    auto ids = tok.encode("xyz");
    REQUIRE(!ids.empty());
    // All subword pieces should be UNK since x, y, z are not in vocab
    for (auto id : ids) {
        CHECK(id == tok.unkTokenId());
    }
}

// ===========================================================================
// SECTION 2 — Unigram (SentencePiece) encoding
// ===========================================================================

TEST_CASE("Unigram: load succeeds on valid tokenizer.json", "[tokenizer][unigram][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kUnigramJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));
    REQUIRE(tok.isLoaded());
}

TEST_CASE("Unigram: unkTokenId returns <unk> id", "[tokenizer][unigram][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kUnigramJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));
    CHECK(tok.unkTokenId() == 0);
}

TEST_CASE("Unigram: encode whole words with spiece prefix", "[tokenizer][unigram][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kUnigramJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "hello" → "▁hello" which is in vocab at id index 1
    auto ids = tok.encode("hello");
    REQUIRE(ids.size() == 1);
    CHECK(ids[0] == tok.tokenToId("\xe2\x96\x81hello"));
}

TEST_CASE("Unigram: encode multi-word text", "[tokenizer][unigram][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kUnigramJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "hello world" → ["▁hello", "▁world"]
    auto ids = tok.encode("hello world");
    REQUIRE(ids.size() == 2);
    CHECK(ids[0] == tok.tokenToId("\xe2\x96\x81hello"));
    CHECK(ids[1] == tok.tokenToId("\xe2\x96\x81world"));
}

TEST_CASE("Unigram: encode breaks unknown word into subword pieces",
          "[tokenizer][unigram][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kUnigramJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "he" → "▁he" not in vocab, will be broken down:
    //   "▁" (id for ▁) + "h" + "e" or similar character-level pieces
    auto ids = tok.encode("he");
    REQUIRE(!ids.empty());
    // The greedy tokenizer should find some decomposition
    CHECK(ids.size() >= 1);
}

TEST_CASE("Unigram: does NOT lowercase input (case-sensitive)", "[tokenizer][unigram][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kUnigramJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "▁hello" is in vocab but "▁Hello" (capital H) is not
    auto lower = tok.encode("hello");
    auto upper = tok.encode("Hello");
    // They should differ since unigram is case-sensitive
    CHECK(lower != upper);
}

// ===========================================================================
// SECTION 3 — Edge cases
// ===========================================================================

TEST_CASE("Tokenizer: encode returns empty for empty input", "[tokenizer][edge][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    auto ids = tok.encode("");
    CHECK(ids.empty());
}

TEST_CASE("Tokenizer: encode returns empty for whitespace-only input",
          "[tokenizer][edge][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    auto ids = tok.encode("   \t\n  ");
    CHECK(ids.empty());
}

TEST_CASE("Tokenizer: unloaded tokenizer returns empty from encode", "[tokenizer][edge][catch2]") {
    HuggingFaceTokenizer tok;
    CHECK(!tok.isLoaded());
    CHECK(tok.encode("hello").empty());
}

TEST_CASE("Tokenizer: load fails on missing file", "[tokenizer][edge][catch2]") {
    HuggingFaceTokenizer tok;
    CHECK(!tok.load("/nonexistent/path/tokenizer.json"));
    CHECK(!tok.isLoaded());
}

TEST_CASE("Tokenizer: load fails on empty path", "[tokenizer][edge][catch2]") {
    HuggingFaceTokenizer tok;
    CHECK(!tok.load(""));
    CHECK(!tok.isLoaded());
}

TEST_CASE("Tokenizer: load fails on invalid JSON", "[tokenizer][edge][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", "{ not valid json !!!");

    HuggingFaceTokenizer tok;
    CHECK(!tok.load(path.string()));
    CHECK(!tok.isLoaded());
}

TEST_CASE("Tokenizer: load fails on JSON with no vocab", "[tokenizer][edge][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", R"({"model": {}})");

    HuggingFaceTokenizer tok;
    CHECK(!tok.load(path.string()));
    CHECK(!tok.isLoaded());
}

// ===========================================================================
// SECTION 4 — Factory function
// ===========================================================================

TEST_CASE("createTokenizer: returns valid pointer on good file", "[tokenizer][factory][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    auto tok = createTokenizer(path.string());
    REQUIRE(tok != nullptr);
    CHECK(tok->isLoaded());
    CHECK(tok->vocabSize() > 0);
}

TEST_CASE("createTokenizer: returns nullptr on bad file", "[tokenizer][factory][catch2]") {
    auto tok = createTokenizer("/nonexistent/tokenizer.json");
    CHECK(tok == nullptr);
}

TEST_CASE("createTokenizer: returns nullptr on invalid JSON", "[tokenizer][factory][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", "garbage");

    auto tok = createTokenizer(path.string());
    CHECK(tok == nullptr);
}

// ===========================================================================
// SECTION 5 — Special token injection (encodeWithSpecialTokens)
// ===========================================================================

TEST_CASE("WordPiece: encodeWithSpecialTokens wraps with [CLS]...[SEP]",
          "[tokenizer][special][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceWithPostProcessorJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // "hello world" → [CLS](101), hello(0), world(1), [SEP](102)
    auto ids = tok.encodeWithSpecialTokens("hello world");
    REQUIRE(ids.size() >= 4);
    CHECK(ids.front() == 101); // [CLS]
    CHECK(ids.back() == 102);  // [SEP]

    // Interior should match raw encode()
    auto raw = tok.encode("hello world");
    std::vector<int32_t> interior(ids.begin() + 1, ids.end() - 1);
    CHECK(interior == raw);
}

TEST_CASE("WordPiece: encodeWithSpecialTokens pair wraps with [CLS] A [SEP] B [SEP]",
          "[tokenizer][special][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceWithPostProcessorJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    auto ids = tok.encodeWithSpecialTokens("hello", "world");
    // Expected: [CLS](101), hello(0), [SEP](102), world(1), [SEP](102)
    REQUIRE(ids.size() >= 5);
    CHECK(ids.front() == 101); // [CLS]
    CHECK(ids.back() == 102);  // trailing [SEP]

    // Find the first [SEP] (between A and B segments)
    auto firstSep = std::find(ids.begin() + 1, ids.end(), 102);
    REQUIRE(firstSep != ids.end());

    // Check it's not the last element (there should be a second [SEP])
    auto secondSep = std::find(firstSep + 1, ids.end(), 102);
    CHECK(secondSep != ids.end());
}

TEST_CASE("Unigram: encodeWithSpecialTokens wraps with <s>...</s>",
          "[tokenizer][special][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kUnigramWithPostProcessorJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    auto ids = tok.encodeWithSpecialTokens("hello");
    REQUIRE(ids.size() >= 3);
    CHECK(ids.front() == tok.tokenToId("<s>")); // BOS
    CHECK(ids.back() == tok.tokenToId("</s>")); // EOS

    // Interior should match raw encode()
    auto raw = tok.encode("hello");
    std::vector<int32_t> interior(ids.begin() + 1, ids.end() - 1);
    CHECK(interior == raw);
}

TEST_CASE("Special tokens: encode() does NOT inject special tokens",
          "[tokenizer][special][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceWithPostProcessorJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    // Plain encode() should NOT have [CLS]/[SEP]
    auto ids = tok.encode("hello");
    CHECK(std::find(ids.begin(), ids.end(), 101) == ids.end()); // no [CLS]
    CHECK(std::find(ids.begin(), ids.end(), 102) == ids.end()); // no [SEP]
}

TEST_CASE("Special tokens: encodeWithSpecialTokens on empty text returns only specials",
          "[tokenizer][special][catch2]") {
    TempDir tmp;
    auto path = tmp.writeFile("tokenizer.json", kWordPieceWithPostProcessorJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    auto ids = tok.encodeWithSpecialTokens("");
    // Empty text → [CLS] [SEP]
    REQUIRE(ids.size() == 2);
    CHECK(ids[0] == 101);
    CHECK(ids[1] == 102);
}

TEST_CASE("Special tokens: no post_processor falls back to raw encode",
          "[tokenizer][special][catch2]") {
    TempDir tmp;
    // Use the basic WordPiece JSON which has no post_processor
    auto path = tmp.writeFile("tokenizer.json", kWordPieceJson);

    HuggingFaceTokenizer tok;
    REQUIRE(tok.load(path.string()));

    auto raw = tok.encode("hello world");
    auto special = tok.encodeWithSpecialTokens("hello world");
    // Without a post_processor, encodeWithSpecialTokens == encode
    CHECK(raw == special);
}

TEST_CASE("Special tokens: unloaded tokenizer returns empty from encodeWithSpecialTokens",
          "[tokenizer][special][catch2]") {
    HuggingFaceTokenizer tok;
    CHECK(tok.encodeWithSpecialTokens("hello").empty());
    CHECK(tok.encodeWithSpecialTokens("hello", "world").empty());
}
