// Host-side retained-byte accounting for one embedding job.
//
// These numbers describe buffers YAMS owns: prepared payload sitting in the channel/pending
// list, chunk text built inside a job, and the float matrix copied out of the provider. They
// deliberately exclude the provider's internal workspace, which is not observable across the
// model ABI.

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/InternalEventBus.h>

#include "src/daemon/components/embedding_memory_accounting.h"

#include <cstddef>
#include <limits>
#include <string>
#include <vector>

using namespace yams::daemon;
using namespace yams::daemon::embed;

namespace {

InternalEventBus::EmbedPreparedChunk chunk(std::string id, std::string content) {
    InternalEventBus::EmbedPreparedChunk out;
    out.chunkId = std::move(id);
    out.content = std::move(content);
    return out;
}

InternalEventBus::EmbedPreparedDoc
preparedDoc(std::string hash, std::vector<InternalEventBus::EmbedPreparedChunk> c) {
    InternalEventBus::EmbedPreparedDoc doc;
    doc.hash = std::move(hash);
    doc.sourceTextHash = "source-hash";
    doc.preparationRecipe = "recipe";
    doc.chunks = std::move(c);
    return doc;
}

} // namespace

TEST_CASE("Embedding memory accounting saturates instead of wrapping",
          "[unit][daemon][embedding][memory][catch2]") {
    const auto max = std::numeric_limits<std::size_t>::max();

    CHECK(saturatingAdd(max, 1) == max);
    CHECK(saturatingAdd(max - 1, 1) == max);
    CHECK(saturatingAdd(10, 5) == 15);
    CHECK(saturatingMul(max, 2) == max);
    CHECK(saturatingMul(max / 2 + 1, 2) == max);
    CHECK(saturatingMul(0, max) == 0);
    CHECK(saturatingMul(max, 0) == 0);
    CHECK(saturatingMul(3, 4) == 12);
}

TEST_CASE("Prepared payload accounting counts the queued chunk text and ids",
          "[unit][daemon][embedding][memory][catch2]") {
    std::vector<InternalEventBus::EmbedPreparedDoc> docs;
    docs.push_back(preparedDoc(
        "hash-a", {chunk("id-1", std::string(100, 'x')), chunk("id-2", std::string(50, 'y'))}));
    docs.push_back(preparedDoc("hash-b", {chunk("id-3", std::string(10, 'z'))}));

    // Two documents: hash + fixed source/recipe identity, plus every chunk id and body.
    const std::size_t expected =
        (docs[0].hash.size() + docs[0].sourceTextHash.size() + docs[0].preparationRecipe.size() +
         std::string("id-1").size() + 100 + std::string("id-2").size() + 50) +
        (docs[1].hash.size() + docs[1].sourceTextHash.size() + docs[1].preparationRecipe.size() +
         std::string("id-3").size() + 10);
    CHECK(preparedPayloadBytes(docs) == expected);

    CHECK(preparedPayloadBytes({}) == 0);

    // A prepared document with no selected chunks still carries its identity.
    std::vector<InternalEventBus::EmbedPreparedDoc> empty = {preparedDoc("h", {})};
    CHECK(preparedPayloadBytes(empty) == empty[0].hash.size() + empty[0].sourceTextHash.size() +
                                             empty[0].preparationRecipe.size());
}

TEST_CASE("Chunk payload accounting matches the buffers a job builds",
          "[unit][daemon][embedding][memory][catch2]") {
    std::vector<InternalEventBus::EmbedPreparedChunk> chunks = {
        chunk("chunk-000", std::string(24000, 'a')),
        chunk("chunk-001", std::string(12000, 'b')),
    };
    const std::size_t expected =
        std::string("chunk-000").size() + 24000 + std::string("chunk-001").size() + 12000;
    CHECK(chunkPayloadBytes(chunks) == expected);
    CHECK(chunkPayloadBytes(std::vector<InternalEventBus::EmbedPreparedChunk>{}) == 0);
}

TEST_CASE("Provider output accounting covers the host copy, not provider workspace",
          "[unit][daemon][embedding][memory][catch2]") {
    // 256 dimensions, 32 items: 32 * 256 floats for the returned matrix and the same again
    // for the host vector-of-vectors.
    CHECK(inferenceOutputBytes(32, 256) ==
          static_cast<std::size_t>(32) * 256u * 2u * sizeof(float));
    CHECK(inferenceOutputBytes(0, 256) == 0);
    CHECK(inferenceOutputBytes(32, 0) == 0);
    CHECK(inferenceOutputBytes(std::numeric_limits<std::size_t>::max(), 2) ==
          std::numeric_limits<std::size_t>::max());
}
