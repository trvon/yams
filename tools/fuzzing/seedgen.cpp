// SPDX-License-Identifier: GPL-3.0-or-later
// Structured seed generator for IPC fuzzers.

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/proto_serializer.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

using namespace yams::daemon;

namespace fs = std::filesystem;

namespace {

struct Args {
    fs::path outRoot;
    uint32_t maxSeeds = 40;
};

bool parseArgs(int argc, char** argv, Args& out) {
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--out" && i + 1 < argc) {
            out.outRoot = fs::path(argv[++i]);
        } else if (arg == "--max-seeds" && i + 1 < argc) {
            out.maxSeeds = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--help" || arg == "-h") {
            return false;
        } else {
            std::cerr << "Unknown arg: " << arg << "\n";
            return false;
        }
    }

    if (out.outRoot.empty()) {
        std::cerr << "Missing required --out <dir>\n";
        return false;
    }
    if (out.maxSeeds == 0) {
        out.maxSeeds = 1;
    }
    return true;
}

bool writeFile(const fs::path& path, std::span<const uint8_t> bytes) {
    std::ofstream out(path, std::ios::binary);
    if (!out.is_open()) {
        return false;
    }
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    return static_cast<bool>(out);
}

std::vector<uint8_t> flipOneByte(std::vector<uint8_t> bytes) {
    if (!bytes.empty()) {
        bytes[bytes.size() / 2] ^= 0xA5;
    }
    return bytes;
}

Message makeMsg(uint64_t requestId, Request req) {
    Message msg{};
    msg.version = 1;
    msg.requestId = requestId;
    msg.payload = std::move(req);
    msg.expectsStreamingResponse = false;
    return msg;
}

std::vector<Message> buildStructuredMessages() {
    std::vector<Message> msgs;
    uint64_t id = 1;

    // SearchRequest edge cases
    {
        SearchRequest r{};
        r.query = "";
        r.limit = 0;
        r.fuzzy = false;
        r.literalText = true;
        r.similarity = 0.0;
        r.timeout = std::chrono::milliseconds{0};
        r.searchType = "keyword";
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<SearchRequest>, std::move(r)}));
    }
    {
        SearchRequest r{};
        r.query = "   ";
        r.limit = 1;
        r.fuzzy = true;
        r.literalText = false;
        r.similarity = 1.0;
        r.timeout = std::chrono::milliseconds{1};
        r.searchType = "semantic";
        r.pathPatterns = {"**/*.cpp", "**/*.hpp", "**/*"};
        r.tags = {"", "pinned", "pinned"};
        r.matchAllTags = true;
        r.afterContext = 3;
        r.beforeContext = 3;
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<SearchRequest>, std::move(r)}));
    }
    {
        SearchRequest r{};
        r.query = std::string(4096, 'A');
        r.limit = static_cast<size_t>(std::numeric_limits<uint32_t>::max());
        r.fuzzy = true;
        r.literalText = true;
        r.similarity = std::numeric_limits<double>::quiet_NaN();
        r.timeout = std::chrono::milliseconds{-1};
        r.searchType = "hybrid";
        r.extension = ".txt";
        r.mimeType = "text/plain";
        r.textOnly = true;
        r.binaryOnly = true; // conflicting on purpose
        r.vectorStageTimeoutMs = -1;
        r.keywordStageTimeoutMs = 0;
        r.snippetHydrationTimeoutMs = 1;
        r.useSession = true;
        r.sessionName = "sess";
        r.instanceId = "instance-00000000-0000-0000-0000-000000000000";
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<SearchRequest>, std::move(r)}));
    }
    {
        SearchRequest r{};
        r.query = std::string("nul\0byte", 8);
        r.limit = 20;
        r.fuzzy = false;
        r.literalText = false;
        r.similarity = std::numeric_limits<double>::infinity();
        r.timeout = std::chrono::milliseconds{5000};
        r.searchType = "unknown-type";
        r.pathsOnly = true;
        r.jsonOutput = true;
        r.showLineNumbers = true;
        r.context = -1;
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<SearchRequest>, std::move(r)}));
    }

    // GrepRequest edge cases
    {
        GrepRequest r{};
        r.pattern = "";
        r.path = "";
        r.paths = {};
        r.caseInsensitive = false;
        r.invertMatch = false;
        r.contextLines = 0;
        r.maxMatches = 0;
        r.recursive = true;
        r.literalText = true;
        r.regexOnly = false;
        r.semanticLimit = 0;
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<GrepRequest>, std::move(r)}));
    }
    {
        GrepRequest r{};
        r.pattern = "(.+)+";
        r.path = "./";
        r.paths = {".", "..", "src", "include"};
        r.caseInsensitive = true;
        r.invertMatch = true;
        r.contextLines = -1;
        r.maxMatches = static_cast<size_t>(std::numeric_limits<uint64_t>::max());
        r.includePatterns = {"**/*.cpp", "**/*.h", "**/*.md"};
        r.recursive = false;
        r.wholeWord = true;
        r.showLineNumbers = true;
        r.showFilename = true;
        r.noFilename = true; // conflicting
        r.countOnly = true;
        r.filesOnly = true;
        r.filesWithoutMatch = true; // conflicting
        r.pathsOnly = true;
        r.literalText = false;
        r.regexOnly = true;
        r.semanticLimit = static_cast<size_t>(std::numeric_limits<uint64_t>::max());
        r.filterTags = {"pinned", "", "fuzz"};
        r.matchAllTags = true;
        r.colorMode = "always";
        r.beforeContext = std::numeric_limits<int>::min();
        r.afterContext = std::numeric_limits<int>::max();
        r.showDiff = true;
        r.useSession = true;
        r.sessionName = "sess";
        r.instanceId = "instance-11111111-1111-1111-1111-111111111111";
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<GrepRequest>, std::move(r)}));
    }
    {
        GrepRequest r{};
        r.pattern = std::string(2048, 'B');
        r.path = "src/daemon/ipc";
        r.paths = {"src/daemon/ipc/proto_serializer.cpp"};
        r.caseInsensitive = false;
        r.invertMatch = false;
        r.contextLines = 999999;
        r.maxMatches = 1;
        r.includePatterns = {"**/*"};
        r.recursive = true;
        r.literalText = true;
        r.regexOnly = false;
        r.semanticLimit = 10;
        r.colorMode = "never";
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<GrepRequest>, std::move(r)}));
    }

    // DeleteRequest edge cases
    {
        DeleteRequest r{};
        r.hash = "";
        r.purge = false;
        r.name = "";
        r.force = false;
        r.dryRun = true;
        r.keepRefs = true;
        r.recursive = false;
        r.verbose = true;
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<DeleteRequest>, std::move(r)}));
    }
    {
        DeleteRequest r{};
        r.hash = "deadbeef";
        r.purge = true;
        r.name = "../../etc/passwd";
        r.names = {"a", "b", "..", "./x", std::string(256, 'C')};
        r.pattern = "**/*";
        r.directory = "/tmp";
        r.force = true;
        r.dryRun = false;
        r.keepRefs = false;
        r.recursive = true;
        r.verbose = true;
        r.sessionId = "sess";
        r.instanceId = "instance-22222222-2222-2222-2222-222222222222";
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<DeleteRequest>, std::move(r)}));
    }
    {
        DeleteRequest r{};
        r.hash = std::string(64, 'f');
        r.purge = false;
        r.pattern = "["; // malformed glob-ish
        r.directory = "";
        r.force = true;
        r.dryRun = true;
        r.recursive = true;
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<DeleteRequest>, std::move(r)}));
    }

    // A couple of simple baseline requests
    {
        PingRequest r{};
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<PingRequest>, std::move(r)}));
    }
    {
        StatusRequest r{};
        msgs.push_back(makeMsg(id++, Request{std::in_place_type<StatusRequest>, std::move(r)}));
    }

    return msgs;
}

} // namespace

int main(int argc, char** argv) {
    Args args;
    if (!parseArgs(argc, argv, args)) {
        std::cerr << "Usage: seedgen --out <dir> [--max-seeds N]\n";
        return 2;
    }

    const fs::path ipcProtocolDir = args.outRoot / "ipc_protocol";
    const fs::path ipcRoundtripDir = args.outRoot / "ipc_roundtrip";
    const fs::path requestHandlerDir = args.outRoot / "request_handler";
    const fs::path protoSerializerDir = args.outRoot / "proto_serializer";

    fs::create_directories(ipcProtocolDir);
    fs::create_directories(ipcRoundtripDir);
    fs::create_directories(requestHandlerDir);
    fs::create_directories(protoSerializerDir);

    MessageFramer framer;
    auto msgs = buildStructuredMessages();

    uint32_t written = 0;
    auto emit = [&](std::string_view stem, const Message& msg) {
        if (written >= args.maxSeeds) {
            return;
        }

        auto payloadRes = ProtoSerializer::encode_payload(msg);
        if (payloadRes) {
            auto payload = std::move(payloadRes.value());
            const auto outPath = protoSerializerDir / (std::string(stem) + "_payload.bin");
            if (writeFile(outPath, payload)) {
                ++written;
            }
        }

        auto frameRes = framer.frame_message(msg);
        if (frameRes) {
            auto frame = std::move(frameRes.value());

            const auto out1 = ipcProtocolDir / (std::string(stem) + "_frame.bin");
            const auto out2 = ipcRoundtripDir / (std::string(stem) + "_frame.bin");
            const auto out3 = requestHandlerDir / (std::string(stem) + "_frame.bin");
            (void)writeFile(out1, frame);
            (void)writeFile(out2, frame);
            (void)writeFile(out3, frame);

            // Mutations that preserve overall shape but exercise error paths
            auto badCrc = flipOneByte(frame);
            (void)writeFile(ipcProtocolDir / (std::string(stem) + "_badcrc.bin"), badCrc);
            (void)writeFile(ipcRoundtripDir / (std::string(stem) + "_badcrc.bin"), badCrc);
            (void)writeFile(requestHandlerDir / (std::string(stem) + "_badcrc.bin"), badCrc);
        }

        auto chunkRes = framer.frame_message_chunk(msg, /*last_chunk=*/true);
        if (chunkRes) {
            auto chunk = std::move(chunkRes.value());
            (void)writeFile(ipcProtocolDir / (std::string(stem) + "_chunk_last.bin"), chunk);
            (void)writeFile(ipcRoundtripDir / (std::string(stem) + "_chunk_last.bin"), chunk);
            (void)writeFile(requestHandlerDir / (std::string(stem) + "_chunk_last.bin"), chunk);
        }
    };

    uint32_t idx = 0;
    for (const auto& msg : msgs) {
        const std::string stem = "llm_" + std::to_string(idx++);
        emit(stem, msg);
    }

    std::cout << "Wrote structured seeds (max=" << args.maxSeeds << ") into "
              << args.outRoot.string() << "\n";
    return 0;
}
