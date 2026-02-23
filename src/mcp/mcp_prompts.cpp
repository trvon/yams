#include <yams/mcp/mcp_server.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <unordered_set>

namespace yams::mcp {

namespace {
json textContent(const std::string& t) {
    return json{{"type", "text"}, {"text", t}};
}

json assistantMsg(const std::string& t) {
    return json{{"role", "assistant"}, {"content", textContent(t)}};
}

json userMsg(const std::string& t) {
    return json{{"role", "user"}, {"content", textContent(t)}};
}

std::optional<std::filesystem::path> promptFileFromName(const std::filesystem::path& promptsDir,
                                                        const std::string& n) {
    if (promptsDir.empty())
        return std::nullopt;
    std::string stem = n;
    std::replace(stem.begin(), stem.end(), '_', '-');
    auto candidate = promptsDir / (std::string("PROMPT-") + stem + ".md");
    if (std::filesystem::exists(candidate))
        return candidate;
    return std::nullopt;
}

std::string sanitizePromptFileName(std::string s) {
    if (s.rfind("PROMPT-", 0) == 0)
        s = s.substr(7);
    auto pos = s.rfind('.');
    if (pos != std::string::npos)
        s = s.substr(0, pos);
    std::replace(s.begin(), s.end(), '-', '_');
    return s;
}
} // namespace

MessageResult MCPServer::handlePromptGet(const nlohmann::json& id, const std::string& name,
                                         const nlohmann::json& args) {
    auto makeResponse = [&](std::vector<json> messages) -> MessageResult {
        return createResponse(id, json{{"messages", std::move(messages)}});
    };

    if (name == "search_codebase") {
        const std::string pattern = args.value("pattern", "");
        const std::string fileType = args.value("file_type", "");
        std::string u = "Goal: find relevant code/docs using a Codex-style grep-first workflow.\n"
                        "Execution order:\n"
                        "1) tools/grep for exact symbols, literals, paths, and signatures.\n"
                        "2) tools/search for semantic context and related files.\n"
                        "3) Return compact candidates for follow-up tools/get reads.\n"
                        "Output requirements:\n"
                        "- Include line numbers and filenames for grep hits.\n"
                        "- Include names/paths and snippets for search hits.\n"
                        "- De-dup near-identical candidates.\n"
                        "- Respect session scoping when present.\n";
        if (!pattern.empty()) {
            u += "\nRequested pattern: " + pattern + "\n";
        }
        if (!fileType.empty()) {
            u += "File type filter hint: " + fileType + "\n";
        }
        return makeResponse(
            {assistantMsg("You are a code navigator that selects between grep and hybrid search."),
             userMsg(u)});
    }

    if (name == "summarize_document") {
        const std::string docName = args.value("document_name", "");
        const int maxLen = args.value("max_length", 200);
        std::string u = "Summarize one YAMS document with grounded citations.\n"
                        "Steps:\n"
                        "1) Retrieve via tools/get (prefer name + latest=true, or hash).\n"
                        "2) Summarize only retrieved content within " +
                        std::to_string(maxLen) +
                        " words.\n"
                        "3) Include citation using name/path/hash.\n";
        if (!docName.empty()) {
            u += "\nDocument name: " + docName + "\n";
        }
        return makeResponse(
            {assistantMsg("You are a precise, grounded summarizer for YAMS artifacts."),
             userMsg(u)});
    }

    if (name == "rag/rewrite_query") {
        const std::string query = args.value("query", "");
        const std::string intent = args.value("intent", "");
        std::string u = "Rewrite the user query for YAMS hybrid retrieval (text + metadata).\n"
                        "Guidelines:\n"
                        "- Preserve exact symbols, paths, identifiers, hashes, and quoted "
                        "literals.\n"
                        "- Add only high-value synonyms or qualifiers.\n"
                        "- Keep it compact and retrieval-oriented.\n"
                        "- If already strong, keep it nearly unchanged.\n";
        if (!query.empty()) {
            u += "\nOriginal query: " + query + "\n";
        }
        if (!intent.empty()) {
            u += "Intent hint: " + intent + "\n";
        }
        return makeResponse(
            {assistantMsg("You are a retrieval query rewriter for hybrid search."), userMsg(u)});
    }

    if (name == "rag/retrieve") {
        const std::string query = args.value("query", "");
        const int k = std::max(1, args.value("k", 10));
        const std::string session = args.value("session", "");
        std::string u = "Retrieve top-" + std::to_string(k) +
                        " YAMS candidates for follow-up reading.\n"
                        "Requirements:\n"
                        "- Use tools/search (hybrid/fuzzy unless exact-match behavior is "
                        "needed).\n"
                        "- Apply session scoping if provided.\n"
                        "- Return compact, citeable candidates: "
                        "name/path/hash/score/snippet.\n"
                        "- De-dup near-identical results and prefer diverse sources.\n";
        if (!query.empty()) {
            u += "\nQuery: " + query + "\n";
        }
        if (!session.empty()) {
            u += "Scope session: " + session + "\n";
        }
        return makeResponse(
            {assistantMsg("You are a retrieval runner that returns compact, citeable "
                          "candidates."),
             userMsg(u)});
    }

    if (name == "rag/retrieve_summarize") {
        const std::string query = args.value("query", "");
        const int k = std::max(1, args.value("k", 5));
        const int maxWords = std::max(50, args.value("max_words", 250));
        std::string u = "YAMS RAG pipeline: retrieve, read, then summarize.\n"
                        "Steps:\n"
                        "1) tools/search with query (hybrid/fuzzy), top-" +
                        std::to_string(k) +
                        ".\n"
                        "2) Fetch candidate content via tools/get (by hash/name, "
                        "include_content=true).\n"
                        "3) Synthesize a grounded summary from retrieved content only in <= " +
                        std::to_string(maxWords) +
                        " words.\n"
                        "4) Include inline citations like (name | hash:abcd...).\n"
                        "5) Note uncertainty or conflicting evidence explicitly.\n";
        if (!query.empty()) {
            u += "\nQuery: " + query + "\n";
        }
        return makeResponse(
            {assistantMsg("You orchestrate retrieval and grounded summarization with citations."),
             userMsg(u)});
    }

    if (name == "rag/extract_citations") {
        const std::string style = args.value("style", "inline");
        const int k = std::max(1, args.value("k", 10));
        std::string u = "From prior retrieval results, produce " + std::to_string(k) +
                        " citations in style '" + style +
                        "'.\n"
                        "Include: name, short hash, path (if available), and date/labels if "
                        "known.\n"
                        "Keep formatting consistent and deterministic.\n";
        return makeResponse({assistantMsg("You format high-quality citations for retrieved "
                                          "artifacts."),
                             userMsg(u)});
    }

    if (name == "rag/code_navigation") {
        const std::string symbol = args.value("symbol", "");
        const std::string lang = args.value("language", "");
        std::string u = "Plan code navigation using a Codex-style grep-first flow.\n"
                        "Output:\n"
                        "- Exact tools/grep patterns or regexes to try first.\n"
                        "- Follow-up tools/search queries for semantic context and related files.\n"
                        "- Expected file/symbol variants to check.\n"
                        "- Respect session include patterns when unspecified.\n";
        if (!symbol.empty()) {
            u += "\nTarget symbol: " + symbol + "\n";
        }
        if (!lang.empty()) {
            u += "Language hint: " + lang + "\n";
        }
        return makeResponse(
            {assistantMsg("You guide symbol discovery and code navigation."), userMsg(u)});
    }

    if (name == "session/codex_repo") {
        std::string u =
            "Create a Codex session setup for this repo using the generic prompt plus "
            "repo supplement.\n"
            "Steps:\n"
            "1) prompts/get name=eng_codex (file-backed generic YAMS/Codex prompt)\n"
            "2) Read AGENTS.md from repo root (repo-specific supplement)\n"
            "3) Apply YAMS-first retrieval/indexing workflow from both\n"
            "4) Prefer concise, actionable outputs and cite retrieved artifacts when used\n";
        return makeResponse(
            {assistantMsg("You assemble a repo-ready Codex prompt from generic + repo "
                          "instructions."),
             userMsg(u)});
    }

    if (!name.empty() && !promptsDir_.empty()) {
        if (auto p = promptFileFromName(promptsDir_, name)) {
            try {
                std::ifstream in(*p);
                std::stringstream buf;
                buf << in.rdbuf();
                return makeResponse({assistantMsg(buf.str())});
            } catch (...) {
                // fall through
            }
        }
    }

    return makeResponse(
        {assistantMsg("You provide retrieval-augmented workflows over YAMS via MCP tools.")});
}

json yams::mcp::MCPServer::listPrompts() {
#if defined(YAMS_WASI)
    return {{"prompts", json::array()}};
#else
    auto builtins = json::array(
        {{{"name", "search_codebase"},
          {"description", "Grep-first codebase search plan (Codex/YAMS workflow)"},
          {"arguments", json::array({{{"name", "pattern"},
                                      {"description", "Code pattern to search for"},
                                      {"required", true}},
                                     {{"name", "file_type"},
                                      {"description", "Filter by file type (e.g., cpp, py, js)"},
                                      {"required", false}}})}},
         {{"name", "summarize_document"},
          {"description", "Grounded summary of one YAMS document with citation"},
          {"arguments", json::array({{{"name", "document_name"},
                                      {"description", "Name of the document to summarize"},
                                      {"required", true}},
                                     {{"name", "max_length"},
                                      {"description", "Maximum summary length in words"},
                                      {"required", false}}})}},
         {{"name", "rag/rewrite_query"},
          {"description", "Rewrite a query for YAMS hybrid retrieval"},
          {"arguments", json::array({{{"name", "query"},
                                      {"description", "Original user query text"},
                                      {"required", true}},
                                     {{"name", "intent"},
                                      {"description", "Optional intent hint to guide rewriting"},
                                      {"required", false}}})}},
         {{"name", "rag/retrieve"},
          {"description", "Retrieve top-k citeable candidates via YAMS search"},
          {"arguments",
           json::array({{{"name", "query"},
                         {"description", "Search query for retrieval"},
                         {"required", true}},
                        {{"name", "k"},
                         {"description", "Number of candidates to return"},
                         {"required", false}},
                        {{"name", "session"},
                         {"description", "Session name to scope retrieval (optional)"},
                         {"required", false}},
                        {{"name", "tags"},
                         {"description", "Optional tag filters (comma-separated or array)"},
                         {"required", false}}})}},
         {{"name", "rag/retrieve_summarize"},
          {"description", "YAMS RAG pipeline: retrieve, read, summarize with citations"},
          {"arguments",
           json::array({{{"name", "query"},
                         {"description", "Search query for retrieval"},
                         {"required", true}},
                        {{"name", "k"},
                         {"description", "Number of items to retrieve before summarization"},
                         {"required", false}},
                        {{"name", "max_words"},
                         {"description", "Maximum words for the synthesized summary"},
                         {"required", false}}})}},
         {{"name", "rag/extract_citations"},
          {"description", "Format citations from retrieved artifacts"},
          {"arguments",
           json::array({{{"name", "style"},
                         {"description", "Citation style (e.g., inline, list)"},
                         {"required", false}},
                        {{"name", "k"},
                         {"description", "Number of citations to produce"},
                         {"required", false}},
                        {{"name", "include_hashes"},
                         {"description", "Whether to include content hashes in citations"},
                         {"required", false}}})}},
         {{"name", "rag/code_navigation"},
          {"description", "Codex-style grep-first symbol discovery plan"},
          {"arguments", json::array({{{"name", "symbol"},
                                      {"description", "Target symbol or identifier to locate"},
                                      {"required", true}},
                                     {{"name", "language"},
                                      {"description", "Language hint (e.g., cpp, py, js)"},
                                      {"required", false}}})}},
         {{"name", "session/codex_repo"},
          {"description", "Compose generic eng_codex prompt plus repo AGENTS.md guidance"},
          {"arguments", json::array()}}});

    std::unordered_set<std::string> seen;
    for (const auto& t : builtins) {
        if (t.is_object() && t.contains("name")) {
            seen.insert(t.at("name").get<std::string>());
        }
    }

    if (!promptsDir_.empty() && std::filesystem::exists(promptsDir_)) {
        try {
            for (const auto& de : std::filesystem::directory_iterator(promptsDir_)) {
                if (!de.is_regular_file())
                    continue;
                auto fname = de.path().filename().string();
                if (fname.rfind("PROMPT-", 0) != 0)
                    continue;
                if (de.path().extension() != ".md")
                    continue;
                auto name = sanitizePromptFileName(fname);
                if (seen.count(name))
                    continue;
                std::ifstream in(de.path());
                std::string firstLine;
                if (in) {
                    std::getline(in, firstLine);
                }
                if (!firstLine.empty() && firstLine[0] == '#') {
                    while (!firstLine.empty() && (firstLine[0] == '#' || firstLine[0] == ' '))
                        firstLine.erase(firstLine.begin());
                }
                json t = {{"name", name},
                          {"description", !firstLine.empty() ? firstLine
                                                             : std::string{"Template from "} +
                                                                   de.path().filename().string()},
                          {"arguments", json::array()}};
                builtins.push_back(std::move(t));
                seen.insert(name);
            }
        } catch (...) {
            // best effort
        }
    }

    return {{"prompts", builtins}};
#endif
}

} // namespace yams::mcp
