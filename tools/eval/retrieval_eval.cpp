/*
  retrieval_eval.cpp

  Scaffold for a retrieval evaluation tool that:
  - Reads a set of queries with relevance judgments
  - Invokes the YAMS CLI to get ranked results per query
  - Computes ranking metrics: nDCG@k and MRR
  - Emits a JSON report with aggregate and per-query metrics

  Expected input format (JSONL recommended):
    JSONL (one query per line):
      {"id":"Q1", "text":"hybrid scoring", "relevant":["<hash1>","<hash2>"]}
      {"id":"Q2", "text":"pdfium macos rpath", "relevant":["<hash3>"]}

    OR a JSON file with a "queries" array:
      {
        "queries": [
          {"id":"Q1", "text":"hybrid scoring", "relevant":["<hash1>","<hash2>"]},
          {"id":"Q2", "text":"pdfium macos rpath", "relevant":["<hash3>"]}
        ]
      }

  CLI invocation (configurable):
    yams --json search "<query_text>" --limit <N> [extra args]

  Simple ablation modes are supported via environment toggles set for the subprocess:
    - YAMS_DISABLE_KEYWORD=1
    - YAMS_DISABLE_VECTOR=1
    - YAMS_DISABLE_KG=1

  Build:
    - Add to your build system as an executable and link standard libraries only.
    - This file avoids third-party JSON libs on purpose (hand-rolled parsing for a scaffold).

  Usage:
    retrieval_eval --queries <file.json|.jsonl> [--cli yams] [--limit 20]
                   [--k 5,10,20] [--extra-args "..."]
                   [--mode hybrid|keyword-only|vector-only|kg-only|fail-open-kg]
                   [--output metrics.json]

  Notes:
    - This is a scaffold: JSON parsing is intentionally minimal and forgiving.
    - Metrics assume binary relevance.
*/

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#if defined(_WIN32)
  #include <windows.h>
  #define POPEN  _popen
  #define PCLOSE _pclose
#else
  #include <unistd.h>
  #define POPEN  popen
  #define PCLOSE pclose
#endif

struct Query {
  std::string id;
  std::string text;
  std::unordered_set<std::string> relevant; // document identifiers (hashes)
};

struct RunConfig {
  std::string queriesPath;
  std::string cliPath = "yams";
  std::string extraArgs;   // appended after "search"
  std::string mode = "hybrid"; // hybrid|keyword-only|vector-only|kg-only|fail-open-kg
  std::string outputPath;  // empty => stdout
  std::vector<int> ks{5, 10, 20};
  int limit = 20;
  bool verboseCli = true;  // pass --json --verbose to CLI
};

// ---------- Utilities ----------

static bool fileExists(const std::string& p) {
  std::error_code ec;
  return std::filesystem::exists(p, ec);
}

static std::string readFileAll(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open file: " + path);
  std::ostringstream oss;
  oss << in.rdbuf();
  return oss.str();
}

static std::vector<std::string> readLines(const std::string& path) {
  std::ifstream in(path);
  if (!in) throw std::runtime_error("Failed to open file: " + path);
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) lines.push_back(line);
  }
  return lines;
}

static std::string trim(const std::string& s) {
  size_t b = 0;
  while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
  size_t e = s.size();
  while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
  return s.substr(b, e - b);
}

static std::vector<int> parseKList(const std::string& s) {
  std::vector<int> ks;
  std::stringstream ss(s);
  std::string tok;
  while (std::getline(ss, tok, ',')) {
    tok = trim(tok);
    if (tok.empty()) continue;
    try {
      ks.push_back(std::stoi(tok));
    } catch (...) {
      // ignore bad entries
    }
  }
  if (ks.empty()) ks = {5, 10, 20};
  std::sort(ks.begin(), ks.end());
  ks.erase(std::unique(ks.begin(), ks.end()), ks.end());
  return ks;
}

static std::string shellEscapeSingleQuoted(const std::string& in) {
  std::string out;
  out.reserve(in.size() + 16);
  out.push_back('\'');
  for (char c : in) {
    if (c == '\'') out += "'\\''";
    else out.push_back(c);
  }
  out.push_back('\'');
  return out;
}

static void setEnvVar(const char* key, const char* val) {
#if defined(_WIN32)
  _putenv_s(key, val);
#else
  setenv(key, val, 1);
#endif
}

static void unsetEnvVar(const char* key) {
#if defined(_WIN32)
  _putenv_s(key, "");
#else
  unsetenv(key);
#endif
}

class ScopedEnv {
 public:
  explicit ScopedEnv(const std::vector<std::pair<std::string, std::optional<std::string>>>& kvs) {
    for (const auto& [k, v] : kvs) {
      const char* prev = std::getenv(k.c_str());
      if (prev) prev_.emplace(k, std::string(prev));
      else prev_.emplace(k, std::nullopt);
      if (v.has_value()) setEnvVar(k.c_str(), v->c_str());
      else unsetEnvVar(k.c_str());
      keys_.push_back(k);
    }
  }
  ~ScopedEnv() {
    for (const auto& k : keys_) {
      auto it = prev_.find(k);
      if (it == prev_.end()) continue;
      if (it->second.has_value()) setEnvVar(k.c_str(), it->second->c_str());
      else unsetEnvVar(k.c_str());
    }
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;

 private:
  std::vector<std::string> keys_;
  std::map<std::string, std::optional<std::string>> prev_;
};

static bool runCommandCapture(const std::string& cmd, std::string& out) {
  out.clear();
  FILE* pipe = POPEN(cmd.c_str(), "r");
  if (!pipe) return false;
  char buffer[4096];
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    out.append(buffer);
  }
  int status = PCLOSE(pipe);
  // Best-effort success detection
  return status == 0;
}

// ---------- Minimal JSON readers ----------

// Try to parse JSONL where each line is a query object.
static bool tryParseQueriesJsonl(const std::vector<std::string>& lines, std::vector<Query>& out) {
  static const std::regex idRe(R"regex("id"\s*:\s*"([^"]+)")regex");
  static const std::regex textRe(R"regex("text"\s*:\s*"([^"]+)")regex");
  static const std::regex relArrRe(R"regex("relevant"\s*:\s*\[(.*?)\])regex");
  static const std::regex strRe(R"regex("([^"]*)")regex");

  out.clear();
  for (const auto& lineRaw : lines) {
    std::string line = trim(lineRaw);
    if (line.empty()) continue;
    if (line.front() != '{') return false; // Not a JSON object line
    std::smatch m;
    Query q;
    if (std::regex_search(line, m, idRe)) q.id = m[1].str();
    else return false;
    if (std::regex_search(line, m, textRe)) q.text = m[1].str();
    else return false;
    if (std::regex_search(line, m, relArrRe)) {
      std::string arr = m[1].str();
      std::sregex_iterator it(arr.begin(), arr.end(), strRe);
      std::sregex_iterator end;
      for (; it != end; ++it) {
        std::string v = (*it)[1].str();
        if (!v.empty()) q.relevant.insert(v);
      }
    }
    out.push_back(std::move(q));
  }
  return !out.empty();
}

// Try to parse a single JSON with "queries" array (flat objects).
static bool tryParseQueriesJsonObject(const std::string& content, std::vector<Query>& out) {
  out.clear();
  auto pos = content.find("\"queries\"");
  if (pos == std::string::npos) return false;
  pos = content.find('[', pos);
  if (pos == std::string::npos) return false;
  auto end = content.find(']', pos);
  if (end == std::string::npos) return false;

  const std::string arr = content.substr(pos + 1, end - pos - 1);
  // Split on "},{" boundaries (naive; adequate for flat objects)
  std::vector<std::string> objs;
  size_t start = 0;
  size_t cursor = 0;
  while (cursor < arr.size()) {
    if (arr[cursor] == '{') {
      size_t close = arr.find('}', cursor);
      if (close == std::string::npos) break;
      objs.push_back(arr.substr(cursor, close - cursor + 1));
      cursor = close + 1;
    } else {
      ++cursor;
    }
  }

  static const std::regex idRe(R"regex("id"\s*:\s*"([^"]+)")regex");
  static const std::regex textRe(R"regex("text"\s*:\s*"([^"]+)")regex");
  static const std::regex relArrRe(R"regex("relevant"\s*:\s*\[(.*?)\])regex");
  static const std::regex strRe(R"regex("([^"]*)")regex");

  for (const auto& obj : objs) {
    Query q;
    std::smatch m;
    if (std::regex_search(obj, m, idRe)) q.id = m[1].str();
    else continue;
    if (std::regex_search(obj, m, textRe)) q.text = m[1].str();
    else continue;
    if (std::regex_search(obj, m, relArrRe)) {
      std::string arrInner = m[1].str();
      std::sregex_iterator it(arrInner.begin(), arrInner.end(), strRe);
      std::sregex_iterator endIt;
      for (; it != endIt; ++it) {
        std::string v = (*it)[1].str();
        if (!v.empty()) q.relevant.insert(v);
      }
    }
    out.push_back(std::move(q));
  }
  return !out.empty();
}

// ---------- CLI result parsing ----------

// Extract document identifiers ("hash") from CLI JSON output.
// This is a naive regex-based extractor; it expects fields like "hash":"<hex>".
// If "hash" is missing, falls back to extracting "path" (string).
static std::vector<std::string> extractDocIdsFromCliJson(const std::string& json) {
  std::vector<std::string> ids;
  // Prefer hash (8-64 hex chars), tolerate varying cases
  static const std::regex hashRe(R"regex("hash"\s*:\s*"([0-9a-fA-F]{8,64})")regex");
  for (std::sregex_iterator it(json.begin(), json.end(), hashRe), end; it != end; ++it) {
    ids.push_back((*it)[1].str());
  }
  if (!ids.empty()) return ids;

  // Fallback to "path":"..."; still usable if relevance uses paths.
  static const std::regex pathRe(R"regex("path"\s*:\s*"([^"]+)")regex");
  for (std::sregex_iterator it(json.begin(), json.end(), pathRe), end; it != end; ++it) {
    ids.push_back((*it)[1].str());
  }
  return ids;
}

// ---------- Metrics ----------

static double log2d(double x) {
  return std::log(x) / std::log(2.0);
}

static double dcgAtK(const std::vector<int>& gains, int k) {
  double dcg = 0.0;
  const int n = std::min<int>(k, static_cast<int>(gains.size()));
  for (int i = 0; i < n; ++i) {
    const int rel = gains[i];
    const int rank = i + 1;
    dcg += static_cast<double>(rel) / log2d(rank + 1.0);
  }
  return dcg;
}

static double idcgAtK(int numRelevant, int k) {
  const int n = std::min(k, numRelevant);
  double idcg = 0.0;
  for (int i = 0; i < n; ++i) {
    const int rank = i + 1;
    idcg += 1.0 / log2d(rank + 1.0);
  }
  return idcg;
}

static double ndcgAtK(const std::vector<int>& gains, int numRelevant, int k) {
  const double dcg = dcgAtK(gains, k);
  const double idcg = idcgAtK(numRelevant, k);
  if (idcg <= 0.0) return 0.0;
  return dcg / idcg;
}

static double mrr(const std::vector<int>& gains) {
  for (size_t i = 0; i < gains.size(); ++i) {
    if (gains[i] > 0) {
      return 1.0 / static_cast<double>(i + 1);
    }
  }
  return 0.0;
}

// ---------- CLI invocation ----------

static std::string buildSearchCommand(const RunConfig& cfg, const std::string& query) {
  std::ostringstream cmd;
#if defined(_WIN32)
  // Best-effort quoting on Windows
  cmd << "\"" << cfg.cliPath << "\" ";
  if (cfg.verboseCli) cmd << "--json --verbose ";
  cmd << "search ";
  if (!cfg.extraArgs.empty()) cmd << cfg.extraArgs << " ";
  cmd << "\"" << query << "\" ";
  cmd << "--limit " << cfg.limit;
#else
  cmd << cfg.cliPath << " ";
  if (cfg.verboseCli) cmd << "--json --verbose ";
  cmd << "search ";
  if (!cfg.extraArgs.empty()) cmd << cfg.extraArgs << " ";
  cmd << shellEscapeSingleQuoted(query) << " ";
  cmd << "--limit " << cfg.limit;
#endif
  return cmd.str();
}

static void applyModeEnv(const std::string& mode, std::vector<std::pair<std::string, std::optional<std::string>>>& env) {
  // By default (hybrid), do nothing (ensure vars are unset)
  if (mode == "keyword-only") {
    env.emplace_back("YAMS_DISABLE_VECTOR", std::make_optional<std::string>("1"));
    env.emplace_back("YAMS_DISABLE_KG",     std::make_optional<std::string>("1"));
    env.emplace_back("YAMS_DISABLE_KEYWORD", std::nullopt);
  } else if (mode == "vector-only") {
    env.emplace_back("YAMS_DISABLE_KEYWORD", std::make_optional<std::string>("1"));
    env.emplace_back("YAMS_DISABLE_KG",      std::make_optional<std::string>("1"));
    env.emplace_back("YAMS_DISABLE_VECTOR",  std::nullopt);
  } else if (mode == "kg-only") {
    env.emplace_back("YAMS_DISABLE_KEYWORD", std::make_optional<std::string>("1"));
    env.emplace_back("YAMS_DISABLE_VECTOR",  std::make_optional<std::string>("1"));
    env.emplace_back("YAMS_DISABLE_KG",      std::nullopt);
  } else if (mode == "fail-open-kg") {
    // Simulate KG unavailable
    env.emplace_back("YAMS_DISABLE_KG", std::make_optional<std::string>("1"));
    env.emplace_back("YAMS_DISABLE_KEYWORD", std::nullopt);
    env.emplace_back("YAMS_DISABLE_VECTOR",  std::nullopt);
  } else {
    // hybrid: unset all
    env.emplace_back("YAMS_DISABLE_KEYWORD", std::nullopt);
    env.emplace_back("YAMS_DISABLE_VECTOR",  std::nullopt);
    env.emplace_back("YAMS_DISABLE_KG",      std::nullopt);
  }
}

// ---------- Query loading ----------

static std::vector<Query> loadQueries(const std::string& path) {
  if (!fileExists(path)) {
    throw std::runtime_error("Queries file does not exist: " + path);
  }
  // Try JSONL first
  std::vector<std::string> lines = readLines(path);
  std::vector<Query> queries;
  if (tryParseQueriesJsonl(lines, queries)) return queries;

  // Fallback to JSON with "queries":[...]
  const std::string content = readFileAll(path);
  if (tryParseQueriesJsonObject(content, queries)) return queries;

  throw std::runtime_error("Failed to parse queries. Expected JSONL or JSON with a 'queries' array.");
}

// ---------- Metrics computation pipeline ----------

struct PerQueryResult {
  std::string id;
  std::string text;
  int num_relevant = 0;
  std::map<int, double> ndcg_at_k; // k -> nDCG
  double mrr = 0.0;
  int retrieved = 0;
  double query_time_ms = 0.0; // total wall time for CLI invocation
};

struct EvalReport {
  RunConfig cfg;
  std::vector<PerQueryResult> per_query;
  std::map<int, double> avg_ndcg_at_k;
  double avg_mrr = 0.0;
};

static EvalReport runEvaluation(const RunConfig& cfg, const std::vector<Query>& queries) {
  EvalReport report;
  report.cfg = cfg;

  // Prepare environment for the chosen mode
  std::vector<std::pair<std::string, std::optional<std::string>>> env;
  applyModeEnv(cfg.mode, env);

  double sum_mrr = 0.0;
  std::map<int, double> sum_ndcg;
  for (int k : cfg.ks) sum_ndcg[k] = 0.0;

  for (const auto& q : queries) {
    PerQueryResult r;
    r.id = q.id;
    r.text = q.text;
    r.num_relevant = static_cast<int>(q.relevant.size());

    const std::string cmd = buildSearchCommand(cfg, q.text);

    const auto t0 = std::chrono::steady_clock::now();
    std::string jsonOut;
    {
      ScopedEnv scope(env);
      (void)runCommandCapture(cmd, jsonOut); // tolerate failures
    }
    const auto t1 = std::chrono::steady_clock::now();
    r.query_time_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0).count();

    std::vector<std::string> docIds = extractDocIdsFromCliJson(jsonOut);
    r.retrieved = static_cast<int>(docIds.size());

    // Build binary gains vector
    std::vector<int> gains;
    gains.reserve(docIds.size());
    for (const auto& id : docIds) {
      gains.push_back(q.relevant.count(id) ? 1 : 0);
    }

    // Compute metrics
    for (int k : cfg.ks) {
      r.ndcg_at_k[k] = ndcgAtK(gains, r.num_relevant, k);
    }
    r.mrr = mrr(gains);

    // Aggregate
    sum_mrr += r.mrr;
    for (int k : cfg.ks) {
      sum_ndcg[k] += r.ndcg_at_k[k];
    }

    report.per_query.push_back(std::move(r));
  }

  const double n = static_cast<double>(report.per_query.size());
  if (n > 0.0) {
    report.avg_mrr = sum_mrr / n;
    for (auto& [k, s] : sum_ndcg) {
      report.avg_ndcg_at_k[k] = s / n;
    }
  }

  return report;
}

// ---------- JSON writer ----------

static std::string jsonEscape(const std::string& s) {
  std::ostringstream o;
  for (char c : s) {
    switch (c) {
      case '"':  o << "\\\""; break;
      case '\\': o << "\\\\"; break;
      case '\b': o << "\\b";  break;
      case '\f': o << "\\f";  break;
      case '\n': o << "\\n";  break;
      case '\r': o << "\\r";  break;
      case '\t': o << "\\t";  break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
        } else {
          o << c;
        }
    }
  }
  return o.str();
}

static void writeReportJson(std::ostream& os, const EvalReport& rep) {
  os << "{\n";
  // config
  os << "  \"config\": {\n";
  os << "    \"queries_path\": \"" << jsonEscape(rep.cfg.queriesPath) << "\",\n";
  os << "    \"cli\": \"" << jsonEscape(rep.cfg.cliPath) << "\",\n";
  os << "    \"limit\": " << rep.cfg.limit << ",\n";
  os << "    \"k_list\": [";
  for (size_t i = 0; i < rep.cfg.ks.size(); ++i) {
    if (i) os << ", ";
    os << rep.cfg.ks[i];
  }
  os << "],\n";
  os << "    \"mode\": \"" << jsonEscape(rep.cfg.mode) << "\",\n";
  os << "    \"extra_args\": \"" << jsonEscape(rep.cfg.extraArgs) << "\"\n";
  os << "  },\n";

  // metrics aggregate
  os << "  \"metrics\": {\n";
  os << "    \"avg_mrr\": " << std::fixed << std::setprecision(6) << rep.avg_mrr << ",\n";
  os << "    \"avg_ndcg\": {";
  bool first = true;
  for (const auto& [k, v] : rep.avg_ndcg_at_k) {
    if (!first) os << ", ";
    first = false;
    os << "\"@" << k << "\": " << std::fixed << std::setprecision(6) << v;
  }
  os << "}\n";
  os << "  },\n";

  // per-query
  os << "  \"by_query\": [\n";
  for (size_t i = 0; i < rep.per_query.size(); ++i) {
    const auto& r = rep.per_query[i];
    os << "    {\n";
    os << "      \"id\": \"" << jsonEscape(r.id) << "\",\n";
    os << "      \"text\": \"" << jsonEscape(r.text) << "\",\n";
    os << "      \"num_relevant\": " << r.num_relevant << ",\n";
    os << "      \"retrieved\": " << r.retrieved << ",\n";
    os << "      \"query_time_ms\": " << std::fixed << std::setprecision(3) << r.query_time_ms << ",\n";
    os << "      \"ndcg\": {";
    bool f2 = true;
    for (const auto& [k, v] : r.ndcg_at_k) {
      if (!f2) os << ", ";
      f2 = false;
      os << "\"@" << k << "\": " << std::fixed << std::setprecision(6) << v;
    }
    os << "},\n";
    os << "      \"mrr\": " << std::fixed << std::setprecision(6) << r.mrr << "\n";
    os << "    }";
    if (i + 1 < rep.per_query.size()) os << ",";
    os << "\n";
  }
  os << "  ]\n";
  os << "}\n";
}

// ---------- CLI parsing ----------

static void printUsage(const char* prog) {
  std::cerr
    << "Usage: " << prog << " --queries <path.json|.jsonl> [options]\n"
    << "Options:\n"
    << "  --cli <path>            Path to yams CLI (default: yams)\n"
    << "  --limit <N>             Search results limit (default: 20)\n"
    << "  --k <list>              Comma-separated k list for nDCG (default: 5,10,20)\n"
    << "  --extra-args \"...\"     Extra args passed after 'search' (e.g., --storage <path>)\n"
    << "  --mode <m>              hybrid|keyword-only|vector-only|kg-only|fail-open-kg (default: hybrid)\n"
    << "  --no-verbose-cli        Do not pass --json --verbose to CLI\n"
    << "  --output <path.json>    Write metrics JSON to file (default: stdout)\n";
}

static RunConfig parseArgs(int argc, char** argv) {
  RunConfig cfg;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto need = [&](const char* name) {
      if (i + 1 >= argc) throw std::runtime_error(std::string("Missing value for ") + name);
      return std::string(argv[++i]);
    };
    if (a == "--queries") cfg.queriesPath = need("--queries");
    else if (a == "--cli") cfg.cliPath = need("--cli");
    else if (a == "--limit") cfg.limit = std::stoi(need("--limit"));
    else if (a == "--k") cfg.ks = parseKList(need("--k"));
    else if (a == "--extra-args") cfg.extraArgs = need("--extra-args");
    else if (a == "--mode") cfg.mode = need("--mode");
    else if (a == "--no-verbose-cli") cfg.verboseCli = false;
    else if (a == "--output") cfg.outputPath = need("--output");
    else if (a == "--help" || a == "-h") {
      printUsage(argv[0]);
      std::exit(0);
    } else {
      throw std::runtime_error("Unknown argument: " + a);
    }
  }
  if (cfg.queriesPath.empty()) throw std::runtime_error("Missing required --queries <path>");
  return cfg;
}

// ---------- main ----------

int main(int argc, char** argv) {
  try {
    RunConfig cfg = parseArgs(argc, argv);
    std::vector<Query> queries = loadQueries(cfg.queriesPath);
    if (queries.empty()) {
      std::cerr << "No queries loaded. Nothing to do.\n";
      return 2;
    }
    EvalReport rep = runEvaluation(cfg, queries);

    if (!cfg.outputPath.empty()) {
      std::ofstream out(cfg.outputPath);
      if (!out) throw std::runtime_error("Failed to open output file: " + cfg.outputPath);
      writeReportJson(out, rep);
      std::cerr << "Wrote metrics to " << cfg.outputPath << "\n";
    } else {
      writeReportJson(std::cout, rep);
    }
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    printUsage(argv[0]);
    return 1;
  }
}