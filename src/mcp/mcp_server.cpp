#include <yams/mcp/mcp_server.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <regex>
#include <set>
#include <filesystem>
#include <algorithm>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <memory>
#include <errno.h>
#include <cstring>
#include <cctype>

// Platform-specific includes for non-blocking I/O
#ifdef _WIN32
#include <windows.h>
#include <conio.h>
#else
#include <poll.h>
#include <unistd.h>
#endif

namespace yams::mcp {

// StdioTransport implementation
StdioTransport::StdioTransport() {
    // Ensure unbuffered I/O for stdio communication
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
}

void StdioTransport::send(const json& message) {
    if (!closed_) {
        std::cout << message.dump() << std::endl;
        std::cout.flush();
    }
}

bool StdioTransport::isInputAvailable(int timeoutMs) const {
#ifdef _WIN32
    // Windows implementation using WaitForSingleObject
    HANDLE stdinHandle = GetStdHandle(STD_INPUT_HANDLE);
    DWORD waitResult = WaitForSingleObject(stdinHandle, timeoutMs);
    return waitResult == WAIT_OBJECT_0;
#else
    // Unix/Linux/macOS implementation using poll
    struct pollfd fds;
    fds.fd = STDIN_FILENO;
    fds.events = POLLIN;
    fds.revents = 0;
    
    int result = poll(&fds, 1, timeoutMs);
    
    if (result == -1) {
        if (errno == EINTR) {
            // Signal interrupted, check shutdown
            if (externalShutdown_ && *externalShutdown_) {
                return false;
            }
        }
        return false;
    } else if (result == 0) {
        // Timeout - check shutdown
        if (externalShutdown_ && *externalShutdown_) {
            return false;
        }
    }
    
    return result > 0 && (fds.revents & POLLIN);
#endif
}

json StdioTransport::receive() {
    if (closed_) {
        return json{};
    }
    
    // Use polling with timeout to check for input
    // This allows us to periodically check the shutdown flag
    while (!closed_) {
        // Check if input is available with 100ms timeout
        if (isInputAvailable(100)) {
            std::string line;
            
            // Clear any error state
            std::cin.clear();
            
            if (std::getline(std::cin, line)) {
                if (!line.empty()) {
                    try {
                        return json::parse(line);
                    } catch (const json::parse_error& e) {
                        spdlog::error("Failed to parse JSON from stdin: {}", e.what());
                    }
                }
            } else if (std::cin.eof()) {
                spdlog::debug("EOF on stdin, closing transport");
                closed_ = true;
                break;
            } else if (std::cin.fail()) {
                // Clear error state and try again
                std::cin.clear();
            }
        }
        
        // Check if external shutdown was requested
        if (externalShutdown_ && *externalShutdown_) {
            spdlog::debug("External shutdown requested, closing transport");
            closed_ = true;
            break;
        }
        
        // No input available, loop will check shutdown flag again
    }
    
    return json{};
}

// MCPServer implementation
MCPServer::MCPServer(std::shared_ptr<api::IContentStore> store,
                     std::shared_ptr<search::SearchExecutor> searchExecutor,
                     std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                     std::shared_ptr<search::HybridSearchEngine> hybridEngine,
                     std::unique_ptr<ITransport> transport,
                     std::atomic<bool>* externalShutdown)
    : store_(std::move(store))
    , searchExecutor_(std::move(searchExecutor))
    , metadataRepo_(std::move(metadataRepo))
    , hybridEngine_(std::move(hybridEngine))
    , transport_(std::move(transport))
    , externalShutdown_(externalShutdown) {
    
    // Set external shutdown flag on StdioTransport if applicable
    if (auto* stdioTransport = dynamic_cast<StdioTransport*>(transport_.get())) {
        stdioTransport->setShutdownFlag(externalShutdown_);
    }
}

MCPServer::~MCPServer() {
    stop();
}

void MCPServer::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }
    
    spdlog::info("MCP server started");
    
    // Main message loop
    while (running_ && (!externalShutdown_ || !*externalShutdown_)) {
        try {
            auto message = transport_->receive();
            
            if (message.is_null() || message.empty()) {
                // Check if transport is still connected
                if (!transport_->isConnected()) {
                    spdlog::info("Transport disconnected, stopping server");
                    break;
                }
                continue;
            }
            
            auto response = handleRequest(message);
            if (!response.is_null()) {
                transport_->send(response);
            }
            
        } catch (const std::exception& e) {
            spdlog::error("Error in main loop: {}", e.what());
            
            // Send error response
            json errorResponse = {
                {"jsonrpc", "2.0"},
                {"error", {
                    {"code", -32603},
                    {"message", std::string("Internal error: ") + e.what()}
                }},
                {"id", nullptr}
            };
            
            try {
                transport_->send(errorResponse);
            } catch (...) {
                // Ignore send errors
            }
        }
    }
    
    running_ = false;
    spdlog::info("MCP server stopped");
}

void MCPServer::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }
    
    if (transport_) {
        transport_->close();
    }
}

json MCPServer::handleRequest(const json& request) {
    try {
        // Validate JSON-RPC request
        if (!request.contains("jsonrpc") || request["jsonrpc"] != "2.0") {
            return {
                {"jsonrpc", "2.0"},
                {"error", {
                    {"code", -32600},
                    {"message", "Invalid Request: Missing or invalid jsonrpc field"}
                }},
                {"id", request.value("id", nullptr)}
            };
        }
        
        if (!request.contains("method")) {
            return {
                {"jsonrpc", "2.0"},
                {"error", {
                    {"code", -32600},
                    {"message", "Invalid Request: Missing method field"}
                }},
                {"id", request.value("id", nullptr)}
            };
        }
        
        std::string method = request["method"];
        json params = request.value("params", json::object());
        
        json result;
        
        // Route to appropriate handler
        if (method == "initialize") {
            result = initialize(params);
        } else if (method == "initialized") {
            // Client notification that initialization is complete
            initialized_ = true;
            return json{}; // No response for notifications
        } else if (method == "resources/list") {
            result = listResources();
        } else if (method == "resources/read") {
            if (!params.contains("uri")) {
                throw std::runtime_error("Missing required parameter: uri");
            }
            result = readResource(params["uri"]);
        } else if (method == "tools/list") {
            result = listTools();
        } else if (method == "tools/call") {
            if (!params.contains("name") || !params.contains("arguments")) {
                throw std::runtime_error("Missing required parameters: name, arguments");
            }
            result = callTool(params["name"], params["arguments"]);
        } else if (method == "prompts/list") {
            result = listPrompts();
        } else if (method == "prompts/get") {
            if (!params.contains("name")) {
                throw std::runtime_error("Missing required parameter: name");
            }
            // Prompts not implemented yet
            return {
                {"jsonrpc", "2.0"},
                {"id", request.value("id", nullptr)},
                {"error", {
                    {"code", -32601},
                    {"message", "Prompts not implemented"}
                }}
            };
        } else if (method == "completion/complete") {
            // Completion not implemented yet
            return {
                {"jsonrpc", "2.0"},
                {"id", request.value("id", nullptr)},
                {"error", {
                    {"code", -32601},
                    {"message", "Completion not implemented"}
                }}
            };
        } else if (method == "logging/setLevel") {
            // Logging level change not implemented
            result = json::object();
        } else if (method == "shutdown" || method == "exit") {
            running_ = false;
            result = json::object();
        } else {
            return {
                {"jsonrpc", "2.0"},
                {"error", {
                    {"code", -32601},
                    {"message", "Method not found: " + method}
                }},
                {"id", request.value("id", nullptr)}
            };
        }
        
        // Return successful response
        return {
            {"jsonrpc", "2.0"},
            {"result", result},
            {"id", request.value("id", nullptr)}
        };
        
    } catch (const std::exception& e) {
        return {
            {"jsonrpc", "2.0"},
            {"error", {
                {"code", -32603},
                {"message", std::string("Internal error: ") + e.what()}
            }},
            {"id", request.value("id", nullptr)}
        };
    }
}

json MCPServer::initialize(const json& params) {
    // Store client info if provided
    if (params.contains("clientInfo")) {
        auto info = params["clientInfo"];
        clientInfo_.name = info.value("name", "unknown");
        clientInfo_.version = info.value("version", "unknown");
        spdlog::info("MCP client connected: {} {}", 
                    clientInfo_.name,
                    clientInfo_.version);
    }
    
    // Return server capabilities
    return {
        {"protocolVersion", "0.1.0"},
        {"capabilities", {
            {"resources", {
                {"subscribe", false},
                {"list", true},
                {"read", true}
            }},
            {"tools", {
                {"list", true},
                {"call", true}
            }},
            {"prompts", {
                {"list", true},
                {"get", true}
            }},
            {"logging", {
                {"setLevel", true}
            }}
        }},
        {"serverInfo", {
            {"name", "YAMS MCP Server"},
            {"version", "0.0.2"}
        }}
    };
}

json MCPServer::listResources() {
    json resources = json::array();
    
    // Add a resource for the YAMS storage statistics
    resources.push_back({
        {"uri", "yams://stats"},
        {"name", "Storage Statistics"},
        {"description", "Current YAMS storage statistics and health status"},
        {"mimeType", "application/json"}
    });
    
    // Add a resource for recent documents
    resources.push_back({
        {"uri", "yams://recent"},
        {"name", "Recent Documents"},
        {"description", "Recently added documents in YAMS storage"},
        {"mimeType", "application/json"}
    });
    
    return {{"resources", resources}};
}

json MCPServer::readResource(const std::string& uri) {
    if (uri == "yams://stats") {
        // Get storage statistics
        auto stats = store_->getStats();
        auto health = store_->checkHealth();
        
        return {
            {"contents", {{
                {"uri", uri},
                {"mimeType", "application/json"},
                {"text", json({
                    {"storage", {
                        {"totalObjects", stats.totalObjects},
                        {"totalBytes", stats.totalBytes},
                        {"uniqueBlocks", stats.uniqueBlocks},
                        {"deduplicatedBytes", stats.deduplicatedBytes}
                    }},
                    {"health", {
                        {"isHealthy", health.isHealthy},
                        {"status", health.status},
                        {"warnings", health.warnings},
                        {"errors", health.errors}
                    }}
                }).dump()}
            }}}
        };
    } else if (uri == "yams://recent") {
        // Get recent documents
        auto docsResult = metadataRepo_->findDocumentsByPath("%");
        if (!docsResult) {
            return {
                {"contents", {{"text", "Failed to list documents"}}}
            };
        }
        auto docs = docsResult.value();
        // Limit to 20 most recent
        if (docs.size() > 20) {
            docs.resize(20);
        }
        
        json docList = json::array();
        for (const auto& doc : docs) {
            docList.push_back({
                {"hash", doc.sha256Hash},
                {"name", doc.fileName},
                {"size", doc.fileSize},
                {"mimeType", doc.mimeType}
            });
        }
        
        return {
            {"contents", {{
                {"uri", uri},
                {"mimeType", "application/json"},
                {"text", json({{"documents", docList}}).dump()}
            }}}
        };
    } else {
        throw std::runtime_error("Unknown resource URI: " + uri);
    }
}

json MCPServer::listTools() {
    return {
        {"tools", json::array({
            // Core document operations
            {
                {"name", "search_documents"},
                {"description", "Search for documents using keywords, fuzzy matching, or similarity"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"query", {
                            {"type", "string"},
                            {"description", "Search query (keywords, phrases, or hash)"}
                        }},
                        {"limit", {
                            {"type", "integer"},
                            {"description", "Maximum number of results"},
                            {"default", 10}
                        }},
                        {"fuzzy", {
                            {"type", "boolean"},
                            {"description", "Enable fuzzy matching"},
                            {"default", false}
                        }},
                        {"similarity", {
                            {"type", "number"},
                            {"description", "Minimum similarity threshold (0-1)"},
                            {"default", 0.7}
                        }},
                        {"paths_only", {
                            {"type", "boolean"},
                            {"description", "Return only file paths (LLM-friendly)"},
                            {"default", false}
                        }},
                        {"line_numbers", {
                            {"type", "boolean"},
                            {"description", "Include line numbers in content"},
                            {"default", false}
                        }},
                        {"after_context", {
                            {"type", "integer"},
                            {"description", "Lines of context after matches"},
                            {"default", 0}
                        }},
                        {"before_context", {
                            {"type", "integer"},
                            {"description", "Lines of context before matches"},
                            {"default", 0}
                        }},
                        {"context", {
                            {"type", "integer"},
                            {"description", "Lines of context around matches"},
                            {"default", 0}
                        }},
                        {"color", {
                            {"type", "string"},
                            {"enum", {"always", "never", "auto"}},
                            {"description", "Color highlighting for matches"},
                            {"default", "auto"}
                        }}
                    }},
                    {"required", {"query"}}
                }}
            },
            {
                {"name", "grep_documents"},
                {"description", "Search document contents using regular expressions"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"pattern", {
                            {"type", "string"},
                            {"description", "Regular expression pattern"}
                        }},
                        {"paths", {
                            {"type", "array"},
                            {"items", {"type", "string"}},
                            {"description", "Specific paths to search (optional)"}
                        }},
                        {"ignore_case", {
                            {"type", "boolean"},
                            {"description", "Case-insensitive search"},
                            {"default", false}
                        }},
                        {"word", {
                            {"type", "boolean"},
                            {"description", "Match whole words only"},
                            {"default", false}
                        }},
                        {"invert", {
                            {"type", "boolean"},
                            {"description", "Invert match (show non-matching lines)"},
                            {"default", false}
                        }},
                        {"line_numbers", {
                            {"type", "boolean"},
                            {"description", "Show line numbers"},
                            {"default", false}
                        }},
                        {"with_filename", {
                            {"type", "boolean"},
                            {"description", "Show filename with matches"},
                            {"default", true}
                        }},
                        {"count", {
                            {"type", "boolean"},
                            {"description", "Count matches instead of showing them"},
                            {"default", false}
                        }},
                        {"files_with_matches", {
                            {"type", "boolean"},
                            {"description", "Show only filenames with matches"},
                            {"default", false}
                        }},
                        {"files_without_match", {
                            {"type", "boolean"},
                            {"description", "Show only filenames without matches"},
                            {"default", false}
                        }},
                        {"after_context", {
                            {"type", "integer"},
                            {"description", "Lines after match"},
                            {"default", 0}
                        }},
                        {"before_context", {
                            {"type", "integer"},
                            {"description", "Lines before match"},
                            {"default", 0}
                        }},
                        {"context", {
                            {"type", "integer"},
                            {"description", "Lines around match"},
                            {"default", 0}
                        }},
                        {"max_count", {
                            {"type", "integer"},
                            {"description", "Maximum matches per file"}
                        }},
                        {"color", {
                            {"type", "string"},
                            {"enum", {"always", "never", "auto"}},
                            {"description", "Color highlighting"},
                            {"default", "auto"}
                        }}
                    }},
                    {"required", {"pattern"}}
                }}
            },
            {
                {"name", "store_document"},
                {"description", "Store a document in YAMS"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"content", {
                            {"type", "string"},
                            {"description", "Document content"}
                        }},
                        {"name", {
                            {"type", "string"},
                            {"description", "Document name/filename"}
                        }},
                        {"mime_type", {
                            {"type", "string"},
                            {"description", "MIME type of the content"}
                        }},
                        {"tags", {
                            {"type", "array"},
                            {"items", {"type", "string"}},
                            {"description", "Tags for the document"}
                        }},
                        {"metadata", {
                            {"type", "object"},
                            {"description", "Additional metadata key-value pairs"}
                        }}
                    }},
                    {"required", {"content", "name"}}
                }}
            },
            {
                {"name", "retrieve_document"},
                {"description", "Retrieve a document by hash or name"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"hash", {
                            {"type", "string"},
                            {"description", "Document SHA-256 hash"}
                        }},
                        {"name", {
                            {"type", "string"},
                            {"description", "Document name"}
                        }},
                        {"graph", {
                            {"type", "boolean"},
                            {"description", "Include knowledge graph relationships"},
                            {"default", false}
                        }},
                        {"depth", {
                            {"type", "integer"},
                            {"description", "Graph traversal depth (1-5)"},
                            {"default", 1},
                            {"minimum", 1},
                            {"maximum", 5}
                        }},
                        {"include_content", {
                            {"type", "boolean"},
                            {"description", "Include full content in graph results"},
                            {"default", false}
                        }}
                    }}
                }}
            },
            {
                {"name", "delete_document"},
                {"description", "Delete a document by hash or name"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"hash", {
                            {"type", "string"},
                            {"description", "Document SHA-256 hash"}
                        }},
                        {"name", {
                            {"type", "string"},
                            {"description", "Document name"}
                        }}
                    }}
                }}
            },
            {
                {"name", "update_metadata"},
                {"description", "Update document metadata"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"hash", {
                            {"type", "string"},
                            {"description", "Document SHA-256 hash"}
                        }},
                        {"name", {
                            {"type", "string"},
                            {"description", "Document name (alternative to hash)"}
                        }},
                        {"metadata", {
                            {"type", "object"},
                            {"description", "Metadata key-value pairs to update"}
                        }},
                        {"tags", {
                            {"type", "array"},
                            {"items", {"type", "string"}},
                            {"description", "Tags to add or update"}
                        }}
                    }}
                }}
            },
            
            // List and filter operations
            {
                {"name", "list_documents"},
                {"description", "List documents with optional filtering"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"limit", {
                            {"type", "integer"},
                            {"description", "Maximum number of results"},
                            {"default", 100}
                        }},
                        {"offset", {
                            {"type", "integer"},
                            {"description", "Offset for pagination"},
                            {"default", 0}
                        }},
                        {"pattern", {
                            {"type", "string"},
                            {"description", "Glob pattern for filtering names"}
                        }},
                        {"tags", {
                            {"type", "array"},
                            {"items", {"type", "string"}},
                            {"description", "Filter by tags"}
                        }},
                        {"type", {
                            {"type", "string"},
                            {"description", "Filter by file type category"}
                        }},
                        {"mime", {
                            {"type", "string"},
                            {"description", "Filter by MIME type pattern"}
                        }},
                        {"extension", {
                            {"type", "string"},
                            {"description", "Filter by file extension"}
                        }},
                        {"binary", {
                            {"type", "boolean"},
                            {"description", "Filter binary files"}
                        }},
                        {"text", {
                            {"type", "boolean"},
                            {"description", "Filter text files"}
                        }},
                        {"created_after", {
                            {"type", "string"},
                            {"description", "ISO 8601 timestamp or relative time"}
                        }},
                        {"created_before", {
                            {"type", "string"},
                            {"description", "ISO 8601 timestamp or relative time"}
                        }},
                        {"modified_after", {
                            {"type", "string"},
                            {"description", "ISO 8601 timestamp or relative time"}
                        }},
                        {"modified_before", {
                            {"type", "string"},
                            {"description", "ISO 8601 timestamp or relative time"}
                        }},
                        {"indexed_after", {
                            {"type", "string"},
                            {"description", "ISO 8601 timestamp or relative time"}
                        }},
                        {"indexed_before", {
                            {"type", "string"},
                            {"description", "ISO 8601 timestamp or relative time"}
                        }},
                        {"recent", {
                            {"type", "integer"},
                            {"description", "Get N most recent documents"}
                        }},
                        {"sort_by", {
                            {"type", "string"},
                            {"enum", {"name", "size", "created", "modified", "indexed"}},
                            {"description", "Sort field"},
                            {"default", "indexed"}
                        }},
                        {"sort_order", {
                            {"type", "string"},
                            {"enum", {"asc", "desc"}},
                            {"description", "Sort order"},
                            {"default", "desc"}
                        }}
                    }}
                }}
            },
            
            // Statistics and maintenance
            {
                {"name", "get_stats"},
                {"description", "Get storage statistics and health status"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"file_types", {
                            {"type", "boolean"},
                            {"description", "Include file type breakdown"},
                            {"default", false}
                        }}
                    }}
                }}
            },
            
            // CLI parity tools from v0.0.2
            {
                {"name", "delete_by_name"},
                {"description", "Delete documents by name with pattern support"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"name", {
                            {"type", "string"},
                            {"description", "Document name"}
                        }},
                        {"names", {
                            {"type", "array"},
                            {"items", {"type", "string"}},
                            {"description", "Multiple document names"}
                        }},
                        {"pattern", {
                            {"type", "string"},
                            {"description", "Glob pattern for matching names"}
                        }},
                        {"dry_run", {
                            {"type", "boolean"},
                            {"description", "Preview what would be deleted"},
                            {"default", false}
                        }}
                    }}
                }}
            },
            {
                {"name", "get_by_name"},
                {"description", "Retrieve document content by name"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"name", {
                            {"type", "string"},
                            {"description", "Document name"}
                        }}
                    }},
                    {"required", {"name"}}
                }}
            },
            {
                {"name", "cat_document"},
                {"description", "Display document content (like cat command)"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"hash", {
                            {"type", "string"},
                            {"description", "Document SHA-256 hash"}
                        }},
                        {"name", {
                            {"type", "string"},
                            {"description", "Document name"}
                        }}
                    }}
                }}
            },
            
            // Directory operations from v0.0.4
            {
                {"name", "add_directory"},
                {"description", "Add all files from a directory"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"path", {
                            {"type", "string"},
                            {"description", "Directory path"}
                        }},
                        {"recursive", {
                            {"type", "boolean"},
                            {"description", "Recursively add subdirectories"},
                            {"default", false}
                        }},
                        {"collection", {
                            {"type", "string"},
                            {"description", "Collection name for grouping"}
                        }},
                        {"snapshot_id", {
                            {"type", "string"},
                            {"description", "Snapshot ID for versioning"}
                        }},
                        {"snapshot_label", {
                            {"type", "string"},
                            {"description", "Human-readable snapshot label"}
                        }},
                        {"include", {
                            {"type", "array"},
                            {"items", {"type", "string"}},
                            {"description", "Include patterns (e.g., *.txt)"}
                        }},
                        {"exclude", {
                            {"type", "array"},
                            {"items", {"type", "string"}},
                            {"description", "Exclude patterns"}
                        }}
                    }},
                    {"required", {"path"}}
                }}
            },
            {
                {"name", "restore_collection"},
                {"description", "Restore all documents from a collection"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"collection", {
                            {"type", "string"},
                            {"description", "Collection name"}
                        }},
                        {"output_dir", {
                            {"type", "string"},
                            {"description", "Output directory"}
                        }},
                        {"layout", {
                            {"type", "string"},
                            {"description", "Layout template (e.g., {collection}/{path})"},
                            {"default", "{path}"}
                        }}
                    }},
                    {"required", {"collection", "output_dir"}}
                }}
            },
            {
                {"name", "restore_snapshot"},
                {"description", "Restore all documents from a snapshot"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"snapshot_id", {
                            {"type", "string"},
                            {"description", "Snapshot ID"}
                        }},
                        {"output_dir", {
                            {"type", "string"},
                            {"description", "Output directory"}
                        }},
                        {"layout", {
                            {"type", "string"},
                            {"description", "Layout template"},
                            {"default", "{path}"}
                        }}
                    }},
                    {"required", {"snapshot_id", "output_dir"}}
                }}
            },
            {
                {"name", "list_collections"},
                {"description", "List available collections"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {}}
                }}
            },
            {
                {"name", "list_snapshots"},
                {"description", "List available snapshots"},
                {"inputSchema", {
                    {"type", "object"},
                    {"properties", {
                        {"collection", {
                            {"type", "string"},
                            {"description", "Filter by collection"}
                        }}
                    }}
                }}
            }
        })}
    };
}

json MCPServer::listPrompts() {
    return {
        {"prompts", json::array({
            {
                {"name", "search_codebase"},
                {"description", "Search for code patterns in the codebase"},
                {"arguments", json::array({
                    {
                        {"name", "pattern"},
                        {"description", "Code pattern to search for"},
                        {"required", true}
                    },
                    {
                        {"name", "file_type"},
                        {"description", "Filter by file type (e.g., cpp, py, js)"},
                        {"required", false}
                    }
                })}
            },
            {
                {"name", "summarize_document"},
                {"description", "Generate a summary of a document"},
                {"arguments", json::array({
                    {
                        {"name", "document_name"},
                        {"description", "Name of the document to summarize"},
                        {"required", true}
                    },
                    {
                        {"name", "max_length"},
                        {"description", "Maximum summary length in words"},
                        {"required", false}
                    }
                })}
            }
        })}
    };
}

json MCPServer::callTool(const std::string& name, const json& arguments) {
    // Route to appropriate tool implementation
    if (name == "search_documents") {
        return searchDocuments(arguments);
    } else if (name == "grep_documents") {
        return grepDocuments(arguments);
    } else if (name == "store_document") {
        return storeDocument(arguments);
    } else if (name == "retrieve_document") {
        return retrieveDocument(arguments);
    } else if (name == "delete_document") {
        return deleteDocument(arguments);
    } else if (name == "update_metadata") {
        return updateMetadata(arguments);
    } else if (name == "update_document_metadata") {
        return updateDocumentMetadata(arguments);
    } else if (name == "list_documents") {
        return listDocuments(arguments);
    } else if (name == "get_stats") {
        return getStats(arguments);
    } else if (name == "delete_by_name") {
        return deleteByName(arguments);
    } else if (name == "get_by_name") {
        return getByName(arguments);
    } else if (name == "cat_document") {
        return catDocument(arguments);
    } else if (name == "add_directory") {
        return addDirectory(arguments);
    } else if (name == "restore_collection") {
        return restoreCollection(arguments);
    } else if (name == "restore_snapshot") {
        return restoreSnapshot(arguments);
    } else if (name == "list_collections") {
        return listCollections(arguments);
    } else if (name == "list_snapshots") {
        return listSnapshots(arguments);
    } else {
        throw std::runtime_error("Unknown tool: " + name);
    }
}

// Tool implementations
json MCPServer::searchDocuments(const json& args) {
    try {
        std::string query = args["query"];
        size_t limit = args.value("limit", 10);
        bool fuzzy = args.value("fuzzy", false);
        float minSimilarity = args.value("similarity", 0.7f);
        std::string hashQuery = args.value("hash", "");
        bool verbose = args.value("verbose", false);
        std::string searchType = args.value("type", "hybrid");
        
        // Handle explicit hash search if --hash parameter is provided
        if (!hashQuery.empty()) {
            if (!isValidHash(hashQuery)) {
                return {{"error", "Invalid hash format. Must be 8-64 hexadecimal characters."}};
            }
            return searchByHash(hashQuery, limit);
        }
        
        // Auto-detect hash format in query for backward compatibility
        if (query.length() >= 8 && query.length() <= 64 && isValidHash(query)) {
            return searchByHash(query, limit);
        }
        
        // Prefer hybrid search by default when available; fail open to metadata search
                if ((searchType == "hybrid" || searchType.empty()) && hybridEngine_) {
                    auto hres = hybridEngine_->search(query, limit);
                    if (hres) {
                        const auto& items = hres.value();
                        json response;
                        response["total"] = items.size();
                        response["type"] = "hybrid";
                        if (verbose) {
                            response["method"] = "hybrid";
                            response["kg_enabled"] = hybridEngine_->getConfig().enable_kg;
                        }
                        json results = json::array();
                        for (const auto& r : items) {
                            json doc;
                            doc["id"] = r.id;
                            auto itTitle = r.metadata.find("title");
                            if (itTitle != r.metadata.end()) doc["title"] = itTitle->second;
                            auto itPath = r.metadata.find("path");
                            if (itPath != r.metadata.end()) doc["path"] = itPath->second;
                            doc["score"] = r.hybrid_score;
                            if (!r.content.empty()) {
                                doc["snippet"] = r.content;
                            }
                            if (verbose) {
                                json breakdown;
                                breakdown["vector_score"] = r.vector_score;
                                breakdown["keyword_score"] = r.keyword_score;
                                breakdown["kg_entity_score"] = r.kg_entity_score;
                                breakdown["structural_score"] = r.structural_score;
                                doc["score_breakdown"] = breakdown;
                            }
                            results.push_back(doc);
                        }
                        response["results"] = results;
                        return response;
                    }
                    // If hybrid failed, fall through to metadata paths
                }
        
                // Metadata repository search (fuzzy or regular) as fallback or when requested
                if (fuzzy) {
                    // Use fuzzy search
                    auto result = metadataRepo_->fuzzySearch(query, minSimilarity, static_cast<int>(limit));
                    if (!result) {
                        return {{"error", result.error().message}};
                    }
            
                    const auto& searchResults = result.value();
            
                    json response;
                    response["total"] = searchResults.totalCount;
                    response["type"] = "fuzzy";
                    response["execution_time_ms"] = searchResults.executionTimeMs;
            
                    json results = json::array();
                    for (const auto& item : searchResults.results) {
                        results.push_back({
                            {"id", item.document.id},
                            {"hash", item.document.sha256Hash},
                            {"title", item.document.fileName},
                            {"path", item.document.filePath},
                            {"score", item.score},
                            {"snippet", item.snippet}
                        });
                    }
                    response["results"] = results;
            
                    return response;
                } else {
                    // Use regular FTS search with v0.0.5 improvements
                    auto result = metadataRepo_->search(query, static_cast<int>(limit), 0);
                    if (!result) {
                        return {{"error", result.error().message}};
                    }
            
                    const auto& searchResults = result.value();
            
                    json response;
                    response["total"] = searchResults.totalCount;
                    response["type"] = "full-text";
                    response["execution_time_ms"] = searchResults.executionTimeMs;
            
                    json results = json::array();
                    for (const auto& item : searchResults.results) {
                        results.push_back({
                            {"id", item.document.id},
                            {"hash", item.document.sha256Hash},
                            {"title", item.document.fileName},
                            {"path", item.document.filePath},
                            {"score", item.score},
                            {"snippet", item.snippet}
                        });
                    }
                    response["results"] = results;
            
                    return response;
                }
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Search failed: ") + e.what()}};
    }
}

json MCPServer::storeDocument(const json& args) {
    try {
        std::string path = args["path"];
        
        api::ContentMetadata metadata;
        if (args.contains("tags")) {
            auto tagsVec = args["tags"].get<std::vector<std::string>>();
            for (const auto& tag : tagsVec) {
                metadata.tags[tag] = ""; // Convert vector to map with empty values
            }
        }
        
        auto result = store_->store(path, metadata);
        if (!result) {
            return {{"error", result.error().message}};
        }
        
        return {
            {"hash", result.value().contentHash},
            {"bytes_stored", result.value().bytesStored},
            {"bytes_deduped", result.value().bytesDeduped}
        };
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Store failed: ") + e.what()}};
    }
}

json MCPServer::retrieveDocument(const json& args) {
    try {
        std::string hash = args["hash"];
        std::string outputPath = args.value("outputPath", hash);
        
        auto result = store_->retrieve(hash, outputPath);
        if (!result) {
            return {{"error", result.error().message}};
        }
        
        return {
            {"found", result.value().found},
            {"size", result.value().size},
            {"path", outputPath}
        };
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Retrieve failed: ") + e.what()}};
    }
}

json MCPServer::getStats(const json& args) {
    try {
        auto stats = store_->getStats();
        
        return {
            {"total_objects", stats.totalObjects},
            {"total_bytes", stats.totalBytes},
            {"unique_blocks", stats.uniqueBlocks},
            {"deduplicated_bytes", stats.deduplicatedBytes},
            {"dedup_ratio", stats.dedupRatio()}
        };
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Stats failed: ") + e.what()}};
    }
}

json MCPServer::deleteByName(const json& args) {
    try {
        bool dry_run = args.value("dry_run", false);
        json result = json::object();
        result["dry_run"] = dry_run;
        
        std::vector<std::pair<std::string, std::string>> toDelete;
        
        if (args.contains("name")) {
            std::string name = args["name"];
            auto resolveResult = resolveNameToHashes(name);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            toDelete = resolveResult.value();
            
        } else if (args.contains("names")) {
            auto names = args["names"].get<std::vector<std::string>>();
            auto resolveResult = resolveNamesToHashes(names);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            toDelete = resolveResult.value();
            
        } else if (args.contains("pattern")) {
            std::string pattern = args["pattern"];
            auto resolveResult = resolvePatternToHashes(pattern);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            toDelete = resolveResult.value();
            result["pattern"] = pattern;
        }
        
        // Build result with actual matches found
        json deleted_items = json::array();
        int successful_deletions = 0;
        json errors = json::array();
        
        for (const auto& [name, hash] : toDelete) {
            if (!dry_run) {
                // Actually delete the document
                auto deleteResult = store_->remove(hash);
                if (deleteResult) {
                    successful_deletions++;
                    deleted_items.push_back({{"name", name}, {"hash", hash}});
                } else {
                    errors.push_back({{"name", name}, {"hash", hash}, {"error", deleteResult.error().message}});
                }
            } else {
                // Dry run - just record what would be deleted
                deleted_items.push_back({{"name", name}, {"hash", hash}});
            }
        }
        
        result["deleted"] = deleted_items;
        result["count"] = dry_run ? toDelete.size() : successful_deletions;
        
        if (!errors.empty()) {
            result["errors"] = errors;
        }
        
        std::string action = dry_run ? "Would delete" : "Deleted";
        result["message"] = action + " " + std::to_string(result["count"].get<int>()) + " document(s)";
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Delete failed: ") + e.what()}};
    }
}

json MCPServer::getByName(const json& args) {
    try {
        std::string name = args["name"];
        std::string output_path = args.value("output_path", "");
        
        // Resolve name to hash using existing CLI logic
        auto resolveResult = resolveNameToHash(name);
        if (!resolveResult) {
            return {{"error", resolveResult.error().message}};
        }
        
        std::string hash = resolveResult.value();
        
        // Check if document exists
        auto existsResult = store_->exists(hash);
        if (!existsResult) {
            return {{"error", existsResult.error().message}};
        }
        
        if (!existsResult.value()) {
            return {{"error", "Document not found: " + hash}};
        }
        
        json result;
        result["name"] = name;
        result["hash"] = hash;
        
        if (!output_path.empty()) {
            // Retrieve to specified file path
            auto retrieveResult = store_->retrieve(hash, output_path);
            if (!retrieveResult) {
                return {{"error", retrieveResult.error().message}};
            }
            
            result["output_path"] = output_path;
            result["size"] = retrieveResult.value().size;
            result["message"] = "Document retrieved to: " + output_path;
        } else {
            // Get content directly (similar to cat functionality)
            std::ostringstream oss;
            auto streamResult = store_->retrieveStream(hash, oss, nullptr);
            if (!streamResult) {
                return {{"error", streamResult.error().message}};
            }
            
            result["content"] = oss.str();
            result["size"] = oss.str().size();
            result["message"] = "Document content retrieved";
        }
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Get by name failed: ") + e.what()}};
    }
}

json MCPServer::catDocument(const json& args) {
    try {
        std::string hashToDisplay;
        json result = json::object();
        
        if (args.contains("hash")) {
            std::string hash = args["hash"];
            hashToDisplay = hash;
            result["hash"] = hash;
        } else if (args.contains("name")) {
            std::string name = args["name"];
            // Resolve name to hash using existing CLI logic
            auto resolveResult = resolveNameToHash(name);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            hashToDisplay = resolveResult.value();
            result["name"] = name;
            result["hash"] = hashToDisplay;
        } else {
            return {{"error", "No document specified (hash or name required)"}};
        }
        
        // Check if document exists
        auto existsResult = store_->exists(hashToDisplay);
        if (!existsResult) {
            return {{"error", existsResult.error().message}};
        }
        
        if (!existsResult.value()) {
            return {{"error", "Document not found: " + hashToDisplay}};
        }
        
        // Get content using stream interface (same as CLI cat command)
        std::ostringstream oss;
        auto streamResult = store_->retrieveStream(hashToDisplay, oss, nullptr);
        if (!streamResult) {
            return {{"error", streamResult.error().message}};
        }
        
        result["content"] = oss.str();
        result["size"] = oss.str().size();
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Cat document failed: ") + e.what()}};
    }
}

json MCPServer::listDocuments(const json& args) {
    try {
        int limit = args.value("limit", 50);
        std::string pattern = args.value("pattern", "");
        auto tags = args.value("tags", std::vector<std::string>{});
        
        if (!metadataRepo_) {
            return {{"error", "Metadata repository not available"}};
        }
        
        json result = json::object();
        result["limit"] = limit;
        
        // Use existing CLI logic - get all documents from metadata repository
        std::string searchPattern = pattern.empty() ? "%" : pattern;
        
        // Convert glob pattern to SQL LIKE pattern if needed
        if (!pattern.empty()) {
            std::replace(searchPattern.begin(), searchPattern.end(), '*', '%');
            std::replace(searchPattern.begin(), searchPattern.end(), '?', '_');
            if (searchPattern.front() != '%') {
                searchPattern = "%/" + searchPattern;
            }
        }
        
        auto documentsResult = metadataRepo_->findDocumentsByPath(searchPattern);
        if (!documentsResult) {
            return {{"error", "Failed to query documents: " + documentsResult.error().message}};
        }
        
        json documents = json::array();
        
        // Process each document (similar to CLI list command)
        for (const auto& docInfo : documentsResult.value()) {
            if (documents.size() >= static_cast<size_t>(limit)) {
                break;
            }
            
            json doc;
            doc["name"] = docInfo.fileName;
            doc["hash"] = docInfo.sha256Hash;
            doc["path"] = docInfo.filePath;
            doc["extension"] = docInfo.fileExtension;
            doc["size"] = docInfo.fileSize;
            doc["mime_type"] = docInfo.mimeType;
            doc["created"] = std::chrono::duration_cast<std::chrono::seconds>(docInfo.createdTime.time_since_epoch()).count();
            doc["modified"] = std::chrono::duration_cast<std::chrono::seconds>(docInfo.modifiedTime.time_since_epoch()).count();
            doc["indexed"] = std::chrono::duration_cast<std::chrono::seconds>(docInfo.indexedTime.time_since_epoch()).count();
            
            // Get metadata including tags if requested
            if (!tags.empty()) {
                auto metadataResult = metadataRepo_->getAllMetadata(docInfo.id);
                if (metadataResult) {
                    const auto& metadata = metadataResult.value();
                    json docTags = json::array();
                    
                    // Extract tags from metadata
                    for (const auto& [key, value] : metadata) {
                        if (key == "tag" || key.starts_with("tag:")) {
                            docTags.push_back(value.value.empty() ? key : value.value);
                        }
                    }
                    
                    // Filter by tags if specified
                    bool hasMatchingTag = tags.empty();
                    if (!tags.empty()) {
                        for (const auto& requiredTag : tags) {
                            for (const auto& docTag : docTags) {
                                if (docTag.get<std::string>() == requiredTag) {
                                    hasMatchingTag = true;
                                    break;
                                }
                            }
                            if (hasMatchingTag) break;
                        }
                    }
                    
                    if (!hasMatchingTag) {
                        continue; // Skip this document
                    }
                    
                    doc["tags"] = docTags;
                }
            }
            
            documents.push_back(doc);
        }
        
        result["documents"] = documents;
        result["count"] = documents.size();
        result["total_found"] = documentsResult.value().size();
        
        if (!pattern.empty()) {
            result["pattern"] = pattern;
        }
        
        if (!tags.empty()) {
            result["filtered_by_tags"] = tags;
        }
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("List documents failed: ") + e.what()}};
    }
}

json MCPServer::createResponse(const json& id, const json& result) {
    json response;
    response["jsonrpc"] = "2.0";
    if (!id.is_null()) {
        response["id"] = id;
    }
    response["result"] = result;
    return response;
}

json MCPServer::createError(const json& id, int code, const std::string& message) {
    json response;
    response["jsonrpc"] = "2.0";
    if (!id.is_null()) {
        response["id"] = id;
    }
    response["error"] = {
        {"code", code},
        {"message", message}
    };
    return response;
}

// Name resolution helper methods (similar to CLI commands)
Result<std::string> MCPServer::resolveNameToHash(const std::string& name) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
    }
    
    // Search for documents with matching fileName
    auto documentsResult = metadataRepo_->findDocumentsByPath("%/" + name);
    if (!documentsResult) {
        // Try exact match
        documentsResult = metadataRepo_->findDocumentsByPath(name);
        if (!documentsResult) {
            return Error{ErrorCode::NotFound, "Document not found: " + name};
        }
    }
    
    const auto& documents = documentsResult.value();
    if (documents.empty()) {
        return Error{ErrorCode::NotFound, "Document not found: " + name};
    }
    
    if (documents.size() > 1) {
        return Error{ErrorCode::InvalidOperation, "Ambiguous name: multiple documents match '" + name + "'"};
    }
    
    return documents[0].sha256Hash;
}

Result<std::vector<std::pair<std::string, std::string>>> MCPServer::resolveNameToHashes(const std::string& name) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }
    
    // Search for documents with matching fileName
    auto documentsResult = metadataRepo_->findDocumentsByPath("%/" + name);
    if (!documentsResult) {
        // Try exact match
        documentsResult = metadataRepo_->findDocumentsByPath(name);
        if (!documentsResult) {
            return Error{ErrorCode::NotFound, "Failed to query documents: " + documentsResult.error().message};
        }
    }
    
    const auto& documents = documentsResult.value();
    if (documents.empty()) {
        return Error{ErrorCode::NotFound, "No documents found with name: " + name};
    }
    
    std::vector<std::pair<std::string, std::string>> results;
    for (const auto& doc : documents) {
        results.emplace_back(doc.fileName, doc.sha256Hash);
    }
    
    return results;
}

Result<std::vector<std::pair<std::string, std::string>>> MCPServer::resolveNamesToHashes(const std::vector<std::string>& names) {
    std::vector<std::pair<std::string, std::string>> allResults;
    
    // Resolve each name
    for (const auto& name : names) {
        auto result = resolveNameToHashes(name);
        if (result) {
            for (const auto& pair : result.value()) {
                allResults.push_back(pair);
            }
        } else if (result.error().code != ErrorCode::NotFound) {
            // Return early on non-NotFound errors
            return result;
        }
        // Skip NotFound errors for individual names
    }
    
    if (allResults.empty()) {
        return Error{ErrorCode::NotFound, "No documents found for the specified names"};
    }
    
    return allResults;
}

Result<std::vector<std::pair<std::string, std::string>>> MCPServer::resolvePatternToHashes(const std::string& pattern) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }
    
    // Convert glob pattern to SQL LIKE pattern
    std::string sqlPattern = pattern;
    // Replace glob wildcards with SQL wildcards
    std::replace(sqlPattern.begin(), sqlPattern.end(), '*', '%');
    std::replace(sqlPattern.begin(), sqlPattern.end(), '?', '_');
    
    // Ensure it matches the end of the path (filename)
    if (sqlPattern.front() != '%') {
        sqlPattern = "%/" + sqlPattern;
    }
    
    auto documentsResult = metadataRepo_->findDocumentsByPath(sqlPattern);
    if (!documentsResult) {
        return Error{ErrorCode::NotFound, "Failed to query documents: " + documentsResult.error().message};
    }
    
    std::vector<std::pair<std::string, std::string>> results;
    for (const auto& doc : documentsResult.value()) {
        results.emplace_back(doc.fileName, doc.sha256Hash);
    }
    
    return results;
}

json MCPServer::addDirectory(const json& args) {
    try {
        std::string directoryPath = args["directory_path"];
        std::string collection = args.value("collection", "");
        std::string snapshotId = args.value("snapshot_id", "");
        std::string snapshotLabel = args.value("snapshot_label", "");
        bool recursive = args.value("recursive", true);
        
        if (!std::filesystem::exists(directoryPath) || !std::filesystem::is_directory(directoryPath)) {
            return {{"error", "Directory does not exist or is not a directory: " + directoryPath}};
        }
        
        if (!recursive) {
            return {{"error", "Non-recursive directory addition not supported via MCP"}};
        }
        
        // Generate snapshot ID if only label provided
        if (snapshotId.empty() && !snapshotLabel.empty()) {
            auto now = std::chrono::system_clock::now();
            auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            snapshotId = "snapshot_" + std::to_string(timestamp);
        }
        
        std::vector<std::filesystem::path> filesToAdd;
        
        // Collect files to add
        try {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(directoryPath)) {
                if (!entry.is_regular_file()) continue;
                
                // TODO: Add include/exclude pattern filtering if needed
                filesToAdd.push_back(entry.path());
            }
        } catch (const std::filesystem::filesystem_error& e) {
            return {{"error", "Failed to traverse directory: " + std::string(e.what())}};
        }
        
        if (filesToAdd.empty()) {
            return {{"error", "No files found in directory"}};
        }
        
        // Process each file
        size_t successCount = 0;
        size_t failureCount = 0;
        json results = json::array();
        
        for (const auto& filePath : filesToAdd) {
            try {
                // Build metadata with collection/snapshot info
                api::ContentMetadata metadata;
                metadata.name = filePath.filename().string();
                
                // Add collection and snapshot metadata
                if (!collection.empty()) {
                    metadata.tags["collection"] = collection;
                }
                if (!snapshotId.empty()) {
                    metadata.tags["snapshot_id"] = snapshotId;
                }
                if (!snapshotLabel.empty()) {
                    metadata.tags["snapshot_label"] = snapshotLabel;
                }
                
                // Add relative path metadata
                auto relativePath = std::filesystem::relative(filePath, directoryPath);
                metadata.tags["path"] = relativePath.string();
                
                // Additional tags from arguments
                if (args.contains("tags")) {
                    for (const auto& tag : args["tags"]) {
                        if (tag.is_string()) {
                            metadata.tags[tag] = "";
                        }
                    }
                }
                
                // Additional metadata from arguments
                if (args.contains("metadata")) {
                    for (const auto& [key, value] : args["metadata"].items()) {
                        if (value.is_string()) {
                            metadata.tags[key] = value;
                        }
                    }
                }
                
                // Store the file
                auto result = store_->store(filePath, metadata);
                if (!result) {
                    failureCount++;
                    results.push_back({
                        {"path", filePath.string()},
                        {"status", "failed"},
                        {"error", result.error().message}
                    });
                    continue;
                }
                
                // Store metadata in database
                auto metadataRepo = metadataRepo_;
                if (metadataRepo) {
                    metadata::DocumentInfo docInfo;
                    docInfo.filePath = filePath.string();
                    docInfo.fileName = filePath.filename().string();
                    docInfo.fileExtension = filePath.extension().string();
                    docInfo.fileSize = std::filesystem::file_size(filePath);
                    docInfo.sha256Hash = result.value().contentHash;
                    docInfo.mimeType = "application/octet-stream";
                    
                    auto now = std::chrono::system_clock::now();
                    docInfo.createdTime = now;
                    // Convert filesystem time to system_clock time
                    auto fsTime = std::filesystem::last_write_time(filePath);
                    auto systemTime = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                        fsTime - std::filesystem::file_time_type::clock::now() + std::chrono::system_clock::now()
                    );
                    docInfo.modifiedTime = systemTime;
                    docInfo.indexedTime = now;
                    
                    auto insertResult = metadataRepo->insertDocument(docInfo);
                    if (insertResult) {
                        int64_t docId = insertResult.value();
                        
                        // Add all metadata
                        for (const auto& [key, value] : metadata.tags) {
                            metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
                        }
                    }
                }
                
                successCount++;
                results.push_back({
                    {"path", filePath.string()},
                    {"status", "success"},
                    {"hash", result.value().contentHash},
                    {"bytes_stored", result.value().bytesStored}
                });
                
            } catch (const std::exception& e) {
                failureCount++;
                results.push_back({
                    {"path", filePath.string()},
                    {"status", "failed"},
                    {"error", std::string("Exception: ") + e.what()}
                });
            }
        }
        
        return {
            {"summary", {
                {"files_processed", filesToAdd.size()},
                {"files_added", successCount},
                {"files_failed", failureCount}
            }},
            {"collection", collection},
            {"snapshot_id", snapshotId},
            {"snapshot_label", snapshotLabel},
            {"results", results}
        };
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in addDirectory: ") + e.what()}};
    }
}

json MCPServer::restoreCollection(const json& args) {
    try {
        std::string collection = args["collection"];
        std::string outputDir = args.value("output_directory", ".");
        std::string layoutTemplate = args.value("layout_template", "{path}");
        bool overwrite = args.value("overwrite", false);
        bool createDirs = args.value("create_dirs", true);
        bool dryRun = args.value("dry_run", false);
        
        // Get documents from collection
        auto documentsResult = metadataRepo_->findDocumentsByCollection(collection);
        if (!documentsResult) {
            return {{"error", "Failed to find documents in collection: " + documentsResult.error().message}};
        }
        
        const auto& documents = documentsResult.value();
        
        if (documents.empty()) {
            return {{"message", "No documents found in collection: " + collection}};
        }
        
        return performRestore(documents, outputDir, layoutTemplate, overwrite, createDirs, dryRun, "collection: " + collection);
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in restoreCollection: ") + e.what()}};
    }
}

json MCPServer::restoreSnapshot(const json& args) {
    try {
        std::string snapshotId = args.value("snapshot_id", "");
        std::string snapshotLabel = args.value("snapshot_label", "");
        std::string outputDir = args.value("output_directory", ".");
        std::string layoutTemplate = args.value("layout_template", "{path}");
        bool overwrite = args.value("overwrite", false);
        bool createDirs = args.value("create_dirs", true);
        bool dryRun = args.value("dry_run", false);
        
        // Get documents from snapshot
        Result<std::vector<metadata::DocumentInfo>> documentsResult = std::vector<metadata::DocumentInfo>();
        std::string scope;
        
        if (!snapshotId.empty()) {
            documentsResult = metadataRepo_->findDocumentsBySnapshot(snapshotId);
            scope = "snapshot ID: " + snapshotId;
        } else if (!snapshotLabel.empty()) {
            documentsResult = metadataRepo_->findDocumentsBySnapshotLabel(snapshotLabel);
            scope = "snapshot label: " + snapshotLabel;
        } else {
            return {{"error", "Either snapshot_id or snapshot_label must be provided"}};
        }
        
        if (!documentsResult) {
            return {{"error", "Failed to find documents in snapshot: " + documentsResult.error().message}};
        }
        
        const auto& documents = documentsResult.value();
        
        if (documents.empty()) {
            return {{"message", "No documents found in snapshot"}};
        }
        
        return performRestore(documents, outputDir, layoutTemplate, overwrite, createDirs, dryRun, scope);
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in restoreSnapshot: ") + e.what()}};
    }
}

json MCPServer::listCollections(const json& args) {
    try {
        auto collectionsResult = metadataRepo_->getCollections();
        if (!collectionsResult) {
            return {{"error", "Failed to get collections: " + collectionsResult.error().message}};
        }
        
        return {{"collections", collectionsResult.value()}};
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in listCollections: ") + e.what()}};
    }
}

json MCPServer::listSnapshots(const json& args) {
    try {
        bool withLabels = args.value("with_labels", true);
        
        auto snapshotsResult = metadataRepo_->getSnapshots();
        if (!snapshotsResult) {
            return {{"error", "Failed to get snapshots: " + snapshotsResult.error().message}};
        }
        
        json response;
        response["snapshot_ids"] = snapshotsResult.value();
        
        if (withLabels) {
            auto labelsResult = metadataRepo_->getSnapshotLabels();
            if (labelsResult) {
                response["snapshot_labels"] = labelsResult.value();
            }
        }
        
        return response;
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in listSnapshots: ") + e.what()}};
    }
}

json MCPServer::performRestore(const std::vector<metadata::DocumentInfo>& documents,
                               const std::string& outputDir,
                               const std::string& layoutTemplate,
                               bool overwrite,
                               bool createDirs,
                               bool dryRun,
                               const std::string& scope) {
    
    size_t successCount = 0;
    size_t failureCount = 0;
    size_t skippedCount = 0;
    json results = json::array();
    
    for (const auto& doc : documents) {
        try {
            // Get metadata for layout expansion
            auto metadataResult = metadataRepo_->getAllMetadata(doc.id);
            if (!metadataResult) {
                failureCount++;
                results.push_back({
                    {"document", doc.fileName},
                    {"status", "failed"},
                    {"error", "Failed to get metadata: " + metadataResult.error().message}
                });
                continue;
            }
            
            // Expand layout template
            std::string layoutPath = expandLayoutTemplate(layoutTemplate, doc, metadataResult.value());
            
            std::filesystem::path outputPath = std::filesystem::path(outputDir) / layoutPath;
            
            if (dryRun) {
                results.push_back({
                    {"document", doc.fileName},
                    {"status", "would_restore"},
                    {"output_path", outputPath.string()},
                    {"size", doc.fileSize}
                });
                successCount++;
                continue;
            }
            
            // Check if file already exists
            if (std::filesystem::exists(outputPath) && !overwrite) {
                skippedCount++;
                results.push_back({
                    {"document", doc.fileName},
                    {"status", "skipped"},
                    {"reason", "File exists and overwrite=false"}
                });
                continue;
            }
            
            // Create parent directories if needed
            if (createDirs) {
                std::filesystem::create_directories(outputPath.parent_path());
            }
            
            // Retrieve document
            auto retrieveResult = store_->retrieve(doc.sha256Hash, outputPath);
            if (!retrieveResult) {
                failureCount++;
                results.push_back({
                    {"document", doc.fileName},
                    {"status", "failed"},
                    {"error", "Failed to retrieve: " + retrieveResult.error().message}
                });
                continue;
            }
            
            successCount++;
            results.push_back({
                {"document", doc.fileName},
                {"status", "restored"},
                {"output_path", outputPath.string()},
                {"size", doc.fileSize}
            });
            
        } catch (const std::exception& e) {
            failureCount++;
            results.push_back({
                {"document", doc.fileName},
                {"status", "failed"},
                {"error", std::string("Exception: ") + e.what()}
            });
        }
    }
    
    return {
        {"summary", {
            {"documents_found", documents.size()},
            {"documents_restored", successCount},
            {"documents_failed", failureCount},
            {"documents_skipped", skippedCount},
            {"dry_run", dryRun}
        }},
        {"scope", scope},
        {"output_directory", outputDir},
        {"layout_template", layoutTemplate},
        {"results", results}
    };
}

std::string MCPServer::expandLayoutTemplate(const std::string& layoutTemplate,
                                           const metadata::DocumentInfo& doc,
                                           const std::unordered_map<std::string, metadata::MetadataValue>& metadata) {
    std::string result = layoutTemplate;
    
    // Replace placeholders
    size_t pos = 0;
    while ((pos = result.find("{", pos)) != std::string::npos) {
        size_t endPos = result.find("}", pos);
        if (endPos == std::string::npos) break;
        
        std::string placeholder = result.substr(pos + 1, endPos - pos - 1);
        std::string replacement;
        
        if (placeholder == "collection") {
            auto it = metadata.find("collection");
            replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
        } else if (placeholder == "snapshot_id") {
            auto it = metadata.find("snapshot_id");
            replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
        } else if (placeholder == "snapshot_label") {
            auto it = metadata.find("snapshot_label");
            replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
        } else if (placeholder == "path") {
            auto it = metadata.find("path");
            replacement = (it != metadata.end()) ? it->second.asString() : doc.fileName;
        } else if (placeholder == "name") {
            replacement = doc.fileName;
        } else if (placeholder == "hash") {
            replacement = doc.sha256Hash.substr(0, 12);
        } else {
            // Unknown placeholder, leave as is
            pos = endPos + 1;
            continue;
        }
        
        result.replace(pos, endPos - pos + 1, replacement);
        pos += replacement.length();
    }
    
    return result;
}

// Helper method implementations
bool MCPServer::isValidHash(const std::string& str) {
    if (str.length() != 64) return false;
    for (char c : str) {
        if (!std::isxdigit(c)) return false;
    }
    return true;
}

json MCPServer::searchByHash(const std::string& hash, size_t limit) {
    // Search for documents by hash prefix - use path pattern as workaround
    auto docsResult = metadataRepo_->findDocumentsByPath("%");
    if (!docsResult) {
        return {{"error", "Failed to search by hash"}};
    }
    
    json results = json::array();
    size_t count = 0;
    for (const auto& doc : docsResult.value()) {
        // Filter by hash prefix
        if (doc.sha256Hash.substr(0, hash.length()) != hash) continue;
        if (count >= limit) break;
        results.push_back({
            {"hash", doc.sha256Hash},
            {"name", doc.fileName},
            {"path", doc.filePath},
            {"size", doc.fileSize}
        });
        count++;
    }
    
    return results;
}

json MCPServer::grepDocuments(const json& args) {
    std::string pattern = args.value("pattern", "");
    if (pattern.empty()) {
        return {{"error", "Pattern is required"}};
    }
    
    // TODO: Implement actual grep functionality
    return {
        {"matches", json::array()},
        {"count", 0},
        {"pattern", pattern}
    };
}

json MCPServer::deleteDocument(const json& args) {
    std::string hash = args.value("hash", "");
    if (hash.empty()) {
        return {{"error", "Hash is required"}};
    }
    
    auto result = store_->remove(hash);
    if (!result) {
        return {{"error", result.error().message}};
    }
    
    if (!result.value()) {
        return {{"error", "Document not found"}};
    }
    
    return {{"success", true}, {"hash", hash}};
}

json MCPServer::updateMetadata(const json& args) {
    // Deprecated - use updateDocumentMetadata instead
    return updateDocumentMetadata(args);
}

json MCPServer::updateDocumentMetadata(const json& args) {
    std::string hash = args.value("hash", "");
    if (hash.empty()) {
        return {{"error", "Hash is required"}};
    }
    
    auto metadata = args.value("metadata", json::object());
    if (metadata.empty()) {
        return {{"error", "Metadata is required"}};
    }
    
    // Get document by hash - use path search as workaround
    auto docsResult = metadataRepo_->findDocumentsByPath("%");
    if (!docsResult) {
        return {{"error", "Failed to search documents"}};
    }
    
    // Find the document with matching hash
    std::optional<metadata::DocumentInfo> targetDoc;
    for (const auto& doc : docsResult.value()) {
        if (doc.sha256Hash == hash) {
            targetDoc = doc;
            break;
        }
    }
    
    if (!targetDoc) {
        return {{"error", "Document not found"}};
    }
    
    auto doc = targetDoc.value();
    
    // Update metadata fields using setMetadata
    int updateCount = 0;
    for (auto& [key, value] : metadata.items()) {
        // Convert JSON value to MetadataValue
        metadata::MetadataValue metaValue("");
        if (value.is_string()) {
            metaValue = metadata::MetadataValue(value.get<std::string>());
        } else if (value.is_number_integer()) {
            metaValue = metadata::MetadataValue(std::to_string(value.get<int64_t>()));
        } else if (value.is_number_float()) {
            metaValue = metadata::MetadataValue(std::to_string(value.get<double>()));
        } else if (value.is_boolean()) {
            metaValue = metadata::MetadataValue(value.get<bool>() ? "true" : "false");
        }
        
        auto updateResult = metadataRepo_->setMetadata(doc.id, key, metaValue);
        if (!updateResult) {
            return {{"error", "Failed to update metadata: " + key}};
        }
        updateCount++;
    }
    
    return {
        {"success", true},
        {"hash", hash},
        {"updated_fields", updateCount}
    };
}

} // namespace yams::mcp

