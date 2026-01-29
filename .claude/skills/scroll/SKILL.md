---
name: scroll
description: >
  ALWAYS use when: building MCP servers, managing prompts/libraries, searching for skills,
  working with DAOs/governance, GitHub operations, or using Sage Protocol tools. Provides
  auto-provisioned MCP servers (memory, context7, brave/duckduckgo, github), skill discovery,
  and prompt management. Invoke with /scroll.
tags:
  - mcp
  - prompts
  - skills
  - governance
  - sage-protocol
  - github
---

# Scroll - Sage Protocol CLI

## Proactive Skill Discovery

**IMPORTANT**: At the start of complex tasks, use these tools to find relevant skills:

```
search_skills(query: "rust development")     # Find skills by keyword
builder_recommend(intent: "build an API")    # AI-powered suggestions
list_skills()                                # See all installed skills
```

If a relevant skill is found, use `get_skill(key)` to load its full content into context.

## For Subagents (Task Tool)

When spawned as a subagent via the Task tool, you have full access to all Scroll MCP tools.
Before starting work:

1. **Search for existing solutions**: `search_prompts(query: "your task description")`
2. **Get AI recommendations**: `builder_recommend(intent: "what you're trying to achieve")`
3. **Start needed servers**: `hub_start_server(server_id: "memory")` if you need persistent knowledge

The MCP tools work identically in subagent sessions - use them proactively to find relevant
prompts, skills, and knowledge before implementing from scratch.

## Auto-Provisioned Tools

These tools auto-start their MCP server when called - no manual setup needed:

### Documentation (context7)
| Tool | Description |
|------|-------------|
| `resolve_library_id` | Find library documentation IDs |
| `get_library_docs` | Fetch library docs by ID |

### Knowledge Graph (memory)
| Tool | Description |
|------|-------------|
| `memory_create_entities` | Store entities in knowledge graph |
| `memory_search_nodes` | Search knowledge graph |
| `memory_open_nodes` | Retrieve entities by name |
| `memory_read_graph` | Read entire knowledge graph |
| `memory_add_observations` | Add facts to existing entities |
| `memory_create_relations` | Link entities with relationships |
| `memory_delete_entities` | Remove entities |
| `memory_delete_relations` | Remove relationships |
| `memory_delete_observations` | Remove facts from entities |

### Web Search (brave/duckduckgo)
| Tool | Description |
|------|-------------|
| `brave_web_search` | **Preferred** - Web search (requires BRAVE_API_KEY) |
| `brave_news_search` | News search via Brave |
| `brave_local_search` | Local business search |
| `brave_video_search` | Video search via Brave |
| `brave_summarizer_search` | AI summary of search results |
| `web_search` | Fallback - DuckDuckGo (free, rate-limited) |

### GitHub (github)
| Tool | Description |
|------|-------------|
| `github_list_issues` | List issues in a repository |
| `github_get_issue` | Get issue details with comments |
| `github_create_issue` | Create a new issue |
| `github_search_issues` | Search issues and PRs across GitHub |
| `github_search_code` | Search code across repositories |
| `github_get_file_contents` | Read file from repository |
| `github_create_pull_request` | Create a pull request |

**Note**: GitHub tools require GITHUB_TOKEN environment variable.

## Internal Tools (always available)

### Prompts & Libraries
| Tool | Description |
|------|-------------|
| `list_libraries` | List local and on-chain libraries |
| `list_prompts` | Browse prompts (source: local, library, synced) |
| `search_prompts` | Hybrid keyword + semantic search |
| `get_prompt` | Get full prompt content by key |
| `quick_create_prompt` | Create/update a prompt |
| `trending_prompts` | Discover popular prompts |

### Skills
| Tool | Description |
|------|-------------|
| `list_skills` | List installed skills from all sources |
| `search_skills` | Search skills across Sage Protocol and GitHub |
| `get_skill` | Get full skill details and SKILL.md content |
| `use_skill` | Activate a skill (auto-provisions MCP servers) |
| `sync_skills` | Scan local/project directories for skills |

### Builder
| Tool | Description |
|------|-------------|
| `builder_recommend` | Get prompt suggestions for your intent |
| `builder_vote` | Rate recommendations (improves future suggestions) |
| `builder_synthesize` | Merge 2-5 prompts into one optimized prompt |

### Governance
| Tool | Description |
|------|-------------|
| `list_subdaos` | List all DAOs |
| `list_proposals` | View governance proposals |
| `get_voting_power` | Check voting power for an account |

### Context & Discovery
| Tool | Description |
|------|-------------|
| `get_project_context` | Project state, wallet, libraries |
| `suggest_sage_tools` | Tool recommendations for a goal |

### Hub Management
| Tool | Description |
|------|-------------|
| `hub_list_servers` | List all available servers |
| `hub_start_server` | Start a server manually |
| `hub_stop_server` | Stop a running server |
| `hub_status` | Show running servers and tools |

## External Servers

Available servers (auto-start via tool aliases, or manual with `hub_start_server`):

| Server | Description | Requires |
|--------|-------------|----------|
| `memory` | Persistent knowledge graph | - |
| `context7` | Library documentation | - |
| `brave-search` | Web/news/local/video search | BRAVE_API_KEY |
| `duckduckgo` | Web search fallback | - |
| `github` | GitHub API | GITHUB_TOKEN |
| `filesystem` | Local file access | - |
| `puppeteer` | Browser automation | - |
| `playwright` | Cross-browser automation | - |

## CLI Quick Reference

```bash
scroll init                              # Setup for Claude/Cursor/Codex
scroll library list                      # List libraries
scroll library sync                      # Sync on-chain libraries
scroll skill suggest "build API"         # Find relevant skills
scroll mcp hub list                      # List available MCP servers
scroll mcp hub start <server>            # Start a server
scroll daemon start                      # Start background daemon
scroll daemon status                     # Check daemon status
```

## Skill Sources

Skills can be installed from:
- **Sage Protocol** - On-chain skills from subdaos
- **GitHub** - Public repos (e.g., `github:user/repo`)
- **Local** - `~/.config/scroll/skills/` or project `.claude/skills/`

## Publishing (sage CLI)

Scroll handles **reading**. For **writing**, use sage CLI:

```bash
sage library quickstart --name "My Library" --governance personal
sage prompts publish --subdao my-library --yes
```
