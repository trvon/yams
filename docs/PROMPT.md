Developer: # Project Policy: Integrated Knowledge Management with YAMS

This document defines a unified, authoritative, machine-readable policy for AI coding agents and human collaborators. It ensures all development activities are governed by explicit, unambiguous workflows and rules, with a comprehensive approach to knowledge management utilizing YAMS (Yet Another Memory System) as the system of record for persistent storage, retrieval, and traceability.

---

## Usage Guidance (for AI Agents)
Begin with a concise checklist (3-7 bullets) summarizing planned actions before executing any multi-step workflow. After each YAMS or external tool call, validate the result in 1-2 lines and proceed or self-correct if validation fails. Use only tools explicitly allowed by the system and default to plain text for outputs unless markdown is explicitly required.

---

## 1. Introduction and Core Components
### 1.1 Key Actors
- **User**: Defines requirements, prioritizes work, approves changes, and is responsible for all code modifications.
- **AI_Agent**: Executes User’s instructions as specified in tasks and PBIs, using YAMS for all knowledge persistence operations.

### 1.2 Knowledge Management System – YAMS
#### What is YAMS?
- YAMS is a bash command-line tool (CLI), working similarly to `grep`, `find`, or `git` for handling knowledge artifacts.
- Core features include content-addressable storage, full-text fuzzy search, tagging, metadata support, and efficient knowledge retrieval.
- YAMS is installed on the system and commands should be run directly in the terminal like any standard shell utility.

#### Mandatory YAMS Usage
- **Before every external or web search:**
  ```bash
  yams search "<query>" --limit 20
  yams search "<query>" --fuzzy --similarity 0.7
  # Web search only if YAMS yields no relevant results
  ```
- **After every web search or external result:**
  ```bash
  echo "$WEB_SEARCH_RESULT" | yams add - --name "topic-$(date +%Y%m%d)" --tags "web,cache,topic" --metadata "url=$SOURCE_URL"
  ```
- **Use YAMS in place of grep/find/cat/cp (storage/search/retrieval):**
  ```bash
  yams list --recent 20
  yams add myfile.txt --tags "code,working"
  yams get <hash>
  ```

#### YAMS Command Reference – Sample Workflows
- Always search YAMS first before external queries
- After obtaining new information, immediately store and tag in YAMS
- Retrieve or reference content by hash in documentation/workflows

---

## 2. Fundamental Principles
### 2.1 Core Development Policies
1. **Knowledge-Driven Development:** Consult YAMS before external research. Build up the project’s knowledge base incrementally.
2. **Task-Driven Changes:** Only perform code changes associated with explicitly approved tasks.
3. **Direct Task–PBI Mapping:** Each task must be directly linked to an agreed PBI.
4. **PRD Alignment:** PBI features must conform to the PRD (when applicable).
5. **User-Exclusive Authority and Responsibility:** Only the User can approve changes; all risks remain with the User.
6. **No Unapproved Changes:** Implement only explicitly authorized changes as per documented tasks.
7. **Task File and Index Sync:** Ensure task status is synchronized in both the task file and project index.
8. **Controlled File Creation:** Never create files outside agreed locations without User approval.
9. **External Package Research:**
   - Search YAMS for package info before any web research.
   - Immediately cache all external results in YAMS.
   - Store documentation in designated markdown files and tag accordingly.
10. **Task Granularity:** Define small, cohesive, testable tasks.
11. **DRY Principle:** Single authoritative locations for knowledge and referencing.
12. **Consistent Use of Constants:** Define repeated values as named constants.
13. **API Documentation:** Maintain and persist API references in both the project and YAMS.

---

## 3. Product Backlog Item (PBI) Management
- Before any new PBI work, search YAMS for prior related PBIs/decisions. Store all findings and research for traceability.
- All PBI lifecycle updates (creation, change, closure) must include YAMS integration steps and include explicit tagging/metadata.

---

## 4. Task Management and Knowledge Persistence
### 4.1 Documentation Standard
- Each task requires a dedicated markdown file with proper naming, status tracking, links, and documentation.
- All research, context, and implementation data must be stored in YAMS using structured tags and relevant metadata.

#### Example Workflow for Task Status and Knowledge Operations
- Pre-task: Search YAMS for prior related tasks and implementations
- During task: Store all context, progress, and significant decisions in YAMS
- On status change: Update both task file and index; store a YAMS entry for the transition
- All task-related events (approval, start, review, done, rejection, block) require corresponding YAMS operations for auditing and discovery

#### One-In-Progress Limit
- Only one task per PBI may be `InProgress` at a time. Search YAMS for status before proceeding.

---

## 5. Test Strategy Integration
- All test approaches and results must be researched via YAMS first, with new strategies and findings recorded back for future reuse.

---

## 6. YAMS Command Reference: Quick Access
- Search before every external action
- Store after every external action or significant change
- Reference YAMS hashes/scripts in code, docs, and reviews

---

## 7. Workflow Integration Summary
- **START:** Query YAMS for existing knowledge
- **RESEARCH:** Use the web only if YAMS is lacking
- **CACHE:** Add all findings and context to YAMS
- **DOCUMENT:** Always reference YAMS artifacts/hashes
- **PERSIST:** Maintain all decisions, code, and changes via YAMS entries

This policy creates a durable, queryable, and self-maintaining project knowledge base: every action, research, and result remains accessible and reusable for all stakeholders.
