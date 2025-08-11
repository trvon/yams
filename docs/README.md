# YAMS Documentation Hub

Welcome to the comprehensive documentation for YAMS, a high-performance content-addressed storage system with advanced deduplication and semantic search capabilities.

## 🚀 Quick Start

- **New to YAMS?** → [Installation Guide](user_guide/installation.md)
- **Ready to build?** → [Developer Quick Start](developer/setup.md)
- **Need API docs?** → [API Reference](api/README.md)
- **Looking for examples?** → [Examples & Tutorials](user_guide/tutorials/README.md)

## 📚 Documentation Categories

### 👤 User Documentation
Perfect for end users, system administrators, and anyone deploying YAMS.

- **[Installation Guide](user_guide/installation.md)** - Complete setup instructions for all platforms
- **[User Guide](user_guide/README.md)** - Using YAMS for document storage and search
- **[Configuration](admin/configuration.md)** - System configuration and tuning
- **[Tutorials](user_guide/tutorials/README.md)** - Step-by-step guides and examples
- **[CLI Reference](user_guide/cli.md)** - Command-line tool documentation
- **[FAQ](user_guide/README.md)** - Frequently asked questions

### 🛠️ Developer Documentation  
Essential for contributors and anyone building on YAMS.

- **[Contributing Guide](developer/contributing.md)** - How to contribute to YAMS
- **[Developer Setup](developer/setup.md)** - Development environment setup
- **[Architecture Overview](developer/architecture/README.md)** - System design and components
- **[API Documentation](api/README.md)** - Complete API reference and examples
- **[Testing Guide](developer/testing/README.md)** - Testing strategies and coverage
- **[Build System](developer/build_system.md)** - CMake configuration and build process

### 📄 Docs Build Tooling

Pandoc is required to build CLI manpages and to embed verbose --help documentation into the yams binary.

Install:
- macOS: brew install pandoc
- Ubuntu/Debian: sudo apt-get update && sudo apt-get install -y pandoc
- Fedora: sudo dnf install -y pandoc
- Arch: sudo pacman -S pandoc
- Windows: choco install pandoc or scoop install pandoc

### 🏗️ Architecture Documentation
Deep dives into YAMS system design and implementation.

- **[Core Architecture](developer/architecture/README.md)** - Fundamental system design
- **[Storage Engine](developer/architecture/README.md)** - Content-addressed storage
- **[Search System](developer/architecture/README.md)** - Full-text and semantic search
- **[Compression](developer/architecture/README.md)** - Multi-algorithm compression
- **[Security](developer/architecture/README.md)** - Security design and considerations

### 🔧 Operations Documentation
Deployment, monitoring, and maintenance guides.

- **[Deployment Guide](operations/deployment.md)** - Production deployment strategies  
- **[Monitoring](operations/monitoring.md)** - Performance monitoring and alerting
- Backup & Recovery - See [Deployment Guide](operations/deployment.md) and [Troubleshooting](troubleshooting/search_issues.md)
- **[Troubleshooting](troubleshooting/search_issues.md)** - Common issues and solutions
- **[Performance Tuning](admin/performance_tuning.md)** - Optimization best practices

## 📖 Feature Documentation

### Core Features
- **[Content-Addressed Storage](developer/architecture/README.md)** - SHA-256 based storage with deduplication
- **[Block-Level Deduplication](developer/architecture/README.md)** - Efficient storage optimization
- **[Multi-Algorithm Compression](developer/architecture/README.md)** - Zstandard and LZMA support
- **[Write-Ahead Logging](developer/architecture/README.md)** - Crash recovery and data integrity

### Search & AI Features  
- **[Full-Text Search](user_guide/search_guide.md)** - SQLite FTS5 based text search
- **[Semantic Search](user_guide/vector_search_guide.md)** - Vector-based similarity search
- **[Hybrid Search](user_guide/vector_search_guide.md)** - Combined keyword and semantic search
- **[Document Extraction](developer/architecture/README.md)** - Multi-format document processing

### Integration Features
- **[MCP Server](mcp_websocket_transport.md)** - Model Context Protocol for AI agents
- **[WebSocket Transport](mcp_websocket_transport.md)** - Network communication
- **[CLI Tools](user_guide/cli.md)** - Command-line utilities
- **[REST API](api/README.md)** - HTTP API for remote access

## 🎯 Use Case Guides

- **[Document Management](user_guide/README.md)** - Personal and enterprise document storage
- **[Knowledge Base](user_guide/README.md)** - Building searchable knowledge repositories  
- **[AI Integration](user_guide/README.md)** - Connecting with LLMs and AI agents
- **[Data Archival](user_guide/README.md)** - Long-term data preservation
- **[Content Deduplication](user_guide/README.md)** - Storage optimization scenarios

## 🛡️ Quality Assurance

- **[Test Coverage Report](developer/testing/README.md)** - Current test coverage analysis
- **[Testing Strategy](developer/testing/README.md)** - Testing approach and standards  
- **[Performance Benchmarks](developer/testing/README.md)** - Performance test results
- **[Security Audit](developer/testing/README.md)** - Security testing and validation

## 📊 Project Information

- **[Changelog](CHANGELOG.md)** - Version history and release notes
- **[Roadmap](ROADMAP.md)** - Planned features and milestones
- **[License](../LICENSE)** - Apache License 2.0 information
- **[Contributors](CONTRIBUTORS.md)** - Project contributors and acknowledgments

## 🔗 External Resources  

- **[GitHub Repository](https://github.com/trvon/yams)** - Source code and issue tracking
- **[Release Downloads](https://github.com/trvon/yams/releases)** - Binary releases
- **[Community Forum](https://github.com/trvon/yams/discussions)** - Questions and discussions
- **[Issue Tracker](https://github.com/trvon/yams/issues)** - Bug reports and feature requests

## 💡 Getting Help

### Quick Help
- Check the **[FAQ](user_guide/README.md)** for common questions
- Search the **[Troubleshooting Guide](troubleshooting/search_issues.md)** for known issues
- Browse **[GitHub Discussions](https://github.com/trvon/yams/discussions)** for community support

### Reporting Issues
1. Check existing **[GitHub Issues](https://github.com/trvon/yams/issues)**
2. Use the appropriate issue template
3. Provide detailed reproduction steps and system information
4. Include relevant log output and configuration

### Contributing
1. Read the **[Contributing Guide](developer/contributing.md)**
2. Set up your **[Development Environment](developer/setup.md)**
3. Follow the **[Code Standards](developer/code_standards.md)**
4. Submit a pull request with tests and documentation

## 📚 Documentation Standards

This documentation follows these principles:
- **Clarity**: Clear, concise explanations with practical examples
- **Completeness**: Comprehensive coverage of features and use cases  
- **Currency**: Up-to-date with the latest YAMS version
- **Accessibility**: Easy navigation and search-friendly structure
- **Quality**: Tested examples and validated procedures

---

**Last Updated**: August 2025 | **YAMS Version**: 1.0.0 | **Documentation Version**: 1.0