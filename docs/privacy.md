---
title: Privacy Policy
description: Privacy Policy for YAMS Memory mobile and desktop applications.
---

# YAMS Memory Privacy Policy

**Effective Date:** November 25, 2026  
**Applies To:** YAMS Memory for Android, iOS, and desktop (`app.yams.memory`)  
**Contact:** [support@yamsmemory.ai](mailto:support@yamsmemory.ai)

## 1. Summary

YAMS Memory is a **local-first** personal knowledge vault. We do not collect, sell, rent, or transmit your personal data, documents, PDFs, notes, search queries, or usage analytics.

## 2. Data Stored on Your Device

All notes, PDFs, images, extracted text, embeddings, and settings are stored exclusively on your device inside your private YAMS storage directory (and any folder you explicitly grant access to, such as an Obsidian vault via Android Storage Access Framework or iOS Files).

- **No account required.**
- **No cloud servers controlled by YAMS** receive your documents.
- **No analytics SDKs, crash-reporting trackers, or advertising SDKs** are bundled in the app.

## 3. Optional Network Features (Opt-In Only)

By default, YAMS Memory opens **zero network sockets**. Network requests are made only when you explicitly invoke or enable an optional feature:

1. **Paperbridge Research Browser (Opt-In):** When you search for academic papers or download an open-access PDF, the app sends your search query or PDF download request directly from your device to public scholarly APIs (**arXiv.org** and **OpenAlex.org**) or to a local/LAN Paperbridge instance that you configure.
2. **LAN / P2P Corpus Sync (Opt-In):** When you explicitly enable peer-to-peer sync, the app communicates directly with peers you authorize on your local network or private overlay (such as Tailscale).

## 4. On-Device AI & Text-to-Speech

- **PDF text extraction, indexing, and search** run 100% locally on your device.
- **Read-Aloud (TTS)** uses your operating system's built-in on-device Text-to-Speech engine.
- **On-device summarization**, when supported by your device, runs locally without sending your documents to external inference servers.

## 5. Permissions

- **Storage Access Framework / Files:** Used only for folders or files you explicitly select (for example, importing a PDF or bridging an Obsidian vault).
- **Foreground Service (`DATA_SYNC`) & Notifications (Android):** Used only while opt-in user-initiated LAN/P2P sync is active.
- **Internet / Network State:** Used only when you explicitly trigger Paperbridge research search/download or enable opt-in P2P sync.

## 6. Data Deletion

Because all data lives locally on your device, you can delete individual items inside the app, clear app storage in your OS settings, or uninstall YAMS Memory at any time to permanently remove all stored data.

## 7. Changes & Contact

If this policy changes, updates will be published at `https://yamsmemory.ai/privacy/` with a new effective date. For questions, contact [support@yamsmemory.ai](mailto:support@yamsmemory.ai).
