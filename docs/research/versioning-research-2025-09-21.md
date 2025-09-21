Title: Document/Data Versioning — Design Options & Trade‑offs (2025‑09‑21)

Context: YAMS uses content‑addressed storage (CAS) with a metadata DB. We need auditable version lineage with cheap “latest” reads and minimal code.

Sources (cached via `yams download`):
- IPFS: Content-addressed Merkle‑DAG and immutable snapshots. (arXiv 1407.3561)
- UStore: Immutable, shareable storage with fast version scans. (arXiv 1702.02799)
- ForkBase: Deduplicating, branchable substrate (“Git for data”). (arXiv 2004.07585)
- DVC: Git‑layered data/model versioning—operational patterns. (dvc.org docs)
- lakeFS: Git‑like object‑store versioning—operational model. (docs.lakefs.io)

Key Options
1) Immutable Snapshot Graph (VersionOf lineage; O(1) latest via metadata)
   - Pros: strong auditability; dedup‑friendly; future‑proof for branching.
   - Cons: need GC policy; must maintain “latest” flags atomically.

2) Path‑Series vs Logical‑Name Series
   - Path‑Series: simple, no schema change; rename breaks chain.
   - Logical‑Name: rename‑stable but needs client‑provided IDs.

3) Delta vs Snapshot
   - Delta saves space for text/binaries but adds complexity and slower restores. Snapshot pairs well with CAS and chunk dedup.

Recommendation
- Adopt immutable Snapshot Graph with Path‑Series now; allow optional logical_name override later. Implement as small C++20 helpers, behind `YAMS_ENABLE_VERSIONING`.

Minimal Implementation Hooks
- After insert: resolve latest by series_key, emit VersionOf edge, set `series_id`, `version`, `is_latest`; flip previous latest.
- Same hash: no new version; keep alternate_location and timestamps.

