# Troubleshooting Search Issues

Quick checks and fixes for common search problems.

## No results or too few results

- Verify content is indexed: run `yams status` and `yams list --limit 5`.
- Check embeddings: `yams stats -v` should show embedding generator ready.
- Try simpler queries: remove field filters and quotes to widen scope.
- Confirm data path: ensure `YAMS_STORAGE` points to the expected location.

## Vector search not working

- Ensure embeddings are enabled and a model is configured.
- Repair embeddings for existing content:
  - `yams repair embeddings --batch-size 64 --skip-existing`
- Check dimension mismatches:
  - Align embedding model dimension with the vector DB configuration.

## Slow or inconsistent results

- Reduce batch sizes for embeddings; monitor CPU/RAM via `yams stats -v`.
- Limit top-k and adjust hybrid weights in configuration.
- Disable KG scoring temporarily to isolate issues.

## CLI and daemon connectivity

- Restart the daemon: `yams daemon restart`.
- Inspect logs for errors; verify plugin trust paths if using plugins.

## When to file an issue

Include YAMS version, OS, a minimal reproduction (commands), and anonymized logs.

