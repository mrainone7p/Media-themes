# Media Tracks State Review (updated implementation status)

This document tracks the current codebase against the intended product direction and what has now been implemented.

## Implemented in this pass

- Navigation and page internals were normalized to **Configuration / Library / Schedule / Tasks** (including hash-route compatibility from legacy `#settings`/`#run`).
- Configuration internals now use `loadConfiguration()` / `saveConfiguration()` with compatibility aliases kept in place to reduce regression risk.
- Native browser `confirm()` dialogs for Library actions were replaced with a shared in-app confirm modal shell.
- Library visible status controls were aligned to the intended model by removing `Removed` from user-facing status filter/options.
- Backend row identity handling was hardened with a shared identity resolver preferring `rating_key`, then `folder`, then `tmdb_id`.
- Delete and manual-download API responses now include `matched_by` diagnostics to make identity matching transparent in the UI.
- Manual download UI now sends additional identity fields (`folder`, `tmdb_id`) to improve reliability in mixed-ledger scenarios.

## Current alignment snapshot

### Aligned

1. **Primary pages and terminology**
   - Visible page naming is Configuration / Library / Schedule / Tasks.
2. **Library operational role**
   - Main management page with filtering, bulk actions, source actions, and imports.
3. **SQLite shared storage direction**
   - Shared runtime storage in `shared/storage.py` with SQLite (`/app/logs/media_tracks.db`).
4. **Golden Source schema**
   - Uses `tmdb_id` + `source_url` (no youtube_url import fallback path in current logic).
5. **Status wording**
   - UI displays `Available` for internal `DOWNLOADED` state.

### Remaining follow-ups (recommended)

1. **Manual search UX polish**
   - The 3-step modal exists; continue refining consistency and reducing edge-case UI roughness.
2. **Regression safety**
   - Add automated smoke checks for nav routing and critical row actions.
3. **Optional naming cleanup**
   - CSS class names still include legacy `settings`/`run` identifiers (internal only, no user-facing impact).

## Acceptance checks to keep using

- Navigation across Configuration / Library / Schedule / Tasks works without JS errors.
- Delete local theme works when matched by `rating_key`, and falls back to `folder` when needed.
- Manual row download succeeds when source exists and no local theme exists, then refreshes row state to Available in UI.
- Golden Source import succeeds using `tmdb_id/source_url`, and keep/overwrite behavior is respected.
