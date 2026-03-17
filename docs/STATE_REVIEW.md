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

## Lean reassessment (keep it human-maintainable)

This reassessment is intentionally optimized for simplicity and low file sprawl. The codebase works today, but most maintainability pressure comes from two large files (`web/template.html` and `web/app.py`). The goal is to reduce complexity without turning this into a framework-heavy rewrite.

### What feels bloated today

- The frontend combines layout, styling, state, and all workflows in one large file, which slows safe edits.
- The backend route file carries mixed concerns (config, run control, media actions, tasks/maintenance), making regressions easier to introduce.
- The Tasks page combines frequent actions (run/export) and high-impact maintenance actions (prune/clear/vacuum), which increases UX risk for first-time users.

### High-impact, low-bloat improvements

1. **Introduce “Basic mode” defaults (no new page required)**
   - Keep current pages, but hide advanced controls behind existing `<details>` sections by default.
   - Surface only the normal user path: Configure → Find Sources → Approve → Download.

2. **Split logic, not architecture**
   - Keep Flask + server-side template structure as-is.
   - Do a minimal extraction only where needed:
     - `web/app.py` → one small helper module for status/transition rules.
     - `web/template.html` → one small JS helper block for shared API/error/toast handling.
   - Avoid large multi-folder frontend refactors for now.

3. **Unify status logic once**
   - Keep a single transition policy and response shape from backend.
   - Frontend should render/validate based on backend-reported capabilities instead of duplicating rules.

4. **Separate “Operations” from “Maintenance” in-place**
   - Keep Tasks page, but visually split into:
     - Routine operations (run tasks, export)
     - Advanced maintenance (cleanup/prune/vacuum/clear URLs)
   - Add stronger warnings and irreversible-action wording for maintenance controls.

5. **Reduce action clutter in Library rows**
   - Keep core action visible and move rare actions into a compact overflow/dropdown pattern.
   - Preserve power-user actions, but make default row interaction calmer.

### Sensible feature scope (what to avoid right now)

- Avoid introducing new frameworks/build systems.
- Avoid aggressive file fragmentation that would burden solo maintenance.
- Avoid adding many “smart automation” features before status/error clarity is solid.

### Suggested next steps (minimal-disruption order)

1. Polish copy and page grouping first (no behavior change).
2. Normalize status/error payloads in API and consume consistently in UI.
3. Reduce row/task action density with clearer defaults.
4. Add a small smoke-check script for critical flows (navigation, status update, run start/stop).

## Acceptance checks to keep using

- Navigation across Configuration / Library / Schedule / Tasks works without JS errors.
- Delete local theme works when matched by `rating_key`, and falls back to `folder` when needed.
- Manual row download succeeds when source exists and no local theme exists, then refreshes row state to Available in UI.
- Golden Source import succeeds using `tmdb_id/source_url`, and keep/overwrite behavior is respected.
