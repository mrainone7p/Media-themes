# Media Tracks State Review (current-state reassessment)

This is the **single source-of-truth state review** for the current codebase. It is intentionally focused on simplification and human maintainability over architecture churn.

## Executive verdict on the latest changes

Your instinct is right: the project is still functional, but in a few places we traded clarity for additional lines and repetition.

**Net result today:**
- ✅ UX coverage increased (more controls and clearer paths exist).
- ⚠️ Code maintainability regressed in key hotspots (duplicate modal logic, repeated task-card markup, repeated API-action boilerplate, and hardcoded display strings).
- ✅ This can be corrected **without** adding many files.

## Current-state snapshot

### Project shape (still intentionally simple)

- Backend/API: `web/app.py`
- Frontend: `web/template.html`
- Pipeline worker: `script/media_tracks.py`
- Storage helpers: `shared/storage.py`

### What is working well

1. Deployment and local operation are still straightforward.
2. Core feature flow remains complete end-to-end.
3. There is no framework/tooling bloat.
4. Reusable UI patterns exist and can be expanded (example: shared run/progress treatment).

## Where line growth is coming from

### 1) Repeated modal behavior (biggest duplication)

Multiple modals reimplement near-identical audio/player and open/close lifecycle logic:
- play/pause icon switching
- metadata/timeupdate handlers
- seek/skip boundaries
- cleanup on close

This is the largest avoidable repetition in `web/template.html`.

### 2) Hardcoded task/schedule blocks with similar structure

Task cards and step sections repeat the same skeleton (title/desc/button/control row) with only small differences.

### 3) Repeated task API action boilerplate

Several task handlers each duplicate:
- POST fetch setup
- JSON parse
- toast success/error
- refresh calls

### 4) Heavy inline `innerHTML` assembly in many places

Dynamic markup is useful, but repeated ad-hoc string templates increase escaping risk and make edits harder to reason about.

### 5) Mixed concerns in one long script

Single-file is fine for this project, but the JS region still mixes utilities, modal logic, DB rendering, run-state orchestration, and task utilities in a way that slows safe edits.

## Reassessed simplification strategy (no file explosion)

### Priority 1 — Consolidate modal primitives in-place

Inside `web/template.html` only (no new frontend file), add minimal shared helpers for:
- modal open/close
- shared audio player binding lifecycle

Then have YT/Theme/Trim modal functions call those helpers instead of reimplementing behavior.

**Expected gain:** substantial line reduction + lower bug drift risk.

### Priority 2 — Convert repeated task cards to config-driven rendering

Keep existing CSS and DOM containers. Define one in-file array describing cards and render them into current sections.

**Guardrail:** keep existing IDs used by handlers so no behavioral surprises.

### Priority 3 — Introduce one shared task-action request helper

Create one helper for POST task actions that centralizes:
- request setup
- response/error handling
- toast conventions
- optional refresh hooks

Use it from cleanup/prune/refresh/sqlite/clear/export actions.

### Priority 4 — Strengthen in-file section boundaries

Keep single-file frontend, but add strict section banners and group code in this order:
1. Core utils & API helpers
2. Shared UI primitives (modals/audio/progress)
3. Database table/status rendering
4. Search + preview flows
5. Schedule/tasks actions
6. Page init

This keeps your no-fragmentation preference while improving scanability.

## What to avoid (for now)

- No framework migration.
- No broad split into many JS/CSS files.
- No rewrite of all rendering patterns in one PR.
- No expansion of feature surface until duplication hotspots are reduced.

## “Simple and maintainable” target state

The codebase is in a good place when:
- Adding a new utility task means editing one config object + one handler only.
- Adding a new modal does **not** require copy/pasting full player lifecycle code.
- Task action endpoints share one request pattern.
- A contributor can locate relevant logic in under 10–15 minutes.

## Recommended next sequence

1. Modal primitives consolidation (highest ROI).
2. Task action helper consolidation.
3. Task/schedule config-driven card rendering.
4. Final in-file section cleanup pass.

If we execute in this order, we reduce code volume and duplication while keeping the project single-file and operator-friendly.
