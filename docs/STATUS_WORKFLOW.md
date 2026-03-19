# Status Workflow

Media Tracks persists exactly six workflow states:

1. `UNMONITORED`
2. `MISSING`
3. `STAGED`
4. `APPROVED`
5. `AVAILABLE`
6. `FAILED`

## What is *not* a persisted state

`REMOVED` is not part of the stored workflow.

If a movie leaves Plex, the next scan removes that ledger row instead of keeping a hidden `REMOVED` record. If the movie later reappears in Plex, it is treated as a fresh library item and re-added during scan.

## Manual transition rules

- Any state can be set to `UNMONITORED`.
- `UNMONITORED` → `MISSING`
- `MISSING` → `STAGED` or `FAILED`
- `STAGED` → `APPROVED`, `MISSING`, or `FAILED`
- `APPROVED` → `AVAILABLE`, `MISSING`, or `FAILED`
- `AVAILABLE` → `MISSING`
- `FAILED` → `MISSING` or `STAGED`

Additional guards:

- `STAGED` requires a saved source URL.
- `APPROVED` requires the current state to already be `STAGED`.
- `AVAILABLE` requires a local theme file to exist.

## Automatic status behavior

- Scan sets current Plex items to `AVAILABLE` when a local theme file exists.
- Scan sets current Plex items to `MISSING` when no local theme file exists.
- Resolve keeps retryable misses at `MISSING`.
- Resolve or download can set unrecoverable items to `FAILED`.
- Clearing a saved source URL resets the item to `MISSING`, unless:
  - the item is `UNMONITORED`, which stays `UNMONITORED`
  - the item is `FAILED`, which stays `FAILED`
  - a local theme already exists, which keeps it `AVAILABLE`
