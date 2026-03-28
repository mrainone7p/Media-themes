# Theme Manager table column schema

The Theme Manager library grid is now driven by a single column definition array in `web/static/js/library.js` (`TABLE_COLUMNS`).

## Column schema

Each column entry supports:

- `id`: stable identifier used for sorting and CSS hook classes (`col-${id}`)
- `label`: header text
- `width`: preferred column width
- `minWidth`: minimum usable width before truncation
- `align`: `left`, `center`, or `right`
- `sortable`: whether clicking the header invokes `sortTable(id)`
- `renderHeader` (optional): custom header renderer (used by checkbox column)
- `renderCell`: cell renderer function
- `sticky` (optional): `first` marks the first column as eligible for sticky behavior

## Reusable cell renderer components

The table body now uses reusable renderers:

- `SelectCell`
- `TitleCell`
- `StatusBadgeCell`
- `ActionCell`
- `SourceBadgeCell`
- `UpdatedCell`
- `NotesCell`

## How to make layout tweaks (config-only)

Examples:

- Make **Action** wider: update only the `action` column entry in `TABLE_COLUMNS`, e.g. adjust `width`/`minWidth`.
- Disable sorting for **Notes**: set `sortable: false` for `id: 'notes'`.
- Add a new column: append a new schema object with `renderCell` and optional `renderHeader`.

No scattered CSS width overrides should be required for routine column changes.

## Sticky behavior

- Header is sticky by default.
- First column becomes sticky when large result sets are rendered (`DB_STICKY_FIRST_COL_THRESHOLD`).
- Sticky behavior uses `.tbl-wrap.has-sticky-first-col` and `.is-sticky-col` classes.
