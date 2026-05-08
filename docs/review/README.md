# GraphReFly Rust Port — Review Site

Static review website that renders the chronological port reports next to the canonical flowcharts. Diagrams open as draggable, pannable, zoomable modals so you can read a report and a flowchart side-by-side without losing scroll position.

## Layout

```
graphrefly-rs/docs/
├── flowcharts.md                                      (canonical — site fetches directly)
└── review/
    ├── README.md                                      (this file)
    ├── reports-000-overview.md
    ├── reports-001-m1-and-m2.md
    ├── reports-002-m3-substrate.md
    ├── reports-003-m3-operators.md
    ├── reports-004-m3-combinators-and-higher-order.md
    ├── reports-005-m3-correctness-and-typed-errors.md
    └── site/
        ├── index.html
        ├── css/style.css
        └── js/app.js
```

## Run

The site is plain HTML/CSS/JS — no build step. **It will not work if you double-click `index.html`** (browsers block `fetch()` from `file://` origins; the page detects this and shows a banner with these instructions).

Serve `graphrefly-rs/docs/` over HTTP so the site can reach both `review/site/` and `flowcharts.md`:

```bash
# bundled shortcut (default port 8765):
docs/review/site/serve.sh

# or override port:
docs/review/site/serve.sh 9000

# or run python directly:
python3 -m http.server 8765 --directory docs

# then open:
open http://localhost:8765/review/site/
```

Or via the configured launch profile (Claude Code `preview_start`): `rust-review` → `http://localhost:8765/review/site/`.

## Canonical flowcharts

`docs/flowcharts.md` (one directory up from `review/`) is the canonical source of truth and is what the site fetches at runtime — there is no longer a sync'd copy. Edit it in place and refresh the browser.

## How references work

Reports cite diagrams via markdown links of the form `[F<batch>.<n>](#fc-<batch>-<n>)`, e.g. `[F7.2](#fc-7.2)`. The renderer rewrites those into chip-styled anchors with `data-fc-id="7.2"`. Clicking a chip opens a draggable modal containing the rendered mermaid diagram with svg-pan-zoom.

## Modal interactions

- Drag the header bar to move.
- Mouse wheel inside the body to zoom; click-drag inside to pan.
- Click `+` / `−` / `⤢` (reset) in the header for explicit zoom controls.
- Drag the bottom-right corner to resize.
- `Esc` closes the focused (last-clicked) modal.
- `Close all` in the topbar closes everything.
- Click a chip while a modal is already open to spawn another — modals stack with z-index management; click any modal to bring it forward.
- Page scrolls underneath open modals — modals are `position: fixed` with no overlay backdrop.

## Adding a new report

1. Create `reports-NNN-<slug>.md` in this directory.
2. Add an entry to the `REPORTS` array at the top of `site/js/app.js`.
3. Reload the site.

## Adding a new flowchart batch

Edit `~/src/graphrefly-rs/docs/flowcharts.md` (the canonical), follow the existing `## Batch <n> — <title>` + `### <batch>.<n> <title>` + `\`\`\`mermaid` convention, then re-sync to `docs/review/flowcharts.md` and refresh the site.
