/* GraphReFly Rust Port Review — Journal site app
 *
 * Lifecycle:
 *   1. Fetch flowcharts.md + reports list, parse into structured stores.
 *   2. Render left-rail report list + canonical figure index.
 *   3. Hash-routed report rendering — for each report:
 *      a. Pre-process the markdown:
 *         - Lift the H1 + adjacent metadata into a header card.
 *         - Translate `::: kind` directives into card-flavored HTML.
 *         - Inline-render mermaid diagrams referenced by `::: figure` blocks.
 *         - Number the H2 sections (01 / 02 / …).
 *      b. Run marked.js, then post-process flowchart-ref anchors and severity
 *         tagging.
 *      c. Build the right-rail outline (h2 + h3) and figures list.
 *   4. Click delegation: chips first try to scroll to an inline figure; if
 *      that figure isn't on the current page, they open a draggable modal.
 *      An "↗ enlarge" button on each inline figure opens the modal directly.
 */

const FLOWCHARTS_PATH = '../../flowcharts.md';
const REPORTS = [
  { id: 'overview',           title: 'Overview & current state',         file: '../reports-000-overview.md' },
  { id: 'm1-m2',              title: '001 — M1 + M2 (closed milestones)', file: '../reports-001-m1-and-m2.md' },
  { id: 'm3-substrate',       title: '002 — M3 Slice A + B (substrate)',  file: '../reports-002-m3-substrate.md' },
  { id: 'm3-operators',       title: '003 — M3 Slice C + D-substrate',    file: '../reports-003-m3-operators.md' },
  { id: 'm3-combinators',     title: '004 — M3 Slice D-ops + Slice E',    file: '../reports-004-m3-combinators-and-higher-order.md' },
  { id: 'm3-correctness',     title: '005 — Slice F + G + E1 + H',        file: '../reports-005-m3-correctness-and-typed-errors.md' },
  { id: 'directive-ref',      title: '✦ Directive reference (showcase)',  inline: () => DIRECTIVE_SHOWCASE_MD },
];

// Inline showcase markdown rendered through the same pipeline as on-disk reports.
// Doubles as living documentation for the directive vocabulary that `/rust-review`
// emits (see SKILL.md Phase 6.5).
const DIRECTIVE_SHOWCASE_MD = `# Directive Reference — Journal Vocabulary

**Use:** documentation · **Audience:** \`/rust-review\` authors · **Output:** in-page showcase

This page demonstrates every directive the journal renderer understands. Use it as a visual checklist when authoring a new \`reports-NNN-<slug>.md\`. The directive grammar is documented in \`.claude/skills/rust-review/SKILL.md\` § Phase 6.5; this page is the *rendered* reference.

::: stats
- Tests: 471 → 488
- 🟢 Slices closed — 2 (Q1, Q2)
- 🟢 Divergences resolved: 4
- 🟡 Open deferred items: 1
- 🔴 Correctness holes: 0
:::

## Trace cards

The \`::: trace\` block wraps a behavioral trace. Spec rules render as green chips in the header; figure references render as the same F-chips you'd type in prose.

::: trace id="T1" title="Pause-overflow ERROR synthesis" rules="R1.3.8.c, Lock 6.A" diagrams="11.1"
| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | \`set_pause_buffer_cap(node, Some(2))\` | NodeRecord.pause_buffer_cap = 2 | — |
| 2 | \`pause(node, lockId=L1)\` | enter PAUSED | — |
| 3 | \`emit(node, h_a)\` | pause_buffer = [h_a] | — |
| 4 | \`emit(node, h_b)\` | pause_buffer = [h_a, h_b] = cap | — |
| 5 | \`emit(node, h_c)\` | overflow → synthesize_pause_overflow_error | sink: \`[Error(diagnostic)]\` after RESUME |

Pre-Slice-F this was a silent drop. Now structured ERROR with \`{nodeId, droppedCount, configuredMax, lockHeldDurationMs}\`.
:::

## Inline figures

A \`::: figure id="x.y"\` block embeds the canonical Mermaid diagram inline as a captioned figure — exactly where the trace cites it.

::: figure id="0.1" caption="Workspace crate map (placeholder — referenced from Phase 7's flowcharts.md)."
:::

Subsequent \`[F0.1](#fc-0.1)\` chips in this report scroll to that figure with a brief flash. Chips that reference a figure *not* embedded inline keep the original behavior — they open the diagram in a draggable modal: try [F11.5](#fc-11.5) for example.

## Findings — four severities, four ribbons

::: finding kind="bug" title="Diamond resolution overflows at fan-in >32" severity="major" where="crates/graphrefly-core/src/dispatcher.rs:142" rule="R5.8" status="open" recommendation="Switch to BigInt-backed mask"

The current bitmask is a \`u32\` and silently overflows when fan-in exceeds 32. Reproducer: \`tests/wide_fanin_diamond.rs::T_diamond_w33\`. TS uses \`Uint32Array\` chained with \`BigInt\` to support arbitrarily wide fans.

Fix is mechanical: lift the bitmask to \`u128\` for fan-in ≤128, fall back to \`Vec<u64>\` chunks above. Test surface already exists.
:::

::: finding kind="limit" title="\`Core::up(INVALIDATE)\` cascades via dep-walk" severity="minor" rule="R1.4.2" slice="Slice F audit /qa D2" status="documented divergence"

R1.4.2 specifies plain-forward semantics; the Rust impl currently runs the cascade through dep-walk for ergonomic reasons. Tracked in \`porting-deferred.md\`. Behavioral effect is identical for all canonical scenarios; only matters for spec-exact replay.
:::

::: finding kind="opp" title="Phase 3 of \`Core::register\` walks deps twice" severity="minor" where="crates/graphrefly-core/src/register.rs:208"

Validation walks \`deps\` twice — once in Phase 1 (lock-released), once in Phase 3 (re-validation under state-lock). Caching the validated NodeId vector between phases removes one O(N) walk and a second \`HashMap::get\` per dep. Estimated win: ~3–5% on \`bench/registration.rs\` for fan-in ≥16. No behavior change.
:::

::: finding kind="note" title="Wave-end rotation hot path is loom-clean"

Slice D /qa added loom verification for \`commit_emission_verbatim\` → \`tier3_emitted_this_wave\` → \`pending_notify\` rotation. The model passes under the cross-thread interleaving loom enumerates. No further hardening needed before binding ships.
:::

## Plain markdown still works

Tables, paragraphs, code, and lists outside any directive render with the journal's default styling — \`Newsreader\` body serif, paper-cool table backgrounds, accent-tinted inline \`code\`.

| # | TS pattern | Rust replacement | Simpler? | Notes |
|---|---|---|---|---|
| 1 | TS pause overflow: silent drop | Synthesized Error with structured diagnostic | Same | Closes documented divergence |
| 2 | TS \`tier3_emitted_this_wave\` Set in closure | AHashSet on CoreState struct field | Same | Lifted for lock-acquired access |
| 3 | TS \`up()\` decomposed (one method per tier) | \`Core::up(node, msg) → Result<(), UpError>\` | Same/cleaner | R1.4.1 single entry point |

## Verdict card

A single \`::: assessment\` block at the bottom turns each \`**Label:**\` line into a row, with colored chips for level keywords (\`very high\`, \`high\`, \`medium\`, \`low\`, \`none\`, \`yes\`, \`no\`).

::: assessment title="Verdict — Slice Q1 + Q2"
**Spec-fidelity:** very high. Closed 4 documented divergences in-slice.
**Over-engineering risk:** low. Two Rust-adds-complexity rows, both forced by the multi-thread state lock.
**Correctness holes:** none.
**HALT:** no.
:::
`;

// flowchart store: id (e.g. "7.2") → { batchTitle, title, code }
const diagrams = new Map();
let topZ = 1000;
let modalSeq = 0;

// per-report runtime state
let inlineFigures = new Map(); // figId → DOM node within current report

// ─── markdown helpers ─────────────────────────────────────────────────
marked.setOptions({ gfm: true, breaks: false });

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escAttr(s) { return escapeHtml(String(s)); }

// Parse a directive attribute string like:  id="T1" rules="R5.1, R5.2" diagrams="11.1"
function parseAttrs(attrStr) {
  const out = {};
  if (!attrStr) return out;
  const re = /(\w+)\s*=\s*"([^"]*)"/g;
  let m;
  while ((m = re.exec(attrStr)) !== null) out[m[1]] = m[2];
  return out;
}

// ─── directive preprocessing ──────────────────────────────────────────
//
// We emit raw HTML (an `<aside class="…">` per directive) and embed the inner
// content so marked.js still parses tables/code/text inside the card. Marked
// preserves block-level HTML when it sees a blank line before the tag, so we
// always wrap with surrounding blank lines.

function preprocessDirectives(md, sectionState) {
  // Match `::: kind ATTRS\n…BODY…\n:::` (non-greedy, multiline). Allow nested
  // markdown inside; we re-parse later with marked.
  return md.replace(
    /^:::\s*(trace|finding|figure|stats|assessment)([^\n]*)\n([\s\S]*?)^:::[ \t]*$/gm,
    (_, kind, attrStr, body) => renderDirective(kind, parseAttrs(attrStr.trim()), body.trim(), sectionState)
  );
}

function renderDirective(kind, attrs, body, st) {
  switch (kind) {
    case 'trace':      return renderTrace(attrs, body);
    case 'finding':    return renderFinding(attrs, body);
    case 'figure':     return renderFigure(attrs, body, st);
    case 'stats':      return renderStats(attrs, body);
    case 'assessment': return renderAssessment(attrs, body);
    default:           return body;
  }
}

function renderTrace(attrs, body) {
  const id = attrs.id || 'T?';
  const title = attrs.title || '';
  const rules = (attrs.rules || '').split(',').map(s => s.trim()).filter(Boolean);
  const diags = (attrs.diagrams || attrs.diagram || '').split(',').map(s => s.trim()).filter(Boolean);
  const ruleChips = rules.map(r => `<span class="spec-chip" title="Spec rule">${escAttr(r)}</span>`).join('');
  const diagChips = diags.map(id => `<a class="flowchart-ref" href="#fc-${escAttr(id)}" data-fc-id="${escAttr(id)}" title="Open figure F${escAttr(id)}">F${escAttr(id)}</a>`).join('');

  // Body is parsed as markdown later (marked sees the inner text after we splice in HTML).
  // We embed the body directly between the opening/closing aside, separated by blank
  // lines so marked treats the inner content as block markdown.
  return `\n\n<aside class="j-card j-trace" data-trace-id="${escAttr(id)}">
  <div class="j-card-head">
    <span class="j-card-id">${escAttr(id)}</span>
    <span class="j-card-title">${escAttr(title)}</span>
    <span class="j-card-meta">${ruleChips}${diagChips}</span>
  </div>
  <div class="j-card-body">

${body}

  </div>
</aside>\n\n`;
}

function renderFinding(attrs, body) {
  const kind = (attrs.kind || 'note').toLowerCase();
  const id = attrs.id || ({bug:'BUG', limit:'LIM', warn:'WRN', opp:'OPP', note:'NOTE'}[kind] || 'NOTE');
  const title = attrs.title || '';
  const severity = (attrs.severity || '').toLowerCase();

  // Optional structured fields: where, rule, recommendation
  const fields = [];
  if (attrs.where)          fields.push(['Where', renderInlineCode(attrs.where)]);
  if (attrs.rule)           fields.push(['Spec rule', `<span class="spec-chip">${escAttr(attrs.rule)}</span>`]);
  if (attrs.slice)          fields.push(['Slice', escAttr(attrs.slice)]);
  if (attrs.status)         fields.push(['Status', escAttr(attrs.status)]);
  if (attrs.recommendation) fields.push(['Recommendation', escAttr(attrs.recommendation)]);
  const fieldHtml = fields.length
    ? `<dl class="finding-fields">${fields.map(([k,v]) => `<dt>${escAttr(k)}</dt><dd>${v}</dd>`).join('')}</dl>`
    : '';

  const sevTag = severity ? `<span class="severity-tag">${escAttr(severity)}</span>` : '';
  const kindLabel = ({bug:'CORRECTNESS', limit:'LIMITATION', warn:'WARN', opp:'OPPORTUNITY', note:'NOTE'}[kind] || kind.toUpperCase());

  return `\n\n<aside class="j-card j-finding" data-kind="${escAttr(kind)}">
  <div class="j-card-head">
    <span class="j-card-id">${escAttr(id)}</span>
    <span class="j-card-title">${escAttr(title)}</span>
    <span class="j-card-meta"><span class="severity-tag">${escAttr(kindLabel)}</span>${sevTag}</span>
  </div>
  <div class="j-card-body">
${fieldHtml}

${body}

  </div>
</aside>\n\n`;
}

function renderFigure(attrs, body, st) {
  const id = attrs.id;
  if (!id) return body;
  // Mark this figure as scheduled to be rendered inline; we leave a hook
  // element that the post-render mermaid pass fills in.
  if (st) st.inlineFigureIds.add(id);
  const captionFromAttr = attrs.caption || '';
  // body can contain extra prose to use as caption; if both, body wins.
  const caption = body || captionFromAttr;
  return `\n\n<figure class="j-figure" id="fig-${escAttr(id)}" data-fig-id="${escAttr(id)}">
  <div class="j-figure-head">
    <span class="j-figure-id">F${escAttr(id)}</span>
    <span class="j-figure-title" data-fig-title-for="${escAttr(id)}">Figure ${escAttr(id)}</span>
    <span class="j-figure-actions">
      <button type="button" class="fig-enlarge" data-fc-id="${escAttr(id)}" title="Open in a draggable, zoomable window">↗ enlarge</button>
    </span>
  </div>
  <div class="j-figure-canvas" data-fig-canvas="${escAttr(id)}"></div>
  ${caption ? `<figcaption class="j-figure-caption">${escapeHtml(caption)}</figcaption>` : ''}
</figure>\n\n`;
}

function renderStats(attrs, body) {
  // Body is a markdown bullet list of "Label: Value" or "Label — Value".
  // We split lines, parse, and emit stat tiles. Tone via leading marker:
  //   - 🔴 Bugs: 0     → bug-tone
  //   - 🟡 …           → warn-tone
  //   - 🟢 …           → opp-tone
  const items = body.split('\n').map(l => l.trim()).filter(l => l.startsWith('-') || l.startsWith('*'));
  const tiles = items.map(line => {
    const cleaned = line.replace(/^[-*]\s+/, '');
    let tone = '';
    let txt = cleaned;
    if (/^🔴|^bug:|^!/i.test(cleaned)) { tone = 'bug-tone';  txt = cleaned.replace(/^🔴\s*/, ''); }
    else if (/^🟡|^warn:/i.test(cleaned)) { tone = 'warn-tone'; txt = cleaned.replace(/^🟡\s*/, ''); }
    else if (/^🟢|^ok:/i.test(cleaned))   { tone = 'opp-tone';  txt = cleaned.replace(/^🟢\s*/, ''); }
    else if (/^🔵|^info:/i.test(cleaned)) { tone = 'note-tone'; txt = cleaned.replace(/^🔵\s*/, ''); }

    // split label : value (em-dash, colon, or " - ")
    const m = txt.match(/^([^:—–]+?)\s*[:—–-]\s*(.+)$/);
    if (m) {
      return `<li class="stat ${tone}">
        <div class="stat-label">${escapeHtml(m[1].trim())}</div>
        <div class="stat-value">${escapeHtml(m[2].trim())}</div>
      </li>`;
    }
    return `<li class="stat ${tone}"><div class="stat-value">${escapeHtml(txt)}</div></li>`;
  }).join('');
  return `\n\n<ul class="j-stats">${tiles}</ul>\n\n`;
}

function renderAssessment(attrs, body) {
  // Body is markdown lines like:
  //   **Spec-fidelity:** very high.
  //   **Over-engineering risk:** low.
  // We split on **…:** prefixes and render as a definition list with level chips.
  const title = attrs.title || 'Overall assessment';
  const lines = body.split('\n').filter(l => l.trim().length > 0);
  const rows = [];
  for (const raw of lines) {
    const m = raw.match(/^\s*\*\*\s*([^*]+?)\s*:\s*\*\*\s*(.*)$/);
    if (m) {
      const label = m[1];
      let value = m[2].trim();
      let levelClass = '';
      const lv = value.match(/^(very high|high|medium|low|none|yes|no)\b/i);
      if (lv) {
        const t = lv[1].toLowerCase();
        if (t === 'very high' || t === 'high' || t === 'none' || t === 'no') levelClass = 'level level-high';
        else if (t === 'medium')                                             levelClass = 'level level-med';
        else                                                                 levelClass = 'level level-low';
        value = `<span class="${levelClass}">${escapeHtml(lv[1])}</span>${escapeHtml(value.slice(lv[1].length))}`;
      } else {
        value = escapeHtml(value);
      }
      rows.push(`<dt>${escapeHtml(label)}</dt><dd>${value}</dd>`);
    } else {
      rows.push(`<dt></dt><dd>${escapeHtml(raw)}</dd>`);
    }
  }
  return `\n\n<section class="j-assessment">
  <header class="j-assessment-head">${escapeHtml(title)}</header>
  <div class="j-assessment-body"><dl>${rows.join('')}</dl></div>
</section>\n\n`;
}

function renderInlineCode(s) { return `<code>${escapeHtml(s)}</code>`; }

// ─── number h2 sections + post-process flowchart anchors ─────────────
function postProcessHtml(html, st) {
  // Number h2 sections sequentially: "## Foo" → "01 / Foo".
  // If the heading already starts with "N." (manually numbered, as in older
  // reports), strip that prefix and use it as the h-num so we don't double-number.
  let n = 0;
  html = html.replace(/<h2(\s[^>]*)?>([\s\S]*?)<\/h2>/g, (_, attrs = '', inner) => {
    n += 1;
    let num = String(n).padStart(2, '0');
    let body = inner;
    const m = inner.match(/^\s*(\d+)\.\s+([\s\S]*)$/);
    if (m) {
      num = m[1].padStart(2, '0');
      body = m[2];
    }
    return `<h2${attrs}><span class="h-num">${num}</span>${body}</h2>`;
  });

  // Upgrade flowchart anchors → chips (inside prose; existing behavior)
  html = html.replace(
    /<a\s+href="#fc-(\d+\.\d+)"([^>]*)>([\s\S]*?)<\/a>/g,
    (_, id, attrs, text) => {
      const cleanAttrs = attrs.replace(/\sclass="[^"]*"/g, '');
      const txt = text.startsWith('F') ? text : `F${id}`;
      return `<a href="#fc-${id}" class="flowchart-ref" data-fc-id="${id}" title="Click — scrolls to inline figure if present, else opens it"${cleanAttrs}>${txt}</a>`;
    }
  );

  // Lift the metadata strip: any `<p><strong>Label:</strong> value …</p>` chain
  // immediately following the report's H1 becomes the meta-strip card.
  html = html.replace(
    /(<h1[^>]*>[\s\S]*?<\/h1>)\s*((?:<p>(?:<strong>[^<]+:<\/strong>)[\s\S]*?<\/p>\s*){1,4})/,
    (_, h1, paras) => {
      const items = [];
      paras.replace(/<p>([\s\S]*?)<\/p>/g, (_, inner) => {
        // Each <strong>Label:</strong> value pair becomes a meta-item.
        // Multiple in one <p> are split by " · " or "—" — take the lot.
        const re = /<strong>([^<]+?):<\/strong>\s*([\s\S]*?)(?=<strong>|$)/g;
        let m;
        while ((m = re.exec(inner)) !== null) {
          const label = m[1].trim();
          const value = m[2].replace(/\s+$/, '').replace(/\s*[·•]\s*$/, '').trim();
          if (value) items.push(`<div class="meta-item"><span class="meta-label">${escapeHtml(label)}</span><span class="meta-value">${value}</span></div>`);
        }
        return '';
      });
      if (items.length === 0) return _;
      return `${h1}\n<div class="meta-strip">${items.join('')}</div>\n`;
    }
  );

  return html;
}

// ─── fetch + parse flowcharts.md ───────────────────────────────────────
async function loadFlowcharts() {
  const md = await fetch(FLOWCHARTS_PATH).then(r => {
    if (!r.ok) throw new Error(`flowcharts.md HTTP ${r.status}`);
    return r.text();
  });

  const lines = md.split('\n');
  let currentBatchTitle = null;
  let currentSection = null;
  let inMermaid = false;
  const batches = [];
  let currentBatchItems = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    let m;
    if ((m = line.match(/^## (Batch [0-9]+ — .+|Cross-references|Maintenance)$/))) {
      currentBatchTitle = m[1];
      currentBatchItems = { title: currentBatchTitle, items: [] };
      batches.push(currentBatchItems);
      continue;
    }
    if ((m = line.match(/^### (\d+\.\d+)\s+(.+)$/))) {
      currentSection = {
        id: m[1],
        title: m[2].trim(),
        batchTitle: currentBatchTitle,
        codeLines: [],
        captured: false,
      };
      if (currentBatchItems) currentBatchItems.items.push({ id: m[1], title: m[2].trim() });
      continue;
    }
    if (currentSection && !currentSection.captured && line.trim() === '```mermaid') {
      inMermaid = true;
      continue;
    }
    if (inMermaid && line.trim() === '```') {
      inMermaid = false;
      currentSection.captured = true;
      diagrams.set(currentSection.id, {
        id: currentSection.id,
        title: currentSection.title,
        batchTitle: currentSection.batchTitle,
        code: currentSection.codeLines.join('\n'),
      });
      continue;
    }
    if (inMermaid && currentSection) {
      currentSection.codeLines.push(line);
      continue;
    }
  }

  return batches;
}

// ─── sidebar render ────────────────────────────────────────────────────
function renderSidebar(batches) {
  const list = document.getElementById('reportList');
  list.innerHTML = REPORTS.map(r => `
    <li><a href="#${r.id}" data-report="${r.id}">${escapeHtml(r.title)}</a></li>
  `).join('');

  const idx = document.getElementById('diagramIndex');
  idx.innerHTML = batches
    .filter(b => b.items.length > 0)
    .map(b => `
      <details ${b.title.startsWith('Batch 0') ? 'open' : ''}>
        <summary>${escapeHtml(b.title)}</summary>
        ${b.items.map(it => `
          <a href="#fc-${it.id}" class="diagram-link" data-fc-id="${it.id}" title="${escAttr(it.title)}">
            <span class="diagram-id">F${it.id}</span><span>${escapeHtml(it.title)}</span>
          </a>
        `).join('')}
      </details>
    `).join('');
}

// ─── report render ─────────────────────────────────────────────────────
const reportCache = new Map();
async function loadReport(id) {
  if (reportCache.has(id)) return reportCache.get(id);
  const r = REPORTS.find(x => x.id === id);
  if (!r) throw new Error(`Unknown report ${id}`);
  let md;
  if (typeof r.inline === 'function') {
    md = r.inline();
  } else {
    md = await fetch(r.file).then(res => {
      if (!res.ok) throw new Error(`${r.file} HTTP ${res.status}`);
      return res.text();
    });
  }
  reportCache.set(id, md);
  return md;
}

async function showReport(id) {
  document.querySelectorAll('#reportList a').forEach(a => {
    a.classList.toggle('active', a.dataset.report === id);
  });
  const container = document.getElementById('reportContent');
  container.innerHTML = '<p class="loading">Loading the review journal…</p>';
  try {
    const md = await loadReport(id);
    const sectionState = { inlineFigureIds: new Set() };
    const md2 = preprocessDirectives(md, sectionState);
    let html = marked.parse(md2);
    html = postProcessHtml(html, sectionState);
    container.innerHTML = html;

    // Render mermaid for any inline figures
    inlineFigures = new Map();
    container.querySelectorAll('.j-figure[data-fig-id]').forEach(fig => {
      const fid = fig.getAttribute('data-fig-id');
      const canvas = fig.querySelector('[data-fig-canvas]');
      const titleHook = fig.querySelector(`[data-fig-title-for="${CSS.escape(fid)}"]`);
      const dia = diagrams.get(fid);
      if (titleHook && dia) titleHook.textContent = dia.title;
      inlineFigures.set(fid, fig);
      if (dia) renderInlineMermaid(dia, canvas, fig);
      else if (canvas) {
        canvas.innerHTML = `<div class="j-figure-error">Diagram F${escapeHtml(fid)} not found in flowcharts.md</div>`;
      }
    });

    // Build right-rail outline + figures list
    buildOutline(container);
    buildPageFigures(container);

    // Scroll to the right place: hash #fig-x.y or #anchor; else top.
    requestAnimationFrame(() => {
      const innerHash = location.hash.replace('#', '');
      if (innerHash && innerHash.startsWith('fig-')) {
        const t = document.getElementById(innerHash);
        if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
      } else {
        window.scrollTo({ top: 0, behavior: 'instant' });
      }
    });
  } catch (e) {
    container.innerHTML = `<p style="color:#9a2424">Failed to load report: ${escapeHtml(e.message)}</p>`;
  }
}

function buildOutline(container) {
  const outlineEl = document.getElementById('pageOutline');
  if (!outlineEl) return;
  const headings = container.querySelectorAll('h2, h3');
  if (headings.length === 0) {
    outlineEl.innerHTML = `<p class="muted small">— this report has no sections —</p>`;
    return;
  }
  const items = [];
  headings.forEach((h, i) => {
    const level = h.tagName === 'H2' ? 2 : 3;
    const id = h.id || `sec-${i}-${slug(h.textContent)}`;
    h.id = id;
    const text = h.textContent.replace(/^\d+\s*\/\s*/, '').trim();
    items.push(`<li><a href="#${id}" data-outline="${id}" class="lvl-${level}">${escapeHtml(text)}</a></li>`);
  });
  outlineEl.innerHTML = `<ol>${items.join('')}</ol>`;

  // Activate-on-scroll
  const links = outlineEl.querySelectorAll('a');
  if ('IntersectionObserver' in window) {
    const visible = new Map();
    const io = new IntersectionObserver((entries) => {
      entries.forEach(e => visible.set(e.target.id, e.isIntersecting ? e.intersectionRatio : 0));
      let bestId = null, bestRatio = 0;
      visible.forEach((r, id) => { if (r > bestRatio) { bestRatio = r; bestId = id; } });
      links.forEach(a => a.classList.toggle('active', a.dataset.outline === bestId));
    }, { rootMargin: '-15% 0px -65% 0px', threshold: [0, 0.25, 0.5, 1] });
    headings.forEach(h => io.observe(h));
    container._outlineObserver = io;
  }
}

function buildPageFigures(container) {
  const el = document.getElementById('pageFigures');
  if (!el) return;
  const figs = container.querySelectorAll('.j-figure[data-fig-id]');
  const refsOnly = new Set();
  container.querySelectorAll('a.flowchart-ref').forEach(a => refsOnly.add(a.dataset.fcId));
  const seenInline = new Set();
  figs.forEach(f => seenInline.add(f.getAttribute('data-fig-id')));

  const inlineList = Array.from(seenInline);
  const refList = Array.from(refsOnly).filter(id => !seenInline.has(id));

  let html = '';
  if (inlineList.length === 0 && refList.length === 0) {
    html = `<li class="muted small">— no figures referenced —</li>`;
  } else {
    if (inlineList.length) {
      html += inlineList.map(id => {
        const dia = diagrams.get(id);
        return `<li><a href="#fig-${escAttr(id)}" data-fig-anchor="${escAttr(id)}"><span class="fig-id">F${escAttr(id)}</span><span>${escapeHtml(dia ? dia.title : '(missing)')}</span></a></li>`;
      }).join('');
    }
    if (refList.length) {
      html += refList.map(id => {
        const dia = diagrams.get(id);
        return `<li><a href="#fc-${escAttr(id)}" data-fc-id="${escAttr(id)}"><span class="fig-id">F${escAttr(id)}</span><span>${escapeHtml(dia ? dia.title : '(missing)')} <em class="muted small">— enlarge</em></span></a></li>`;
      }).join('');
    }
  }
  el.innerHTML = html;
}

function slug(s) {
  return String(s || '').toLowerCase()
    .replace(/[^\w\s-]/g, '')
    .trim().replace(/\s+/g, '-').slice(0, 60);
}

// ─── inline mermaid (renders into a figure canvas) ───────────────────
async function renderInlineMermaid(d, canvas, fig) {
  if (!canvas) return;
  const renderId = `mer-inline-${d.id.replace('.', '-')}-${Date.now()}-${Math.random().toString(36).slice(2,7)}`;
  try {
    const { svg } = await mermaid.render(renderId, d.code);
    canvas.innerHTML = svg;
    const svgEl = canvas.querySelector('svg');
    if (svgEl) {
      // Let the svg be naturally responsive and scrollable
      svgEl.removeAttribute('width');
      svgEl.removeAttribute('height');
      svgEl.style.maxWidth = '100%';
      svgEl.style.height = 'auto';
    }
  } catch (err) {
    console.error('Inline mermaid render error:', err);
    canvas.innerHTML = `<div class="j-figure-error">
      <strong>Failed to render F${escapeHtml(d.id)}</strong>
      <pre>${escapeHtml(String(err && err.message || err))}</pre>
    </div>`;
  }
}

// ─── flowchart modal ───────────────────────────────────────────────────
function openFlowchartModal(id) {
  const d = diagrams.get(id);
  if (!d) {
    console.warn('No diagram for', id);
    return;
  }
  modalSeq++;
  const modal = document.createElement('div');
  modal.className = 'fc-modal focused';
  modal.id = `fc-modal-${modalSeq}`;
  modal.style.zIndex = String(++topZ);

  const baseWidth = 760;
  const baseHeight = 540;
  const offset = (modalSeq * 26) % 200;
  const left = Math.min(window.innerWidth - baseWidth - 20, 100 + offset);
  const top  = Math.min(window.innerHeight - baseHeight - 20, 90 + offset);
  modal.style.width = baseWidth + 'px';
  modal.style.height = baseHeight + 'px';
  modal.style.left = Math.max(10, left) + 'px';
  modal.style.top = Math.max(10, top) + 'px';

  modal.innerHTML = `
    <div class="fc-modal-header">
      <span class="fc-id">F${escapeHtml(d.id)}</span>
      <span class="fc-title" title="${escapeHtml(d.title)}">${escapeHtml(d.title)}</span>
      <span class="fc-batch">${escapeHtml(d.batchTitle || '')}</span>
      <span class="fc-controls">
        <button class="zoom-out" title="Zoom out">−</button>
        <button class="zoom-reset" title="Reset zoom">⤢</button>
        <button class="zoom-in" title="Zoom in">+</button>
        <button class="close" title="Close (Esc)">×</button>
      </span>
    </div>
    <div class="fc-modal-body">
      <div class="fc-mermaid" data-fc="${escapeHtml(d.id)}"></div>
      <div class="fc-modal-zoom-info">100%</div>
    </div>
    <div class="fc-modal-footer">
      <span>Drag header to move · scroll inside to zoom · drag corner to resize</span>
      <code>#fc-${escapeHtml(d.id)}</code>
    </div>
  `;

  document.getElementById('modalRoot').appendChild(modal);
  const target = modal.querySelector('.fc-mermaid');
  renderModalMermaid(d, target, modal);

  attachModalDrag(modal);
  attachModalFocus(modal);
  attachModalControls(modal);
  updateModalCount();
  return modal;
}

async function renderModalMermaid(d, target, modal) {
  const renderId = `mer-modal-${d.id.replace('.', '-')}-${modalSeq}-${Date.now()}`;
  try {
    const { svg } = await mermaid.render(renderId, d.code);
    target.innerHTML = svg;
    const svgEl = target.querySelector('svg');
    if (!svgEl) throw new Error('Mermaid produced no SVG');
    svgEl.removeAttribute('width');
    svgEl.removeAttribute('height');
    svgEl.style.width = '100%';
    svgEl.style.height = '100%';
    svgEl.style.maxWidth = 'none';
    svgEl.style.maxHeight = 'none';

    const pz = svgPanZoom(svgEl, {
      controlIconsEnabled: false,
      fit: true,
      center: true,
      minZoom: 0.2,
      maxZoom: 8,
      zoomScaleSensitivity: 0.35,
      panEnabled: true,
      onZoom: (newZoom) => {
        const info = modal.querySelector('.fc-modal-zoom-info');
        if (info) info.textContent = Math.round(newZoom * 100) + '%';
      },
    });
    modal._panzoom = pz;
    if (window.ResizeObserver) {
      const ro = new ResizeObserver(() => {
        try { pz.resize(); pz.fit(); pz.center(); } catch {}
      });
      ro.observe(modal);
      modal._resizeObserver = ro;
    }
  } catch (err) {
    console.error('Mermaid render error:', err);
    target.innerHTML = `
      <div class="fc-modal-error">
        <strong>Failed to render diagram F${escapeHtml(d.id)}</strong>
        <pre>${escapeHtml(String(err && err.message || err))}</pre>
      </div>
    `;
  }
}

function attachModalDrag(modal) {
  const header = modal.querySelector('.fc-modal-header');
  let startX = 0, startY = 0, origLeft = 0, origTop = 0, dragging = false;
  header.addEventListener('mousedown', (e) => {
    if (e.target.closest('button')) return;
    dragging = true;
    startX = e.clientX; startY = e.clientY;
    const rect = modal.getBoundingClientRect();
    origLeft = rect.left; origTop = rect.top;
    bringToFront(modal);
    document.body.style.cursor = 'grabbing';
    e.preventDefault();
  });
  window.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const dx = e.clientX - startX;
    const dy = e.clientY - startY;
    const newLeft = Math.min(Math.max(origLeft + dx, -80), window.innerWidth - 80);
    const newTop  = Math.min(Math.max(origTop + dy, 0),  window.innerHeight - 50);
    modal.style.left = newLeft + 'px';
    modal.style.top = newTop + 'px';
  });
  window.addEventListener('mouseup', () => {
    if (dragging) { dragging = false; document.body.style.cursor = ''; }
  });
}

function attachModalFocus(modal) {
  modal.addEventListener('mousedown', () => bringToFront(modal), true);
}
function bringToFront(modal) {
  document.querySelectorAll('.fc-modal').forEach(m => m.classList.remove('focused'));
  modal.classList.add('focused');
  modal.style.zIndex = String(++topZ);
}
function attachModalControls(modal) {
  modal.querySelector('button.close').addEventListener('click', () => closeModal(modal));
  modal.querySelector('button.zoom-in').addEventListener('click', () => modal._panzoom?.zoomIn());
  modal.querySelector('button.zoom-out').addEventListener('click', () => modal._panzoom?.zoomOut());
  modal.querySelector('button.zoom-reset').addEventListener('click', () => {
    if (!modal._panzoom) return;
    modal._panzoom.resetZoom();
    modal._panzoom.center();
    modal._panzoom.fit();
  });
}
function closeModal(modal) {
  try { modal._panzoom?.destroy(); } catch {}
  try { modal._resizeObserver?.disconnect(); } catch {}
  modal.remove();
  updateModalCount();
}
function updateModalCount() {
  const n = document.querySelectorAll('.fc-modal').length;
  const el = document.getElementById('modalCount');
  el.textContent = n === 1 ? '1 enlarged' : `${n} enlarged`;
  el.classList.toggle('ghost', n === 0);
}

// ─── click delegation ──────────────────────────────────────────────────
document.addEventListener('click', (e) => {
  // explicit "enlarge" buttons inside an inline figure
  const enlargeBtn = e.target.closest('button.fig-enlarge[data-fc-id]');
  if (enlargeBtn) {
    e.preventDefault();
    openFlowchartModal(enlargeBtn.dataset.fcId);
    return;
  }

  // chip click (inside a report) — scroll to inline figure if present, else open modal
  const fc = e.target.closest('a[data-fc-id]');
  if (fc) {
    e.preventDefault();
    const id = fc.dataset.fcId;
    if (inlineFigures.has(id)) {
      const node = inlineFigures.get(id);
      node.scrollIntoView({ behavior: 'smooth', block: 'start' });
      // brief highlight
      node.classList.remove('flash');
      void node.offsetWidth;
      node.classList.add('flash');
      // remove flash after animation
      setTimeout(() => node.classList.remove('flash'), 1300);
    } else {
      openFlowchartModal(id);
    }
    return;
  }

  // page-figures rail link to a fig-anchor
  const figAnchor = e.target.closest('a[data-fig-anchor]');
  if (figAnchor) {
    e.preventDefault();
    const id = figAnchor.dataset.figAnchor;
    const node = inlineFigures.get(id);
    if (node) node.scrollIntoView({ behavior: 'smooth', block: 'start' });
    return;
  }

  // report nav link
  const r = e.target.closest('a[data-report]');
  if (r) {
    e.preventDefault();
    location.hash = r.dataset.report;
    return;
  }
});

// hash routing
window.addEventListener('hashchange', () => {
  const id = location.hash.replace('#', '') || 'overview';
  // accept both bare report id and fragment-within-report (we don't currently
  // route to per-report fragments; just re-route to overview if unknown)
  if (REPORTS.find(r => r.id === id)) showReport(id);
});

// keyboard
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    const focused = document.querySelector('.fc-modal.focused');
    if (focused) closeModal(focused);
  }
});

document.getElementById('closeAllModals').addEventListener('click', () => {
  document.querySelectorAll('.fc-modal').forEach(closeModal);
});

// ─── boot ──────────────────────────────────────────────────────────────
async function boot() {
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: 'loose',
    theme: 'base',
    themeVariables: {
      fontSize: '13px',
      fontFamily: '"IBM Plex Sans", system-ui, sans-serif',
      primaryColor: '#ece4d1',
      primaryTextColor: '#181b27',
      primaryBorderColor: '#1f4e44',
      lineColor: '#4d5063',
      secondaryColor: '#dbe8d6',
      tertiaryColor: '#f6f1e6',
      noteBkgColor: '#f9eccd',
      noteBorderColor: '#c79139',
    },
    flowchart: { htmlLabels: true, curve: 'basis' },
    sequence: { showSequenceNumbers: false },
  });

  try {
    const batches = await loadFlowcharts();
    renderSidebar(batches);
    const initial = location.hash.replace('#', '') || 'overview';
    showReport(REPORTS.find(r => r.id === initial) ? initial : 'overview');
  } catch (e) {
    document.getElementById('reportContent').innerHTML =
      `<h1>Failed to boot</h1><p>${escapeHtml(e.message)}</p>
       <p>Run a static server from <code>docs/review/</code>:</p>
       <pre>cd docs/review && python3 -m http.server 8765
# then open http://localhost:8765/site/</pre>`;
  }
}

boot();
