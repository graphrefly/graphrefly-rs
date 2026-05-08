/* GraphReFly Rust Port Review — site app
 *
 * Lifecycle:
 *   1. Fetch flowcharts.md + reports list, parse into structured stores.
 *   2. Render sidebar (reports + flowchart index).
 *   3. Hash-routed report rendering (clicking a report swaps content).
 *   4. Intercept clicks on flowchart-ref links → spawn draggable modals.
 *   5. Each modal: lazy-render mermaid, wrap with svg-pan-zoom, drag/resize.
 */

// Site lives at docs/review/site/ and the canonical flowcharts.md is one dir up from review/.
const FLOWCHARTS_PATH = '../../flowcharts.md';
const REPORTS = [
  { id: 'overview',           title: 'Overview & current state',         file: '../reports-000-overview.md' },
  { id: 'm1-m2',              title: '001 — M1 + M2 (closed milestones)', file: '../reports-001-m1-and-m2.md' },
  { id: 'm3-substrate',       title: '002 — M3 Slice A + B (substrate)',  file: '../reports-002-m3-substrate.md' },
  { id: 'm3-operators',       title: '003 — M3 Slice C + D-substrate',    file: '../reports-003-m3-operators.md' },
  { id: 'm3-combinators',     title: '004 — M3 Slice D-ops + Slice E',    file: '../reports-004-m3-combinators-and-higher-order.md' },
  { id: 'm3-correctness',     title: '005 — Slice F + G + E1 + H',        file: '../reports-005-m3-correctness-and-typed-errors.md' },
];

// flowchart store: id (e.g. "7.2") → { batchTitle, title, code }
const diagrams = new Map();
let topZ = 1000;
let modalSeq = 0;

// ─── markdown helpers ─────────────────────────────────────────────────
marked.setOptions({ gfm: true, breaks: false });

// marked v9+ renderer.link receives a token object; v4 receives (href, title, text).
// We post-process the rendered HTML string to upgrade flowchart anchors — works on either version.
function renderMarkdown(md) {
  let html = marked.parse(md);
  html = html.replace(
    /<a\s+href="#fc-(\d+\.\d+)"([^>]*)>([\s\S]*?)<\/a>/g,
    (_, id, attrs, text) => {
      // strip any existing class= attr
      const cleanAttrs = attrs.replace(/\sclass="[^"]*"/g, '');
      return `<a href="#fc-${id}" class="flowchart-ref" data-fc-id="${id}" title="Click to open diagram ${id} in a draggable modal"${cleanAttrs}>${text}</a>`;
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

  // Split by lines; iterate; track current batch heading + per-section heading
  const lines = md.split('\n');
  let currentBatchTitle = null;
  let currentSection = null; // { id, title, codeLines: [] }
  let inMermaid = false;
  const batches = []; // [{ title, items: [{id, title}] }]
  let currentBatchItems = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // ## Batch heading
    let m;
    if ((m = line.match(/^## (Batch [0-9]+ — .+|Cross-references|Maintenance)$/))) {
      currentBatchTitle = m[1];
      currentBatchItems = { title: currentBatchTitle, items: [] };
      batches.push(currentBatchItems);
      continue;
    }
    // ### x.y heading — start of a diagram section
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
    // mermaid fence open
    if (currentSection && !currentSection.captured && line.trim() === '```mermaid') {
      inMermaid = true;
      continue;
    }
    // fence close
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
          <a href="#fc-${it.id}" class="diagram-link" data-fc-id="${it.id}">
            <span class="diagram-id">${it.id}</span>${escapeHtml(it.title)}
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
  const md = await fetch(r.file).then(res => {
    if (!res.ok) throw new Error(`${r.file} HTTP ${res.status}`);
    return res.text();
  });
  reportCache.set(id, md);
  return md;
}

async function showReport(id) {
  // Update active sidebar item
  document.querySelectorAll('#reportList a').forEach(a => {
    a.classList.toggle('active', a.dataset.report === id);
  });
  const container = document.getElementById('reportContent');
  container.innerHTML = '<p class="loading">Loading…</p>';
  try {
    const md = await loadReport(id);
    container.innerHTML = renderMarkdown(md);
    window.scrollTo({ top: 0, behavior: 'instant' });
  } catch (e) {
    container.innerHTML = `<p style="color:#b91c1c">Failed to load report: ${escapeHtml(e.message)}</p>`;
  }
}

// ─── flowchart modal ───────────────────────────────────────────────────
function openFlowchartModal(id, originEvent) {
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

  // Initial size + position — cascade down-and-right
  const baseWidth = 720;
  const baseHeight = 520;
  const offset = (modalSeq * 28) % 220;
  const left = Math.min(window.innerWidth - baseWidth - 20, 80 + offset);
  const top = Math.min(window.innerHeight - baseHeight - 20, 80 + offset);
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

  // Render mermaid then wrap in panzoom
  const target = modal.querySelector('.fc-mermaid');
  renderMermaidIntoTarget(d, target, modal);

  attachModalDrag(modal);
  attachModalFocus(modal);
  attachModalControls(modal);
  updateModalCount();

  return modal;
}

async function renderMermaidIntoTarget(d, target, modal) {
  // Each render needs a unique id for mermaid's internal tracking
  const renderId = `mer-${d.id.replace('.', '-')}-${modalSeq}-${Date.now()}`;
  try {
    const { svg } = await mermaid.render(renderId, d.code);
    target.innerHTML = svg;
    const svgEl = target.querySelector('svg');
    if (!svgEl) throw new Error('Mermaid produced no SVG');
    // Mermaid sometimes sets width/height attrs that block svg-pan-zoom; clear them.
    svgEl.removeAttribute('width');
    svgEl.removeAttribute('height');
    svgEl.style.width = '100%';
    svgEl.style.height = '100%';
    svgEl.style.maxWidth = 'none';
    svgEl.style.maxHeight = 'none';

    // panzoom
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
    // Re-fit on resize
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
    // skip when clicking buttons
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
    let newLeft = Math.min(Math.max(origLeft + dx, -80), window.innerWidth - 80);
    let newTop = Math.min(Math.max(origTop + dy, 0), window.innerHeight - 50);
    modal.style.left = newLeft + 'px';
    modal.style.top = newTop + 'px';
  });
  window.addEventListener('mouseup', () => {
    if (dragging) {
      dragging = false;
      document.body.style.cursor = '';
    }
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
  el.textContent = n === 1 ? '1 diagram open' : `${n} diagrams open`;
}

// ─── click delegation ──────────────────────────────────────────────────
document.addEventListener('click', (e) => {
  // flowchart reference clicks (chips in reports + sidebar diagram index)
  const fc = e.target.closest('a[data-fc-id]');
  if (fc) {
    e.preventDefault();
    openFlowchartModal(fc.dataset.fcId, e);
    return;
  }
  // report nav links
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

// ─── helpers ───────────────────────────────────────────────────────────
function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ─── boot ──────────────────────────────────────────────────────────────
async function boot() {
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: 'loose',
    theme: 'default',
    themeVariables: { fontSize: '13px' },
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
