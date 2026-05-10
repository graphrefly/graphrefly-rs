/* GraphReFly Rust audit console — v0
 *
 * Loads three JSONL files (items, rules, findings) plus meta.json from
 * `../data/`, then renders three views:
 *   - Repo Map:        D3 treemap (workspace → crate → file)
 *   - Findings Ledger: sortable + filterable table with detail drawer
 *   - Spec ⇄ Impl:     row-per-rule matrix with cite/finding counts
 *
 * No build step — d3 v7 is loaded from a CDN by index.html. The audit
 * data is the source of truth; this file is just visualization.
 */

const DATA = {
  items: [],
  files: [],
  itemsOnly: [],
  rules: [],
  findings: [],
  tests: [],
  topology: [],
  locks: [],
  flowcharts: [],
  meta: null,
};

// ─── jsonl loader ──────────────────────────────────────────────────
async function loadJsonl(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} HTTP ${res.status}`);
  const text = await res.text();
  const out = [];
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try { out.push(JSON.parse(trimmed)); }
    catch (e) { console.warn(`[${url}] bad line:`, trimmed.slice(0, 80)); }
  }
  return out;
}

async function loadAll() {
  const [items, rules, findings, tests, topology, locks, flowcharts, metaResp] = await Promise.all([
    loadJsonl("../data/items.jsonl"),
    loadJsonl("../data/rules.jsonl"),
    loadJsonl("../data/findings.jsonl").catch(() => []),
    loadJsonl("../data/tests.jsonl").catch(() => []),
    loadJsonl("../data/topology.jsonl").catch(() => []),
    loadJsonl("../data/locks.jsonl").catch(() => []),
    loadJsonl("../data/flowcharts.jsonl").catch(() => []),
    fetch("../data/meta.json").then(r => r.ok ? r.json() : null).catch(() => null),
  ]);
  DATA.items = items;
  DATA.files = items.filter(i => i.kind === "file");
  DATA.itemsOnly = items.filter(i => i.kind !== "file");
  DATA.rules = rules;
  DATA.findings = findings;
  DATA.tests = tests;
  DATA.topology = topology;
  DATA.locks = locks;
  DATA.flowcharts = flowcharts;
  DATA.meta = metaResp;
}

// ─── tab router ────────────────────────────────────────────────────
function setView(view) {
  document.querySelectorAll(".tab").forEach(t => {
    t.setAttribute("aria-selected", t.dataset.view === view ? "true" : "false");
  });
  document.querySelectorAll(".view").forEach(v => {
    v.classList.toggle("hidden", v.dataset.view !== view);
  });
  if (view === "map" && !document.querySelector("#treemap svg")) renderTreemap();
  if (view === "matrix" && !DATA._matrixBuilt) renderMatrix();
  if (view === "architecture" && !DATA._archBuilt) renderArchitecture();
  if (view === "flowcharts" && !DATA._flowBuilt) renderFlowcharts();
  history.replaceState(null, "", `#${view}`);
}

// ─── heartbeat strip ───────────────────────────────────────────────
function renderHeartbeat() {
  const m = DATA.meta;
  const open = DATA.findings.filter(f => f.status === "open");
  const byKind = (kind) => open.filter(f => f.kind === kind).length;
  const bySev  = (sev)  => open.filter(f => f.severity === sev).length;

  // Rule coverage signals
  const rulesCitedInImpl = new Set();
  for (const it of DATA.itemsOnly) {
    if (it.role !== "src") continue;  // only impl-side citations
    for (const r of it.rules_cited || []) rulesCitedInImpl.add(r);
  }
  const rulesWithTest = new Set();
  for (const t of DATA.tests) for (const r of t.covers_rules || []) rulesWithTest.add(r);
  const rulesWithEither = new Set([...rulesCitedInImpl, ...rulesWithTest]);
  const totalRules = DATA.rules.length || 1;
  const coverageBoth = Math.round((new Set([...rulesCitedInImpl].filter(r => rulesWithTest.has(r))).size) / totalRules * 100);
  const coverageEither = Math.round(rulesWithEither.size / totalRules * 100);

  const t = m?.totals || {};
  const tests = DATA.tests;
  const testsWithRules = tests.filter(x => (x.covers_rules || []).length).length;
  const testsBreadcrumbPct = tests.length ? Math.round(testsWithRules / tests.length * 100) : 0;

  const kpis = [
    { label: "Crates",     value: t.crates ?? "—", sub: `${t.src_files ?? 0} src · ${t.test_files ?? 0} test` },
    { label: "Source LOC", value: (t.src_loc ?? 0).toLocaleString(), sub: `${(t.test_loc ?? 0).toLocaleString()} test LOC` },
    { label: "Items",      value: t.items ?? "—", sub: `${t.items_unsafe ?? 0} unsafe` },
    { label: "Tests",      value: tests.length || (t.tests ?? "—"),
      sub: `${testsWithRules} cite ≥1 rule · ${testsBreadcrumbPct}% breadcrumb`,
      tone: testsBreadcrumbPct < 25 ? "warn" : "" },
    { label: "Spec rules", value: totalRules, sub: `${rulesCitedInImpl.size} impl · ${rulesWithTest.size} test · ${coverageBoth}% both` },
    { label: "Rule coverage", value: `${coverageEither}%`,
      sub: `${rulesWithEither.size} of ${totalRules} have impl OR test`,
      tone: coverageEither < 50 ? "warn" : "opp" },
    { label: "Open findings", value: open.length, sub: `${bySev("critical")} crit · ${bySev("major")} maj · ${bySev("minor")} min`, tone: open.length ? "bug" : "opp" },
    { label: "Bugs open",     value: byKind("bug"), sub: "correctness divergences", tone: byKind("bug") ? "bug" : "opp" },
    { label: "Limits / gaps", value: byKind("limit") + byKind("complete-gap"), sub: "deferred + missing coverage", tone: "warn" },
    { label: "Opportunities", value: byKind("opp"), sub: "simplification + perf", tone: "opp" },
    { label: "Flowcharts", value: DATA.flowcharts.length, sub: `${DATA.flowcharts.filter(f => (f.rules_cited || []).length).length} cite ≥1 rule`, tone: "note" },
  ];
  const ul = document.getElementById("kpis");
  ul.innerHTML = kpis.map(k => `
    <li class="kpi ${k.tone ? "tone-" + k.tone : ""}">
      <span class="kpi-label">${esc(k.label)}</span>
      <span class="kpi-value">${esc(k.value)}</span>
      <span class="kpi-sub">${esc(k.sub)}</span>
    </li>
  `).join("");
  document.getElementById("generatedAt").textContent =
    m?.generated_at ? `extracted ${m.generated_at}` : "";
}

// ─── view 1: repo map (treemap) ───────────────────────────────────
const MAP_STATE = {
  color: "findings",
  role: "src",
  selectedFile: null,
  zoomedCrate: null,  // when set, treemap shows that crate's files
  zoomedFile:  null,  // when set (and zoomedCrate is set), treemap shows that file's items
};

const CRATE_BAND_H = 26;

function renderTreemap() {
  const container = document.getElementById("treemap");
  container.innerHTML = "";
  const role = MAP_STATE.role;
  let files = DATA.files.slice();
  if (role !== "all") files = files.filter(f => f.role === role);
  if (MAP_STATE.zoomedCrate) files = files.filter(f => f.crate === MAP_STATE.zoomedCrate);

  // Aggregate findings per file
  const findingCountByFile = new Map();
  const allFindings = [...DATA.findings, ...loadDrafts()];
  for (const f of allFindings) {
    if (f.status !== "open" && f.status !== "draft") continue;
    if (!f.where) continue;
    findingCountByFile.set(f.where, (findingCountByFile.get(f.where) || 0) + 1);
  }
  for (const f of files) f._open_findings = findingCountByFile.get(f.file) || 0;

  // Three render modes:
  //   1. workspace → crate → file (default)
  //   2. crate → file              (when zoomedCrate set)
  //   3. file → item               (when zoomedFile set)
  let root;
  let mode = "workspace";
  if (MAP_STATE.zoomedFile) {
    mode = "file";
    const items = DATA.itemsOnly.filter(i => i.file === MAP_STATE.zoomedFile);
    if (items.length === 0) {
      // Empty file (or modules-only file) — show a placeholder
      container.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--ink-mute);font-style:italic">No top-level items extracted from this file. Click <strong>← Back</strong> to return.</div>`;
      updateZoomAffordance();
      return;
    }
    root = {
      name: MAP_STATE.zoomedFile,
      children: items.map(i => itemNode(i)),
    };
  } else if (MAP_STATE.zoomedCrate) {
    mode = "crate";
    root = {
      name: MAP_STATE.zoomedCrate, crate: MAP_STATE.zoomedCrate,
      children: files.map(f => fileNode(f)),
    };
  } else {
    const byCrate = d3.group(files, f => f.crate);
    root = {
      name: "workspace",
      children: Array.from(byCrate, ([crate, fs]) => ({
        name: crate, crate,
        children: fs.map(f => fileNode(f)),
      })),
    };
  }

  const rect = container.getBoundingClientRect();
  const W = Math.max(rect.width, 600);
  const H = Math.max(rect.height, 480);

  const hierarchy = d3.hierarchy(root)
    .sum(d => d.leaf ? Math.max(d.loc, 8) : 0)
    .sort((a, b) => b.value - a.value);

  d3.treemap()
    .size([W, H])
    .paddingTop(d => (!MAP_STATE.zoomedCrate && d.depth === 1) ? CRATE_BAND_H : 2)
    .paddingInner(3)
    .paddingOuter(4)
    .round(true)(hierarchy);

  const scale = colorScaleFor(MAP_STATE.color, hierarchy.leaves());

  const svg = d3.select(container).append("svg")
    .attr("viewBox", `0 0 ${W} ${H}`)
    .attr("preserveAspectRatio", "none")
    .attr("width", "100%").attr("height", "100%");

  // Wrap all rendered content in a <g> so d3.zoom can transform it.
  const g = svg.append("g").attr("class", "zoom-target");

  // d3.zoom: wheel + drag pan. We manually re-bind dblclick on cells/crate
  // bands AFTER attaching zoom so our drill-down handler still fires (zoom's
  // own dblclick step-zoom is suppressed).
  const zoomBehavior = d3.zoom()
    .scaleExtent([0.5, 12])
    .filter(event => {
      // Skip d3.zoom's default dblclick step-zoom (we use dblclick for drill-down).
      if (event.type === "dblclick") return false;
      // Otherwise let pan/zoom take any pointer / wheel — d3.zoom natively
      // suppresses 'click' events that follow a real drag, so cell-selection
      // (the click handler on .tm-cell) still fires for taps without movement.
      return !event.ctrlKey && !event.button;
    })
    .on("zoom", (event) => g.attr("transform", event.transform));
  svg.call(zoomBehavior);
  // Make the cursor a hint that the surface is pannable
  svg.style("cursor", "grab");
  svg.on("mousedown.cursor", () => svg.style("cursor", "grabbing"));
  svg.on("mouseup.cursor",   () => svg.style("cursor", "grab"));
  // Stash so the Reset button can call .transform(svg, identity)
  MAP_STATE._svg = svg;
  MAP_STATE._zoom = zoomBehavior;

  // Crate group rectangles (only when not zoomed)
  if (!MAP_STATE.zoomedCrate) {
    const crates = g.selectAll("g.crate")
      .data(hierarchy.descendants().filter(d => d.depth === 1))
      .join("g").attr("class", "crate");

    crates.append("rect")
      .attr("class", "tm-crate-rect")
      .attr("x", d => d.x0).attr("y", d => d.y0)
      .attr("width", d => d.x1 - d.x0).attr("height", d => d.y1 - d.y0)
      .attr("rx", 4).attr("ry", 4)
      .attr("fill", "var(--accent-deep)")
      .attr("stroke", "var(--paper-cool)")
      .attr("stroke-width", 1);

    // Crate band: only the top header strip filled solid; below it the cells take over
    crates.append("rect")
      .attr("class", "tm-crate-band-clip")
      .attr("x", d => d.x0).attr("y", d => d.y1 - 0)
      .attr("width", 0).attr("height", 0); // placeholder — kept for symmetry; actual band is drawn in fill above

    crates.append("text").attr("class", "tm-crate-label")
      .attr("x", d => d.x0 + 10)
      .attr("y", d => d.y0 + Math.round(CRATE_BAND_H * 0.62))
      .text(d => {
        const w = d.x1 - d.x0;
        if (w < 96) return "";  // too narrow for a label
        const max = Math.floor((w - 12) / 7.4);
        const n = d.data.crate;
        return n.length > max ? n.slice(0, max - 1) + "…" : n;
      });

    // "zoom in" affordance: double-click crate band to zoom
    crates.on("dblclick", (_, d) => {
      MAP_STATE.zoomedCrate = d.data.crate;
      renderTreemap();
      updateZoomAffordance();
    });
  }

  // Leaf cells
  const leaves = g.selectAll("g.leaf").data(hierarchy.leaves())
    .join("g").attr("class", "leaf");

  leaves.append("rect")
    .attr("class", d => "tm-cell" + (d.data.file === MAP_STATE.selectedFile ? " selected" : ""))
    .attr("data-file", d => d.data.file)
    .attr("x", d => d.x0).attr("y", d => d.y0)
    .attr("width", d => Math.max(d.x1 - d.x0, 0))
    .attr("height", d => Math.max(d.y1 - d.y0, 0))
    .attr("rx", 3).attr("ry", 3)
    .attr("fill", d => scale(d.data))
    .attr("stroke", "var(--paper-cool)")
    .attr("stroke-width", 0.6)
    .on("click", (ev, d) => {
      g.selectAll(".tm-cell").classed("selected", false);
      d3.select(ev.currentTarget).classed("selected", true);
      if (d.data.nodeKind === "item") {
        // Show the item's parent file in the sidecar
        const parent = DATA.files.find(f => f.file === d.data.file);
        if (parent) renderMapSidecar(parent, d.data.item);
      } else {
        MAP_STATE.selectedFile = d.data.file;
        renderMapSidecar(d.data.item);
      }
    })
    .on("dblclick", (_, d) => {
      if (d.data.nodeKind === "item") {
        // Items don't drill further; they navigate to source line in a future feature
        return;
      }
      if (MAP_STATE.zoomedCrate) {
        MAP_STATE.zoomedFile = d.data.file;
      } else {
        MAP_STATE.zoomedCrate = d.data.crate;
      }
      renderTreemap();
      updateZoomAffordance();
    })
    .on("mouseenter", (ev, d) => showMapTooltip(ev, d))
    .on("mousemove",  (ev, d) => showMapTooltip(ev, d))
    .on("mouseleave", () => hideMapTooltip());

  // Both labels live at the BOTTOM-LEFT of the cell so they never collide with
  // the crate-band header above. Primary (file name) on top of secondary (metrics).
  leaves.append("text")
    .attr("class", d => "tm-label" + (textColorIsDark(scale(d.data)) ? " dim" : ""))
    .attr("x", d => d.x0 + 8)
    .attr("y", d => d.y1 - 22)
    .text(d => {
      const w = d.x1 - d.x0;
      const h = d.y1 - d.y0;
      if (w < 70 || h < 38) return "";
      const max = Math.floor((w - 14) / 6.6);
      const name = d.data.name;
      return name.length > max ? name.slice(0, max - 1) + "…" : name;
    });

  leaves.append("text")
    .attr("class", d => "tm-label-sub" + (textColorIsDark(scale(d.data)) ? " dim" : ""))
    .attr("x", d => d.x0 + 8)
    .attr("y", d => d.y1 - 8)
    .text(d => {
      const w = d.x1 - d.x0;
      const h = d.y1 - d.y0;
      if (w < 110 || h < 60) return "";
      const f = d.data;
      const bits = [];
      if (f.nodeKind === "item") {
        bits.push(f.itemKind);
        if (f.visibility && f.visibility !== "priv") bits.push(f.visibility);
        bits.push(`${f.loc} loc`);
        if (f.unsafe) bits.push("unsafe");
        if (f.rules) bits.push(`${f.rules} R`);
      } else {
        bits.push(`${f.loc} loc`);
        if (f.findings) bits.push(`${f.findings} fnd`);
        if (f.rules)    bits.push(`${f.rules} R`);
      }
      return bits.join(" · ");
    });
}

function fileNode(f) {
  return {
    name: f.name + ".rs",
    crate: f.crate,
    file: f.file,
    loc: f.loc,
    rules: (f.rules_cited || []).length,
    tests: f.tests_in_file || 0,
    findings: f._open_findings,
    unsafe: f.unsafe_count || 0,
    leaf: true,
    item: f,
    nodeKind: "file",
  };
}

function itemNode(it) {
  return {
    name: (it.name || "(anon)") + (it.kind === "fn" ? "()" : ""),
    crate: it.crate,
    file: it.file,
    loc: it.loc,
    rules: (it.rules_cited || []).length,
    tests: 0,
    findings: 0,
    unsafe: it.unsafe ? 1 : 0,
    visibility: it.visibility,
    itemKind: it.kind,
    leaf: true,
    item: it,
    nodeKind: "item",
  };
}

// Heuristic: is the cell fill dark enough that we should use light text?
function textColorIsDark(rgba) {
  const m = String(rgba).match(/rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
  if (!m) return false;
  const [r, g, b] = [+m[1], +m[2], +m[3]];
  const lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  return lum < 0.55;
}

function updateZoomAffordance() {
  const btn = document.getElementById("mapReset");
  if (MAP_STATE.zoomedFile) {
    btn.textContent = "← Back to crate";
  } else if (MAP_STATE.zoomedCrate) {
    btn.textContent = "← Back to all crates";
  } else {
    btn.textContent = "Reset zoom";
  }
}

function colorScaleFor(metric, leaves) {
  if (metric === "findings") {
    const max = Math.max(1, d3.max(leaves, l => l.data.findings));
    // No-findings cells: solid forest green (a healthy "ok" tint).
    // Findings cells: red ramp scaled by count.
    return d => d.findings === 0
      ? `rgba(35, 86, 76, ${0.55 + 0.18 * Math.log2(d.loc + 8) / 14})`
      : `rgba(177, 56, 48, ${0.65 + 0.30 * (d.findings / max)})`;
  }
  if (metric === "rules") {
    const max = Math.max(1, d3.max(leaves, l => l.data.rules));
    return d => d.rules === 0
      ? "rgba(143, 145, 156, 0.45)"  // muted grey for "no breadcrumb"
      : `rgba(35, 86, 76, ${0.55 + 0.40 * (d.rules / max)})`;
  }
  if (metric === "tests") {
    const max = Math.max(1, d3.max(leaves, l => l.data.tests));
    return d => d.tests === 0
      ? "rgba(143, 145, 156, 0.40)"
      : `rgba(72, 102, 158, ${0.55 + 0.40 * (d.tests / max)})`;
  }
  // loc heat — burnt sienna ramp
  const max = Math.max(1, d3.max(leaves, l => l.data.loc));
  return d => `rgba(178, 90, 30, ${0.40 + 0.45 * Math.sqrt(d.loc / max)})`;
}

function showMapTooltip(ev, d) {
  const tip = document.getElementById("mapTooltip");
  const data = d.data;
  tip.innerHTML = `
    <div class="mt-title">${esc(data.crate)} / ${esc(data.name)}</div>
    <div class="mt-row"><span class="k">LOC</span><span>${data.loc}</span></div>
    <div class="mt-row"><span class="k">Rules cited</span><span>${data.rules}</span></div>
    <div class="mt-row"><span class="k">Tests</span><span>${data.tests}</span></div>
    <div class="mt-row"><span class="k">Open findings</span><span>${data.findings}</span></div>
    <div class="mt-row"><span class="k">Unsafe blocks</span><span>${data.unsafe}</span></div>
  `;
  tip.classList.remove("hidden");
  const pad = 12;
  tip.style.left = Math.min(window.innerWidth - 340, ev.clientX + pad) + "px";
  tip.style.top  = Math.min(window.innerHeight - 120, ev.clientY + pad) + "px";
}
function hideMapTooltip() {
  document.getElementById("mapTooltip").classList.add("hidden");
}

function renderMapSidecar(file) {
  const el = document.getElementById("mapSidecar");
  if (!file) {
    el.innerHTML = `<p class="sidecar-empty">Click a cell to inspect.</p>`;
    return;
  }
  // Items in this file
  const items = DATA.itemsOnly.filter(i => i.file === file.file);
  const itemsByKind = d3.group(items, i => i.kind);
  const kindRows = Array.from(itemsByKind, ([k, list]) => `
    <li><strong>${esc(k)}</strong> · ${list.length} ${list.length === 1 ? "item" : "items"}</li>
  `).join("");

  // Findings touching this file
  const findings = DATA.findings.filter(f => f.where === file.file);
  const findingsHtml = findings.length
    ? `<ul>${findings.map(f => `
        <li>
          <span class="chip ${f.kind}">${esc(f.kind)}</span>
          <span class="chip ${f.severity === 'critical' || f.severity === 'major' ? 'bug' : ''}">${esc(f.severity)}</span>
          ${esc(f.title)}
        </li>`).join("")}</ul>`
    : `<p class="sidecar-empty">No findings on this file.</p>`;

  // Rules cited (in src doc-comments)
  const rules = file.rules_cited || [];
  const itemRules = new Set();
  for (const it of items) for (const r of it.rules_cited || []) itemRules.add(r);
  const fileRules = new Set([...rules, ...itemRules]);
  const allRules = Array.from(fileRules).sort();
  const rulesHtml = allRules.length
    ? allRules.map(r => `<span class="chip">${esc(r)}</span>`).join("")
    : `<span class="sidecar-empty">— none cited in doc comments —</span>`;

  // Tests covering the same rules as this file (cross-reference)
  let testsHtml;
  if (file.role === "src") {
    const matchingTests = DATA.tests.filter(t =>
      (t.covers_rules || []).some(r => fileRules.has(r))
    );
    if (matchingTests.length) {
      const byFile = new Map();
      for (const t of matchingTests) {
        const arr = byFile.get(t.file) || [];
        arr.push(t);
        byFile.set(t.file, arr);
      }
      testsHtml = `<ul>${Array.from(byFile, ([f, arr]) => `
        <li><code style="font-size:11px;">${esc(f.replace(/^crates\//, ""))}</code> · ${arr.length} test${arr.length > 1 ? "s" : ""}
          <span class="muted small">(${arr.slice(0, 3).map(t => esc(t.covers_rules.join(", "))).join("; ")}${arr.length > 3 ? "; …" : ""})</span>
        </li>
      `).join("")}</ul>`;
    } else {
      testsHtml = `<p class="sidecar-empty">— no tests cite any of this file's rules —</p>`;
    }
  } else {
    // Test file: show what rules its own tests cite
    const fileTests = DATA.tests.filter(t => t.file === file.file);
    const ruleHits = new Map();
    for (const t of fileTests) for (const r of (t.covers_rules || [])) ruleHits.set(r, (ruleHits.get(r) || 0) + 1);
    if (ruleHits.size) {
      testsHtml = `<div>${Array.from(ruleHits.entries()).sort().map(([r, n]) => `<span class="chip">${esc(r)} <span class="muted small">×${n}</span></span>`).join("")}</div>`;
    } else {
      testsHtml = `<p class="sidecar-empty">— no rule citations across ${fileTests.length} test fn${fileTests.length === 1 ? "" : "s"} —</p>`;
    }
  }

  el.innerHTML = `
    <h3>${esc(file.name)}.rs</h3>
    <div class="path">${esc(file.file)}</div>
    <dl>
      <dt>Crate</dt>      <dd>${esc(file.crate)}</dd>
      <dt>Module</dt>     <dd>${esc(file.module || "—")}</dd>
      <dt>Role</dt>       <dd>${esc(file.role)}</dd>
      <dt>LOC</dt>        <dd>${file.loc} (${file.lines_total} total)</dd>
      <dt>Tests in file</dt> <dd>${file.tests_in_file || 0}${file.ignored_tests_in_file ? ` (${file.ignored_tests_in_file} ignored)` : ""}</dd>
      <dt>Unsafe</dt>     <dd>${file.unsafe_count || 0} occurrences</dd>
    </dl>
    ${file.doc_summary ? `<p style="font-style:italic; color:var(--ink-soft); font-size:12px; margin: -8px 0 12px 0;">“${esc(file.doc_summary)}”</p>` : ""}

    <h4>Items</h4>
    <ul>${kindRows || `<li class="sidecar-empty">— no public items —</li>`}</ul>

    <h4>Rules cited (in doc-comments)</h4>
    <div>${rulesHtml}</div>

    <h4>${file.role === "src" ? "Tests covering these rules" : "Rules covered by tests"}</h4>
    ${testsHtml}

    <h4>Findings on this file</h4>
    ${findingsHtml}
  `;
}

// ─── view 2: findings ledger ───────────────────────────────────────
const FINDINGS_STATE = {
  sortKey: "severity",
  sortDir: 1,            // 1 = ascending; severity ordinals make critical=0 (so asc = top)
  search: "",
  kinds:    new Set(["bug", "limit", "opp", "note", "complete-gap"]),
  statuses: new Set(["open", "draft"]),
  selectedId: null,
};

// Drafts live in localStorage so they persist across reloads. Each draft has
// the same shape as a findings.jsonl row plus status:"draft".
const DRAFTS_KEY = "graphrefly_audit_finding_drafts_v1";
function loadDrafts() {
  try { return JSON.parse(localStorage.getItem(DRAFTS_KEY) || "[]"); }
  catch { return []; }
}
function saveDrafts(drafts) { localStorage.setItem(DRAFTS_KEY, JSON.stringify(drafts)); }
function nextDraftId() {
  const all = [...DATA.findings, ...loadDrafts()].map(f => f.id || "").filter(id => /^F\d+$/.test(id));
  const max = all.reduce((m, id) => Math.max(m, +id.slice(1)), 0);
  return "F" + String(max + 1).padStart(3, "0");
}

const SEV_ORDER = { critical: 0, major: 1, minor: 2 };

function bindFindingsControls() {
  const search = document.getElementById("findingsSearch");
  search.addEventListener("input", () => {
    FINDINGS_STATE.search = search.value.trim().toLowerCase();
    renderFindings();
  });
  document.querySelectorAll('.kind-filter input[data-kind]').forEach(cb => {
    cb.addEventListener("change", () => {
      if (cb.checked) FINDINGS_STATE.kinds.add(cb.dataset.kind);
      else FINDINGS_STATE.kinds.delete(cb.dataset.kind);
      renderFindings();
    });
  });
  document.querySelectorAll('.kind-filter input[data-status]').forEach(cb => {
    cb.addEventListener("change", () => {
      if (cb.checked) FINDINGS_STATE.statuses.add(cb.dataset.status);
      else FINDINGS_STATE.statuses.delete(cb.dataset.status);
      renderFindings();
    });
  });
  document.querySelectorAll("#findingsTable th.sortable").forEach(th => {
    th.addEventListener("click", () => {
      const k = th.dataset.sort;
      if (FINDINGS_STATE.sortKey === k) FINDINGS_STATE.sortDir *= -1;
      else { FINDINGS_STATE.sortKey = k; FINDINGS_STATE.sortDir = 1; }
      renderFindings();
    });
  });
  document.getElementById("findingDetailClose").addEventListener("click", closeFindingDetail);
  document.getElementById("findingsNewBtn").addEventListener("click", openFindingAuthor);
  document.getElementById("findingsExportDrafts").addEventListener("click", exportDrafts);
  document.getElementById("findingAuthorClose").addEventListener("click", closeFindingAuthor);
  document.getElementById("findingAuthorCancel").addEventListener("click", closeFindingAuthor);
  document.getElementById("findingAuthorForm").addEventListener("submit", saveFindingDraft);

  // "/" focuses search
  document.addEventListener("keydown", (e) => {
    if (e.key === "/" && document.activeElement.tagName !== "INPUT" && document.activeElement.tagName !== "TEXTAREA") {
      e.preventDefault(); search.focus();
    }
    if (e.key === "Escape") {
      closeFindingDetail();
      closeFindingAuthor();
    }
  });
}

function openFindingAuthor() {
  const drawer = document.getElementById("findingAuthor");
  drawer.classList.remove("hidden");
  // Populate datalists once (idempotent — overwrite is fine)
  const fileList = document.getElementById("findingFileList");
  fileList.innerHTML = DATA.files.map(f => `<option value="${esc(f.file)}">`).join("");
  const ruleList = document.getElementById("findingRuleList");
  ruleList.innerHTML = DATA.rules.map(r => `<option value="${esc(r.id)}">${esc(r.title)}</option>`).join("");
  const form = document.getElementById("findingAuthorForm");
  form.reset();
  // Pre-fill where with the currently-selected file in repo map (if any)
  if (MAP_STATE.selectedFile) form.elements["where"].value = MAP_STATE.selectedFile;
  // Default severity = major
  form.elements["severity"].value = "major";
  setTimeout(() => form.elements["title"].focus(), 100);
}
function closeFindingAuthor() {
  document.getElementById("findingAuthor").classList.add("hidden");
}
function saveFindingDraft(ev) {
  ev.preventDefault();
  const fd = new FormData(ev.target);
  const today = new Date().toISOString().slice(0, 10);
  const draft = {
    id: nextDraftId(),
    kind: fd.get("kind"),
    severity: fd.get("severity"),
    title: (fd.get("title") || "").trim(),
    where: (fd.get("where") || "").trim() || null,
    where_line: fd.get("where_line") ? +fd.get("where_line") : null,
    rule: (fd.get("rule") || "").trim() || null,
    slice: (fd.get("slice") || "").trim() || null,
    evidence: (fd.get("evidence") || "").trim(),
    recommendation: (fd.get("recommendation") || "").trim() || null,
    status: "draft",
    opened_at: today,
    closed_at: null,
    supersedes: null,
    source: "drawer-draft",
  };
  if (!draft.title || !draft.evidence) return;
  const drafts = loadDrafts();
  drafts.push(draft);
  saveDrafts(drafts);
  closeFindingAuthor();
  renderFindings();
  renderHeartbeat();  // KPIs reflect new draft if status=open visible
  // Brief flash: scroll to the new row
  requestAnimationFrame(() => {
    const row = document.querySelector(`#findingsTable tbody tr[data-id="${CSS.escape(draft.id)}"]`);
    if (row) {
      row.scrollIntoView({ behavior: "smooth", block: "center" });
      row.classList.add("flash");
      setTimeout(() => row.classList.remove("flash"), 1200);
    }
  });
}
function exportDrafts() {
  const drafts = loadDrafts();
  if (drafts.length === 0) {
    alert("No drafts saved. Click '+ New finding' to add one.");
    return;
  }
  const jsonl = drafts.map(d => JSON.stringify(d)).join("\n") + "\n";
  navigator.clipboard.writeText(jsonl)
    .then(() => alert(`Copied ${drafts.length} draft${drafts.length > 1 ? "s" : ""} as JSONL. Append to data/findings.jsonl, then re-run the extractor or just reload.`))
    .catch(() => {
      // Fallback: show a textarea modal-style for manual copy
      const w = window.open("", "_blank", "width=720,height=500");
      w.document.body.innerHTML = `<pre style="white-space:pre-wrap;font:13px monospace">${jsonl.replace(/[<>&]/g, c => ({"<":"&lt;",">":"&gt;","&":"&amp;"})[c])}</pre>`;
    });
}

function renderFindings() {
  const tb = document.querySelector("#findingsTable tbody");
  const allFindings = [...DATA.findings, ...loadDrafts()];
  const rows = allFindings
    .filter(f => FINDINGS_STATE.kinds.has(f.kind))
    .filter(f => FINDINGS_STATE.statuses.has(f.status))
    .filter(f => {
      const q = FINDINGS_STATE.search;
      if (!q) return true;
      const hay = `${f.title} ${f.evidence} ${f.where} ${f.rule || ""} ${f.slice || ""}`.toLowerCase();
      return hay.includes(q);
    });
  rows.sort((a, b) => {
    const k = FINDINGS_STATE.sortKey;
    let av, bv;
    if (k === "severity") { av = SEV_ORDER[a.severity] ?? 9; bv = SEV_ORDER[b.severity] ?? 9; }
    else { av = (a[k] ?? "") + ""; bv = (b[k] ?? "") + ""; }
    return (av < bv ? -1 : av > bv ? 1 : 0) * FINDINGS_STATE.sortDir;
  });
  tb.innerHTML = rows.map(f => `
    <tr data-id="${esc(f.id)}" class="${f.id === FINDINGS_STATE.selectedId ? 'selected' : ''}">
      <td><span class="severity-pill ${esc(f.severity)}">${esc(f.severity)}</span></td>
      <td><span class="kind-pill" data-kind="${esc(f.kind)}">${esc(f.kind)}</span></td>
      <td>${esc(f.title)}</td>
      <td>${f.rule ? `<span class="rule-chip clickable" data-rule="${esc(f.rule)}">${esc(f.rule)}</span>` : '<span class="sidecar-empty">—</span>'}</td>
      <td class="where-cell">${
        f.where
          ? `<a class="file-link" data-file="${esc(f.where)}" href="#">${esc(f.where)}</a>`
          : "—"
      }</td>
      <td>${esc(f.slice || "—")}</td>
      <td>${esc(f.opened_at || "—")}</td>
      <td><span class="status-tag ${esc(f.status)}">${esc(f.status)}</span></td>
    </tr>
  `).join("");
  document.getElementById("findingsEmpty").classList.toggle("hidden", rows.length > 0);
  // Bind row clicks. Stop propagation on links/chips so they jump-link instead
  // of opening the detail drawer.
  tb.querySelectorAll("tr").forEach(tr => {
    tr.addEventListener("click", (ev) => {
      const link = ev.target.closest("a.file-link");
      if (link) { ev.preventDefault(); ev.stopPropagation(); jumpToRepoMap(link.dataset.file); return; }
      const chip = ev.target.closest(".rule-chip.clickable");
      if (chip) { ev.stopPropagation(); jumpToMatrixRule(chip.dataset.rule); return; }
      openFindingDetail(tr.dataset.id);
    });
  });
}

function openFindingDetail(id) {
  const all = [...DATA.findings, ...loadDrafts()];
  const f = all.find(x => x.id === id);
  if (!f) return;
  FINDINGS_STATE.selectedId = id;
  document.querySelectorAll("#findingsTable tbody tr").forEach(tr => {
    tr.classList.toggle("selected", tr.dataset.id === id);
  });
  const drawer = document.getElementById("findingDetail");
  drawer.classList.remove("hidden");
  drawer.querySelector(".detail-body").innerHTML = `
    <h3>${esc(f.title)}</h3>
    <div class="meta-row">
      <span class="kind-pill" data-kind="${esc(f.kind)}">${esc(f.kind)}</span>
      <span class="severity-pill ${esc(f.severity)}">${esc(f.severity)}</span>
      <span class="status-tag ${esc(f.status)}">${esc(f.status)}</span>
      ${f.rule ? `<span class="rule-chip">${esc(f.rule)}</span>` : ""}
    </div>
    <dl>
      <dt>ID</dt>          <dd><code>${esc(f.id)}</code></dd>
      <dt>Where</dt>       <dd><code>${esc(f.where || "—")}</code>${f.where_line ? `:${f.where_line}` : ""}</dd>
      <dt>Slice</dt>       <dd>${esc(f.slice || "—")}</dd>
      <dt>Opened</dt>      <dd>${esc(f.opened_at || "—")}</dd>
      <dt>Closed</dt>      <dd>${esc(f.closed_at || "—")}</dd>
      <dt>Source</dt>      <dd>${esc(f.source || "manual")}</dd>
    </dl>
    <h4>Evidence</h4>
    <p>${esc(f.evidence || "—")}</p>
    <h4>Recommendation</h4>
    <p>${esc(f.recommendation || "—")}</p>
  `;
}
function closeFindingDetail() {
  document.getElementById("findingDetail").classList.add("hidden");
  FINDINGS_STATE.selectedId = null;
  document.querySelectorAll("#findingsTable tbody tr").forEach(tr => tr.classList.remove("selected"));
}

// ─── view 3: spec ⇄ impl matrix ───────────────────────────────────
const MATRIX_STATE = {
  sortKey: "id",
  sortDir: 1,
  search: "",
  unimplOnly: false,
  untestedOnly: false,
  openBugOnly: false,
  collapsedSections: new Set(),
};

function bindMatrixControls() {
  document.getElementById("matrixSearch").addEventListener("input", (e) => {
    MATRIX_STATE.search = e.target.value.trim().toLowerCase();
    renderMatrix();
  });
  document.getElementById("matrixUnimpl").addEventListener("change", (e) => {
    MATRIX_STATE.unimplOnly = e.target.checked; renderMatrix();
  });
  document.getElementById("matrixUntested").addEventListener("change", (e) => {
    MATRIX_STATE.untestedOnly = e.target.checked; renderMatrix();
  });
  document.getElementById("matrixOpenBug").addEventListener("change", (e) => {
    MATRIX_STATE.openBugOnly = e.target.checked; renderMatrix();
  });
  document.querySelectorAll("#matrixTable th.sortable").forEach(th => {
    th.addEventListener("click", () => {
      const k = th.dataset.sort;
      if (MATRIX_STATE.sortKey === k) MATRIX_STATE.sortDir *= -1;
      else { MATRIX_STATE.sortKey = k; MATRIX_STATE.sortDir = 1; }
      renderMatrix();
    });
  });
  document.getElementById("matrixExpandAll").addEventListener("click", () => {
    MATRIX_STATE.collapsedSections.clear();
    renderMatrix();
  });
  document.getElementById("matrixCollapseAll").addEventListener("click", () => {
    // Collect all currently-displayed sections, mark them collapsed
    const sections = new Set();
    for (const r of DATA.rules) sections.add(r.section || "—");
    MATRIX_STATE.collapsedSections = sections;
    renderMatrix();
  });
}

function renderMatrix() {
  DATA._matrixBuilt = true;
  // Build per-rule aggregates
  const byRule = new Map();
  for (const r of DATA.rules) byRule.set(r.id, {
    ...r,
    cites: 0,        // doc-comment citations on src items
    files: new Set(),
    tests: 0,        // tests citing this rule (active or ignored)
    activeTests: 0,
    testFiles: new Set(),
    findings: 0,
    openBugs: 0,
  });

  for (const it of DATA.itemsOnly) {
    if (it.role !== "src") continue;
    for (const rid of it.rules_cited || []) {
      const row = byRule.get(rid);
      if (row) { row.cites += 1; row.files.add(it.file); }
    }
  }
  for (const t of DATA.tests) {
    for (const rid of t.covers_rules || []) {
      const row = byRule.get(rid);
      if (!row) continue;
      row.tests += 1;
      if (t.status === "active") row.activeTests += 1;
      row.testFiles.add(t.file);
    }
  }
  for (const f of DATA.findings) {
    if (!f.rule) continue;
    const row = byRule.get(f.rule);
    if (!row) continue;
    row.findings += 1;
    if (f.status === "open" && f.kind === "bug") row.openBugs += 1;
  }

  let rows = Array.from(byRule.values());
  if (MATRIX_STATE.unimplOnly)   rows = rows.filter(r => r.cites === 0);
  if (MATRIX_STATE.untestedOnly) rows = rows.filter(r => r.tests === 0);
  if (MATRIX_STATE.openBugOnly)  rows = rows.filter(r => r.openBugs > 0);
  if (MATRIX_STATE.search) {
    const q = MATRIX_STATE.search;
    rows = rows.filter(r => `${r.id} ${r.title} ${r.section}`.toLowerCase().includes(q));
  }

  rows.sort((a, b) => {
    const k = MATRIX_STATE.sortKey;
    let av, bv;
    if (k === "id") {
      av = ruleSortKey(a.id); bv = ruleSortKey(b.id);
      for (let i = 0; i < Math.max(av.length, bv.length); i++) {
        const ai = av[i] ?? -1, bi = bv[i] ?? -1;
        if (ai !== bi) return (ai < bi ? -1 : 1) * MATRIX_STATE.sortDir;
      }
      return 0;
    }
    if (k === "health") {
      av = healthScore(a); bv = healthScore(b);
    } else if (k === "cites" || k === "findings" || k === "tests") {
      av = a[k]; bv = b[k];
    } else {
      av = (a[k] ?? "") + ""; bv = (b[k] ?? "") + "";
    }
    return (av < bv ? -1 : av > bv ? 1 : 0) * MATRIX_STATE.sortDir;
  });

  // Group by section header (only when sorted by id — otherwise grouping
  // would interleave with the user's ordering).
  const groupBySection = MATRIX_STATE.sortKey === "id" && !MATRIX_STATE.search;
  const tb = document.querySelector("#matrixTable tbody");
  if (!groupBySection) {
    tb.innerHTML = rows.map(matrixRowHtml).join("");
    bindSectionToggles();
    return;
  }

  // Walk rows and emit a section-header row each time the section changes
  let lastSection = null;
  let html = "";
  // Pre-compute per-section aggregates so the header can show summary counts
  const sectionAgg = new Map();
  for (const r of rows) {
    const k = r.section || "—";
    const agg = sectionAgg.get(k) || { rules: 0, withBoth: 0, openBugs: 0, untested: 0 };
    agg.rules += 1;
    if (r.cites > 0 && r.tests > 0) agg.withBoth += 1;
    if (r.openBugs > 0) agg.openBugs += 1;
    if (r.tests === 0) agg.untested += 1;
    sectionAgg.set(k, agg);
  }

  for (const r of rows) {
    const sec = r.section || "—";
    if (sec !== lastSection) {
      lastSection = sec;
      const agg = sectionAgg.get(sec);
      const collapsed = MATRIX_STATE.collapsedSections.has(sec);
      html += `<tr class="section-header${collapsed ? " collapsed" : ""}" data-section="${esc(sec)}">
        <td colspan="7">
          <span class="toggle">▾</span>
          ${esc(sec)}
          <span class="section-counts">${agg.rules} rule${agg.rules === 1 ? "" : "s"}
            · ${agg.withBoth} fully covered
            ${agg.openBugs ? ` · <span style="color:var(--bug)">${agg.openBugs} open bug${agg.openBugs > 1 ? "s" : ""}</span>` : ""}
            · ${agg.untested} untested
          </span>
        </td>
      </tr>`;
    }
    if (!MATRIX_STATE.collapsedSections.has(sec)) {
      html += matrixRowHtml(r);
    }
  }
  tb.innerHTML = html;
  bindSectionToggles();
}

function matrixRowHtml(r) {
  const score = healthScore(r);
  const status = healthStatus(r);
  const rowCls = [
    r.openBugs > 0 ? "has-bug" : "",
    r.cites === 0 ? "unimpl" : "",
    r.tests === 0 ? "untested" : "",
  ].filter(Boolean).join(" ");
  const flows = (DATA.flowcharts || []).filter(f => (f.rules_cited || []).includes(r.id));
  const flowChip = flows.length
    ? ` <a class="flow-jump" data-fc-id="${esc(flows[0].id)}" title="Jump to flowchart F${esc(flows[0].id)} — ${esc(flows[0].title)}">📊 F${esc(flows[0].id)}</a>`
    : "";
  return `
    <tr class="${rowCls}">
      <td><span class="rule-chip">${esc(r.id)}</span></td>
      <td>${esc(r.title)}${flowChip}</td>
      <td><span class="muted">${esc(r.section || "—")}</span></td>
      <td class="num">${r.cites}${r.files.size > 1 ? ` <span class="muted">/ ${r.files.size}f</span>` : ""}</td>
      <td class="num">${r.tests}${r.tests > 0 ? ` <span class="muted">/ ${r.testFiles.size}f</span>` : ""}</td>
      <td class="num">${r.findings}${r.openBugs ? ` <span class="kind-pill" data-kind="bug">${r.openBugs} bug</span>` : ""}</td>
      <td>
        <span class="health-bar ${status.tone}" style="--health:${score}%"></span>
        <span class="health-label">${esc(status.label)}</span>
      </td>
    </tr>
  `;
}

function bindSectionToggles() {
  document.querySelectorAll("#matrixTable tr.section-header").forEach(tr => {
    tr.addEventListener("click", () => {
      const sec = tr.dataset.section;
      if (MATRIX_STATE.collapsedSections.has(sec)) MATRIX_STATE.collapsedSections.delete(sec);
      else MATRIX_STATE.collapsedSections.add(sec);
      renderMatrix();
    });
  });
  // Make matrix rule rows clickable → highlight
  document.querySelectorAll("#matrixTable tbody tr:not(.section-header) td:first-child .rule-chip")
    .forEach(chip => {
      chip.classList.add("clickable");
      chip.addEventListener("click", (ev) => {
        ev.stopPropagation();
        document.querySelectorAll("#matrixTable tbody tr.rule-selected").forEach(t => t.classList.remove("rule-selected"));
        chip.closest("tr").classList.add("rule-selected");
      });
    });
  // Flow-jump chips → switch to Flowcharts tab and select the flowchart
  document.querySelectorAll("#matrixTable a.flow-jump").forEach(a => {
    a.addEventListener("click", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      FLOW_STATE.selectedId = a.dataset.fcId;
      setView("flowcharts");
    });
  });
}

// Health scoring buckets:
//   open bugs   → bug (red, low %)
//   has impl + has tests → opp (green, high %)
//   has impl, no tests   → warn (amber)
//   no impl, has tests   → note (blue) — tests cover but no breadcrumb
//   no impl, no tests    → warn dim (the big completeness pile)
function healthStatus(r) {
  if (r.openBugs > 0) return { tone: "bug", label: `${r.openBugs} open bug${r.openBugs > 1 ? "s" : ""}` };
  if (r.cites > 0 && r.tests > 0) return { tone: "opp", label: `${r.cites} impl · ${r.tests} test` };
  if (r.cites > 0 && r.tests === 0) return { tone: "warn", label: `${r.cites} impl · no test` };
  if (r.cites === 0 && r.tests > 0) return { tone: "note", label: `${r.tests} test · no impl breadcrumb` };
  return { tone: "warn", label: "no breadcrumb" };
}
function healthScore(r) {
  if (r.openBugs > 0) return Math.min(95, 30 + r.openBugs * 35);
  if (r.cites > 0 && r.tests > 0) return Math.min(100, 60 + r.tests * 8 + r.cites * 5);
  if (r.cites > 0 && r.tests === 0) return Math.min(85, 30 + r.cites * 10);
  if (r.cites === 0 && r.tests > 0) return Math.min(75, 25 + r.tests * 8);
  return 8;
}

function ruleSortKey(rid) {
  const out = [];
  for (const p of rid.replace(/^R/, "").split(".")) {
    if (/^\d+$/.test(p)) out.push(parseInt(p));
    else out.push(p.charCodeAt(0) + 1000); // letters after numbers
  }
  return out;
}
function healthScore(r) {
  if (r.openBugs > 0) return Math.min(95, r.openBugs * 35);
  if (r.cites === 0)  return 12;
  return Math.min(100, 30 + r.cites * 14);
}

function showTopoTooltip(ev, title, rows) {
  const tip = document.getElementById("mapTooltip");
  tip.innerHTML = `
    <div class="mt-title">${esc(title)}</div>
    ${rows.map(([k, v]) => `<div class="mt-row"><span class="k">${esc(k)}</span><span>${esc(v)}</span></div>`).join("")}
  `;
  tip.classList.remove("hidden");
  positionTopoTooltip(ev);
}
function positionTopoTooltip(ev) {
  const tip = document.getElementById("mapTooltip");
  const pad = 12;
  tip.style.left = Math.min(window.innerWidth - 320, ev.clientX + pad) + "px";
  tip.style.top  = Math.min(window.innerHeight - 120, ev.clientY + pad) + "px";
}
function hideTopoTooltip() {
  document.getElementById("mapTooltip").classList.add("hidden");
}

// ─── view 4: architecture (force-graph + locks table) ───────────────
const ARCH_STATE = {
  edgeKind: "all",
  color: "locks",
  selectedCrate: null,
  acquisitionsOnly: true,
  sortKey: "crate",
  sortDir: 1,
};

function bindArchControls() {
  document.getElementById("topoEdgeKind").addEventListener("change", (e) => {
    ARCH_STATE.edgeKind = e.target.value; renderArchitecture();
  });
  document.getElementById("topoColor").addEventListener("change", (e) => {
    ARCH_STATE.color = e.target.value; renderArchitecture();
  });
  document.getElementById("locksOnlyCore").addEventListener("change", (e) => {
    ARCH_STATE.acquisitionsOnly = e.target.checked; renderLocksTable();
  });
  document.getElementById("archReset").addEventListener("click", () => {
    ARCH_STATE.selectedCrate = null;
    ARCH_STATE.edgeKind = "all";
    ARCH_STATE.color = "locks";
    document.getElementById("topoEdgeKind").value = "all";
    document.getElementById("topoColor").value = "locks";
    // Reset pan/zoom transform too
    if (ARCH_STATE._svg && ARCH_STATE._zoom) {
      ARCH_STATE._svg.transition().duration(180).call(ARCH_STATE._zoom.transform, d3.zoomIdentity);
    }
    renderArchitecture();
  });
  document.querySelectorAll("#locksTable th.sortable").forEach(th => {
    th.addEventListener("click", () => {
      const k = th.dataset.sort;
      if (ARCH_STATE.sortKey === k) ARCH_STATE.sortDir *= -1;
      else { ARCH_STATE.sortKey = k; ARCH_STATE.sortDir = 1; }
      renderLocksTable();
    });
  });
}

function renderArchitecture() {
  DATA._archBuilt = true;
  renderTopologyGraph();
  renderLocksTable();
}

function renderTopologyGraph() {
  const container = document.getElementById("topoGraph");
  container.innerHTML = "";

  // Aggregate per-crate stats
  const crates = (DATA.meta?.crates || []).slice();
  if (crates.length === 0) {
    container.innerHTML = `<p class="empty">No topology data — re-run the extractor.</p>`;
    return;
  }
  const locsByCrate = new Map();
  const findingsByCrate = new Map();
  const lockOpsByCrate = new Map();
  for (const f of DATA.files) {
    if (f.role !== "src") continue;
    locsByCrate.set(f.crate, (locsByCrate.get(f.crate) || 0) + (f.loc || 0));
  }
  for (const f of DATA.findings) {
    if (f.status !== "open" || !f.where) continue;
    const m = f.where.match(/^crates\/([^/]+)/);
    if (m) findingsByCrate.set(m[1], (findingsByCrate.get(m[1]) || 0) + 1);
  }
  for (const l of DATA.locks) {
    if (l.role !== "src") continue;
    if (l.op === "new") continue;  // construction, not acquisition
    lockOpsByCrate.set(l.crate, (lockOpsByCrate.get(l.crate) || 0) + 1);
  }

  const nodes = crates.map(c => ({
    id: c,
    loc:      locsByCrate.get(c)      || 0,
    findings: findingsByCrate.get(c)  || 0,
    locks:    lockOpsByCrate.get(c)   || 0,
  }));

  // Filter edges + remap from/to → source/target (d3.forceLink convention)
  const nodeIds = new Set(nodes.map(n => n.id));
  let edges = DATA.topology
    .filter(e => ARCH_STATE.edgeKind === "all" || e.kind === ARCH_STATE.edgeKind)
    .filter(e => nodeIds.has(e.from) && nodeIds.has(e.to))
    .map(e => ({ source: e.from, target: e.to, kind: e.kind, count: e.count, files: e.files }));

  // Color scale per metric
  const metricKey = ARCH_STATE.color;
  const maxMetric = Math.max(1, d3.max(nodes, n => n[metricKey]) || 1);
  const fill = (n) => {
    const v = n[metricKey];
    if (metricKey === "findings" && v > 0) {
      return `rgba(177, 56, 48, ${0.55 + 0.40 * v / maxMetric})`;
    }
    if (metricKey === "locks") {
      return `rgba(31, 78, 68, ${0.45 + 0.45 * v / maxMetric})`;
    }
    return `rgba(178, 90, 30, ${0.40 + 0.45 * Math.sqrt(v / maxMetric)})`;
  };
  const radius = (n) => 18 + Math.sqrt(n.loc) / 4;  // 18px min, scales with sqrt(loc)

  const rect = container.getBoundingClientRect();
  const W = Math.max(rect.width, 480);
  const H = Math.max(rect.height, 380);

  const svg = d3.select(container).append("svg")
    .attr("viewBox", `0 0 ${W} ${H}`)
    .attr("preserveAspectRatio", "xMidYMid meet")
    .attr("width", "100%").attr("height", "100%");

  // Arrow markers (one per kind tone)
  const defs = svg.append("defs");
  defs.append("marker")
    .attr("id", "arrow-use").attr("viewBox", "0 -5 10 10")
    .attr("refX", 14).attr("refY", 0).attr("markerWidth", 8).attr("markerHeight", 8)
    .attr("orient", "auto").append("path").attr("d", "M0,-4L10,0L0,4Z").attr("fill", "var(--accent)");
  defs.append("marker")
    .attr("id", "arrow-ref").attr("viewBox", "0 -5 10 10")
    .attr("refX", 14).attr("refY", 0).attr("markerWidth", 7).attr("markerHeight", 7)
    .attr("orient", "auto").append("path").attr("d", "M0,-4L10,0L0,4Z").attr("fill", "var(--ink-mute)");

  // Zoom/pan target — everything that should scale lives inside this <g>.
  const zoomG = svg.append("g").attr("class", "topo-zoom-target");
  const linkG = zoomG.append("g").attr("class", "links");
  const nodeG = zoomG.append("g").attr("class", "nodes");

  // d3.zoom: wheel zoom + drag pan. We don't filter mousedown so dragging
  // anywhere (background, edges, between nodes) pans. Click-to-select on
  // nodes still fires because d3.zoom suppresses click-after-drag natively.
  // Node-drag (the d3.drag() below) consumes its own pointer events first
  // via stopPropagation, so dragging a node moves it instead of panning.
  const zoomBehavior = d3.zoom()
    .scaleExtent([0.4, 6])
    .filter(event => {
      if (event.type === "dblclick") return false;       // dblclick reserved for node interactions
      return !event.ctrlKey && !event.button;
    })
    .on("zoom", (event) => zoomG.attr("transform", event.transform));
  svg.call(zoomBehavior);
  svg.style("cursor", "grab");
  svg.on("mousedown.cursor", () => svg.style("cursor", "grabbing"));
  svg.on("mouseup.cursor",   () => svg.style("cursor", "grab"));
  ARCH_STATE._svg = svg;
  ARCH_STATE._zoom = zoomBehavior;

  // Edge stroke-width scales with log(count) so a 13-import edge is visibly
  // heavier than a 1-import edge, but doesn't dominate.
  const edgeWidth = (d) => {
    const base = d.kind === "use" ? 1.4 : 0.8;
    return base + Math.log2((d.count || 1) + 1) * 0.6;
  };
  const link = linkG.selectAll("line")
    .data(edges).join("line")
    .attr("class", d => "topo-link kind-" + d.kind)
    .attr("stroke", d => d.kind === "use" ? "var(--accent)" : "var(--ink-mute)")
    .attr("stroke-width", edgeWidth)
    .attr("stroke-opacity", d => d.kind === "use" ? 0.55 : 0.30)
    .attr("stroke-dasharray", d => d.kind === "ref" ? "4,3" : null)
    .attr("marker-end", d => `url(#arrow-${d.kind})`)
    .on("mouseenter", (ev, d) => showTopoTooltip(ev,
      `${d.source.id || d.source} → ${d.target.id || d.target}`,
      [["kind", d.kind], ["count", d.count], ["files", d.files.length]]))
    .on("mousemove",  (ev) => positionTopoTooltip(ev))
    .on("mouseleave", hideTopoTooltip);

  const node = nodeG.selectAll("g.crate-node")
    .data(nodes, d => d.id)
    .join("g").attr("class", "crate-node")
    .style("cursor", "pointer")
    .on("click", (_, d) => {
      ARCH_STATE.selectedCrate = ARCH_STATE.selectedCrate === d.id ? null : d.id;
      svg.selectAll("g.crate-node circle").classed("selected", n => n.id === ARCH_STATE.selectedCrate);
      renderLocksTable();
    });

  node.append("circle")
    .attr("r", radius)
    .attr("fill", fill)
    .attr("stroke", "var(--paper-cool)")
    .attr("stroke-width", 2)
    .on("mouseenter", (ev, d) => showTopoTooltip(ev, d.id, [
      ["LOC",      d.loc.toLocaleString()],
      ["Locks",    d.locks],
      ["Findings", d.findings],
    ]))
    .on("mousemove",  (ev) => positionTopoTooltip(ev))
    .on("mouseleave", hideTopoTooltip);

  node.append("text").attr("class", "crate-label")
    .attr("text-anchor", "middle").attr("dy", "0.32em")
    .text(d => d.id.replace(/^graphrefly-/, "")); // shorten

  // Numeric badge below name
  node.append("text").attr("class", "crate-stat")
    .attr("text-anchor", "middle").attr("y", d => radius(d) + 14)
    .text(d => {
      if (metricKey === "loc")      return d.loc + " loc";
      if (metricKey === "findings") return d.findings + " open";
      return d.locks + " locks";
    });

  // Force simulation
  const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(edges).id(d => d.id).distance(140).strength(0.5))
    .force("charge", d3.forceManyBody().strength(-360))
    .force("center", d3.forceCenter(W / 2, H / 2))
    .force("collide", d3.forceCollide().radius(d => radius(d) + 12))
    .on("tick", () => {
      link
        .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
      node.attr("transform", d => `translate(${d.x},${d.y})`);
    });

  // Drag — d3.drag's filter excludes events already claimed by d3.zoom, so
  // node-drag wins over canvas-pan when the pointer starts on a node circle.
  node.call(d3.drag()
    .on("start", (event, d) => {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      d.fx = d.x; d.fy = d.y;
      event.sourceEvent.stopPropagation();  // don't let d3.zoom also see the mousedown
    })
    .on("drag", (event, d) => { d.fx = event.x; d.fy = event.y; })
    .on("end", (event, d) => {
      if (!event.active) simulation.alphaTarget(0);
      d.fx = null; d.fy = null;
    }));
}

function renderLocksTable() {
  const tb = document.querySelector("#locksTable tbody");
  let rows = DATA.locks.filter(l => l.role === "src");
  if (ARCH_STATE.acquisitionsOnly) rows = rows.filter(l => l.op !== "new");
  if (ARCH_STATE.selectedCrate) rows = rows.filter(l => l.crate === ARCH_STATE.selectedCrate);

  rows.sort((a, b) => {
    const k = ARCH_STATE.sortKey;
    let av = a[k], bv = b[k];
    if (k === "line") { av = +av; bv = +bv; }
    return (av < bv ? -1 : av > bv ? 1 : 0) * ARCH_STATE.sortDir;
  });

  document.getElementById("locksHeader").textContent =
    ARCH_STATE.selectedCrate ? `Locks in ${ARCH_STATE.selectedCrate}` : "Lock acquisitions";
  document.getElementById("locksCount").textContent =
    `${rows.length} ${rows.length === 1 ? "site" : "sites"}` +
    (ARCH_STATE.selectedCrate ? "" : ` across ${new Set(rows.map(r => r.crate)).size} crates`);

  // Cap at 200 rows for perf; the table is for inspection, not exhaustive lists
  const view = rows.slice(0, 200);
  tb.innerHTML = view.map(l => `
    <tr>
      <td><span class="rule-chip">${esc(l.crate.replace(/^graphrefly-/, ""))}</span></td>
      <td><code>${esc(l.op)}</code></td>
      <td>${esc(l.lock_type)}</td>
      <td class="where-cell"><a class="file-link" data-file="${esc(l.file)}" href="#">${esc(l.file.replace(/^crates\//, ""))}</a></td>
      <td class="num">${esc(l.line)}</td>
    </tr>
  `).join("");
  if (rows.length > view.length) {
    tb.innerHTML += `<tr><td colspan="5" class="muted small" style="text-align:center; font-style:italic;">… ${rows.length - view.length} more · click a crate node to filter</td></tr>`;
  }
  tb.querySelectorAll("a.file-link").forEach(a => {
    a.addEventListener("click", (ev) => { ev.preventDefault(); jumpToRepoMap(a.dataset.file); });
  });
}

// ─── view 5: flowcharts (mermaid + svg-pan-zoom) ────────────────────
const FLOW_STATE = {
  search: "",
  kind: "all",
  selectedId: null,
  _panZoom: null,
};

function bindFlowControls() {
  document.getElementById("flowSearch").addEventListener("input", (e) => {
    FLOW_STATE.search = e.target.value.trim().toLowerCase();
    renderFlowRail();
  });
  document.getElementById("flowKind").addEventListener("change", (e) => {
    FLOW_STATE.kind = e.target.value;
    renderFlowRail();
  });
}

// The splitter sits between the canvas and the prose pane. Drag it up to grow
// the annotations area, drag it down to shrink. Bounds: ≥60px and leaving
// ≥180px for the canvas so the diagram stays usable.
function attachFlowSplitter() {
  const splitter = document.getElementById("flowSplitter");
  if (!splitter) return;
  const stage = document.getElementById("flowStage");
  const prose = stage?.querySelector(".flow-prose");
  if (!stage || !prose) return;

  let dragging = false;
  let startY = 0;
  let startH = 0;

  splitter.addEventListener("mousedown", (e) => {
    dragging = true;
    startY = e.clientY;
    startH = prose.getBoundingClientRect().height;
    splitter.classList.add("dragging");
    document.body.style.cursor = "ns-resize";
    e.preventDefault();
  });
  // Use bubbling listeners on window so the drag survives quick mouseouts
  window.addEventListener("mousemove", (e) => {
    if (!dragging) return;
    const dy = startY - e.clientY;          // up = bigger prose
    const stageH = stage.clientHeight;
    const newH = Math.max(60, Math.min(stageH - 180, startH + dy));
    prose.style.height = newH + "px";
    prose.style.maxHeight = "none";
  });
  window.addEventListener("mouseup", () => {
    if (!dragging) return;
    dragging = false;
    splitter.classList.remove("dragging");
    document.body.style.cursor = "";
    // Persist the dragged height so it sticks across flowchart switches + reloads
    const h = prose.getBoundingClientRect().height;
    localStorage.setItem("audit_flow_prose_height", String(Math.round(h)));
    // Re-fit the diagram to the new canvas size
    try { FLOW_STATE._panZoom?.resize(); FLOW_STATE._panZoom?.fit(); FLOW_STATE._panZoom?.center(); } catch {}
  });
  // Double-click resets to the default
  splitter.addEventListener("dblclick", () => {
    prose.style.height = "";
    prose.style.maxHeight = "";
    localStorage.removeItem("audit_flow_prose_height");
    try { FLOW_STATE._panZoom?.resize(); FLOW_STATE._panZoom?.fit(); FLOW_STATE._panZoom?.center(); } catch {}
  });
}

function renderFlowcharts() {
  DATA._flowBuilt = true;
  // Init mermaid once with a palette matching the audit site
  if (!FLOW_STATE._mermaidInit && window.mermaid) {
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: "loose",
      theme: "base",
      themeVariables: {
        fontSize: "13px",
        fontFamily: '"IBM Plex Sans", system-ui, sans-serif',
        primaryColor: "#ece4d1",
        primaryTextColor: "#181b27",
        primaryBorderColor: "#1f4e44",
        lineColor: "#4d5063",
        secondaryColor: "#dbe8d6",
        tertiaryColor: "#f6f1e6",
        noteBkgColor: "#f9eccd",
        noteBorderColor: "#c79139",
      },
      flowchart: { htmlLabels: true, curve: "basis" },
    });
    FLOW_STATE._mermaidInit = true;
  }
  renderFlowRail();
  // Auto-select the first flowchart on initial entry
  if (!FLOW_STATE.selectedId && DATA.flowcharts.length) {
    selectFlowchart(DATA.flowcharts[0].id);
  } else if (FLOW_STATE.selectedId) {
    selectFlowchart(FLOW_STATE.selectedId);
  }
}

function renderFlowRail() {
  const rail = document.getElementById("flowRail");
  if (DATA.flowcharts.length === 0) {
    rail.innerHTML = `<p class="muted small">No flowcharts found. Re-run the extractor.</p>`;
    return;
  }
  // Filter
  const q = FLOW_STATE.search;
  const kindFilter = FLOW_STATE.kind;
  const visible = DATA.flowcharts.filter(f => {
    if (kindFilter !== "all" && f.kind !== kindFilter) return false;
    if (!q) return true;
    return (`${f.id} ${f.title} ${f.prose}`).toLowerCase().includes(q);
  });

  // Group by batch
  const byBatch = new Map();
  for (const f of visible) {
    const list = byBatch.get(f.batch) || [];
    list.push(f);
    byBatch.set(f.batch, list);
  }

  rail.innerHTML = Array.from(byBatch, ([batch, items], idx) => `
    <details ${idx === 0 || items.some(it => it.id === FLOW_STATE.selectedId) ? "open" : ""}>
      <summary>${esc(batch || "—")}<span class="batch-count">${items.length}</span></summary>
      ${items.map(f => `
        <a class="flow-link ${f.id === FLOW_STATE.selectedId ? "active" : ""}" data-fc-id="${esc(f.id)}" title="${esc(f.title)}">
          <span class="flow-id">F${esc(f.id)}</span>
          <span style="flex:1; min-width: 0; overflow: hidden; text-overflow: ellipsis;">${esc(f.title)}</span>
          <span class="kind-tag">${esc(f.kind)}</span>
        </a>
      `).join("")}
    </details>
  `).join("");

  rail.querySelectorAll(".flow-link[data-fc-id]").forEach(a => {
    a.addEventListener("click", (ev) => {
      ev.preventDefault();
      selectFlowchart(a.dataset.fcId);
    });
  });
}

async function selectFlowchart(id) {
  const f = DATA.flowcharts.find(x => x.id === id);
  if (!f) return;
  FLOW_STATE.selectedId = id;
  // Update active highlight in rail
  document.querySelectorAll("#flowRail .flow-link").forEach(a => {
    a.classList.toggle("active", a.dataset.fcId === id);
  });
  // Render canvas
  const stage = document.getElementById("flowStage");
  const ruleChips = (f.rules_cited || []).map(r =>
    `<span class="rule-chip clickable" data-rule="${esc(r)}">${esc(r)}</span>`
  ).join("");
  // Restore the user's last prose-pane height from a previous drag, if any.
  const savedProseH = parseInt(localStorage.getItem("audit_flow_prose_height") || "0", 10);
  const proseStyle = (f.prose && savedProseH > 0)
    ? `style="height:${savedProseH}px;max-height:none;"`
    : "";

  stage.innerHTML = `
    <div class="flow-stage-header">
      <span class="flow-stage-id">F${esc(f.id)}</span>
      <span class="flow-stage-title">${esc(f.title)}</span>
      <span class="flow-stage-meta">${ruleChips || '<span class="muted small">no rule citations</span>'}</span>
    </div>
    <div class="flow-canvas" id="flowCanvas"></div>
    ${f.prose ? `
      <div class="flow-splitter" id="flowSplitter" title="Drag to resize the annotations pane">
        <span class="flow-splitter-grip"></span>
      </div>
      <div class="flow-prose" ${proseStyle}><p>${esc(f.prose).split(/\n\n+/).join("</p><p>")}</p></div>
    ` : ""}
  `;
  attachFlowSplitter();
  stage.querySelectorAll(".rule-chip.clickable").forEach(chip => {
    chip.addEventListener("click", () => jumpToMatrixRule(chip.dataset.rule));
  });

  // Render Mermaid diagram into the canvas
  const canvas = document.getElementById("flowCanvas");
  if (!f.source) {
    canvas.innerHTML = `<p class="empty muted">No diagram source captured for F${esc(f.id)}.</p>`;
    return;
  }
  try {
    const renderId = `mer-${f.id.replace(".", "-")}-${Date.now()}`;
    const { svg } = await mermaid.render(renderId, f.source);
    canvas.innerHTML = svg;
    const svgEl = canvas.querySelector("svg");
    if (svgEl) {
      svgEl.removeAttribute("width");
      svgEl.removeAttribute("height");
      svgEl.style.width = "100%";
      svgEl.style.height = "100%";
      svgEl.style.maxWidth = "none";
      svgEl.style.maxHeight = "none";
      // Clean up prior pan-zoom and attach a fresh one for wheel/drag
      try { FLOW_STATE._panZoom?.destroy(); } catch {}
      FLOW_STATE._panZoom = svgPanZoom(svgEl, {
        controlIconsEnabled: false,
        fit: true,
        center: true,
        minZoom: 0.2,
        maxZoom: 8,
        zoomScaleSensitivity: 0.35,
      });
    }
  } catch (err) {
    console.error("Mermaid render error:", err);
    canvas.innerHTML = `
      <div style="padding:24px;color:var(--bug);font-family:var(--sans);">
        <strong>Failed to render F${esc(f.id)}</strong>
        <pre style="margin-top:8px;white-space:pre-wrap;">${esc(String(err && err.message || err))}</pre>
      </div>
    `;
  }
}

// ─── cross-view linking ───────────────────────────────────────────
// Jump from any file reference (in findings, locks, sidecar, etc.) to
// the Repo Map with that file selected and the parent crate auto-zoomed.
function jumpToRepoMap(filePath) {
  if (!filePath) return;
  const fileRow = DATA.files.find(f => f.file === filePath);
  if (!fileRow) return;
  MAP_STATE.zoomedCrate = fileRow.crate;
  MAP_STATE.selectedFile = filePath;
  MAP_STATE.role = fileRow.role;
  setView("map");
  // Re-render after the view becomes visible so getBoundingClientRect is valid
  requestAnimationFrame(() => {
    const roleSel = document.getElementById("mapRole");
    if (roleSel) roleSel.value = fileRow.role;
    renderTreemap();
    updateZoomAffordance();
    renderMapSidecar(fileRow);
    // brief flash on the matching cell
    const cell = document.querySelector(`[data-file="${CSS.escape(filePath)}"]`);
    if (cell) {
      cell.classList.add("flash");
      setTimeout(() => cell.classList.remove("flash"), 1100);
    }
  });
}

// Highlight a clicked rule chip's row across views — in the matrix, scroll
// to the rule and pulse its row; in any future cross-link, also surface tests
// covering that rule.
function jumpToMatrixRule(ruleId) {
  if (!ruleId) return;
  setView("matrix");
  // Make sure the section is expanded
  const rule = DATA.rules.find(r => r.id === ruleId);
  if (rule) MATRIX_STATE.collapsedSections.delete(rule.section || "—");
  // Clear filters that might hide the row
  MATRIX_STATE.search = "";
  MATRIX_STATE.unimplOnly = false;
  MATRIX_STATE.untestedOnly = false;
  MATRIX_STATE.openBugOnly = false;
  document.getElementById("matrixSearch").value = "";
  for (const id of ["matrixUnimpl","matrixUntested","matrixOpenBug"]) {
    const cb = document.getElementById(id);
    if (cb && cb.checked) cb.checked = false;
  }
  renderMatrix();
  requestAnimationFrame(() => {
    const row = Array.from(document.querySelectorAll("#matrixTable tbody tr"))
      .find(tr => tr.querySelector("td .rule-chip")?.textContent === ruleId);
    if (row) {
      row.scrollIntoView({ behavior: "smooth", block: "center" });
      row.classList.add("flash");
      setTimeout(() => row.classList.remove("flash"), 1200);
    }
  });
}

// ─── helpers ───────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// ─── boot ──────────────────────────────────────────────────────────
async function boot() {
  try {
    await loadAll();
  } catch (e) {
    document.getElementById("kpis").innerHTML =
      `<li class="kpi tone-bug" style="grid-column: 1 / -1"><span class="kpi-label">Failed to load</span><span class="kpi-value" style="font-size:14px">${esc(e.message)}</span><span class="kpi-sub">Run: <code>python3 docs/audit/extract.py</code> from the repo root</span></li>`;
    return;
  }
  renderHeartbeat();
  bindFindingsControls();
  bindMatrixControls();
  bindArchControls();
  bindFlowControls();

  document.querySelectorAll(".tab").forEach(t => {
    t.addEventListener("click", () => setView(t.dataset.view));
  });

  // initial view
  const initial = (location.hash.replace("#", "") || "map");
  if (["map", "findings", "matrix"].includes(initial)) setView(initial);
  else setView("map");

  renderFindings();

  // Map controls
  document.getElementById("mapColor").addEventListener("change", (e) => {
    MAP_STATE.color = e.target.value;
    renderTreemap();
    if (MAP_STATE.selectedFile) {
      const cell = document.querySelector(`[data-file="${CSS.escape(MAP_STATE.selectedFile)}"]`);
      if (cell) cell.classList.add("selected");
    }
  });
  document.getElementById("mapRole").addEventListener("change", (e) => {
    MAP_STATE.role = e.target.value;
    renderTreemap();
  });
  document.getElementById("mapReset").addEventListener("click", () => {
    if (MAP_STATE.zoomedFile) {
      MAP_STATE.zoomedFile = null;
      renderTreemap();
      updateZoomAffordance();
    } else if (MAP_STATE.zoomedCrate) {
      MAP_STATE.zoomedCrate = null;
      renderTreemap();
      updateZoomAffordance();
    } else if (MAP_STATE._svg && MAP_STATE._zoom) {
      // Clear pan/zoom transform without redrawing
      MAP_STATE._svg.transition().duration(180).call(MAP_STATE._zoom.transform, d3.zoomIdentity);
    } else {
      MAP_STATE.selectedFile = null;
      document.getElementById("mapSidecar").innerHTML = `<p class="sidecar-empty">Click a cell to inspect.<br><span style="font-size:11px;opacity:.7">Double-click to drill (crate → file → items) · scroll to zoom · drag to pan.</span></p>`;
      renderTreemap();
      updateZoomAffordance();
    }
  });

  // Re-render treemap on viewport resize
  let resizeT;
  window.addEventListener("resize", () => {
    clearTimeout(resizeT);
    resizeT = setTimeout(() => {
      if (!document.querySelector(".view-map").classList.contains("hidden")) renderTreemap();
    }, 200);
  });
}

boot();
