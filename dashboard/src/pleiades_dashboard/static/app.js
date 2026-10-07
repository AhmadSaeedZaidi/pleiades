"use strict";

const $ = id => document.getElementById(id);
const number = value => value == null ? "—" : Number(value).toLocaleString();
const names = {raw: "Source media", audio: "Audio", visuals: "Keyframes", transcript: "Transcripts", clip: "Full clips"};
const colors = ["#8d80ed", "#6c9ce8", "#79b6a2", "#d8b47a", "#b5a4d8"];
const state = {view: "overview", page: 1, token: "", demo: false, request: 0, detailRequest: 0, topic: "", graph: null};
const pages = {
  overview: ["Pipeline overview", "Follow your YouTube data from discovery to extraction.", "Overview"],
  videos: ["Video library", "Explore the videos and artifacts in your collection.", "Video library"],
  queries: ["Discovery queries", "See what your pipeline is searching for next.", "Discovery queries"],
  activity: ["Pipeline activity", "A record of persisted events across your pipeline.", "Activity"],
  graph: ["Knowledge graph", "Explore the topics connecting your videos and publishing channels.", "Knowledge graph"],
};

function node(tag, cls, text) {
  const el = document.createElement(tag);
  if (cls) el.className = cls;
  if (text != null) el.textContent = text;
  return el;
}
function time(value, relative = true) {
  if (!value) return "Never";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "Unknown";
  if (!relative) return date.toLocaleString(undefined, {dateStyle: "medium", timeStyle: "short"});
  const minutes = Math.max(0, Math.floor((Date.now() - date) / 60000));
  if (minutes < 1) return "Just now";
  if (minutes < 60) return `${minutes}m ago`;
  if (minutes < 1440) return `${Math.floor(minutes / 60)}h ago`;
  return `${Math.floor(minutes / 1440)}d ago`;
}
function duration(seconds) {
  if (seconds == null) return "—";
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;
}
function empty(parent, message, columns) {
  parent.replaceChildren();
  if (columns) {
    const row = node("tr");
    const cell = node("td", "empty-state", message);
    cell.colSpan = columns;
    row.append(cell);
    parent.append(row);
  } else parent.append(node("p", "empty-state", message));
}
async function api(path) {
  const response = await fetch(new URL(path.replace(/^\//, ""), document.baseURI), {
    headers: state.token ? {Authorization: `Bearer ${state.token}`} : {},
    signal: AbortSignal.timeout(15000),
  });
  if (response.status === 401) {
    if ($("video-dialog").open) $("video-dialog").close();
    if (!$("access-dialog").open) $("access-dialog").showModal();
    throw new Error("Enter your dashboard access token to connect.");
  }
  if (!response.ok) throw new Error(response.status === 503
    ? "Pipeline data is unavailable. Check the dashboard service, then refresh."
    : `Unable to load this view (${response.status}). Please refresh.`);
  return response.json();
}
function error(message) {
  $("error").textContent = message;
  $("error").hidden = false;
}

function chart(items) {
  const target = $("discovery-chart");
  target.replaceChildren();
  if (!items.length) return empty(target, "No discovery samples available.");
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 580 170");
  svg.setAttribute("preserveAspectRatio", "none");
  function shape(tag, attrs, text) {
    const el = document.createElementNS(ns, tag);
    Object.entries(attrs).forEach(([key, value]) => el.setAttribute(key, String(value)));
    if (text != null) el.textContent = text;
    svg.append(el);
    return el;
  }
  const max = Math.max(4, ...items.map(i => Number(i.count) || 0));
  for (let i = 0; i < 4; i++) {
    const y = 20 + i * 40;
    shape("line", {x1: 32, x2: 572, y1: y, y2: y, stroke: "#f0f1f7", "stroke-dasharray": "3 4"});
    shape("text", {x: 0, y: y + 3, fill: "#b3b7c7", "font-size": 9}, Math.round(max * (1 - i / 3)));
  }
  const points = items.map((item, i) => [32 + i * 538 / Math.max(1, items.length - 1), 140 - Number(item.count) / max * 120]);
  const line = points.map(p => p.join(",")).join(" ");
  shape("polygon", {points: `32,140 ${line} 570,140`, fill: "#f0edff", opacity: .7});
  shape("polyline", {points: line, fill: "none", stroke: "#9586ec", "stroke-width": 2.2, "stroke-linecap": "round", "stroke-linejoin": "round"});
  items.forEach((item, i) => {
    const c = shape("circle", {cx: points[i][0], cy: points[i][1], r: 3, fill: "#9586ec"});
    const tip = document.createElementNS(ns, "title");
    tip.textContent = `${new Date(item.hour).getUTCHours()}:00 UTC · ${number(item.count)} videos`;
    c.append(tip);
    if (i % 5 === 0 || i === items.length - 1) {
      shape("text", {x: points[i][0], y: 162, fill: "#b0b5c5", "font-size": 9, "text-anchor": i === items.length - 1 ? "end" : "middle"}, `${String(new Date(item.hour).getUTCHours()).padStart(2, "0")}:00`);
    }
  });
  target.append(svg);
}

function overview(data) {
  const c = data.counts;
  state.demo = Boolean(data.demo);
  $("demo-notice").hidden = !state.demo;
  $("mode-badge").textContent = state.demo ? "DEMO WORKSPACE" : "READ ONLY";
  $("metric-total").textContent = number(c.total);
  $("nav-total").textContent = number(c.total);
  $("metric-discovered").textContent = number(c.discovered);
  $("chart-total").textContent = number(c.discovered);
  $("metric-tracked").textContent = number(data.tracking.total);
  $("metric-tracking-caption").textContent = `${number(data.tracking.due)} due for a stats update`;
  $("metric-failed").textContent = number(c.failed);
  $("vault-label").textContent = `${number(c.vault_pending)} awaiting vault storage`;
  $("service-label").textContent = data.service.state === "active" ? "Ingestion scheduler active" : data.service.state === "demo" ? "Sample environment" : `Scheduler: ${data.service.state}`;
  $("service-dot").className = `dot ${data.service.state === "active" ? "green" : data.service.state === "demo" ? "purple" : "red"}`;
  $("updated").textContent = `Snapshot · ${time(data.updated_at, false)}`;
  const coverage = $("coverage");
  coverage.replaceChildren();
  data.stages.forEach((stage, i) => {
    const percentage = c.total ? Math.round(stage.done / c.total * 100) : 0;
    const row = node("div", "coverage-row");
    const label = node("div", "coverage-label");
    const value = node("strong", "", number(stage.done));
    value.append(node("small", "", `${percentage}%`));
    label.append(node("span", "", names[stage.name] || stage.name), value);
    const bar = node("div", "coverage-bar");
    const fill = node("div", "coverage-fill");
    fill.style.width = `${Math.min(100, percentage)}%`;
    fill.style.backgroundColor = colors[i];
    bar.append(fill);
    row.append(label, bar);
    row.title = `${number(stage.failed)} failed`;
    coverage.append(row);
  });
  chart(data.timeline);
}

function badge(value) {
  const safe = ["PENDING", "PROCESSING", "PROCESSED", "ARCHIVED", "FAILED"].includes(value) ? value : "UNKNOWN";
  return node("span", `status-badge ${safe.toLowerCase()}`, safe.toLowerCase());
}
function videos(data) {
  const body = $("video-rows");
  body.replaceChildren();
  if (!data.items.length) empty(body, "No videos match these filters.", 6);
  data.items.forEach(video => {
    const row = node("tr");
    const cell = node("td");
    const item = node("div", "video-cell");
    const thumb = node("span", "video-thumb", "▷");
    if (!state.demo && /^[A-Za-z0-9_-]{11}$/.test(video.id)) {
      const img = node("img", "video-thumb");
      img.src = `https://i.ytimg.com/vi/${encodeURIComponent(video.id)}/mqdefault.jpg`;
      img.alt = "";
      img.loading = "lazy";
      img.addEventListener("error", () => img.replaceWith(thumb), {once: true});
      item.append(img);
    } else item.append(thumb);
    const text = node("div", "video-text");
    const title = node("button", "video-title", video.title || video.id);
    title.title = video.title || video.id;
    title.addEventListener("click", () => detail(video.id));
    text.append(title, node("span", "video-channel", `${video.channel || "Unknown channel"} · ${duration(video.duration)}`));
    item.append(text);
    cell.append(item);
    const status = node("td");
    status.append(badge(video.status));
    if (video.retired_at) status.append(node("span", "pill", "parked"));
    const stages = node("td");
    const indicators = node("div", "stage-indicators");
    ["raw", "audio", "visuals", "transcript"].forEach(stage => {
      const phase = video[`${stage}_phase`] || "PENDING";
      const indicator = node("span", `stage-indicator ${phase.toLowerCase()}`, phase === "DONE" ? "✓" : phase === "FAILED" ? "!" : phase === "PROCESSING" ? "·" : "−");
      indicator.title = `${names[stage]}: ${phase.toLowerCase()}`;
      indicator.setAttribute("aria-label", indicator.title);
      indicators.append(indicator);
    });
    stages.append(indicators);
    const arrowCell = node("td");
    const arrow = node("button", "row-arrow", "→");
    arrow.setAttribute("aria-label", `Inspect ${video.title || video.id}`);
    arrow.addEventListener("click", () => detail(video.id));
    arrowCell.append(arrow);
    row.append(cell, status, stages, node("td", "", number(video.views)), node("td", "", time(video.discovered_at)), arrowCell);
    body.append(row);
  });
  const start = data.total ? (data.page - 1) * data.page_size + 1 : 0;
  $("page-summary").textContent = `Showing ${number(start)}–${number(Math.min(data.total, data.page * data.page_size))} of ${number(data.total)} videos`;
  $("page-number").textContent = data.page;
  $("previous").disabled = data.page === 1;
  $("next").disabled = data.page * data.page_size >= data.total || data.page >= 1000;
}

function queries(data) {
  const body = $("query-rows");
  body.replaceChildren();
  if (!data.items.length) return empty(body, "The discovery queue is empty.", 6);
  data.items.forEach(q => {
    const row = node("tr");
    row.append(node("td", "query-term", q.query_term), node("td", "", number(q.priority)), node("td", "", number(q.mention_count)), node("td", "", number(q.result_count_total)), node("td", "", q.status), node("td", "", time(q.last_searched_at)));
    body.append(row);
  });
}
function activity(data) {
  const list = $("event-list");
  list.replaceChildren();
  if (!data.items.length) return empty(list, "No persisted events yet.");
  data.items.forEach(event => {
    const row = node("div", "event");
    const content = node("div");
    content.append(node("strong", "", event.event_type), node("p", "", event.entity_id || "Pipeline"));
    row.append(node("span", "event-icon", "↗"), content, node("time", "", time(event.created_at, false)));
    list.append(row);
  });
}
async function detail(id) {
  const sequence = ++state.detailRequest;
  const box = $("video-detail");
  box.replaceChildren(node("p", "empty-state", "Loading video…"));
  if (!$("video-dialog").open) $("video-dialog").showModal();
  try {
    const v = await api(`/api/videos/${encodeURIComponent(id)}`);
    if (sequence !== state.detailRequest) return;
    box.replaceChildren(node("h2", "detail-title", v.title), node("p", "detail-channel", `${v.channel || "Unknown channel"} · ${v.id}`), badge(v.status));
    const metrics = node("div", "detail-metrics");
    [["Views", v.views], ["Likes", v.likes], ["Comments", v.comments]].forEach(([label, value]) => {
      const metric = node("div", "detail-metric");
      metric.append(node("small", "", label), node("strong", "", number(value)));
      metrics.append(metric);
    });
    box.append(metrics);
    const phases = node("div", "detail-phases");
    Object.keys(names).forEach(s => phases.append(node("span", "pill", `${names[s]} · ${(v[`${s}_phase`] || "unknown").toLowerCase()}`)));
    box.append(phases, node("p", "detail-meta", `Published ${time(v.published_at, false)} · Duration ${duration(v.duration)}\nDiscovered ${time(v.discovered_at, false)} · Tracking ${v.tracking_tier || "unscheduled"}`));
    if (v.retired_at) box.append(node("p", "detail-meta", `Parked as unavailable ${time(v.retired_at, false)}. Data is preserved; Tracker will recheck availability.`));
    if (v.tags?.length) box.append(node("p", "detail-meta", v.tags.join(" · ")));
    box.append(node("h2", "", "Transcript preview"));
    if (v.transcript_preview) {
      let preview = v.transcript_preview;
      try {
        const payload = JSON.parse(preview);
        const segments = Array.isArray(payload) ? payload : payload.segments;
        if (Array.isArray(segments)) preview = segments.map(s => s.text || "").join("\n");
      } catch { /* A bounded JSON preview may end in the middle of a segment. */ }
      box.append(node("pre", "detail-transcript", preview || "The staged transcript contains no text."));
      if (v.transcript_truncated) box.append(node("p", "detail-meta", "Preview limited to 24,000 characters."));
    } else box.append(node("p", "detail-meta", v.transcript_vaulted || v.transcript_phase === "DONE" || v.status === "ARCHIVED" ? "Transcript payload may be stored in the vault. This interface previews staged content only." : "No staged transcript is available for this video."));
    if (!state.demo) {
      const link = node("a", "detail-link", "Open source on YouTube ↗");
      link.href = `https://www.youtube.com/watch?v=${encodeURIComponent(v.id)}`;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      box.append(link);
    }
  } catch (e) { if (sequence === state.detailRequest) box.replaceChildren(node("p", "empty-state", e.message)); }
}

async function refresh() {
  const sequence = ++state.request;
  $("refresh").disabled = true;
  const view = state.view;
  const params = new URLSearchParams({q: $("search").value.trim(), status: $("status-filter").value, stage: $("stage-filter").value, page: state.page, page_size: view === "overview" ? 8 : 25});
  const requests = [api("/api/overview")];
  if (["overview", "videos"].includes(view)) requests.push(api(`/api/videos?${params}`));
  if (view === "queries") requests.push(api("/api/queries"));
  if (view === "activity") requests.push(api("/api/events"));
  if (view === "graph") requests.push(api(`/api/graph?${new URLSearchParams({q: $("topic-search").value.trim(), topic: state.topic, limit: $("graph-limit").value})}`));
  const results = await Promise.allSettled(requests);
  if (sequence !== state.request) return;
  $("error").hidden = true;
  if (results[0].status === "fulfilled") overview(results[0].value);
  else error(results[0].reason.message);
  if (results[1].status === "fulfilled") {
    ({overview: videos, videos, queries, activity, graph: renderGraph})[view](results[1].value);
  } else {
    error(results[1].reason.message);
    if (["overview", "videos"].includes(view)) empty($("video-rows"), "This view could not be refreshed. Your previous snapshot may be out of date.", 6);
    if (view === "queries") empty($("query-rows"), "Discovery queue unavailable. Try refreshing.", 6);
    if (view === "activity") empty($("event-list"), "Event log unavailable. Try refreshing.");
    if (view === "graph") { state.graph = null; $("graph-export").disabled = true; empty($("graph-canvas"), "Graph data is unavailable. Try refreshing."); }
  }
  $("refresh").disabled = false;
}
function navigate() {
  const view = location.hash.slice(1);
  state.view = Object.hasOwn(pages, view) ? view : "overview";
  state.page = 1;
  const [title, description, crumb] = pages[state.view];
  $("page-title").textContent = title;
  $("page-description").textContent = description;
  $("breadcrumb-page").textContent = crumb;
  $("overview-view").hidden = state.view !== "overview";
  $("library-section").hidden = !["overview", "videos"].includes(state.view);
  $("queries-view").hidden = state.view !== "queries";
  $("activity-view").hidden = state.view !== "activity";
  $("graph-view").hidden = state.view !== "graph";
  $("all-videos").hidden = state.view === "videos";
  $("library-title").textContent = state.view === "videos" ? "Collected videos" : "Recent videos";
  $("library-description").textContent = state.view === "videos" ? "Search, filter and inspect extracted content" : "The latest arrivals in your collection";
  document.querySelectorAll(".nav-link").forEach(link => {
    link.classList.toggle("active", link.dataset.view === state.view);
    if (link.dataset.view === state.view) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  refresh();
}

$("today").textContent = new Date().toLocaleDateString(undefined, {month: "short", day: "numeric", year: "numeric"});
$("refresh").addEventListener("click", refresh);
$("previous").addEventListener("click", () => {state.page--; refresh();});
$("next").addEventListener("click", () => {state.page++; refresh();});
$("close-video").addEventListener("click", () => {state.detailRequest++; $("video-dialog").close();});
let debounce;
$("search").addEventListener("input", () => {clearTimeout(debounce); debounce = setTimeout(() => {state.page = 1; refresh();}, 300);});
["status-filter", "stage-filter"].forEach(id => $(id).addEventListener("change", () => {state.page = 1; refresh();}));
$("access-form").addEventListener("submit", e => {e.preventDefault(); state.token = $("access-token").value.trim(); $("access-token").value = ""; $("access-dialog").close(); refresh();});
$("topic-search").addEventListener("input", () => {clearTimeout(debounce); debounce = setTimeout(() => {state.topic = ""; refresh();}, 300);});
$("graph-limit").addEventListener("change", refresh);
$("graph-export").addEventListener("click", () => {
  if (!state.graph) return;
  const blob = new Blob([JSON.stringify({...state.graph, exported_at: new Date().toISOString()}, null, 2)], {type: "application/json"});
  const url = URL.createObjectURL(blob);
  const link = node("a"); link.href = url; link.download = "pleiades-topic-graph.json";
  link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
});
window.addEventListener("hashchange", navigate);
setInterval(() => {if ($("auto-refresh").checked && !document.hidden && !$("video-dialog").open && !$("access-dialog").open && !$("refresh").disabled) refresh();}, 30000);
navigate();
