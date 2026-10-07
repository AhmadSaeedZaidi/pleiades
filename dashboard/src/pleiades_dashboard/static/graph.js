"use strict";

function renderGraph(data) {
  state.graph = data;
  state.topic = data.selected || "";
  $("graph-export").disabled = !data.nodes.length;
  const s = data.summary;
  $("graph-topic-count").textContent = number(s.topic_count);
  $("graph-edge-count").textContent = number(Number(s.video_edges) + Number(s.channel_edges));
  $("graph-video-count").textContent = number(s.video_checked);
  $("graph-channel-count").textContent = number(s.channel_checked);
  $("graph-video-coverage").textContent = `of ${number(s.video_total)} videos · background enrichment`;
  $("graph-channel-coverage").textContent = `of ${number(s.channel_total)} publishing channels`;
  const catalog = $("topic-list");
  catalog.replaceChildren();
  if (!data.topics.length) empty(catalog, "No matching topics yet.");
  data.topics.forEach(topic => {
    const button = node("button", `topic-choice ${topic.url === data.selected ? "selected" : ""}`);
    button.append(node("strong", "", topic.label), node("small", "", `${number(topic.videos)} videos · ${number(topic.channels)} channels · ${topic.language}`));
    button.addEventListener("click", () => {state.topic = topic.url; refresh();});
    catalog.append(button);
  });
  const selected = data.nodes.find(n => n.kind === "topic" && n.key === data.selected);
  $("graph-heading").textContent = selected?.label || "Your knowledge graph";
  $("graph-notes").textContent = `${number(data.nodes.length)} nodes · ${number(data.edges.length)} connections in this bounded view. ${number(s.empty)} resources have no reported topics; ${number(s.unavailable)} were unavailable on their latest check.`;
  const canvas = $("graph-canvas");
  canvas.replaceChildren();
  if (!data.nodes.length) return empty(canvas, "Topics will appear as the background enrichment checks your existing collection. YouTube does not provide topics for every resource.");

  const ns = "http://www.w3.org/2000/svg";
  const groups = ["topic", "video", "channel"].map(kind => data.nodes.filter(n => n.kind === kind));
  const height = Math.max(650, ...groups.map(items => items.length * 18 + 100));
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", `0 0 1000 ${height}`);
  svg.setAttribute("role", "group");
  svg.setAttribute("aria-label", "Topics connect to videos and channels; videos connect to their publishers");
  function shape(tag, attrs, text) {
    const el = document.createElementNS(ns, tag);
    Object.entries(attrs).forEach(([key, value]) => el.setAttribute(key, String(value)));
    if (text != null) el.textContent = text;
    return el;
  }
  const positions = new Map();
  const columns = [120, 465, 820];
  groups.forEach((items, column) => {
    svg.append(shape("text", {x: columns[column], y: 25, class: "graph-column-label"}, ["WIKIPEDIA TOPICS", "COLLECTED VIDEOS", "PUBLISHING CHANNELS"][column]));
    items.forEach((item, i) => positions.set(item.id, {x: columns[column], y: 55 + (i + .5) * (height - 100) / Math.max(1, items.length)}));
  });
  const lines = new Map();
  data.edges.forEach(edge => {
    const a = positions.get(edge.source), b = positions.get(edge.target);
    if (!a || !b) return;
    const line = shape("path", {d: `M ${a.x} ${a.y} C ${(a.x + b.x) / 2} ${a.y}, ${(a.x + b.x) / 2} ${b.y}, ${b.x} ${b.y}`, class: "graph-edge"});
    line.append(shape("title", {}, `${edge.relation.replaceAll("_", " ")} · ${edge.provenance}${edge.observed_at ? ` · ${time(edge.observed_at, false)}` : ""}`));
    svg.append(line); lines.set(line, edge);
  });
  function highlight(id) {
    lines.forEach((edge, line) => {
      line.classList.toggle("highlight", Boolean(id) && (edge.source === id || edge.target === id));
      line.classList.toggle("dim", Boolean(id) && edge.source !== id && edge.target !== id);
    });
  }
  data.nodes.forEach(item => {
    const p = positions.get(item.id);
    const g = shape("g", {class: `graph-node ${item.kind} ${item.key === data.selected ? "chosen" : ""}`, tabindex: 0, role: "button", "aria-label": `${item.kind}: ${item.label}`});
    g.append(shape("circle", {cx: p.x, cy: p.y, r: item.kind === "topic" ? 8 : 5}));
    const max = item.kind === "channel" ? 23 : 38;
    g.append(shape("text", {x: p.x + 13, y: p.y + 4}, item.label.length > max ? item.label.slice(0, max - 1) + "…" : item.label));
    g.append(shape("title", {}, `${item.label} · ${item.kind}`));
    const activate = () => {
      if (item.kind === "video") detail(item.key);
      else if (item.kind === "topic") { state.topic = item.key; refresh(); }
      else if (/^https:\/\/www\.youtube\.com\/channel\//.test(item.url)) window.open(item.url, "_blank", "noopener,noreferrer");
    };
    g.addEventListener("click", activate);
    g.addEventListener("keydown", e => {if (e.key === "Enter" || e.key === " ") {e.preventDefault(); activate();}});
    g.addEventListener("mouseenter", () => highlight(item.id));
    g.addEventListener("mouseleave", () => highlight(null));
    g.addEventListener("focus", () => highlight(item.id));
    g.addEventListener("blur", () => highlight(null));
    svg.append(g);
  });
  canvas.append(svg);
  if (selected) {
    const link = node("a", "detail-link", "Read this topic on Wikipedia ↗");
    if (/^https:\/\/[a-z][a-z0-9-]*\.wikipedia\.org\/wiki\//.test(selected.url)) {
      link.href = selected.url; link.target = "_blank"; link.rel = "noopener noreferrer"; canvas.append(link);
    }
  }
}
