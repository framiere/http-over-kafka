// Playground page. Two sources, both real: the JSON the playground returns
// after calling the gateway, and the Kafka records it streams over SSE.
// Record content is untrusted: everything goes through textContent.
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);

function el(tag, attrs, ...children) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === "class") n.className = v;
    else if (k === "text") n.textContent = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat()) {
    if (c == null || c === false) continue;
    n.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return n;
}

const shortKey = (k) => (k.length > 14 ? k.slice(0, 7) + "…" + k.slice(-4) : k);
const shortId = (id) => (id && id.length > 12 ? id.slice(0, 6) + "…" + id.slice(-4) : id || "");
const clock = (t) => new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const where = (it) => `${it.topic}  p${it.partition}  #${it.offset}`;

function pretty(text) {
  if (!text) return "";
  try { return JSON.stringify(JSON.parse(text), null, 2); } catch { return text; }
}

/* ---------- connection status ---------- */

const probeText = {
  gateway: { checking: "checking", up: "ready", degraded: "not ready", down: "unreachable" },
  kafka: { checking: "checking", up: "connected", degraded: "degraded", down: "unreachable" },
  stream: { checking: "connecting", up: "live", down: "reconnecting" },
};

function setProbe(name, state) {
  const li = $(`[data-probe="${name}"]`);
  li.dataset.state = state;
  $("b", li).textContent = probeText[name][state] || state;
}

let lastStatus = null;
let streamUp = false;

function renderAlerts() {
  const box = $("[data-alerts]");
  box.replaceChildren();
  const alert = (kind, title, text, detail) =>
    box.append(el("div", { class: "alert " + kind }, el("strong", { text: title }), el("p", { text }), detail ? el("p", { class: "detail", text: detail }) : null));

  if (!streamUp && lastStatus !== null) {
    alert("", "Lost the connection to the playground.",
      "Reconnecting on its own. If it lasts, check the playground: docker compose logs playground");
  }
  const s = lastStatus;
  if (!s) return;
  if (s.gateway.state === "down") {
    alert("", "The gateway is not answering.",
      "Requests cannot be sent. Start the stack with docker compose up -d, then check docker compose logs gateway.",
      `${s.gateway.target}: ${s.gateway.detail || ""}`);
  } else if (s.gateway.state === "degraded") {
    alert("warn", "The gateway is up but not ready.",
      "It cannot write commands to Kafka yet, so requests get 503. It needs Kafka: check docker compose logs gateway.",
      s.gateway.detail);
  }
  if (s.kafka.state === "down") {
    alert("", "The playground cannot reach Kafka.",
      "Nothing can be shown from Kafka until it is back. Check docker compose ps kafka.",
      `${s.kafka.target}: ${s.kafka.detail || ""}`);
  } else if (s.kafka.state === "up" && s.kafka.detail) {
    alert("info", "Kafka is reachable, but there is nothing to read yet.", s.kafka.detail);
  }
}

function onStatus(s) {
  lastStatus = s;
  for (const n of document.querySelectorAll("[data-app]")) n.textContent = s.caller;
  renderConsole(s);
  setProbe("gateway", s.gateway.state);
  setProbe("kafka", s.kafka.state);
  const meta = $("[data-feed-meta]");
  const topics = s.topics || [];
  meta.replaceChildren(
    "Committed records only, since ", el("time", { datetime: s.since, text: clock(s.since) }),
    topics.length ? `, from ${topics.length} topics` : "",
  );
  meta.title = topics.join("\n");
  renderAlerts();
}

const watchRole = (topic) =>
  topic.startsWith("http.requests.") ? "commands" : topic.startsWith("http.results.") ? "results" : "business events";

function renderConsole(s) {
  const p = $("[data-console]");
  p.hidden = !s.console;
  if (!s.console) return;
  $("[data-console-link]").href = s.console;
  const watch = s.watch || [];
  $("[data-watch]", p).replaceChildren(...watch.flatMap((t, i) => [
    i === 0 ? "" : i === watch.length - 1 ? " and " : ", ",
    el("code", { text: t }), ` for ${watchRole(t)}`,
  ]));
}

/* ---------- traces: the records of one request, grouped ---------- */

const traces = new Map(); // requestId -> trace
const mine = new Set(); // requestIds sent from this page
const MAX_TRACES = 200;

function traceFor(it) {
  const id = it.requestId || `seq-${it.seq}`;
  let t = traces.get(id);
  if (t) return t;
  const node = $("#tpl-trace").content.firstElementChild.cloneNode(true);
  node.dataset.rid = id;
  t = { id, node, records: [], first: it };
  traces.set(id, t);
  const list = $("[data-traces]");
  list.prepend(node);
  node.classList.add("is-new");
  node.addEventListener("animationend", () => node.classList.remove("is-new"), { once: true });
  while (list.children.length > MAX_TRACES) {
    const last = list.lastElementChild;
    traces.delete(last.dataset.rid);
    last.remove();
  }
  $("[data-feed-empty]").hidden = true;
  return t;
}

function onRecord(it) {
  const t = traceFor(it);
  if (t.records.some((r) => r.topic === it.topic && r.partition === it.partition && r.offset === it.offset)) return;
  t.records.push(it);
  if (it.kind === "command") t.command = it;
  else if (it.kind === "response") t.response = it;
  else if (it.kind === "result") t.result = it;
  else if (it.kind === "event") t.event = it;
  renderTrace(t);
}

function authNote(it, role) {
  switch (it.authenticity) {
    case "authentic": return { text: `Signature valid (${role})`, cls: "ok" };
    case "not-authentic": return { text: `Signature invalid: ${it.authError || "not authentic"}`, cls: "bad" };
    case "unverified": return { text: `Signature not checked: no ${role} public key configured`, cls: "dim" };
    default: return null;
  }
}

function station(name, color, state, value, whereText, ...notes) {
  return el("li", { class: `station ${color} is-${state}` },
    el("span", { class: "station-name", text: name }),
    el("span", { class: "station-value", text: value }),
    whereText ? el("span", { class: "station-where", text: whereText }) : null,
    notes.filter(Boolean).map((n) => el("span", { class: "station-note " + (n.cls || ""), text: n.text })),
  );
}

function commandStation(t) {
  const c = t.command;
  if (!c) return station("Command", "kafka", "pending", "Not in Kafka yet");
  const forged = c.authenticity === "not-authentic";
  const secrets = c.command.callerSecrets || [];
  return station("Command", forged ? "forged" : "kafka", "done", c.command.operationId, where(c),
    authNote(c, "gateway"),
    secrets.length
      ? { text: `Caller secret in the record: ${secrets.join(", ")}`, cls: "bad" }
      : { text: "No Authorization header in the record", cls: "ok" },
    c.command.idempotent ? { text: "Carries an Idempotency-Key", cls: "dim" } : null);
}

function responseStation(t) {
  const r = t.response;
  if (!r) return station("Response", "green", "pending", "Not in Kafka yet");
  const ok = r.response.status < 400;
  const replay = r.response.replayOf;
  return station("Response", r.authenticity === "not-authentic" ? "forged" : ok ? "green" : "fail", "done",
    `${r.response.status}${r.response.fault ? " " + r.response.fault : ""}`, where(r),
    replay ? { text: `Stored answer of ${shortId(replay)}, replayed`, cls: "ok" } : null,
    authNote(r, "bridge"));
}

function resultStation(t) {
  const r = t.result;
  if (r) {
    const x = r.result;
    const color = r.authenticity === "not-authentic" ? "forged" : x.outcome === "Succeeded" ? "kafka" : "fail";
    return station("Result", color, "done", x.type, where(r),
      { text: `${x.outcome}, the service answered ${x.status}`, cls: x.outcome === "Succeeded" ? "" : "dim" },
      authNote(r, "bridge"));
  }
  if (t.response && t.response.response.replayOf) {
    return station("Result", "kafka", "none", "None written", null,
      { text: `This is a retry: the bridge returned the stored outcome of ${shortId(t.response.response.replayOf)}. The service was not called.` });
  }
  return station("Result", "kafka", "pending", "Not in Kafka yet");
}

function eventStation(t) {
  const e = t.event;
  if (e) {
    const fromSigned = t.result && t.result.authenticity === "authentic";
    let value = e.event.type;
    try { const v = JSON.parse(e.value); if (v && v.id) value += ` ${v.id}`; } catch { /* not JSON: type only */ }
    return station("Event", "green", "done", value, where(e),
      { text: fromSigned ? "CloudEvent, derived from the bridge-signed result" : "CloudEvent (events carry no signature)", cls: "dim" });
  }
  if (t.response && t.response.response.replayOf) {
    return station("Event", "green", "none", "None", null, { text: "Nothing new happened, so there is nothing to publish." });
  }
  if (!t.result) return station("Event", "green", "pending", "Waits for the result");
  const x = t.result.result.expect;
  if (x.fires) return station("Event", "green", "pending", `Waiting for ${x.type}`, x.topic);
  return station("Event", "green", "none", "No event", null, { text: x.reason });
}

function renderTrace(t) {
  const n = t.node;
  const info = (t.command && t.command.command) || (t.result && t.result.result);
  const req = $(".trace-req", n);
  if (info) {
    req.replaceChildren(el("b", { text: info.method }), ` ${info.path}`);
    const caller = t.command && t.command.command.caller;
    const who = $(".trace-who", n);
    who.textContent = caller ? `${caller.application} called ${info.service}` : `call to ${info.service}`;
    who.title = caller ? `caller instance: ${caller.instance}` : "";
  } else if (t.event) {
    req.textContent = t.event.event.type;
  } else {
    req.textContent = t.first.kind === "unreadable" ? `Unreadable record: ${t.first.error}` : t.first.kind;
  }
  const time = $(".trace-time", n);
  time.dateTime = t.first.timestamp;
  time.textContent = clock(t.first.timestamp);
  n.classList.toggle("is-mine", mine.has(t.id));
  $(".trace-mine", n).hidden = !mine.has(t.id);

  if (t.first.kind === "unreadable" && !info) {
    $(".rail", n).replaceChildren(station("Record", "forged", "done", "Not a record of this contract", where(t.first),
      { text: t.first.error, cls: "bad" }));
  } else {
    $(".rail", n).replaceChildren(commandStation(t), responseStation(t), resultStation(t), eventStation(t));
  }

  const raw = $(".raw-body", n);
  raw.replaceChildren(...t.records.map((r) => el("div", { class: "raw-rec" },
    el("h3", { text: `${where(r)}  key ${r.key || "(none)"}` }),
    el("pre", { class: "wire" },
      r.headers.map((h) => el("span", {}, el("span", { class: "hk", text: h.key + ": " }), h.value + "\n")),
      "\n" + pretty(r.value) + (r.truncated ? "\n… (cut for display)" : "")),
  )));
}

function focusTrace(rid) {
  const t = traces.get(rid);
  if (!t) return;
  for (const other of document.querySelectorAll(".trace.is-focus")) other.classList.remove("is-focus");
  t.node.classList.add("is-focus");
  t.node.scrollIntoView({ block: "nearest", behavior: matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth" });
}

/* ---------- actions ---------- */

const titles = {
  "create-order": "Create an order",
  "invalid-order": "Send an invalid order",
  charge: "Charge a card",
  retry: "Retry with the same Idempotency-Key",
};
let retryTarget = null; // { id, title, key }

const hotResponse = new Set(["x-request-id", "idempotent-replayed", "location"]);
const hotRequest = new Set(["host", "idempotency-key"]);

function wireText(start, headers, body, hot, hotCls) {
  return el("pre", { class: "wire" },
    el("span", { class: "start", text: start + "\n" }),
    headers.map((h) => el("span", {},
      el("span", { class: hot.has(h.key.toLowerCase()) ? hotCls : "hk", text: h.key + ":" }), " " + h.value + "\n")),
    body ? "\n" + pretty(body) : "");
}

function reading(ex) {
  const r = ex.response;
  if (!r) return null;
  const replayed = r.headers.some((h) => h.key.toLowerCase() === "idempotent-replayed" && h.value === "true");
  if (replayed) return "The gateway returned the original answer, marked Idempotent-Replayed. The service was not called again: its own data is read below.";
  if (r.status === 201) return "The service answered 201, through Kafka. The caller only saw HTTP.";
  if (r.status === 400) return "The service refused the request. Kafka records it as a failed result, and no business event follows.";
  if (r.status === 401) return "The gateway refused the caller token. Check that the playground and the gateway use the same dev IdP keys.";
  if (r.status === 503) return "The gateway could not hand the request to Kafka: it was not applied.";
  if (r.status === 504) return "No answer within the gateway's deadline. The command may still run; a retry with the same Idempotency-Key gets its outcome.";
  return null;
}

function copyButton(text) {
  return el("button", {
    class: "copy", type: "button", text: "Copy",
    onclick: async (e) => {
      const b = e.currentTarget;
      try {
        await navigator.clipboard.writeText(text);
      } catch {
        const ta = el("textarea", { style: "position:fixed;opacity:0" });
        ta.value = text;
        document.body.append(ta);
        ta.select();
        document.execCommand("copy");
        ta.remove();
      }
      b.textContent = "Copied";
      b.dataset.copied = "";
      setTimeout(() => { b.textContent = "Copy"; delete b.dataset.copied; }, 1600);
    },
  });
}

function renderExchange(ex) {
  const box = $("[data-exchange]");
  const parts = [];
  const meta = [];
  if (ex.response) meta.push(`${ex.response.durationMs.toFixed(1)} ms`);
  if (ex.requestId) {
    meta.push(el("a", { href: "#", text: `request ${shortId(ex.requestId)} in Kafka`, onclick: (e) => { e.preventDefault(); focusTrace(ex.requestId); } }));
  }
  const read = reading(ex);
  parts.push(el("header", { class: "ex-head" },
    el("span", { class: "ex-title", text: titles[ex.action] || ex.action }),
    ex.response ? el("span", { class: "ex-status status-" + String(ex.response.status)[0], text: `${ex.response.status} ${ex.response.statusText}` }) : null,
    el("span", { class: "ex-meta" }, meta.flatMap((m, i) => (i ? ["  ", m] : [m]))),
    read ? el("p", { class: "ex-reading", text: read }) : null));

  const r = ex.request;
  const request = r.method ? el("section", { class: "block block-req" },
    el("p", { class: "block-label" }, el("span", { class: "lbl lbl-req", text: "Request sent" })),
    wireText(`${r.method} ${r.url}`, r.headers, r.body, hotRequest, "hot-req")) : null;

  if (ex.error) {
    parts.push(el("section", { class: "block err" },
      el("p", { class: "block-label" }, el("span", { class: "lbl lbl-err", text: ex.error.title })),
      el("p", { text: ex.error.hint }),
      el("p", { class: "detail", text: ex.error.detail })));
  }
  if (ex.response) {
    const s = ex.response;
    parts.push(el("section", { class: "block block-resp" },
      el("p", { class: "block-label" }, el("span", { class: "lbl lbl-resp", text: "Response received" })),
      wireText(`${s.status} ${s.statusText}`, s.headers, s.body, hotResponse, "hot")));
  }
  if (ex.check) {
    const c = ex.check;
    parts.push(el("section", { class: "block" },
      el("p", { class: "block-label" }, el("span", { class: "lbl lbl-check", text: "The service's own data" })),
      c.summary ? el("p", { class: "check-summary", text: c.summary }) : null,
      el("p", { class: "check-note", text: `${c.request}, through the gateway. A GET goes straight to the service and writes nothing to Kafka.` }),
      c.error ? el("p", { class: "station-note bad", text: c.error }) : null,
      c.body ? el("pre", { class: "wire", text: pretty(c.body) }) : null));
  }
  parts.push(request);
  if (ex.curl) {
    parts.push(el("section", { class: "block" },
      el("p", { class: "block-label" }, el("span", { class: "lbl lbl-curl", text: "The same request from your terminal" }), copyButton(ex.curl)),
      el("pre", { class: "wire", text: ex.curl })));
  }
  box.replaceChildren(...parts);
  const status = $(".block-resp .start", box);
  if (status && ex.response) status.classList.add("status-line", "status-" + String(ex.response.status)[0]);
  box.scrollTop = 0;
}

async function runAction(button) {
  const name = button.dataset.action;
  const box = $("[data-exchange]");
  const all = document.querySelectorAll("[data-action]");
  all.forEach((b) => { b.dataset.wasDisabled = b.disabled ? "1" : ""; b.disabled = true; });
  button.setAttribute("aria-busy", "true");
  box.setAttribute("aria-busy", "true");
  try {
    const res = await fetch(`api/actions/${name}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: name === "retry" ? JSON.stringify({ of: retryTarget && retryTarget.id }) : "{}",
    });
    const data = await res.json();
    if (!res.ok) {
      renderExchange({ action: name, request: { method: "", url: "", headers: [], body: "" }, error: data });
      return;
    }
    if (data.requestId) {
      mine.add(data.requestId);
      const t = traces.get(data.requestId);
      if (t) renderTrace(t);
    }
    if (data.retryable && data.action !== "retry" && data.response && data.response.status < 500) {
      const key = data.request.headers.find((h) => h.key === "Idempotency-Key");
      retryTarget = { id: data.id, title: titles[data.action], key: key ? key.value : "" };
      updateRetry();
    }
    renderExchange(data);
    if (data.requestId) focusTrace(data.requestId);
  } catch (err) {
    renderExchange({ action: name, request: { method: "", url: "", headers: [], body: "" },
      error: { title: "The playground did not answer", detail: String(err), hint: "Check that it runs: docker compose ps playground" } });
  } finally {
    button.removeAttribute("aria-busy");
    box.setAttribute("aria-busy", "false");
    all.forEach((b) => { b.disabled = b.dataset.wasDisabled === "1"; });
    updateRetry();
  }
}

function updateRetry() {
  const b = $('[data-action="retry"]');
  if (b.getAttribute("aria-busy") === "true") return;
  b.disabled = !retryTarget;
  $("#retry-target").textContent = retryTarget ? `key ${shortKey(retryTarget.key)}` : "nothing to retry yet";
  $("#retry-exp").textContent = retryTarget
    ? `Resends your last "${retryTarget.title}", same key and body. The gateway returns the original answer; the service is not called again.`
    : "Create an order or charge a card first, then retry it here.";
}

/* ---------- wiring ---------- */

for (const b of document.querySelectorAll("[data-action]")) b.addEventListener("click", () => runAction(b));
$("[data-token-cmd]").textContent = `TOKEN=$(curl -s ${location.origin}/token)`;

const source = new EventSource("api/events");
source.addEventListener("open", () => { streamUp = true; setProbe("stream", "up"); renderAlerts(); });
source.addEventListener("error", () => { streamUp = false; setProbe("stream", "down"); renderAlerts(); });
source.addEventListener("status", (e) => onStatus(JSON.parse(e.data)));
source.addEventListener("record", (e) => onRecord(JSON.parse(e.data)));
