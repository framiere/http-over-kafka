// GitHub community numbers: fetched live, left as "–" if the API is unavailable.
(async function repoStats() {
  try {
    const res = await fetch("https://api.github.com/repos/sderosiaux/http-over-kafka");
    if (!res.ok) return;
    const repo = await res.json();
    const fmt = (n) => (n >= 1000 ? (n / 1000).toFixed(1).replace(/\.0$/, "") + "k" : String(n));
    const set = (key, value) => document.querySelectorAll(`[data-gh="${key}"]`).forEach((el) => (el.textContent = fmt(value)));
    set("stars", repo.stargazers_count);
    set("forks", repo.forks_count);
    set("issues", repo.open_issues_count);
    document.querySelectorAll('[data-gh-label="issues"]').forEach((el) => (el.textContent = repo.open_issues_count === 1 ? "open issue" : "open issues"));
    // In the hero, small counts read as noise rather than proof: show them once they mean something.
    const counts = { stars: repo.stargazers_count, forks: repo.forks_count };
    document.querySelectorAll(".hero-meta [data-min]").forEach((li) => {
      li.hidden = counts[li.querySelector("[data-gh]").dataset.gh] < Number(li.dataset.min);
    });
  } catch (_) { /* offline or rate-limited: keep the placeholders */ }
})();

const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

// Scroll reveal: one gentle entrance per block.
(function reveal() {
  const items = document.querySelectorAll(".reveal");
  if (reducedMotion || !("IntersectionObserver" in window)) { items.forEach((el) => el.classList.add("in")); return; }
  const io = new IntersectionObserver((entries) => {
    entries.forEach((e) => { if (e.isIntersecting) { e.target.classList.add("in"); io.unobserve(e.target); } });
  }, { rootMargin: "0px 0px -8% 0px", threshold: 0.08 });
  items.forEach((el, i) => { el.style.transitionDelay = `${(i % 4) * 70}ms`; io.observe(el); });
})();

// Ticker: duplicate the run so the loop is seamless.
(function ticker() {
  const track = document.querySelector("[data-ticker]");
  if (!track || reducedMotion) return;
  [...track.children].forEach((c) => { const d = c.cloneNode(true); d.setAttribute("aria-hidden", "true"); track.appendChild(d); });
})();

// Copy buttons on code blocks.
document.querySelectorAll(".copy").forEach((btn) => {
  btn.addEventListener("click", async () => {
    const code = btn.parentElement.querySelector("code").innerText;
    try { await navigator.clipboard.writeText(code); btn.textContent = "Copied"; }
    catch (_) { btn.textContent = "Select and copy"; }
    setTimeout(() => (btn.textContent = "Copy"), 1600);
  });
});

// The stage: services send HTTP calls through a Kafka backbone; the stream feeds new consumers.
(function stage() {
  const canvas = document.querySelector("[data-stage]");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const services = ["checkout", "orders", "payments", "inventory", "shipping"];
  const consumers = ["analytics", "fraud detection", "search index", "AI agent", "audit"];
  const events = ["OrderCreated", "PaymentCaptured", "StockReserved", "AddressChanged", "RefundIssued", "ShipmentSent", "CartCheckedOut"];
  const C = { ink: "#0f172a", mute: "#64748b", hair: "rgba(15,23,42,0.10)", req: "#4f46e5", log: "#f97316", logFill: "#fff3e8", ev: "#10b981" };

  let W = 0, H = 0, dpr = 1, geo = null;
  const calls = [], segs = [], outs = [], queue = [];
  let offset = 4811;

  function layout() {
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = canvas.clientWidth; H = canvas.clientHeight;
    canvas.width = W * dpr; canvas.height = H * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const compact = W < 640;
    const n = services.length;
    const top = 56, bottom = H - 40, step = (bottom - top) / (n - 1);
    geo = {
      compact,
      nodeW: compact ? 84 : 132, nodeH: compact ? 26 : 34,
      left: compact ? 12 : 40,
      right: W - (compact ? 12 : 40),
      ys: services.map((_, i) => top + i * step),
      band: { x0: W * (compact ? 0.33 : 0.3), x1: W * (compact ? 0.67 : 0.7), y: H / 2 + 6, h: compact ? 44 : 58 },
    };
  }

  const ease = (t) => 1 - Math.pow(1 - t, 3);
  function bez(p0, p1, t) {
    const mx = (p0.x + p1.x) / 2;
    const u = 1 - t;
    return {
      x: u * u * u * p0.x + 3 * u * u * t * mx + 3 * u * t * t * mx + t * t * t * p1.x,
      y: u * u * u * p0.y + 3 * u * u * t * p0.y + 3 * u * t * t * p1.y + t * t * t * p1.y,
    };
  }
  const svcPort = (i) => ({ x: geo.left + geo.nodeW, y: geo.ys[i] });
  const conPort = (i) => ({ x: geo.right - geo.nodeW, y: geo.ys[i] });
  const bandIn = () => ({ x: geo.band.x0, y: geo.band.y });
  const bandOut = () => ({ x: geo.band.x1, y: geo.band.y });

  function spawnCall() {
    const src = Math.floor(Math.random() * services.length);
    let dst = Math.floor(Math.random() * services.length);
    if (dst === src) dst = (dst + 2) % services.length;
    calls.push({ src, dst, t: 0, phase: 0, ev: events[Math.floor(Math.random() * events.length)], fail: Math.random() < 0.12 });
  }

  function roundRect(x, y, w, h, r) { ctx.beginPath(); ctx.roundRect(x, y, w, h, r); }
  function node(x, y, label, color, align) {
    const { nodeW: w, nodeH: h, compact } = geo;
    const x0 = align === "left" ? x : x - w;
    roundRect(x0, y - h / 2, w, h, h / 2);
    ctx.fillStyle = "#fff"; ctx.fill();
    ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.stroke();
    ctx.fillStyle = C.ink; ctx.font = `500 ${compact ? 10.5 : 13}px Geist, system-ui, sans-serif`;
    ctx.textAlign = "center"; ctx.textBaseline = "middle";
    ctx.fillText(label, x0 + w / 2, y + 0.5);
  }

  let last = 0, spawnAcc = 0, running = true;
  function frame(now) {
    if (!running) { last = now; requestAnimationFrame(frame); return; }
    const dt = Math.min(0.05, (now - (last || now)) / 1000); last = now;
    spawnAcc += dt;
    if (spawnAcc > (geo.compact ? 0.8 : 0.5) && queue.length < 4) { spawnAcc = 0; spawnCall(); }
    draw(dt);
    requestAnimationFrame(frame);
  }

  function draw(dt) {
    ctx.clearRect(0, 0, W, H);
    const { band, compact } = geo;

    // dotted grid
    ctx.fillStyle = "rgba(15,23,42,0.06)";
    for (let x = 16; x < W; x += 24) for (let y = 16; y < H; y += 24) { ctx.fillRect(x, y, 1.4, 1.4); }

    // column captions
    ctx.font = `600 ${compact ? 10.5 : 12}px Geist, system-ui, sans-serif`;
    ctx.fillStyle = C.mute; ctx.textBaseline = "alphabetic";
    ctx.textAlign = "left"; ctx.fillText("Your HTTP services", geo.left, 30);
    ctx.textAlign = "right"; ctx.fillText("New real-time consumers", geo.right, 30);
    ctx.textAlign = "center"; ctx.fillStyle = "#c2410c"; ctx.fillText("Kafka backbone", (band.x0 + band.x1) / 2, band.y - band.h / 2 - 14);

    // wiring
    ctx.lineWidth = 1; ctx.strokeStyle = C.hair;
    services.forEach((_, i) => { const a = svcPort(i), b = bandIn(); ctx.beginPath(); for (let t = 0; t <= 1.001; t += 0.05) { const p = bez(a, b, t); t === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y); } ctx.stroke(); });
    consumers.forEach((_, i) => { const a = bandOut(), b = conPort(i); ctx.beginPath(); for (let t = 0; t <= 1.001; t += 0.05) { const p = bez(a, b, t); t === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y); } ctx.stroke(); });

    // backbone
    roundRect(band.x0, band.y - band.h / 2, band.x1 - band.x0, band.h, 14);
    ctx.fillStyle = C.logFill; ctx.fill(); ctx.strokeStyle = "#fdba74"; ctx.lineWidth = 1.5; ctx.stroke();

    // log segments travel along the backbone: the stream. Records enter one at a
    // time, in order, like appends to a partition.
    const speed = (band.x1 - band.x0) / 5;
    const segW = compact ? 40 : 92, gap = 8;
    const lastSeg = segs[segs.length - 1];
    if (queue.length && (!lastSeg || lastSeg.x >= gap)) {
      const q = queue.shift();
      segs.push({ x: -segW, ev: q.ev, fail: q.fail, off: ++offset });
    }
    ctx.save(); roundRect(band.x0, band.y - band.h / 2, band.x1 - band.x0, band.h, 14); ctx.clip();
    for (let i = segs.length - 1; i >= 0; i--) {
      const s = segs[i];
      s.x += speed * dt;
      const w = segW, h = band.h - 18;
      const x = band.x0 + s.x;
      roundRect(x, band.y - h / 2, w, h, 6);
      ctx.fillStyle = s.fail ? "#fef3c7" : "#ffffff"; ctx.fill();
      ctx.fillStyle = s.fail ? "#d97706" : C.log; ctx.fillRect(x, band.y - h / 2, 3, h);
      if (!compact) {
        ctx.fillStyle = C.ink; ctx.font = "500 10.5px Geist Mono, monospace"; ctx.textAlign = "left"; ctx.textBaseline = "middle";
        ctx.fillText(s.fail ? "Failed" : s.ev, x + 9, band.y - 7);
        ctx.fillStyle = C.mute; ctx.fillText(`off ${s.off}`, x + 9, band.y + 9);
      }
      if (x > band.x1 - w && !s.emitted) {
        s.emitted = true;
        if (!s.fail) outs.push({ dst: Math.floor(Math.random() * consumers.length), t: 0 });
      }
      if (x > band.x1 + 4) segs.splice(i, 1);
    }
    ctx.restore();

    // HTTP calls: caller -> backbone -> target service
    for (let i = calls.length - 1; i >= 0; i--) {
      const c = calls[i];
      c.t += dt * 1.15;
      let p;
      if (c.phase === 0) {
        p = bez(svcPort(c.src), bandIn(), ease(Math.min(c.t, 1)));
        if (c.t >= 1) { c.phase = 1; c.t = 0; queue.push({ ev: c.ev, fail: c.fail }); }
      } else {
        p = bez(bandIn(), svcPort(c.dst), ease(Math.min(c.t, 1)));
        if (c.t >= 1) { calls.splice(i, 1); continue; }
      }
      ctx.beginPath(); ctx.arc(p.x, p.y, c.phase === 0 ? 4.5 : 3.5, 0, Math.PI * 2);
      ctx.fillStyle = c.phase === 0 ? C.req : "rgba(79,70,229,0.45)"; ctx.fill();
    }

    // events out to consumers
    for (let i = outs.length - 1; i >= 0; i--) {
      const o = outs[i];
      o.t += dt * 1.1;
      const p = bez(bandOut(), conPort(o.dst), ease(Math.min(o.t, 1)));
      ctx.beginPath(); ctx.arc(p.x, p.y, 4.5, 0, Math.PI * 2); ctx.fillStyle = C.ev; ctx.fill();
      ctx.beginPath(); ctx.arc(p.x, p.y, 9, 0, Math.PI * 2); ctx.fillStyle = "rgba(16,185,129,0.15)"; ctx.fill();
      if (o.t >= 1) outs.splice(i, 1);
    }

    services.forEach((s, i) => node(geo.left, geo.ys[i], s, "rgba(79,70,229,0.55)", "left"));
    consumers.forEach((s, i) => node(geo.right, geo.ys[i], s, "rgba(16,185,129,0.6)", "right"));
  }

  layout();
  window.addEventListener("resize", () => { layout(); });
  if (reducedMotion) {
    for (let k = 0; k < 5; k++) segs.push({ x: k * (geo.compact ? 50 : 110), ev: events[k], fail: k === 3, off: 4812 + k });
    (document.fonts ? document.fonts.ready : Promise.resolve()).then(() => draw(0));
    return;
  }
  new IntersectionObserver(([e]) => (running = e.isIntersecting)).observe(canvas);
  (document.fonts ? document.fonts.ready : Promise.resolve()).then(() => requestAnimationFrame(frame));
})();

// The wire: three requests crossing Kafka, replayed in a loop.
(function wire() {
  const caller = document.querySelector('[data-term="caller"]');
  const service = document.querySelector('[data-term="service"]');
  const log = document.querySelector("[data-log]");
  if (!caller || !service || !log) return;

  const offsets = { "http.requests.orders": 4811, "http.results.orders": 4811, "orders.events": 2290 };
  const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;");
  const line = (cls, text) => `<span class="${cls}">${esc(text)}</span>\n`;
  const plain = (text) => esc(text) + "\n";

  const scenarios = [
    {
      callerReq: [line("req", "POST /orders HTTP/1.1"), plain("Host: orders.internal"), plain("Authorization: Bearer eyJhb…"), plain("Idempotency-Key: k-81f2"), plain(""), plain('{"customerId":"c-1042",'), plain(' "items":[{"sku":"A123","qty":2}]}')],
      serviceReq: [line("req", "POST /orders HTTP/1.1"), plain("X-Caller-Application: checkout"), plain("X-Request-Id: 01K7Q2…"), plain(""), plain('{"customerId":"c-1042",'), plain(' "items":[{"sku":"A123","qty":2}]}')],
      serviceResp: [line("ok", "HTTP/1.1 201 Created"), plain("Location: /orders/ord_7Q2K")],
      callerResp: [line("ok", "HTTP/1.1 201 Created"), plain("Location: /orders/ord_7Q2K"), line("dim", "same bytes the service sent")],
      command: "createOrder, signed",
      result: { text: "CreateOrderSucceeded", cls: "" },
      event: "OrderCreated ord_7Q2K",
    },
    {
      callerReq: [line("req", "POST /orders HTTP/1.1"), plain("Host: orders.internal"), plain("Idempotency-Key: k-90c4"), plain(""), plain('{"customerId":"c-2210","items":[]}')],
      serviceReq: [line("req", "POST /orders HTTP/1.1"), plain("X-Caller-Application: checkout"), plain(""), plain('{"customerId":"c-2210","items":[]}')],
      serviceResp: [line("ko", "HTTP/1.1 400 Bad Request"), plain('{"detail":"items is empty"}')],
      callerResp: [line("ko", "HTTP/1.1 400 Bad Request"), plain('{"detail":"items is empty"}'), line("dim", "a failure, recorded as one: no event")],
      command: "createOrder, signed",
      result: { text: "CreateOrderFailed (400)", cls: "fail" },
      event: null,
    },
    {
      callerReq: [line("req", "POST /orders HTTP/1.1"), plain("Host: orders.internal"), plain("Idempotency-Key: k-81f2"), line("dim", "retry after a network blip")],
      serviceReq: [line("dim", "not called"), line("dim", "the first attempt already ran")],
      serviceResp: [],
      callerResp: [line("ok", "HTTP/1.1 201 Created"), plain("Idempotent-Replayed: true"), plain("Location: /orders/ord_7Q2K")],
      command: "createOrder, retry with key k-81f2",
      result: null,
      event: null,
    },
  ];

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function append(topic, text, cls) {
    offsets[topic] += 1;
    const li = document.createElement("li");
    li.className = [cls, reduced ? "" : "is-new"].filter(Boolean).join(" ");
    li.innerHTML = `<span class="off">${offsets[topic]}</span><span><span class="topic">${topic}</span><br>${esc(text)}</span>`;
    log.prepend(li);
    while (log.children.length > 6) log.lastElementChild.remove();
  }

  async function type(el, lines, step) {
    for (const l of lines) {
      el.innerHTML += l;
      if (step) await sleep(step);
    }
  }

  async function play(s, fast) {
    const d = fast ? 0 : 1;
    caller.innerHTML = "";
    service.innerHTML = "";
    await type(caller, s.callerReq, 110 * d);
    await sleep(500 * d);
    append("http.requests.orders", s.command);
    await sleep(700 * d);
    await type(service, s.serviceReq, 90 * d);
    await sleep(600 * d);
    if (s.serviceResp.length) {
      service.innerHTML += "\n";
      await type(service, s.serviceResp, 90 * d);
      await sleep(500 * d);
    }
    if (s.result) append("http.results.orders", s.result.text, s.result.cls);
    if (s.event) { await sleep(450 * d); append("orders.events", s.event, "ev"); }
    await sleep(500 * d);
    caller.innerHTML += "\n";
    await type(caller, s.callerResp, 110 * d);
  }

  if (reduced) {
    // Static picture: all three requests in the log, the first one in the panes.
    (async () => { for (const s of [...scenarios].reverse()) await play(s, true); })();
    return;
  }

  let visible = true;
  new IntersectionObserver(([e]) => (visible = e.isIntersecting)).observe(log);

  (async function loop() {
    let i = 0;
    for (;;) {
      if (!visible) { await sleep(400); continue; }
      await play(scenarios[i % scenarios.length]);
      await sleep(3200);
      i += 1;
    }
  })();
})();
