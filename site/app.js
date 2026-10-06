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
    // In the hero strip, small counts read as noise rather than proof: show them once they mean something.
    const counts = { stars: repo.stargazers_count, forks: repo.forks_count };
    document.querySelectorAll(".repo-stats [data-min]").forEach((li) => {
      const key = li.querySelector("[data-gh]").dataset.gh;
      li.hidden = counts[key] < Number(li.dataset.min);
    });
  } catch (_) { /* offline or rate-limited: keep the placeholders */ }
})();

// Copy buttons on code blocks.
document.querySelectorAll(".copy").forEach((btn) => {
  btn.addEventListener("click", async () => {
    const code = btn.parentElement.querySelector("code").innerText;
    try {
      await navigator.clipboard.writeText(code);
      btn.textContent = "Copied";
    } catch (_) {
      btn.textContent = "Select and copy";
    }
    setTimeout(() => (btn.textContent = "Copy"), 1600);
  });
});

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
