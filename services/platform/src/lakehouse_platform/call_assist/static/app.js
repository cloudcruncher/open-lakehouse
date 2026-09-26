// Live Call Assist console. OIDC authorization code + PKCE (no client secret, no
// password handling): tokens live in memory only and are refreshed before expiry.
const $ = (id) => document.getElementById(id);
const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text !== undefined) e.textContent = text; return e; };

let cfg, tokens = null, currentCall = null, verified = false, refreshTimer = null;

// ------------------------------------------------------------------ auth (PKCE)
const b64url = (bytes) => btoa(String.fromCharCode(...new Uint8Array(bytes))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const randomString = () => b64url(crypto.getRandomValues(new Uint8Array(32)));
const oidc = (path) => `${cfg.issuer}/protocol/openid-connect/${path}`;

async function login() {
  const verifier = randomString(), state = randomString();
  const challenge = b64url(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier)));
  sessionStorage.setItem("pkce", JSON.stringify({ verifier, state }));
  const q = new URLSearchParams({ client_id: cfg.client_id, response_type: "code", scope: "openid",
    redirect_uri: location.origin + "/", state, code_challenge: challenge, code_challenge_method: "S256" });
  location.assign(`${oidc("auth")}?${q}`);
}

async function tokenRequest(body) {
  const r = await fetch(oidc("token"), { method: "POST", body: new URLSearchParams({ client_id: cfg.client_id, ...body }) });
  if (!r.ok) throw new Error(`token endpoint ${r.status}`);
  return r.json();
}

async function completeLogin() {
  const p = new URLSearchParams(location.search);
  const saved = JSON.parse(sessionStorage.getItem("pkce") || "null");
  sessionStorage.removeItem("pkce");
  history.replaceState(null, "", "/");
  if (!p.get("code") || !saved || p.get("state") !== saved.state) return false;
  setTokens(await tokenRequest({ grant_type: "authorization_code", code: p.get("code"), redirect_uri: location.origin + "/", code_verifier: saved.verifier }));
  return true;
}

function setTokens(t) {
  tokens = t;
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(refresh, Math.max(10, t.expires_in - 45) * 1000);
}

async function refresh() {
  try {
    setTokens(await tokenRequest({ grant_type: "refresh_token", refresh_token: tokens.refresh_token }));
    if (currentCall) await api(`/api/calls/${currentCall}/token`, { method: "POST" });
  } catch { tokens = null; show("signin"); }
}

function claims() { return JSON.parse(atob(tokens.access_token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/"))); }

function api(path, opts = {}) {
  return fetch(path, { ...opts, headers: { ...(opts.headers || {}), Authorization: `Bearer ${tokens.access_token}`, "Content-Type": "application/json" } });
}

function show(which) { $("signin").hidden = which !== "signin"; $("console").hidden = which !== "console"; $("signout").hidden = which !== "console"; }

// --------------------------------------------------------------------- calls
async function loadScenarios() {
  const list = await (await fetch("/api/scenarios")).json();
  const box = $("scenarios"); box.replaceChildren();
  for (const s of list) {
    const b = el("button", "", s.title);
    b.onclick = () => startCall(s.id);
    box.append(b);
  }
}

function resetCall() {
  verified = false;
  $("lines").replaceChildren(); $("cards").replaceChildren(); $("trace").replaceChildren();
  $("customer").replaceChildren(el("p", "empty", "Not identified yet."));
  $("verifybar").hidden = true; $("status").hidden = true;
}

async function startCall(scenario) {
  resetCall();
  document.querySelectorAll(".scenarios button").forEach((b) => (b.disabled = true));
  const r = await api("/api/calls", { method: "POST", body: JSON.stringify({ scenario }) });
  const body = await r.json();
  if (!r.ok) { $("status").hidden = false; $("status").textContent = body.error || "could not start call"; return; }
  currentCall = body.call_id;
  $("ask").querySelectorAll("input, button").forEach((x) => (x.disabled = false));
  $("callinfo").textContent = `${body.call_id} · ${body.title}`;
  $("live").hidden = false;
  streamEvents(body.call_id);
}

async function streamEvents(callId) {
  const r = await api(`/api/calls/${callId}/events`);
  const reader = r.body.getReader(), dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let i;
    while ((i = buf.indexOf("\n\n")) >= 0) {
      const chunk = buf.slice(0, i); buf = buf.slice(i + 2);
      if (chunk.startsWith("data: ")) handle(JSON.parse(chunk.slice(6)));
    }
  }
}

function handle(ev) {
  switch (ev.type) {
    case "transcript": return addLine(ev);
    case "signals": return addSignals(ev);
    case "understanding": return addUnderstanding(ev);
    case "card": return addCard(ev.card);
    case "customer": return showCustomer(ev);
    case "tool_call": return addTrace(ev);
    case "status": $("status").hidden = false; $("status").textContent = ev.text; return;
    case "verified": return unlock();
    case "ended":
      $("live").hidden = true; $("status").hidden = true;
      document.querySelectorAll(".scenarios button").forEach((b) => (b.disabled = false));
      return;
  }
}

function addLine(ev) {
  $("status").hidden = true;
  const d = el("div", `line ${ev.speaker}`);
  d.id = `u${ev.seq}`;
  d.append(el("span", "who", ev.speaker === "customer" ? "Caller" : "You"), document.createTextNode(ev.text));
  $("lines").append(d); d.scrollIntoView({ block: "end", behavior: "smooth" });
}

// Which engine understood each caller line. With a key set, a line shows the model, its
// latency and what it added beyond the rules; a fallback says why (timeout, bad key...).
let fallbacks = 0;
function addUnderstanding(ev) {
  const host = $(`u${ev.seq}`); if (!host) return;
  const line = el("div", `llm ${ev.engine === "claude" ? "claude" : ev.fallback ? "fallback" : "rules"}`);
  if (ev.engine === "claude") {
    const added = Object.values(ev.added ?? {}).flat().map((x) => x.replaceAll("_", " "));
    // The label guide is a cached prefix: cache reads bill at 0.1x, so show them apart.
    const cache = ev.cache_read_tokens ? ` (+${ev.cache_read_tokens} cached)`
      : ev.cache_write_tokens ? ` (+${ev.cache_write_tokens} cache write)` : "";
    line.textContent = `✦ ${ev.model} · ${ev.ms} ms · ${ev.input_tokens}${cache}→${ev.output_tokens} tok · ` +
      (added.length ? `added: ${added.join(", ")}` : "agreed with rules");
    line.title = `request ${ev.request_id ?? ""}`;
  } else if (ev.fallback) {
    fallbacks += 1;
    line.textContent = `rules only · Claude not used: ${ev.fallback} (${ev.ms} ms)`;
    $("mode").classList.add("warn"); $("mode").title = `${fallbacks} fallback(s) this session`;
  } else {
    return; // rules-only mode: the header chip already says so
  }
  host.append(line);
}

function addSignals(ev) {
  const host = $(`u${ev.seq}`); if (!host) return;
  const s = el("div", "sig");
  for (const [kind, arr] of [["intent", ev.intents], ["vulnerability", ev.vulnerabilities], ["risk", ev.risks]])
    for (const v of arr) s.append(el("span", kind, v.replaceAll("_", " ")));
  host.append(s);
}

function addCard(c) {
  const card = el("article", `card ${c.kind}${c.requires_verification && !verified ? " locked" : ""}`);
  card.dataset.lock = c.requires_verification ? "1" : "";
  const head = el("div", "head");
  head.append(el("span", "kind", c.kind), el("span", "kind", c.latency_ms != null ? "" : ""));
  card.append(head, el("h3", "", c.title), el("p", "", c.body));
  if (c.ai) {
    // Who wrote the call note: Claude (with cost) or the template, and why.
    card.append(el("div", `llm ${c.ai.engine === "claude" ? "claude" : "fallback"}`, c.ai.engine === "claude"
      ? `✦ ${c.ai.model} · ${c.ai.ms} ms · ${c.ai.input_tokens}→${c.ai.output_tokens} tok · ≈ $${c.ai.cost_usd.toFixed(4)}`
      : `${c.ai.engine === "search" ? "procedure search" : "template note"} · AI not used: ${c.ai.fallback}`));
  }
  const meta = el("div", "meta");
  if (c.latency_ms != null) meta.append(el("span", "lat", `⚡ ${c.latency_ms} ms`));
  if (c.procedure) meta.append(el("span", "", `📘 ${c.procedure.id}`));
  if (c.evidence?.length) meta.append(el("span", "", `🔎 ${c.evidence.length} evidence`));
  card.append(meta);
  if (c.procedure) { const d = el("details"); d.append(el("summary", "", c.procedure.title), el("p", "", c.procedure.excerpt)); card.append(d); }
  if (c.evidence?.length) { const d = el("details"); d.append(el("summary", "", "Evidence (what this card is based on)"), el("pre", "", JSON.stringify(c.evidence, null, 1))); card.append(d); }
  for (const x of c.xray ?? []) card.append(xray(x));
  if (c.requires_verification && !verified) $("verifybar").hidden = false;
  $("cards").prepend(card);
}

function unlock() {
  verified = true; $("verifybar").hidden = true;
  document.querySelectorAll(".card.locked").forEach((c) => c.classList.remove("locked"));
}

function showCustomer(ev) {
  const p = ev.profile, box = $("customer"); box.replaceChildren();
  const dl = el("dl");
  const fields = [["Name", `${p.first_name ?? "—"} ${p.last_name ?? ""}`], ["Customer", p.customer_id], ["Brand", p.brand], ["Segment", p.segment],
    ["Date of birth", p.date_of_birth ?? "masked"], ["Phone", p.phone ?? "masked"], ["Email", p.email ?? "masked"], ["Postcode", p.postcode ?? "masked"],
    ["Vulnerability", p.vulnerability_flag == null ? "masked" : p.vulnerability_flag ? "YES" : "no"], ["Open complaints", p.open_complaints]];
  for (const [k, v] of fields) dl.append(el("dt", "", k), el("dd", "", String(v ?? "—")));
  box.append(dl);
  if (ev.accounts?.length) {
    const t = el("table"); const h = el("tr");
    ["Account", "Product", "Status", "Balance"].forEach((x) => h.append(el("th", "", x))); t.append(h);
    for (const a of ev.accounts) {
      const r = el("tr");
      [a.account_id, a.product.replaceAll("_", " "), a.status, `£${Number(a.balance).toLocaleString("en-GB", { minimumFractionDigits: 2 })}`].forEach((x) => r.append(el("td", "", x)));
      t.append(r);
    }
    box.append(t);
  }
  box.append(el("p", "fresh", `Profile as of ${String(ev.data_as_of).slice(0, 19).replace("T", " ")} UTC · accounts live (CDC)`));
}

function addTrace(ev) {
  const li = el("li");
  li.append(el("span", "", ev.tool), el("span", ev.outcome, `${ev.outcome} ${ev.ms ?? ""}ms`));
  if (ev.provenance) li.append(xray({ tool: ev.tool, ...ev.provenance }));
  $("trace").append(li);
}

// ------------------------------------------------------------ platform x-ray
// How the lakehouse produced the data behind a card: pipeline -> Iceberg snapshot ->
// Trino (as the colleague, pinned to that snapshot) -> OPA decision -> audit row.
const ago = (s) => (s == null ? "" : s < 90 ? `${Math.round(s)} s` : s < 5400 ? `${Math.round(s / 60)} min` : `${Math.round(s / 3600)} h`);

function xray(x) {
  const d = el("details", "xray");
  const snap = x.snapshot, q = x.query ?? {}, pol = x.policy ?? {}, aud = x.audit ?? {};
  const masks = Object.keys(pol.masked_columns ?? {});
  d.append(el("summary", "", `🔬 How the platform answered this · ${x.table?.replace("lakehouse.", "")}` +
    (snap ? ` @ snapshot …${snap.id.slice(-6)}` : "") + (aud.seq ? ` · audit #${aud.seq}` : "")));
  const steps = el("ol", "steps");
  const step = (icon, title, lines) => {
    const li = el("li"); li.append(el("b", "", `${icon} ${title}`));
    for (const l of lines.filter(Boolean)) li.append(el("span", "", l));
    steps.append(li);
  };
  step("🏭", "Maintained by", [x.maintained_by]);
  step("🧊", "Iceberg snapshot", snap ? [
    `id ${snap.id}`,
    `this commit: ${snap.committed_by}`,
    `committed ${ago(snap.committed_seconds_before_query)} before this lookup`,
    ...Object.entries(snap.summary ?? {}).map(([k, v]) => `${k}: ${v}`),
  ] : ["snapshot not resolved: query read the table's current state"]);
  step("⚙️", "Trino query", [
    `as ${q.as_user} (their own token, not a service account)`,
    q.pinned_to_snapshot ? "pinned: FOR VERSION AS OF this snapshot, reproducible by time travel" : null,
    `${q.rows} row(s) · ${q.query_id ?? ""}`,
  ]);
  step("🛡️", "OPA policy for this colleague", pol.unavailable ? ["explanation unavailable (Trino still enforced it)"] : [
    `rows: ${pol.row_filter ?? "no filter (all brands)"}`,
    masks.length ? `masked: ${masks.join(", ")}` : "masked: none of the columns read",
  ]);
  step("🧾", "Audit", [`row #${aud.seq} · chain hash ${aud.row_hash}…`, `purpose ${aud.purpose} · ${aud.latency_ms} ms end to end`]);
  d.append(steps);
  return d;
}

// ---------------------------------------------------------------------- boot
async function boot() {
  cfg = await (await fetch("/config")).json();
  $("mode").textContent = cfg.model ? `understanding: Claude (${cfg.model}) + rules` : "understanding: rules only (no API key)";
  $("login").onclick = login;
  $("signout").onclick = () => location.assign(`${oidc("logout")}?client_id=${cfg.client_id}&post_logout_redirect_uri=${encodeURIComponent(location.origin + "/")}`);
  $("verify").onclick = async () => { if (currentCall) await api(`/api/calls/${currentCall}/verified`, { method: "POST" }); };
  $("ask").onsubmit = async (e) => {
    e.preventDefault();
    const q = $("question").value.trim();
    if (!currentCall || !q) return;
    $("question").value = "";
    await api(`/api/calls/${currentCall}/ask`, { method: "POST", body: JSON.stringify({ question: q }) });
  };
  if (!(await completeLogin())) { show("signin"); return; }
  const c = claims();
  $("user").textContent = `${c.preferred_username} · ${c.name ?? ""}`;
  show("console");
  await loadScenarios();
}
boot();
